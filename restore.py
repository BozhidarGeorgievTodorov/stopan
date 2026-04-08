from __future__ import annotations

import argparse
import concurrent.futures
import os
import threading
import time
from dataclasses import dataclass

from core.database import MetadataDB
from core.repository import CASRepository


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
DEFAULT_SEED = os.getenv("MEMBERSHIP_SEED", "localhost:50051")
DEFAULT_RPC_TIMEOUT_S = float(os.getenv("RESTORE_RPC_TIMEOUT_S", "5.0"))
DEFAULT_BATCH_TARGET_PARALLELISM = int(os.getenv("RESTORE_BATCH_TARGET_PARALLELISM", "4"))
DEFAULT_PREFETCH_WINDOW = int(os.getenv("RESTORE_PREFETCH_WINDOW", "32"))
GRPC_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


def _safe_restore_path(base_dir, rel_path):
    if rel_path == ".":
        return base_dir

    normalized_rel = os.path.normpath(rel_path)
    full_path = os.path.abspath(os.path.join(base_dir, normalized_rel))
    base_dir_abs = os.path.abspath(base_dir)

    if os.path.commonpath([base_dir_abs, full_path]) != base_dir_abs:
        raise ValueError(f"Path escapes restore directory: {rel_path}")

    return full_path


@dataclass(frozen=True)
class RestorePaths:
    final_dir: str
    incomplete_dir: str

    @staticmethod
    def for_snapshot(base_output_dir, snapshot_uuid):
        final_dir = os.path.join(base_output_dir, f"snapshot_{snapshot_uuid}")
        return RestorePaths(final_dir=final_dir, incomplete_dir=final_dir + ".incomplete")


@dataclass(frozen=True)
class BatchRetrieveItemResult:
    chunk_hash: str
    status: int
    chunk_data: bytes
    detail: str

    def is_found(self, retrieve_status_found):
        return self.status == retrieve_status_found


class RemoteStorageClientPool:
    """
    Pool de clientes remotos para restore.

    La carga de grpc/protobuf se retrasa hasta que realmente falta un chunk
    local y hay que consultar la red. La lectura remota usa únicamente
    RetrieveChunkBatch.
    """

    def __init__(self, *, timeout_s=DEFAULT_RPC_TIMEOUT_S):
        self.timeout_s = float(timeout_s)
        self._channels = {}
        self._stubs = {}
        self._grpc = None
        self._p2p_storage_pb2 = None
        self._p2p_storage_pb2_grpc = None
        self._lock = threading.Lock()

    def _ensure_runtime(self):
        if self._grpc is not None:
            return

        import grpc
        from protos import p2p_storage_pb2
        from protos import p2p_storage_pb2_grpc

        self._grpc = grpc
        self._p2p_storage_pb2 = p2p_storage_pb2
        self._p2p_storage_pb2_grpc = p2p_storage_pb2_grpc

    @property
    def retrieve_status_found(self):
        self._ensure_runtime()
        return self._p2p_storage_pb2.RETRIEVE_STATUS_FOUND

    def _get_stub(self, address):
        self._ensure_runtime()

        with self._lock:
            if address not in self._stubs:
                channel = self._grpc.insecure_channel(
                    address,
                    options=[
                        ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES),
                        ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_BYTES),
                    ],
                )
                self._channels[address] = channel
                self._stubs[address] = self._p2p_storage_pb2_grpc.P2PStorageStub(channel)
            return self._stubs[address]

    def retrieve_chunk_batch(self, *, address, chunk_hashes):
        self._ensure_runtime()
        if not chunk_hashes:
            return {}

        stub = self._get_stub(address)
        request = self._p2p_storage_pb2.RetrieveChunkBatchRequest(chunk_hashes=list(chunk_hashes))
        response = stub.RetrieveChunkBatch(request, timeout=self.timeout_s)

        results = {}
        for item in response.results:
            if not item.chunk_hash:
                continue
            results[item.chunk_hash] = BatchRetrieveItemResult(
                chunk_hash=item.chunk_hash,
                status=item.status,
                chunk_data=bytes(item.chunk_data),
                detail=item.detail or "",
            )

        for chunk_hash in chunk_hashes:
            if chunk_hash not in results:
                results[chunk_hash] = BatchRetrieveItemResult(
                    chunk_hash=chunk_hash,
                    status=self._p2p_storage_pb2.RETRIEVE_STATUS_ERROR,
                    chunk_data=b"",
                    detail="missing batch result",
                )

        return results

    def is_rpc_error(self, exc):
        self._ensure_runtime()
        return isinstance(exc, self._grpc.RpcError)

    def close(self):
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()


class LazyClusterResolver:
    """
    Resuelve membership solo cuando aparece un miss local.

    Evita abrir canales de red en restores que pueden resolverse íntegramente
    desde la caché local.
    """

    def __init__(self, *, seed, rf):
        self.seed = seed
        self.rf = max(int(rf), 1)
        self._cluster = None
        self._announced = False
        self._lock = threading.Lock()

    def get_cluster(self):
        if self._cluster is not None:
            return self._cluster

        with self._lock:
            if self._cluster is not None:
                return self._cluster

            from core.cluster_view import ClusterMembershipClient

            self_addr = os.getenv("ADVERTISE_ADDR", "")
            cluster = ClusterMembershipClient(self.seed, self_addr=self_addr).get_cluster_view()
            if not cluster.members:
                raise RuntimeError(f"No eligible members returned by seed {self.seed}.")

            self._cluster = cluster
            return cluster

    def announce_once(self):
        cluster = self.get_cluster()
        with self._lock:
            if self._announced:
                return
            self._announced = True

        self_addr = os.getenv("ADVERTISE_ADDR", "")
        print(f"Remote restore enabled. Eligible members: {[f'{m.node_id[:8]}@{m.address}' for m in cluster.members]}")
        if cluster.self_node_id:
            print(f"Self: {cluster.self_node_id[:8]}@{self_addr}")
        print(f"RF targets: {min(max(self.rf, 1), len(cluster.members))}")


class ChunkFetchService:
    """
    Lee chunks con prioridad local y recuperación remota por lotes bajo demanda.
    """

    def __init__(self, *, repo, cluster_resolver, remote_pool, rf):
        self.repo = repo
        self.cluster_resolver = cluster_resolver
        self.remote_pool = remote_pool
        self.rf = max(int(rf), 1)

    def fetch_many_raw_chunks(self, chunk_hashes, *, target_parallelism):
        ordered_hashes = list(chunk_hashes)
        results = {}
        missing = []

        for chunk_hash in ordered_hashes:
            try:
                results[chunk_hash] = self.repo.get(chunk_hash)
            except FileNotFoundError:
                missing.append(chunk_hash)
            except Exception as exc:
                results[chunk_hash] = exc

        if missing:
            results.update(
                self._fetch_missing_many_from_remote(
                    missing,
                    target_parallelism=max(int(target_parallelism), 1),
                )
            )

        return results

    def _fetch_missing_many_from_remote(self, missing_hashes, *, target_parallelism):
        cluster = self.cluster_resolver.get_cluster()
        self.cluster_resolver.announce_once()

        target_lists = {}
        result_map = {}
        error_map = {chunk_hash: [] for chunk_hash in missing_hashes}

        for chunk_hash in missing_hashes:
            targets = cluster.hrw_remote_targets(chunk_hash, rf=self.rf, salt=CLUSTER_TOKEN)
            target_lists[chunk_hash] = targets
            if not targets:
                result_map[chunk_hash] = FileNotFoundError(
                    f"Chunk {chunk_hash[:8]} is not local and has no usable HRW targets."
                )

        unresolved = {chunk_hash for chunk_hash in missing_hashes if chunk_hash not in result_map}
        max_depth = max((len(target_lists[chunk_hash]) for chunk_hash in unresolved), default=0)

        for rank in range(max_depth):
            if not unresolved:
                break

            groups = {}
            for chunk_hash in list(unresolved):
                targets = target_lists[chunk_hash]
                if rank >= len(targets):
                    continue

                member = targets[rank]
                if member.address not in groups:
                    groups[member.address] = (member, [])
                groups[member.address][1].append(chunk_hash)

            if not groups:
                break

            max_workers = min(max(int(target_parallelism), 1), len(groups))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(
                        self.remote_pool.retrieve_chunk_batch,
                        address=member.address,
                        chunk_hashes=group_hashes,
                    ): (member, group_hashes)
                    for member, group_hashes in groups.values()
                }

                for future in concurrent.futures.as_completed(future_map):
                    member, group_hashes = future_map[future]

                    try:
                        batch_results = future.result()
                    except Exception as exc:
                        if self.remote_pool.is_rpc_error(exc):
                            details = getattr(exc, "details", lambda: str(exc))()
                            message = f"{member.address}: RPC {details}"
                        else:
                            message = f"{member.address}: {exc}"

                        for chunk_hash in group_hashes:
                            error_map[chunk_hash].append(message)
                        continue

                    for chunk_hash in group_hashes:
                        result = batch_results.get(chunk_hash)
                        if result is None:
                            error_map[chunk_hash].append(f"{member.address}: missing batch result")
                            continue

                        if not result.is_found(self.remote_pool.retrieve_status_found):
                            error_map[chunk_hash].append(f"{member.address}: {result.detail}")
                            continue

                        try:
                            self.repo.put_compressed(chunk_hash, result.chunk_data)
                            result_map[chunk_hash] = self.repo.get(chunk_hash)
                            unresolved.discard(chunk_hash)
                        except Exception as exc:
                            result_map[chunk_hash] = exc
                            unresolved.discard(chunk_hash)

        for chunk_hash in unresolved:
            result_map[chunk_hash] = FileNotFoundError(
                f"No HRW target returned chunk {chunk_hash[:8]}. " + " | ".join(error_map[chunk_hash])
            )

        return result_map


class OrderedBatchChunkPrefetcher:
    """
    Lee chunks por ventanas y conserva el orden de escritura del archivo.

    Cada ventana se resuelve local-first. Si faltan chunks, se agrupan por rank
    HRW y por nodo remoto para reducir llamadas gRPC durante restore.
    """

    def __init__(self, fetch_service, *, target_parallelism, window):
        self.fetch_service = fetch_service
        self.target_parallelism = max(int(target_parallelism), 1)
        self.window = max(int(window), 1)

    def iter_raw_chunks(self, chunk_hashes):
        window = []

        for chunk_hash in chunk_hashes:
            window.append(chunk_hash)
            if len(window) >= self.window:
                yield from self._drain_window(window)
                window = []

        if window:
            yield from self._drain_window(window)

    def _drain_window(self, window_hashes):
        result_map = self.fetch_service.fetch_many_raw_chunks(
            window_hashes,
            target_parallelism=self.target_parallelism,
        )

        for chunk_hash in window_hashes:
            value = result_map[chunk_hash]
            if isinstance(value, Exception):
                raise value
            yield chunk_hash, value


class SnapshotRestorer:
    def __init__(
        self,
        *,
        db,
        repo,
        fetch_service,
        base_output_dir,
        batch_target_parallelism,
        prefetch_window,
    ):
        self.db = db
        self.repo = repo
        self.fetch_service = fetch_service
        self.base_output_dir = base_output_dir
        self.batch_target_parallelism = max(int(batch_target_parallelism), 1)
        self.prefetch_window = max(int(prefetch_window), 1)

    def restore(self, snapshot_id):
        status, error = self.db.get_snapshot_status(snapshot_id)
        if status is None:
            print(f"Snapshot ID {snapshot_id} does not exist.")
            return

        if status != "COMPLETE":
            print(f"Snapshot {snapshot_id} is not restorable (status={status}).")
            if error:
                print(f"Recorded error: {error}")
            return

        snapshot_uuid = self.db.get_snapshot_uuid(snapshot_id)
        if not snapshot_uuid:
            raise RuntimeError("Snapshot has no UUID.")

        paths = RestorePaths.for_snapshot(self.base_output_dir, snapshot_uuid)

        if os.path.exists(paths.final_dir):
            print(f"Snapshot {snapshot_id} is already restored at: {paths.final_dir}")
            return

        os.makedirs(paths.incomplete_dir, exist_ok=True)
        print(f"Restoring snapshot {snapshot_id} into {paths.incomplete_dir}")
        print(
            f"Batch read: target_parallelism={self.batch_target_parallelism} "
            f"window={self.prefetch_window}"
        )

        items_gen = self.db.get_snapshot_items(snapshot_id)
        first_item = next(items_gen, None)
        if first_item is None:
            print(f"Snapshot {snapshot_id} is empty.")
            return

        def iter_items():
            yield first_item
            yield from items_gen

        directories = []
        current_tmp_path = None
        processed_items = 0
        successful_items = 0

        try:
            for item in iter_items():
                processed_items += 1

                try:
                    full_path = _safe_restore_path(paths.incomplete_dir, item["path"])
                except ValueError as exc:
                    print(f"Skipping unsafe path: {exc}")
                    continue

                if item["item_type"] == "dir":
                    os.makedirs(full_path, exist_ok=True)
                    directories.append((full_path, item))
                    successful_items += 1
                    continue

                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                if os.path.isdir(full_path):
                    print(f"Expected file but found directory at {item['path']}")
                    continue

                print(f"Restoring item {processed_items}: {item['path']}")
                current_tmp_path = full_path + ".tmp"
                success_file = True

                try:
                    chunk_hashes = list(self.db.get_item_chunks(item["id"]))
                    prefetcher = OrderedBatchChunkPrefetcher(
                        self.fetch_service,
                        target_parallelism=self.batch_target_parallelism,
                        window=self.prefetch_window,
                    )

                    with open(current_tmp_path, "wb") as handle:
                        for chunk_hash, raw in prefetcher.iter_raw_chunks(chunk_hashes):
                            try:
                                handle.write(raw)
                            except Exception as exc:
                                print(f"Could not write chunk {chunk_hash[:8]} for {item['path']}: {exc}")
                                success_file = False
                                break

                except Exception as exc:
                    print(f"Could not restore file {item['path']}: {exc}")
                    success_file = False

                if success_file:
                    os.replace(current_tmp_path, full_path)
                    current_tmp_path = None
                    self._apply_item_metadata(full_path, item, is_dir=False)
                    successful_items += 1
                else:
                    if current_tmp_path and os.path.exists(current_tmp_path):
                        os.remove(current_tmp_path)
                    current_tmp_path = None
                    print(f"File {item['path']} was not restored. It can be retried later.")

        except KeyboardInterrupt:
            print("\nRestore interrupted by user.")
            if current_tmp_path and os.path.exists(current_tmp_path):
                os.remove(current_tmp_path)
                print("Temporary file removed.")
            print(f"Partial restore kept at: {paths.incomplete_dir}")
            return

        finally:
            directories.sort(key=lambda pair: len(pair[0]), reverse=True)
            for directory_path, item in directories:
                self._apply_item_metadata(directory_path, item, is_dir=True)

        if successful_items == processed_items and processed_items > 0:
            try:
                os.replace(paths.incomplete_dir, paths.final_dir)
                print("-" * 40)
                print(f"Restore completed for snapshot {snapshot_id}.")
                print(f"Final directory: {paths.final_dir}")
            except OSError as exc:
                print(f"Could not rename final directory: {exc}")
        else:
            print("-" * 40)
            print(f"Restore incomplete: {successful_items}/{processed_items} items restored.")
            print(f"Work directory: {paths.incomplete_dir}")

    @staticmethod
    def _apply_item_metadata(path, item, *, is_dir):
        kind = "directory" if is_dir else "file"
        try:
            os.chmod(path, item["mode"])
            os.utime(path, (item["mtime"], item["mtime"]))
        except OSError:
            print(f"Could not restore metadata for {kind}: {item['path']}")


def restore_snapshot(
    snapshot_id,
    *,
    seed,
    base_output_dir,
    rf,
    batch_target_parallelism,
    prefetch_window,
):
    db = MetadataDB(DB_FILE)
    repo = CASRepository(LOCAL_SHARD_DIR)
    remote_pool = RemoteStorageClientPool(timeout_s=DEFAULT_RPC_TIMEOUT_S)
    cluster_resolver = LazyClusterResolver(seed=seed, rf=rf)
    fetch_service = ChunkFetchService(
        repo=repo,
        cluster_resolver=cluster_resolver,
        remote_pool=remote_pool,
        rf=rf,
    )
    restorer = SnapshotRestorer(
        db=db,
        repo=repo,
        fetch_service=fetch_service,
        base_output_dir=base_output_dir,
        batch_target_parallelism=batch_target_parallelism,
        prefetch_window=prefetch_window,
    )

    try:
        restorer.restore(snapshot_id)
    finally:
        db.close()
        remote_pool.close()


def parse_args():
    parser = argparse.ArgumentParser(
        prog="restore.py",
        description="Restaura snapshots desde CAS local y red bajo demanda.",
    )
    parser.add_argument("snapshot_id", type=int)
    parser.add_argument("out", nargs="?", default="restore_out", help="Directorio base de salida")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Seed de membership para recuperación remota")
    parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor HRW")
    parser.add_argument(
        "--prefetch-window",
        type=int,
        default=DEFAULT_PREFETCH_WINDOW,
        help="Número de chunks a resolver por ventana",
    )
    parser.add_argument(
        "--batch-target-parallelism",
        type=int,
        default=DEFAULT_BATCH_TARGET_PARALLELISM,
        help="Número máximo de targets remotos consultados en paralelo por ronda HRW",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_time = time.perf_counter()

    restore_snapshot(
        args.snapshot_id,
        seed=args.seed,
        base_output_dir=args.out,
        rf=args.rf,
        batch_target_parallelism=args.batch_target_parallelism,
        prefetch_window=args.prefetch_window,
    )

    elapsed = time.perf_counter() - start_time
    print(f"Restore command finished in {elapsed:.2f} seconds.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
