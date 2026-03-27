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
DEFAULT_PREFETCH_WORKERS = int(os.getenv("RESTORE_PREFETCH_WORKERS", "8"))
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


class RemoteStorageClientPool:
    """
    Pool de clientes remotos para restore.

    La carga de grpc/protobuf se retrasa hasta que realmente falta un chunk
    local y hay que consultar la red.
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

    def retrieve_chunk(self, *, address, chunk_hash):
        stub = self._get_stub(address)
        request = self._p2p_storage_pb2.RetrieveRequest(chunk_hash=chunk_hash)
        return stub.RetrieveChunk(request, timeout=self.timeout_s)

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
    Lee chunks con prioridad local y recuperación remota bajo demanda.
    """

    def __init__(self, *, repo, cluster_resolver, remote_pool, rf):
        self.repo = repo
        self.cluster_resolver = cluster_resolver
        self.remote_pool = remote_pool
        self.rf = max(int(rf), 1)

    def fetch_raw_chunk(self, chunk_hash):
        try:
            return self.repo.get(chunk_hash)
        except FileNotFoundError:
            return self._fetch_from_remote(chunk_hash)

    def _fetch_from_remote(self, chunk_hash):
        cluster = self.cluster_resolver.get_cluster()
        self.cluster_resolver.announce_once()

        targets = cluster.hrw_remote_targets(chunk_hash, rf=self.rf, salt=CLUSTER_TOKEN)
        if not targets:
            raise FileNotFoundError(f"Missing local chunk and no remote targets available: {chunk_hash}")

        errors = []
        for member in targets:
            try:
                response = self.remote_pool.retrieve_chunk(address=member.address, chunk_hash=chunk_hash)
                if not response.success:
                    errors.append(f"{member.address}: {response.message}")
                    continue

                self.repo.put_compressed(chunk_hash, response.chunk_data)
                return self.repo.get(chunk_hash)

            except Exception as exc:
                if self.remote_pool.is_rpc_error(exc):
                    details = getattr(exc, "details", lambda: str(exc))()
                    errors.append(f"{member.address}: RPC {details}")
                else:
                    errors.append(f"{member.address}: {exc}")

        raise FileNotFoundError(
            f"Chunk {chunk_hash[:8]} not returned by any HRW target. " + " | ".join(errors)
        )


class OrderedChunkPrefetcher:
    """
    Prefetch ordenado y acotado de chunks.

    Lanza varias lecturas en paralelo, pero entrega los bytes en el orden de la
    receta para poder escribir cada archivo secuencialmente.
    """

    def __init__(self, fetch_service, *, workers, window):
        self.fetch_service = fetch_service
        self.workers = max(int(workers), 1)
        self.window = max(int(window), 1)

    def iter_raw_chunks(self, chunk_hashes):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            iterator = iter(chunk_hashes)
            in_flight = {}
            next_submit_idx = 0
            next_yield_idx = 0

            def submit_one(chunk_hash):
                nonlocal next_submit_idx
                future = executor.submit(self.fetch_service.fetch_raw_chunk, chunk_hash)
                in_flight[next_submit_idx] = (chunk_hash, future)
                next_submit_idx += 1

            while len(in_flight) < self.window:
                try:
                    submit_one(next(iterator))
                except StopIteration:
                    break

            while in_flight:
                chunk_hash, future = in_flight[next_yield_idx]
                raw = future.result()
                yield chunk_hash, raw
                del in_flight[next_yield_idx]
                next_yield_idx += 1

                while len(in_flight) < self.window:
                    try:
                        submit_one(next(iterator))
                    except StopIteration:
                        break


class SnapshotRestorer:
    def __init__(
        self,
        *,
        db,
        repo,
        fetch_service,
        output_dir,
        prefetch_workers,
        prefetch_window,
    ):
        self.db = db
        self.repo = repo
        self.fetch_service = fetch_service
        self.output_dir = output_dir
        self.prefetch_workers = max(int(prefetch_workers), 1)
        self.prefetch_window = max(int(prefetch_window), 1)

    def restore(self, snapshot_id):
        status, error = self.db.get_snapshot_status(snapshot_id)
        if status is None:
            print(f"Snapshot {snapshot_id} not found.")
            return False

        if status != "COMPLETE":
            print(f"Snapshot {snapshot_id} is not restorable: {status}")
            if error:
                print(f"Reason: {error}")
            return False

        snapshot_uuid = self.db.get_snapshot_uuid(snapshot_id)
        if not snapshot_uuid:
            raise RuntimeError(f"Snapshot {snapshot_id} does not have a UUID.")

        paths = RestorePaths.for_snapshot(self.output_dir, snapshot_uuid)
        if os.path.exists(paths.final_dir):
            print(f"Snapshot {snapshot_id} already restored at: {paths.final_dir}")
            return True

        os.makedirs(paths.incomplete_dir, exist_ok=True)
        print(f"Restoring snapshot {snapshot_id} into {paths.incomplete_dir}")
        print(f"Prefetch: workers={self.prefetch_workers} window={self.prefetch_window}")
        start_time = time.perf_counter()

        directories = []
        current_temp_path = None
        processed_items = 0
        successful_items = 0

        try:
            for item in self.db.get_snapshot_items(snapshot_id):
                processed_items += 1

                try:
                    target_path = _safe_restore_path(paths.incomplete_dir, item["path"])
                except ValueError as exc:
                    print(f"Skipping unsafe path: {exc}")
                    continue

                if item["item_type"] == "dir":
                    os.makedirs(target_path, exist_ok=True)
                    directories.append((target_path, item))
                    successful_items += 1
                    continue

                os.makedirs(os.path.dirname(target_path), exist_ok=True)

                if os.path.isdir(target_path):
                    print(f"Expected file but found directory: {item['path']}")
                    continue

                if os.path.exists(target_path) and os.path.getsize(target_path) == item["size"]:
                    _restore_metadata(target_path, item)
                    successful_items += 1
                    continue

                current_temp_path = f"{target_path}.tmp"
                file_ok = True

                try:
                    prefetcher = OrderedChunkPrefetcher(
                        self.fetch_service,
                        workers=self.prefetch_workers,
                        window=self.prefetch_window,
                    )
                    with open(current_temp_path, "wb") as restored_file:
                        for _chunk_hash, raw in prefetcher.iter_raw_chunks(self.db.get_item_chunks(item["id"])):
                            restored_file.write(raw)
                except Exception as exc:
                    print(f"Could not restore file {item['path']}: {exc}")
                    file_ok = False

                if file_ok:
                    os.replace(current_temp_path, target_path)
                    current_temp_path = None
                    _restore_metadata(target_path, item)
                    successful_items += 1
                else:
                    if current_temp_path and os.path.exists(current_temp_path):
                        os.remove(current_temp_path)
                    current_temp_path = None

        except KeyboardInterrupt:
            print("Restore interrupted. Partial progress is kept in the .incomplete directory.")
            if current_temp_path and os.path.exists(current_temp_path):
                os.remove(current_temp_path)
            return False

        finally:
            directories.sort(key=lambda pair: len(pair[0]), reverse=True)
            for directory_path, item in directories:
                _restore_metadata(directory_path, item)

        if successful_items == processed_items and processed_items > 0:
            os.replace(paths.incomplete_dir, paths.final_dir)
            elapsed = time.perf_counter() - start_time
            print(f"Restore completed: {paths.final_dir}")
            print(f"Time: {elapsed:.2f} seconds")
            return True

        print(f"Restore incomplete: {successful_items}/{processed_items} items restored.")
        print(f"Work directory: {paths.incomplete_dir}")
        return False


def _restore_metadata(path, item):
    try:
        if item.get("mtime") is not None:
            os.utime(path, (item["mtime"], item["mtime"]))
        if item.get("mode") is not None:
            os.chmod(path, item["mode"])
    except OSError:
        print(f"Could not restore metadata for: {item['path']}")


def restore(snapshot_id, output_dir, *, seed=DEFAULT_SEED, rf=DEFAULT_RF, prefetch_workers=DEFAULT_PREFETCH_WORKERS, prefetch_window=DEFAULT_PREFETCH_WINDOW):
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
        output_dir=output_dir,
        prefetch_workers=prefetch_workers,
        prefetch_window=prefetch_window,
    )

    try:
        return restorer.restore(snapshot_id)
    finally:
        db.close()
        remote_pool.close()


def _build_parser():
    parser = argparse.ArgumentParser(description="Restaura snapshots desde caché local o nodos P2P.")
    parser.add_argument("snapshot_id", type=int)
    parser.add_argument("output_dir")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Nodo seed de membership")
    parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor esperado")
    parser.add_argument("--prefetch-workers", type=int, default=DEFAULT_PREFETCH_WORKERS, help="Número de workers para prefetch ordenado")
    parser.add_argument("--prefetch-window", type=int, default=DEFAULT_PREFETCH_WINDOW, help="Ventana máxima de chunks en vuelo por archivo")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    ok = restore(
        args.snapshot_id,
        args.output_dir,
        seed=args.seed,
        rf=args.rf,
        prefetch_workers=args.prefetch_workers,
        prefetch_window=args.prefetch_window,
    )
    if not ok:
        raise SystemExit(1)
