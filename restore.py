import argparse
import os
import time

import grpc

from core.cluster_view import ClusterMembershipClient
from core.database import MetadataDB
from core.repository import CASRepository
from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
DEFAULT_SEED = os.getenv("MEMBERSHIP_SEED", "localhost:50051")


class StorageStubPool:
    """Reutiliza canales gRPC durante una restauración."""

    def __init__(self):
        self._channels = {}
        self._stubs = {}

    def get(self, address):
        if address not in self._stubs:
            channel = grpc.insecure_channel(address)
            self._channels[address] = channel
            self._stubs[address] = p2p_storage_pb2_grpc.P2PStorageStub(channel)
        return self._stubs[address]

    def close(self):
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()
        self._stubs.clear()


def _safe_restore_path(base_dir, rel_path):
    if rel_path == ".":
        return base_dir

    normalized_rel = os.path.normpath(rel_path)
    full_path = os.path.abspath(os.path.join(base_dir, normalized_rel))
    base_dir_abs = os.path.abspath(base_dir)

    if os.path.commonpath([base_dir_abs, full_path]) != base_dir_abs:
        raise ValueError(f"Path escapes restore directory: {rel_path}")

    return full_path


def _fetch_chunk(chunk_hash, repo, *, cluster_provider, rf, stub_pool):
    try:
        return repo.get(chunk_hash)
    except FileNotFoundError:
        pass

    cluster = cluster_provider()
    targets = cluster.hrw_remote_targets(chunk_hash, rf=rf, salt=CLUSTER_TOKEN)
    if not targets:
        raise RuntimeError("No remote targets available for missing chunk.")

    last_error = None
    for member in targets:
        request = p2p_storage_pb2.RetrieveRequest(chunk_hash=chunk_hash)
        try:
            response = stub_pool.get(member.address).RetrieveChunk(request, timeout=5)
            if not response.success:
                last_error = RuntimeError(response.message or f"{member.address} did not return the chunk")
                continue

            repo.put_compressed(chunk_hash, response.chunk_data)
            return repo.get(chunk_hash)

        except grpc.RpcError as exc:
            last_error = RuntimeError(f"RPC error with {member.address}: {exc.details()}")

    if last_error:
        raise last_error
    raise FileNotFoundError(f"Chunk not found in remote targets: {chunk_hash}")


def restore(snapshot_id, output_dir, *, seed=DEFAULT_SEED, rf=DEFAULT_RF):
    """Reconstruye un snapshot usando la caché local y la red P2P si falta algún chunk."""
    db = MetadataDB(DB_FILE)
    repo = CASRepository(LOCAL_SHARD_DIR)
    stubs = StorageStubPool()
    cluster_cache = {}

    def get_cluster():
        if "view" not in cluster_cache:
            self_addr = os.getenv("ADVERTISE_ADDR", "")
            cluster = ClusterMembershipClient(seed, self_addr=self_addr).get_cluster_view()
            if not cluster.members:
                raise RuntimeError(f"No eligible members returned by seed {seed}.")
            cluster_cache["view"] = cluster
        return cluster_cache["view"]

    status, error = db.get_snapshot_status(snapshot_id)
    if status is None:
        print(f"Snapshot {snapshot_id} not found.")
        db.close()
        return False

    if status != "COMPLETE":
        print(f"Snapshot {snapshot_id} is not restorable: {status}")
        if error:
            print(f"Reason: {error}")
        db.close()
        return False

    snapshot_uuid = db.get_snapshot_uuid(snapshot_id)
    if not snapshot_uuid:
        db.close()
        raise RuntimeError(f"Snapshot {snapshot_id} does not have a UUID.")

    final_dir = os.path.join(output_dir, f"snapshot_{snapshot_uuid}")
    incomplete_dir = final_dir + ".incomplete"

    if os.path.exists(final_dir):
        print(f"Snapshot {snapshot_id} already restored at: {final_dir}")
        db.close()
        return True

    os.makedirs(incomplete_dir, exist_ok=True)
    print(f"Restoring snapshot {snapshot_id} into {incomplete_dir}")
    start_time = time.perf_counter()

    directories = []
    current_temp_path = None
    processed_items = 0
    successful_items = 0

    try:
        for item in db.get_snapshot_items(snapshot_id):
            processed_items += 1

            try:
                target_path = _safe_restore_path(incomplete_dir, item["path"])
            except ValueError as exc:
                print(f"Skipping unsafe path: {exc}")
                continue

            if item["item_type"] == "dir":
                os.makedirs(target_path, exist_ok=True)
                directories.append((target_path, item))
                successful_items += 1
                continue

            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            if os.path.exists(target_path) and os.path.getsize(target_path) == item["size"]:
                _restore_metadata(target_path, item)
                successful_items += 1
                continue

            current_temp_path = f"{target_path}.tmp"
            file_ok = True

            try:
                with open(current_temp_path, "wb") as restored_file:
                    for chunk_hash in db.get_item_chunks(item["id"]):
                        try:
                            restored_file.write(
                                _fetch_chunk(
                                    chunk_hash,
                                    repo,
                                    cluster_provider=get_cluster,
                                    rf=rf,
                                    stub_pool=stubs,
                                )
                            )
                        except Exception as exc:
                            print(f"Could not restore chunk {chunk_hash[:8]} for {item['path']}: {exc}")
                            file_ok = False
                            break
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

        stubs.close()
        db.close()

    if successful_items == processed_items and processed_items > 0:
        os.replace(incomplete_dir, final_dir)
        elapsed = time.perf_counter() - start_time
        print(f"Restore completed: {final_dir}")
        print(f"Time: {elapsed:.2f} seconds")
        return True

    print(f"Restore incomplete: {successful_items}/{processed_items} items restored.")
    print(f"Work directory: {incomplete_dir}")
    return False


def _restore_metadata(path, item):
    try:
        if item.get("mtime") is not None:
            os.utime(path, (item["mtime"], item["mtime"]))
        if item.get("mode") is not None:
            os.chmod(path, item["mode"])
    except OSError:
        print(f"Could not restore metadata for: {item['path']}")


def _build_parser():
    parser = argparse.ArgumentParser(description="Restaura snapshots desde caché local o nodos P2P.")
    parser.add_argument("snapshot_id", type=int)
    parser.add_argument("output_dir")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Nodo seed de membership")
    parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor esperado")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    ok = restore(args.snapshot_id, args.output_dir, seed=args.seed, rf=args.rf)
    if not ok:
        raise SystemExit(1)
