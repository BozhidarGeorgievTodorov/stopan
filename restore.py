import bisect
import hashlib
import os
import sys
import time

import grpc

from core.database import MetadataDB
from core.repository import CASRepository
from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc

NODES = [
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
    "localhost:50054",
]

MAX_RETRIES = 3
RETRY_DELAY = 1


class ConsistentHashRing:
    """Mapa simple de hashes de chunk a nodos de almacenamiento."""

    def __init__(self, nodes, virtual_nodes=3):
        self.ring = []
        self.node_map = {}

        for node in nodes:
            self.add_node(node, virtual_nodes)

    def add_node(self, node, virtual_nodes):
        for index in range(virtual_nodes):
            name = f"{node}-{index}"
            value = int(hashlib.sha256(name.encode("utf-8")).hexdigest(), 16)
            bisect.insort(self.ring, value)
            self.node_map[value] = node

    def get_node(self, chunk_hash):
        if not self.ring:
            return None

        value = int(chunk_hash, 16)
        index = bisect.bisect(self.ring, value)
        if index == len(self.ring):
            index = 0

        return self.node_map[self.ring[index]]


def restore(snapshot_id, output_dir):
    """Reconstruye un snapshot usando caché local y nodos P2P si faltan bloques."""
    db = MetadataDB()
    repo = CASRepository()
    ring = ConsistentHashRing(NODES)

    print(f"Restoring snapshot {snapshot_id} to {output_dir}")
    start_time = time.perf_counter()

    try:
        items = db.get_snapshot_items(snapshot_id)
        if not items:
            print(f"Snapshot {snapshot_id} not found.")
            return

        output_root = os.path.abspath(output_dir)
        os.makedirs(output_root, exist_ok=True)
        directories = []

        for item in items:
            target_path = _safe_target_path(output_root, item['path'])
            if target_path is None:
                print(f"Skipping unsafe path: {item['path']}")
                continue

            if item['item_type'] == 'dir':
                os.makedirs(target_path, exist_ok=True)
                directories.append((target_path, item))
                continue

            if item['item_type'] == 'file':
                _restore_file(item, target_path, db, repo, ring)

        directories.sort(key=lambda pair: len(pair[0]), reverse=True)
        for dir_path, item in directories:
            _restore_metadata(dir_path, item)

        elapsed = time.perf_counter() - start_time
        print("Restore completed successfully")
        print(f"Time: {elapsed:.2f} seconds")

    except Exception as e:
        print(f"Error restoring snapshot: {e}")

    finally:
        db.close()


def _restore_file(item, target_path, db, repo, ring):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if os.path.exists(target_path) and os.path.getsize(target_path) == item['size']:
        print(f"Skipping existing file: {item['path']}")
        return

    chunks = db.get_item_chunks(item['id'])
    temp_path = f"{target_path}.tmp"

    try:
        with open(temp_path, 'wb') as f:
            for chunk_hash in chunks:
                f.write(_fetch_chunk(chunk_hash, repo, ring))

        os.replace(temp_path, target_path)
        _restore_metadata(target_path, item)
        print(f"Restored: {item['path']}")

    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def _fetch_chunk(chunk_hash, repo, ring):
    try:
        return repo.get(chunk_hash)
    except FileNotFoundError:
        pass

    node = ring.get_node(chunk_hash)
    if node is None:
        raise RuntimeError("No storage nodes configured.")

    request = p2p_storage_pb2.RetrieveRequest(chunk_hash=chunk_hash)

    for attempt in range(MAX_RETRIES):
        try:
            with grpc.insecure_channel(node) as channel:
                stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
                response = stub.RetrieveChunk(request, timeout=5)

            if not response.success:
                raise RuntimeError(response.message or f"Chunk not found on node {node}")

            repo.put_compressed(chunk_hash, response.chunk_data)
            return repo.get(chunk_hash)

        except grpc.RpcError as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Could not retrieve {chunk_hash} from {node}: {exc.details()}") from exc
            time.sleep(RETRY_DELAY)

    raise RuntimeError(f"Could not retrieve chunk: {chunk_hash}")


def _safe_target_path(output_root, rel_path):
    if rel_path == ".":
        return output_root

    normalized_path = os.path.normpath(rel_path)
    target_path = os.path.abspath(os.path.join(output_root, normalized_path))

    if os.path.commonpath([output_root, target_path]) != output_root:
        return None

    return target_path


def _restore_metadata(path, item):
    if item.get('mtime') is not None:
        os.utime(path, (item['mtime'], item['mtime']))

    if item.get('mode') is not None:
        os.chmod(path, item['mode'])


def print_usage():
    print("Usage:")
    print("  python restore.py <snapshot_id> <output_dir>")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        restore(int(sys.argv[1]), sys.argv[2])
    else:
        print_usage()
        sys.exit(1)
