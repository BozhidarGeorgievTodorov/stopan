import bisect
import hashlib
import itertools
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


class StubCache:
    """Mantiene abiertos los canales gRPC usados durante una restauración."""

    def __init__(self):
        self.channels = {}
        self.stubs = {}

    def get(self, node):
        if node not in self.stubs:
            channel = grpc.insecure_channel(node)
            self.channels[node] = channel
            self.stubs[node] = p2p_storage_pb2_grpc.P2PStorageStub(channel)

        return self.stubs[node]

    def close(self):
        for channel in self.channels.values():
            channel.close()


def restore(snapshot_id, output_dir):
    """Reconstruye un snapshot usando caché local y nodos P2P si faltan bloques."""
    db = MetadataDB()
    repo = CASRepository()
    ring = ConsistentHashRing(NODES)
    stubs = StubCache()

    output_root = os.path.abspath(output_dir)
    work_root = f"{output_root}.incomplete"
    current_temp_path = None

    print(f"Restoring snapshot {snapshot_id} to {output_root}")
    start_time = time.perf_counter()

    try:
        if os.path.exists(output_root):
            print(f"Output directory already exists: {output_root}")
            return

        items_iter = iter(db.get_snapshot_items(snapshot_id))
        try:
            first_item = next(items_iter)
        except StopIteration:
            print(f"Snapshot {snapshot_id} not found.")
            return

        os.makedirs(work_root, exist_ok=True)
        directories = []
        processed = 0
        restored = 0

        for item in itertools.chain([first_item], items_iter):
            processed += 1
            target_path = _safe_target_path(work_root, item['path'])
            if target_path is None:
                print(f"Skipping unsafe path: {item['path']}")
                continue

            if item['item_type'] == 'dir':
                os.makedirs(target_path, exist_ok=True)
                directories.append((target_path, item))
                restored += 1
                continue

            if item['item_type'] == 'file':
                current_temp_path = _restore_file(item, target_path, db, repo, ring, stubs)
                current_temp_path = None
                restored += 1

        directories.sort(key=lambda pair: len(pair[0]), reverse=True)
        for dir_path, item in directories:
            _restore_metadata(dir_path, item)

        if restored == processed:
            os.replace(work_root, output_root)
            elapsed = time.perf_counter() - start_time
            print("Restore completed successfully")
            print(f"Time: {elapsed:.2f} seconds")
        else:
            print(f"Restore incomplete: {restored}/{processed} items restored")
            print(f"Work directory: {work_root}")

    except KeyboardInterrupt:
        if current_temp_path and os.path.exists(current_temp_path):
            os.remove(current_temp_path)
        print(f"Restore interrupted. Work directory kept at: {work_root}")

    except Exception as e:
        if current_temp_path and os.path.exists(current_temp_path):
            os.remove(current_temp_path)
        print(f"Error restoring snapshot: {e}")
        print(f"Work directory kept at: {work_root}")

    finally:
        stubs.close()
        db.close()


def _restore_file(item, target_path, db, repo, ring, stubs):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if os.path.exists(target_path) and os.path.getsize(target_path) == item['size']:
        print(f"Skipping existing file: {item['path']}")
        return None

    chunks = db.get_item_chunks(item['id'])
    temp_path = f"{target_path}.tmp"

    try:
        with open(temp_path, 'wb') as f:
            for chunk_hash in chunks:
                f.write(_fetch_chunk(chunk_hash, repo, ring, stubs))

        os.replace(temp_path, target_path)
        _restore_metadata(target_path, item)
        print(f"Restored: {item['path']}")
        return None

    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def _fetch_chunk(chunk_hash, repo, ring, stubs):
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
            stub = stubs.get(node)
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
