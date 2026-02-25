import bisect
import hashlib
import sys

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

COMMIT_EVERY = 100


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


def push():
    """Envía a la red los chunks locales que aún no están sincronizados."""
    repo = CASRepository()
    db = MetadataDB()
    ring = ConsistentHashRing(NODES)
    channels = {}
    stubs = {}

    try:
        pending_chunks = db.get_pending_sync_chunks()
        if not pending_chunks:
            print("No pending chunks to sync.")
            return

        for node in NODES:
            channel = grpc.insecure_channel(node)
            channels[node] = channel
            stubs[node] = p2p_storage_pb2_grpc.P2PStorageStub(channel)

        print(f"Syncing {len(pending_chunks)} chunks")
        sent = 0
        synced_batch = []

        for chunk_hash in pending_chunks:
            node = ring.get_node(chunk_hash)
            if node is None:
                raise RuntimeError("No storage nodes configured.")

            try:
                compressed_data = repo.get_compressed(chunk_hash)
                request = p2p_storage_pb2.StoreRequest(
                    chunk_hash=chunk_hash,
                    chunk_data=compressed_data,
                )
                response = stubs[node].StoreChunk(request, timeout=10)

                if not response.success:
                    print(f"Node {node} rejected {chunk_hash[:8]}: {response.message}")
                    break

                synced_batch.append(chunk_hash)
                sent += 1

                if len(synced_batch) >= COMMIT_EVERY:
                    db.mark_chunks_as_synced(synced_batch)
                    db.commit()
                    synced_batch = []

            except grpc.RpcError as exc:
                print(f"Network error with {node}: {exc.details()}")
                break

        if synced_batch:
            db.mark_chunks_as_synced(synced_batch)

        db.commit()
        print(f"Push completed: {sent}/{len(pending_chunks)} chunks sent")

    finally:
        for channel in channels.values():
            channel.close()
        db.close()


def print_usage():
    print("Usage:")
    print("  python sync_cli.py push")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "push":
        push()
    else:
        print_usage()
        sys.exit(1)
