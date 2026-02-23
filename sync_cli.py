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

    try:
        pending_chunks = db.get_pending_sync_chunks()
        if not pending_chunks:
            print("No pending chunks to sync.")
            return

        print(f"Syncing {len(pending_chunks)} chunks")
        sent = 0

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

                with grpc.insecure_channel(node) as channel:
                    stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
                    response = stub.StoreChunk(request, timeout=10)

                if response.success:
                    db.mark_chunk_as_synced(chunk_hash)
                    sent += 1

                    if sent % 100 == 0:
                        db.commit()
                else:
                    print(f"Node {node} rejected {chunk_hash[:8]}: {response.message}")
                    break

            except grpc.RpcError as exc:
                print(f"Network error with {node}: {exc.details()}")
                break

        db.commit()
        print(f"Push completed: {sent}/{len(pending_chunks)} chunks sent")

    finally:
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
