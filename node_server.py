import os
import sys
from concurrent import futures

import blake3
import grpc
import zstandard as zstd

from core.repository import CASRepository
from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc

DEFAULT_PORT = 50051
MAX_CHUNK_SIZE = 8 * 1024 * 1024


class StorageNodeServicer(p2p_storage_pb2_grpc.P2PStorageServicer):
    """Servidor gRPC que almacena y sirve chunks comprimidos."""

    def __init__(self, repo_path="node_store"):
        self.repo = CASRepository(repo_path)
        self.decompressor = zstd.ZstdDecompressor()
        print(f"Storage node using repository: {os.path.abspath(repo_path)}")

    def StoreChunk(self, request, context):
        try:
            if self.repo.exists_local(request.chunk_hash):
                return p2p_storage_pb2.StoreResponse(success=True, message="already present")

            if not self._is_valid_chunk(request.chunk_hash, request.chunk_data):
                return p2p_storage_pb2.StoreResponse(success=False, message="invalid chunk")

            is_new = self.repo.put_compressed(request.chunk_hash, request.chunk_data)
            message = "stored" if is_new else "already present"
            return p2p_storage_pb2.StoreResponse(success=True, message=message)
        except Exception as exc:
            return p2p_storage_pb2.StoreResponse(success=False, message=str(exc))

    def RetrieveChunk(self, request, context):
        try:
            data = self.repo.get_compressed(request.chunk_hash)
            return p2p_storage_pb2.RetrieveResponse(
                success=True,
                chunk_data=data,
                message="ok",
            )
        except Exception as exc:
            return p2p_storage_pb2.RetrieveResponse(success=False, message=str(exc))

    def _is_valid_chunk(self, chunk_hash, compressed_data):
        try:
            data = self.decompressor.decompress(
                compressed_data,
                max_output_size=MAX_CHUNK_SIZE,
            )
        except zstd.ZstdError:
            return False

        return blake3.blake3(data).hexdigest() == chunk_hash


def serve(port=DEFAULT_PORT):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    p2p_storage_pb2_grpc.add_P2PStorageServicer_to_server(
        StorageNodeServicer(),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Storage node listening on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    serve(port)
