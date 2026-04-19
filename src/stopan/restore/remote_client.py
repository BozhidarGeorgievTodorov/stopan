from __future__ import annotations

import threading
from dataclasses import dataclass
from collections.abc import Sequence

from stopan.restore.config import DEFAULT_RPC_TIMEOUT_S, GRPC_MAX_MESSAGE_BYTES


@dataclass(frozen=True)
class BatchRetrieveItemResult:
    chunk_hash: str
    status: int
    chunk_data: bytes
    detail: str

    def is_found(self, retrieve_status_found: int) -> bool:
        return self.status == retrieve_status_found


class RemoteStorageClientPool:
    """
    Pool de clientes remotos para restore.

    La carga de grpc/protobuf se retrasa hasta que realmente falta un chunk
    local y hay que consultar la red. La lectura remota usa RetrieveChunkBatch.
    """

    def __init__(self, *, timeout_s: float = DEFAULT_RPC_TIMEOUT_S):
        self.timeout_s = float(timeout_s)
        self._channels = {}
        self._stubs = {}
        self._grpc = None
        self._pb = None
        self._pb_grpc = None
        self._lock = threading.Lock()

    def _ensure_runtime(self) -> None:
        if self._grpc is not None:
            return

        import grpc
        from stopan.protos import p2p_storage_pb2
        from stopan.protos import p2p_storage_pb2_grpc

        self._grpc = grpc
        self._pb = p2p_storage_pb2
        self._pb_grpc = p2p_storage_pb2_grpc

    def _get_stub(self, address: str):
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
                self._stubs[address] = self._pb_grpc.P2PStorageStub(channel)

            return self._stubs[address]

    @property
    def retrieve_status_found(self) -> int:
        self._ensure_runtime()
        return self._pb.RETRIEVE_STATUS_FOUND

    @property
    def retrieve_status_not_found(self) -> int:
        self._ensure_runtime()
        return self._pb.RETRIEVE_STATUS_NOT_FOUND

    @property
    def retrieve_status_error(self) -> int:
        self._ensure_runtime()
        return self._pb.RETRIEVE_STATUS_ERROR

    def retrieve_chunk_batch(
        self,
        *,
        address: str,
        chunk_hashes: Sequence[str],
    ) -> dict[str, BatchRetrieveItemResult]:
        self._ensure_runtime()

        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}

        return self._retrieve_chunk_batch_adaptive(
            address=address,
            chunk_hashes=ordered_hashes,
        )

    def _retrieve_chunk_batch_adaptive(
        self,
        *,
        address: str,
        chunk_hashes: list[str],
    ) -> dict[str, BatchRetrieveItemResult]:
        try:
            return self._retrieve_chunk_batch_once(
                address=address,
                chunk_hashes=chunk_hashes,
            )

        except Exception as exc:
            if len(chunk_hashes) > 1 and self.is_message_too_large_error(exc):
                midpoint = max(1, len(chunk_hashes) // 2)

                left = self._retrieve_chunk_batch_adaptive(
                    address=address,
                    chunk_hashes=chunk_hashes[:midpoint],
                )
                right = self._retrieve_chunk_batch_adaptive(
                    address=address,
                    chunk_hashes=chunk_hashes[midpoint:],
                )

                left.update(right)
                return left

            raise

    def _retrieve_chunk_batch_once(
        self,
        *,
        address: str,
        chunk_hashes: list[str],
    ) -> dict[str, BatchRetrieveItemResult]:
        if not chunk_hashes:
            return {}

        stub = self._get_stub(address)
        request = self._pb.RetrieveChunkBatchRequest(chunk_hashes=chunk_hashes)
        response = stub.RetrieveChunkBatch(request, timeout=self.timeout_s)

        results: dict[str, BatchRetrieveItemResult] = {}
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
                    status=self._pb.RETRIEVE_STATUS_ERROR,
                    chunk_data=b"",
                    detail="missing batch result",
                )

        return results

    def is_message_too_large_error(self, exc: Exception) -> bool:
        self._ensure_runtime()

        if not isinstance(exc, self._grpc.RpcError):
            return False

        try:
            if exc.code() == self._grpc.StatusCode.RESOURCE_EXHAUSTED:
                return True
        except Exception:
            pass

        parts: list[str] = []

        try:
            details = exc.details()
            if details:
                parts.append(str(details))
        except Exception:
            pass

        parts.append(str(exc))
        text = " ".join(parts).lower()

        return (
            "message larger than max" in text
            or "sent message larger than max" in text
            or "received message larger than max" in text
            or "resource exhausted" in text
        )

    def is_rpc_error(self, exc: Exception) -> bool:
        self._ensure_runtime()
        return isinstance(exc, self._grpc.RpcError)

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()
