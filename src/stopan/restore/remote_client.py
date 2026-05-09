"""
Cliente remoto para recuperar chunks por RetrieveChunkBatch.

Encapsula canales gRPC, stubs y degradación adaptativa del tamaño de batch
cuando una respuesta supera el límite de mensaje configurado.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from stopan.rpc.errors import is_message_too_large_error
from stopan.rpc.options import grpc_channel_options


@dataclass(frozen=True, slots=True)
class BatchRetrieveItemResult:
    """Resultado normalizado de un item devuelto por RetrieveChunkBatch."""
    chunk_hash: str
    status: int
    chunk_data: bytes
    detail: str

    def is_found(self, found_status: int) -> bool:
        return self.status == found_status


class RemoteStorageClientPool:
    """
    Adaptador de infraestructura para lectura remota.

    Decisiones de estabilización:
      - import diferido de grpc/protobuf;
      - thread-safe;
      - usa únicamente RetrieveChunkBatch;
      - sin retrocompatibilidad con RetrieveChunk unary.
    """

    def __init__(self, *, timeout_s: float, max_message_bytes: int):
        self.timeout_s = float(timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)
        self._channels = {}
        self._stubs = {}
        self._grpc = None
        self._pb = None
        self._pb_grpc = None
        self._lock = threading.Lock()
        self._closed = threading.Event()

    def _ensure_open(self) -> None:
        if self._closed.is_set():
            raise RuntimeError("RemoteStorageClientPool cerrado")

    def _ensure_runtime(self) -> None:
        """Carga grpc/protobuf bajo demanda."""

        if self._grpc is not None:
            return

        import grpc
        from stopan.protos import p2p_storage_pb2
        from stopan.protos import p2p_storage_pb2_grpc

        self._grpc = grpc
        self._pb = p2p_storage_pb2
        self._pb_grpc = p2p_storage_pb2_grpc

    def _get_stub(self, addr: str):
        """Devuelve un stub reutilizable para addr, creándolo si hace falta."""
        
        self._ensure_runtime()

        with self._lock:
            self._ensure_open()
            if addr not in self._stubs:
                channel = self._grpc.insecure_channel(
                    addr,
                    options=grpc_channel_options(self.max_message_bytes),
                )
                self._channels[addr] = channel
                self._stubs[addr] = self._pb_grpc.P2PStorageStub(channel)

            return self._stubs[addr]

    @property
    def retrieve_status_found(self) -> int:
        self._ensure_runtime()
        return self._pb.RETRIEVE_STATUS_FOUND

    @property
    def retrieve_status_not_found(self) -> int:
        self._ensure_runtime()
        return self._pb.RETRIEVE_STATUS_NOT_FOUND

    def retrieve_chunk_batch(self, *, addr: str, chunk_hashes: list[str]) -> dict[str, BatchRetrieveItemResult]:
        """
        Recupera chunks por lotes con degradación adaptativa.

        Caso normal:
          - intenta resolver el batch completo en un único RetrieveChunkBatch.

        Caso de transporte:
          - si la respuesta supera max_message_bytes, divide el batch en dos
            y reintenta recursivamente.
        """
        self._ensure_open()
        self._ensure_runtime()

        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}

        return self._retrieve_chunk_batch_adaptive(
            addr=addr,
            chunk_hashes=ordered_hashes,
        )

    def _retrieve_chunk_batch_adaptive(
        self,
        *,
        addr: str,
        chunk_hashes: list[str],
    ) -> dict[str, BatchRetrieveItemResult]:
        """Ejecuta RetrieveChunkBatch y divide el batch si la respuesta es demasiado grande."""

        try:
            return self._retrieve_chunk_batch_once(
                addr=addr,
                chunk_hashes=chunk_hashes,
            )

        except Exception as exc:
            if len(chunk_hashes) > 1 and is_message_too_large_error(exc):
                mid = max(1, len(chunk_hashes) // 2)

                left = self._retrieve_chunk_batch_adaptive(
                    addr=addr,
                    chunk_hashes=chunk_hashes[:mid],
                )
                right = self._retrieve_chunk_batch_adaptive(
                    addr=addr,
                    chunk_hashes=chunk_hashes[mid:],
                )

                left.update(right)
                return left

            raise

    def _retrieve_chunk_batch_once(
        self,
        *,
        addr: str,
        chunk_hashes: list[str],
    ) -> dict[str, BatchRetrieveItemResult]:
        """Ejecuta una llamada RetrieveChunkBatch sin subdividir el batch."""

        if not chunk_hashes:
            return {}

        stub = self._get_stub(addr)
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
                    detail="sin resultado en RetrieveChunkBatch",
                )

        return results

    def close(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return

            self._closed.set()

            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()
