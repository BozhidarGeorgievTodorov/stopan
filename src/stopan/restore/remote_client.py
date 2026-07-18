"""
Cliente remoto para recuperar chunks por RetrieveChunkBatch.

Encapsula canales gRPC, stubs y degradación adaptativa del tamaño de batch
cuando una respuesta supera el límite de mensaje configurado.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.rpc.p2p_storage_client import P2PStorageClientRuntime, run_adaptive_batch_call


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

    def __init__(
        self,
        *,
        cluster_token: str,
        timeout_s: float,
        max_message_bytes: int,
    ):
        self.timeout_s = float(timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)
        self._runtime = P2PStorageClientRuntime(
            cluster_token=cluster_token,
            max_message_bytes=self.max_message_bytes,
            closed_message="RemoteStorageClientPool cerrado",
            closed_error_factory=RuntimeError,
        )

    def _ensure_open(self) -> None:
        self._runtime.ensure_open()

    def _ensure_runtime(self) -> None:
        """Carga grpc/protobuf bajo demanda."""
        self._runtime.ensure_runtime()

    @property
    def _pb(self):
        return self._runtime.pb

    def _get_stub(self, addr: str):
        """Devuelve un stub reutilizable para addr, creándolo si hace falta."""
        return self._runtime.get_stub(addr)

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

        return run_adaptive_batch_call(
            items=ordered_hashes,
            call_once=lambda batch: self._retrieve_chunk_batch_once(
                addr=addr,
                chunk_hashes=batch,
            ),
        )

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
        response = stub.RetrieveChunkBatch(
            request,
            timeout=self.timeout_s,
            metadata=self._runtime.call_metadata,
        )

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
        self._runtime.close()
