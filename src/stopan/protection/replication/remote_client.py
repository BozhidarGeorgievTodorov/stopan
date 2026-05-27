"""
Cliente remoto para replicación clásica de chunks completos.

Encapsula ProbeMissingChunks y ReplicateChunks, incluyendo ACKs parciales cuando
un stream falla después de haber confirmado algunos chunks.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass

from stopan.common.batching import iter_batches
from stopan.common.sequences import ordered_unique
from stopan.config.defaults import (
    DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    DEFAULT_REPLICATION_STREAM_INFLIGHT,
    DEFAULT_REPLICATION_STREAM_TIMEOUT_S,
)
from stopan.errors import StopanNetworkError
from stopan.protos import p2p_storage_pb2
from stopan.rpc.errors import format_remote_error
from stopan.protection.remote_client_base import P2PStorageProtectionClient

from .queue_iterator import QueueIterator


_SENDER_JOIN_TIMEOUT_S = 2.0


@dataclass(frozen=True, slots=True)
class ProbeExecutionResult:
    node_id: str
    address: str
    requested_hashes: tuple[str, ...]
    present_hashes: frozenset[str]
    transport_error: str | None = None


@dataclass(frozen=True, slots=True)
class TargetAck:
    """ACK recibido desde un target remoto para un chunk concreto."""

    chunk_hash: str
    node_id: str
    address: str
    status: int
    detail: str

    @property
    def is_success(self) -> bool:
        return self.status in (
            p2p_storage_pb2.STORE_STATUS_STORED,
            p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
        )

    @property
    def is_already_present(self) -> bool:
        return self.status == p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT

    @property
    def is_stored(self) -> bool:
        return self.status == p2p_storage_pb2.STORE_STATUS_STORED


class StreamingReplicationError(StopanNetworkError, RuntimeError):
    """Error de stream que conserva los ACKs recibidos antes del fallo."""

    def __init__(self, message: str, *, acks: dict[str, TargetAck]):
        super().__init__(message)
        self.acks = dict(acks)


class RemoteChunkClientPool(P2PStorageProtectionClient):
    def __init__(
        self,
        *,
        probe_timeout_s: float,
        probe_batch_hashes: int,
        stream_timeout_s: float = DEFAULT_REPLICATION_STREAM_TIMEOUT_S,
        stream_inflight: int = DEFAULT_REPLICATION_STREAM_INFLIGHT,
        max_message_bytes: int | None = None,
    ):
        self.probe_timeout_s = float(probe_timeout_s)
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self.stream_timeout_s = float(stream_timeout_s)
        self.stream_inflight = max(1, int(stream_inflight))
        super().__init__(
            max_message_bytes=max_message_bytes or DEFAULT_GRPC_MAX_MESSAGE_BYTES,
            closed_message="RemoteChunkClientPool cerrado",
            closed_error_factory=StopanNetworkError,
        )

    def probe_missing_hashes(
        self,
        *,
        address: str,
        chunk_hashes: Sequence[str],
    ) -> set[str]:
        self._ensure_open()
        self._ensure_runtime()

        ordered_hashes = tuple(ordered_unique(chunk_hashes))
        missing: set[str] = set()
        if not ordered_hashes:
            return missing

        stub = self._get_stub(address)
        for batch in iter_batches(ordered_hashes, self.probe_batch_hashes):
            response = stub.ProbeMissingChunks(
                self._pb.ProbeMissingChunksRequest(chunk_hashes=batch),
                timeout=self.probe_timeout_s,
            )
            missing.update(chunk_hash for chunk_hash in response.missing_hashes if chunk_hash)

        return missing

    def probe_target(
        self,
        *,
        node_id: str,
        address: str,
        chunk_hashes: Sequence[str],
    ) -> ProbeExecutionResult:
        ordered_hashes = tuple(ordered_unique(chunk_hashes))
        requested = frozenset(ordered_hashes)

        try:
            missing = self.probe_missing_hashes(
                address=address,
                chunk_hashes=ordered_hashes,
            )
            return ProbeExecutionResult(
                node_id=node_id,
                address=address,
                requested_hashes=ordered_hashes,
                present_hashes=frozenset(requested - missing),
                transport_error=None,
            )

        except Exception as exc:
            transport_error = format_remote_error(exc)
            return ProbeExecutionResult(
                node_id=node_id,
                address=address,
                requested_hashes=ordered_hashes,
                present_hashes=frozenset(),
                transport_error=transport_error,
            )

    def replicate_missing_hashes(
        self,
        *,
        node_id: str,
        address: str,
        chunk_hashes: Sequence[str],
        repo,
    ) -> dict[str, TargetAck]:
        """
        Replica hashes ausentes leyendo blobs comprimidos desde el CAS local.

        Devuelve ACKs por chunk. Si el stream falla, lanza
        StreamingReplicationError conservando los ACKs recibidos antes del fallo.
        """
        self._ensure_open()
        self._ensure_runtime()

        ordered_hashes = ordered_unique(chunk_hashes)
        if not ordered_hashes:
            return {}

        stub = self._get_stub(address)
        request_iter = QueueIterator(maxsize=self.stream_inflight)
        acks: dict[str, TargetAck] = {}
        local_failures: dict[str, TargetAck] = {}
        local_failures_lock = threading.Lock()
        sender_error: Exception | None = None
        sender_error_lock = threading.Lock()
        stop_event = threading.Event()
        stream_error: Exception | None = None

        def sender() -> None:
            nonlocal sender_error
            try:
                for chunk_hash in ordered_hashes:
                    if stop_event.is_set():
                        break

                    try:
                        chunk_data = repo.get_compressed(chunk_hash)
                    except Exception as exc:
                        with local_failures_lock:
                            local_failures[chunk_hash] = TargetAck(
                                chunk_hash=chunk_hash,
                                node_id=node_id,
                                address=address,
                                status=p2p_storage_pb2.STORE_STATUS_ERROR,
                                detail=f"lectura local falló: {exc}",
                            )
                        continue

                    accepted = request_iter.put(
                        self._pb.ReplicateChunkRequest(
                            chunk_hash=chunk_hash,
                            chunk_data=chunk_data,
                        )
                    )
                    if not accepted:
                        break
            except Exception as exc:
                with sender_error_lock:
                    if sender_error is None:
                        sender_error = exc
            finally:
                if stop_event.is_set():
                    request_iter.cancel()
                else:
                    request_iter.finish()

        sender_thread = threading.Thread(
            target=sender,
            name=f"replicate-send-{node_id[:8]}",
            daemon=True,
        )
        sender_thread.start()

        try:
            responses = stub.ReplicateChunks(request_iter, timeout=self.stream_timeout_s)
            for result in responses:
                if not result.chunk_hash:
                    continue

                acks[result.chunk_hash] = TargetAck(
                    chunk_hash=result.chunk_hash,
                    node_id=node_id,
                    address=address,
                    status=result.status,
                    detail=result.detail or "",
                )

        except Exception as exc:
            stream_error = exc
        finally:
            # Desbloquea siempre el productor. El stream de respuestas puede terminar
            # antes de que el servidor consuma toda la entrada. Solo los ACKs explícitos
            # recibidos arriba cuentan como evidencia.
            stop_event.set()
            request_iter.cancel()
            sender_thread.join(timeout=_SENDER_JOIN_TIMEOUT_S)

        with local_failures_lock:
            acks.update(local_failures)

        if sender_thread.is_alive():
            raise StreamingReplicationError(
                "el productor de requests no se detuvo tras completar/cancelar el stream",
                acks=acks,
            )

        with sender_error_lock:
            if sender_error is not None:
                raise StreamingReplicationError(
                    f"el productor de requests falló: {sender_error}",
                    acks=acks,
                )

        if stream_error is not None:
            detail = format_remote_error(stream_error)

            raise StreamingReplicationError(
                f"replicación por stream falló: {detail}",
                acks=acks,
            ) from stream_error

        for chunk_hash in ordered_hashes:
            if chunk_hash not in acks:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=node_id,
                    address=address,
                    status=p2p_storage_pb2.STORE_STATUS_ERROR,
                    detail="el stream terminó sin ACK para el chunk",
                )

        return acks
