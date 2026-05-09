"""
Sesión de streaming hacia un target remoto.

Cada sesión consulta primero qué chunks faltan en el nodo remoto y después envía
por stream únicamente los blobs comprimidos ausentes. Si el stream falla, los
ACKs recibidos antes del fallo se conservan para permitir estados DEGRADED.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence

import grpc

from stopan.common.batching import iter_batches
from stopan.protos import p2p_storage_pb2
from stopan.replication.outcomes import StreamingReplicationError, TargetAck
from stopan.replication.queue_iterator import QueueIterator
from stopan.rpc.errors import format_rpc_error
from stopan.rpc.p2p_storage_pool import P2PStorageStubPool


_SENDER_JOIN_TIMEOUT_S = 2.0


class TargetStreamingSession:
    """
    Sesión de replicación contra un único target remoto.

    Estrategia:
      1. ProbeMissingChunks descubre hashes ausentes por lotes.
      2. ReplicateChunks envía por stream solo los blobs comprimidos que faltan.
    """

    def __init__(
        self,
        *,
        node_id: str,
        address: str,
        rpc_pool: P2PStorageStubPool,
        probe_timeout_s: float,
        stream_timeout_s: float,
        probe_batch_hashes: int,
        stream_inflight: int,
    ):
        self.node_id = node_id
        self.address = address
        self._rpc_pool = rpc_pool
        self._probe_timeout_s = float(probe_timeout_s)
        self._stream_timeout_s = float(stream_timeout_s)
        self._probe_batch_hashes = max(1, int(probe_batch_hashes))
        self._stream_inflight = max(1, int(stream_inflight))

    @property
    def _stub(self):
        return self._rpc_pool.get_stub(self.address)

    def probe_missing_hashes(self, chunk_hashes: Sequence[str]) -> set[str]:
        """
        Devuelve los hashes que faltan en el target remoto.

        La consulta se divide en lotes para limitar el tamaño de cada mensaje gRPC.
        """
        missing: set[str] = set()
        if not chunk_hashes:
            return missing

        for batch in iter_batches(chunk_hashes, self._probe_batch_hashes):
            request = p2p_storage_pb2.ProbeMissingChunksRequest(chunk_hashes=batch)
            response = self._stub.ProbeMissingChunks(request, timeout=self._probe_timeout_s)
            missing.update(chunk_hash for chunk_hash in response.missing_hashes if chunk_hash)

        return missing

    def replicate_missing_hashes(self, chunk_hashes: Sequence[str], repo) -> dict[str, TargetAck]:
        """
        Replica hashes ausentes leyendo blobs comprimidos desde el CAS local.

        Devuelve ACKs por chunk. Si el stream falla, lanza StreamingReplicationError
        conservando los ACKs recibidos antes del fallo.
        """
        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}

        request_iter = QueueIterator(maxsize=self._stream_inflight)
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
                                node_id=self.node_id,
                                address=self.address,
                                status=p2p_storage_pb2.STORE_STATUS_ERROR,
                                detail=f"lectura local falló: {exc}",
                            )
                        continue

                    accepted = request_iter.put(
                        p2p_storage_pb2.ReplicateChunkRequest(
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
            name=f"replicate-send-{self.node_id[:8]}",
            daemon=True,
        )
        sender_thread.start()

        try:
            responses = self._stub.ReplicateChunks(request_iter, timeout=self._stream_timeout_s)
            for result in responses:
                if not result.chunk_hash:
                    continue

                acks[result.chunk_hash] = TargetAck(
                    chunk_hash=result.chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    status=result.status,
                    detail=result.detail or "",
                )

        except grpc.RpcError as exc:
            stream_error = exc
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
            if isinstance(stream_error, grpc.RpcError):
                detail = format_rpc_error(stream_error)
            else:
                detail = str(stream_error)

            raise StreamingReplicationError(
                f"replicación por stream falló: {detail}",
                acks=acks,
            ) from stream_error

        for chunk_hash in ordered_hashes:
            if chunk_hash not in acks:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    status=p2p_storage_pb2.STORE_STATUS_ERROR,
                    detail="el stream terminó sin ACK para el chunk",
                )

        return acks
