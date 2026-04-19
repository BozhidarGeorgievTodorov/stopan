from __future__ import annotations

import threading
from collections.abc import Iterator, Sequence

import grpc

from stopan.protos import p2p_storage_pb2
from stopan.protos import p2p_storage_pb2_grpc
from stopan.replication.outcomes import StreamingReplicationError, TargetAck
from stopan.replication.queue_iterator import QueueIterator
from stopan.replication.rpc_pool import StorageRpcPool


class TargetStreamingSession:
    """
    Sesión persistente hacia un target remoto.

    Estrategia:
      - ProbeMissingChunks para descubrir hashes ausentes.
      - ReplicateChunks streaming para enviar solo los blobs faltantes.
    """

    def __init__(
        self,
        *,
        node_id: str,
        address: str,
        rpc_pool: StorageRpcPool,
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
    def _stub(self) -> p2p_storage_pb2_grpc.P2PStorageStub:
        return self._rpc_pool.get_stub(self.address)

    def probe_missing_hashes(self, chunk_hashes: Sequence[str]) -> set[str]:
        missing: set[str] = set()
        if not chunk_hashes:
            return missing

        for batch in iter_hash_batches(chunk_hashes, self._probe_batch_hashes):
            request = p2p_storage_pb2.ProbeMissingChunksRequest(chunk_hashes=batch)
            response = self._stub.ProbeMissingChunks(
                request,
                timeout=self._probe_timeout_s,
            )
            missing.update(chunk_hash for chunk_hash in response.missing_hashes if chunk_hash)

        return missing

    def replicate_missing_hashes(self, chunk_hashes: Sequence[str], repo) -> dict[str, TargetAck]:
        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}

        request_iter = QueueIterator(maxsize=self._stream_inflight)
        acks: dict[str, TargetAck] = {}
        local_failures: dict[str, TargetAck] = {}
        stop_event = threading.Event()
        stream_error: Exception | None = None

        def sender() -> None:
            try:
                for chunk_hash in ordered_hashes:
                    if stop_event.is_set():
                        break

                    try:
                        chunk_data = repo.get_compressed(chunk_hash)
                    except Exception as exc:
                        local_failures[chunk_hash] = TargetAck(
                            chunk_hash=chunk_hash,
                            node_id=self.node_id,
                            address=self.address,
                            status=p2p_storage_pb2.STORE_STATUS_ERROR,
                            detail=f"Local read failed: {exc}",
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
            responses = self._stub.ReplicateChunks(
                request_iter,
                timeout=self._stream_timeout_s,
            )
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
            stop_event.set()
            request_iter.cancel()

        except Exception as exc:
            stream_error = exc
            stop_event.set()
            request_iter.cancel()

        finally:
            stop_event.set()
            if stream_error is not None:
                request_iter.cancel()
            sender_thread.join(timeout=2.0)

        acks.update(local_failures)

        if stream_error is not None:
            if isinstance(stream_error, grpc.RpcError):
                detail = format_rpc_error(stream_error)
            else:
                detail = str(stream_error)

            raise StreamingReplicationError(
                f"stream replication failed: {detail}",
                acks=acks,
            ) from stream_error

        for chunk_hash in ordered_hashes:
            if chunk_hash not in acks:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    status=p2p_storage_pb2.STORE_STATUS_ERROR,
                    detail="Stream ended without per-chunk ack",
                )

        return acks


def iter_hash_batches(chunk_hashes: Sequence[str], batch_size: int) -> Iterator[list[str]]:
    batch_size = max(1, int(batch_size))
    current: list[str] = []

    for chunk_hash in chunk_hashes:
        current.append(chunk_hash)
        if len(current) >= batch_size:
            yield current
            current = []

    if current:
        yield current


def format_rpc_error(exc: grpc.RpcError) -> str:
    code = "UNKNOWN"
    details = str(exc)

    try:
        code = exc.code().name
    except Exception:
        pass

    try:
        details = exc.details() or details
    except Exception:
        pass

    return f"{code}: {details}"
