from __future__ import annotations

import os
import queue
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Sequence

import grpc

from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc


DEFAULT_PROBE_TIMEOUT_S = float(os.getenv("REPLICATION_PROBE_TIMEOUT_S", "10.0"))
DEFAULT_STREAM_TIMEOUT_S = float(os.getenv("REPLICATION_STREAM_TIMEOUT_S", "60.0"))
DEFAULT_TARGET_PARALLELISM = int(os.getenv("REPLICATION_TARGET_PARALLELISM", "4"))
DEFAULT_PROBE_BATCH_HASHES = int(os.getenv("REPLICATION_PROBE_BATCH_HASHES", "2048"))
DEFAULT_STREAM_INFLIGHT = int(os.getenv("REPLICATION_STREAM_INFLIGHT", "64"))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


@dataclass(frozen=True)
class TargetAck:
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


@dataclass(frozen=True)
class TargetExecutionResult:
    node_id: str
    address: str
    acks: Dict[str, TargetAck]
    transport_error: str | None = None


@dataclass(frozen=True)
class ChunkReplicationOutcome:
    chunk_hash: str
    success: bool
    required_remote_copies: int
    protected_remote_copies: int
    stored_remote_copies: int
    already_present_remote_copies: int
    error: str | None = None


@dataclass
class ChunkAccumulator:
    required_remote_copies: int
    stored_remote_copies: int = 0
    already_present_remote_copies: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def protected_remote_copies(self) -> int:
        return self.stored_remote_copies + self.already_present_remote_copies

    @property
    def success(self) -> bool:
        return self.protected_remote_copies >= self.required_remote_copies


class StreamingReplicationError(RuntimeError):
    """
    Error de stream que conserva ACKs recibidos antes del fallo.
    """

    def __init__(self, message: str, *, acks: Dict[str, TargetAck]):
        super().__init__(message)
        self.acks = dict(acks)


class StorageRpcPool:
    """
    Pool reutilizable de canales y stubs gRPC para replicación de chunks.
    """

    def __init__(self, *, max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES):
        self._lock = threading.Lock()
        self._channels: Dict[str, grpc.Channel] = {}
        self._stubs: Dict[str, p2p_storage_pb2_grpc.P2PStorageStub] = {}
        self._options = [
            ("grpc.max_send_message_length", int(max_message_bytes)),
            ("grpc.max_receive_message_length", int(max_message_bytes)),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ]

    def get_stub(self, address: str) -> p2p_storage_pb2_grpc.P2PStorageStub:
        with self._lock:
            stub = self._stubs.get(address)
            if stub is not None:
                return stub

            channel = grpc.insecure_channel(address, options=self._options)
            stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
            self._channels[address] = channel
            self._stubs[address] = stub
            return stub

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()


class _QueueIterator:
    """
    Iterador productor/consumidor para requests streaming.
    """

    def __init__(self, maxsize: int):
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, int(maxsize)))
        self._sentinel = object()
        self._closed = threading.Event()

    def put(self, item) -> bool:
        while not self._closed.is_set():
            try:
                self._queue.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def finish(self) -> None:
        if not self._closed.is_set():
            self._closed.set()
            self._put_sentinel()

    def cancel(self) -> None:
        self._closed.set()
        self._drain()
        self._put_sentinel()

    def _put_sentinel(self) -> None:
        while True:
            try:
                self._queue.put_nowait(self._sentinel)
                return
            except queue.Full:
                self._drain_one()

    def _drain(self) -> None:
        while self._drain_one():
            pass

    def _drain_one(self) -> bool:
        try:
            self._queue.get_nowait()
            return True
        except queue.Empty:
            return False

    def __iter__(self):
        return self

    def __next__(self):
        item = self._queue.get()
        if item is self._sentinel:
            raise StopIteration
        return item


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

    def replicate_missing_hashes(self, chunk_hashes: Sequence[str], repo) -> Dict[str, TargetAck]:
        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}
        return self._replicate_with_stream(ordered_hashes, repo)

    def _replicate_with_stream(self, chunk_hashes: Sequence[str], repo) -> Dict[str, TargetAck]:
        request_iter = _QueueIterator(maxsize=self._stream_inflight)
        acks: Dict[str, TargetAck] = {}
        local_failures: Dict[str, TargetAck] = {}
        stop_event = threading.Event()

        def merged_acks() -> Dict[str, TargetAck]:
            merged = dict(acks)
            merged.update(local_failures)
            return merged

        def sender() -> None:
            try:
                for chunk_hash in chunk_hashes:
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
                request_iter.finish()

        sender_thread = threading.Thread(
            target=sender,
            name=f"stream-send-{self.node_id[:8]}",
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
            stop_event.set()
            request_iter.cancel()
            raise StreamingReplicationError(
                f"stream replication failed: {format_rpc_error(exc)}",
                acks=merged_acks(),
            ) from exc

        except Exception as exc:
            stop_event.set()
            request_iter.cancel()
            raise StreamingReplicationError(
                f"stream replication failed: {exc}",
                acks=merged_acks(),
            ) from exc

        finally:
            stop_event.set()
            sender_thread.join(timeout=2.0)

        acks.update(local_failures)

        for chunk_hash in chunk_hashes:
            if chunk_hash not in acks:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    status=p2p_storage_pb2.STORE_STATUS_ERROR,
                    detail="Stream ended without per-chunk ack",
                )

        return acks


class StreamingReplicationCoordinator:
    """Coordina la protección RF usando probe por lotes y streaming de chunks."""

    def __init__(
        self,
        *,
        repo,
        cluster,
        rf: int,
        cluster_token: str,
        origin_node_id: str,
        probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
        stream_timeout_s: float = DEFAULT_STREAM_TIMEOUT_S,
        target_parallelism: int = DEFAULT_TARGET_PARALLELISM,
        probe_batch_hashes: int = DEFAULT_PROBE_BATCH_HASHES,
        stream_inflight: int = DEFAULT_STREAM_INFLIGHT,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ):
        origin_node_id = str(origin_node_id).strip()
        if not origin_node_id:
            raise ValueError("StreamingReplicationCoordinator requires a non-empty origin_node_id.")

        self.repo = repo
        self.cluster = cluster
        self.rf = max(1, int(rf))
        self.cluster_token = cluster_token
        self.origin_node_id = origin_node_id
        self.probe_timeout_s = float(probe_timeout_s)
        self.stream_timeout_s = float(stream_timeout_s)
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self.stream_inflight = max(1, int(stream_inflight))
        self._rpc_pool = StorageRpcPool(max_message_bytes=max_message_bytes)
        self._sessions: Dict[str, TargetStreamingSession] = {}
        self._sessions_lock = threading.Lock()

    def close(self) -> None:
        with self._sessions_lock:
            self._sessions.clear()
        self._rpc_pool.close()

    def _get_session(self, member) -> TargetStreamingSession:
        with self._sessions_lock:
            session = self._sessions.get(member.address)
            if session is not None:
                return session

            session = TargetStreamingSession(
                node_id=member.node_id,
                address=member.address,
                rpc_pool=self._rpc_pool,
                probe_timeout_s=self.probe_timeout_s,
                stream_timeout_s=self.stream_timeout_s,
                probe_batch_hashes=self.probe_batch_hashes,
                stream_inflight=self.stream_inflight,
            )
            self._sessions[member.address] = session
            return session

    def _plan(self, chunk_hashes: Sequence[str]):
        chunk_targets: Dict[str, List] = {}
        target_chunks: Dict[str, List[str]] = defaultdict(list)
        target_member: Dict[str, object] = {}

        excluded_node_ids = {self.origin_node_id}

        for chunk_hash in chunk_hashes:
            remote_targets = self.cluster.hrw_targets_excluding(
                chunk_hash,
                rf=self.rf,
                salt=self.cluster_token,
                excluded_node_ids=excluded_node_ids,
            )
            chunk_targets[chunk_hash] = remote_targets
            for member in remote_targets:
                target_chunks[member.address].append(chunk_hash)
                target_member[member.address] = member

        return chunk_targets, target_chunks, target_member

    def _execute_target_plan(self, member, chunk_hashes: Sequence[str]) -> TargetExecutionResult:
        session = self._get_session(member)
        ordered_hashes = list(dict.fromkeys(chunk_hashes))

        try:
            missing_hashes = session.probe_missing_hashes(ordered_hashes)
        except Exception as exc:
            return TargetExecutionResult(
                node_id=member.node_id,
                address=member.address,
                acks={},
                transport_error=f"probe failed: {exc}",
            )

        acks: Dict[str, TargetAck] = {}
        to_send = [chunk_hash for chunk_hash in ordered_hashes if chunk_hash in missing_hashes]

        for chunk_hash in ordered_hashes:
            if chunk_hash not in missing_hashes:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=member.node_id,
                    address=member.address,
                    status=p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
                    detail="already present on remote target",
                )

        if to_send:
            try:
                acks.update(session.replicate_missing_hashes(to_send, self.repo))
            except StreamingReplicationError as exc:
                partial_acks = dict(acks)
                partial_acks.update(exc.acks)
                return TargetExecutionResult(
                    node_id=member.node_id,
                    address=member.address,
                    acks=partial_acks,
                    transport_error=str(exc),
                )
            except Exception as exc:
                return TargetExecutionResult(
                    node_id=member.node_id,
                    address=member.address,
                    acks=acks,
                    transport_error=f"stream replication failed: {exc}",
                )

        return TargetExecutionResult(node_id=member.node_id, address=member.address, acks=acks)

    def replicate_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[ChunkReplicationOutcome]:
        ordered_hashes = list(chunk_hashes)
        chunk_targets, target_chunks, target_member = self._plan(ordered_hashes)

        accumulators: Dict[str, ChunkAccumulator] = {
            chunk_hash: ChunkAccumulator(required_remote_copies=len(chunk_targets[chunk_hash]))
            for chunk_hash in ordered_hashes
        }

        completed_without_remote = {
            chunk_hash for chunk_hash, targets in chunk_targets.items() if not targets
        }

        with ThreadPoolExecutor(max_workers=self.target_parallelism, thread_name_prefix="target-stream") as executor:
            future_map = {
                executor.submit(self._execute_target_plan, target_member[address], hashes): address
                for address, hashes in target_chunks.items()
            }

            for future in as_completed(future_map):
                address = future_map[future]
                try:
                    target_result = future.result()
                except Exception as exc:
                    member = target_member[address]
                    target_result = TargetExecutionResult(
                        node_id=member.node_id,
                        address=member.address,
                        acks={},
                        transport_error=f"target execution crashed: {exc}",
                    )

                planned_hashes = target_chunks[address]

                for chunk_hash in planned_hashes:
                    ack = target_result.acks.get(chunk_hash)
                    if ack is None:
                        if target_result.transport_error:
                            accumulators[chunk_hash].errors.append(
                                f"{target_result.node_id[:8]}@{target_result.address}: "
                                f"{target_result.transport_error}"
                            )
                        else:
                            accumulators[chunk_hash].errors.append(
                                f"{target_result.node_id[:8]}@{target_result.address}: missing ack"
                            )
                        continue

                    if ack.is_success:
                        if ack.is_already_present:
                            accumulators[chunk_hash].already_present_remote_copies += 1
                        elif ack.is_stored:
                            accumulators[chunk_hash].stored_remote_copies += 1
                    else:
                        accumulators[chunk_hash].errors.append(
                            f"{ack.node_id[:8]}@{ack.address}: {ack.detail}"
                        )

        for chunk_hash in ordered_hashes:
            accumulator = accumulators[chunk_hash]
            if chunk_hash in completed_without_remote:
                yield ChunkReplicationOutcome(
                    chunk_hash=chunk_hash,
                    success=True,
                    required_remote_copies=0,
                    protected_remote_copies=0,
                    stored_remote_copies=0,
                    already_present_remote_copies=0,
                    error=None,
                )
                continue

            yield ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                success=accumulator.success,
                required_remote_copies=accumulator.required_remote_copies,
                protected_remote_copies=accumulator.protected_remote_copies,
                stored_remote_copies=accumulator.stored_remote_copies,
                already_present_remote_copies=accumulator.already_present_remote_copies,
                error=("; ".join(accumulator.errors)[:2000] if accumulator.errors else None),
            )


def iter_hash_batches(chunk_hashes: Sequence[str], batch_size: int) -> Iterator[List[str]]:
    batch_size = max(1, int(batch_size))
    current: List[str] = []
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
