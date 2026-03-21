from __future__ import annotations

import os
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Sequence

import grpc

from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc


DEFAULT_RPC_TIMEOUT_S = float(os.getenv("REPLICATION_RPC_TIMEOUT_S", "10.0"))
DEFAULT_TARGET_PARALLELISM = int(os.getenv("REPLICATION_TARGET_PARALLELISM", "4"))
DEFAULT_PROBE_BATCH_HASHES = int(os.getenv("REPLICATION_PROBE_BATCH_HASHES", "2048"))
DEFAULT_STORE_BATCH_ITEMS = int(os.getenv("REPLICATION_STORE_BATCH_ITEMS", "32"))
DEFAULT_STORE_BATCH_BYTES = int(os.getenv("REPLICATION_STORE_BATCH_BYTES", str(4 * 1024 * 1024)))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


@dataclass(frozen=True)
class TargetAck:
    chunk_hash: str
    node_id: str
    address: str
    success: bool
    already_present: bool
    message: str


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


class TargetReplicationSession:
    """
    Sesión persistente hacia un nodo destino.
    """

    def __init__(
        self,
        *,
        node_id: str,
        address: str,
        rpc_pool: StorageRpcPool,
        rpc_timeout_s: float,
        probe_batch_hashes: int,
        store_batch_items: int,
        store_batch_bytes: int,
    ):
        self.node_id = node_id
        self.address = address
        self._rpc_pool = rpc_pool
        self._rpc_timeout_s = float(rpc_timeout_s)
        self._probe_batch_hashes = max(1, int(probe_batch_hashes))
        self._store_batch_items = max(1, int(store_batch_items))
        self._store_batch_bytes = max(1, int(store_batch_bytes))

    @property
    def _stub(self) -> p2p_storage_pb2_grpc.P2PStorageStub:
        return self._rpc_pool.get_stub(self.address)

    def probe_missing_hashes(self, chunk_hashes: Sequence[str]) -> set[str]:
        """Pregunta al nodo remoto qué hashes no tiene todavía."""
        missing: set[str] = set()
        if not chunk_hashes:
            return missing

        for batch in _iter_hash_batches(chunk_hashes, self._probe_batch_hashes):
            request = p2p_storage_pb2.MissingChunksRequest(chunk_hashes=batch)
            try:
                response = self._stub.ProbeMissingChunks(request, timeout=self._rpc_timeout_s)
                missing.update(chunk_hash for chunk_hash in response.missing_hashes if chunk_hash)
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
                    # Permite una transición gradual si algún nodo todavía no soporta ProbeMissingChunks.
                    missing.update(batch)
                    continue
                raise

        return missing

    def store_missing_chunks(self, items: Sequence[tuple[str, bytes]]) -> Dict[str, TargetAck]:
        """Envía al nodo remoto los chunks que faltan, agrupados en lotes."""
        results: Dict[str, TargetAck] = {}
        if not items:
            return results

        for batch in _iter_store_batches(items, self._store_batch_items, self._store_batch_bytes):
            try:
                response = self._stub.StoreChunkBatch(
                    p2p_storage_pb2.StoreChunkBatchRequest(
                        items=[
                            p2p_storage_pb2.BatchStoreItem(
                                chunk_hash=chunk_hash,
                                chunk_data=chunk_data,
                            )
                            for chunk_hash, chunk_data in batch
                        ]
                    ),
                    timeout=self._rpc_timeout_s,
                )
                results.update(
                    _normalize_batch_results(
                        node_id=self.node_id,
                        address=self.address,
                        batch=batch,
                        response=response,
                    )
                )
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
                    results.update(self._store_with_unary_fallback(batch))
                    continue
                raise

        return results

    def _store_with_unary_fallback(self, batch: Sequence[tuple[str, bytes]]) -> Dict[str, TargetAck]:
        """Fallback para nodos que todavía solo implementan StoreChunk unary."""
        out: Dict[str, TargetAck] = {}
        for chunk_hash, chunk_data in batch:
            request = p2p_storage_pb2.StoreRequest(chunk_hash=chunk_hash, chunk_data=chunk_data)
            try:
                response = self._stub.StoreChunk(request, timeout=self._rpc_timeout_s)
                message = response.message or ""
                out[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    success=bool(response.success),
                    already_present=("already present" in message),
                    message=message,
                )
            except grpc.RpcError as exc:
                out[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=self.node_id,
                    address=self.address,
                    success=False,
                    already_present=False,
                    message=f"RPC {exc.code().name}: {exc.details()}",
                )

        return out


class BatchReplicationCoordinator:
    """
    Coordina la replicación por RF usando preflight y escritura por lotes.
    """

    def __init__(
        self,
        *,
        repo,
        cluster,
        rf: int,
        cluster_token: str,
        rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
        target_parallelism: int = DEFAULT_TARGET_PARALLELISM,
        probe_batch_hashes: int = DEFAULT_PROBE_BATCH_HASHES,
        store_batch_items: int = DEFAULT_STORE_BATCH_ITEMS,
        store_batch_bytes: int = DEFAULT_STORE_BATCH_BYTES,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ):
        self.repo = repo
        self.cluster = cluster
        self.rf = max(1, int(rf))
        self.cluster_token = cluster_token
        self.rpc_timeout_s = float(rpc_timeout_s)
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self.store_batch_items = max(1, int(store_batch_items))
        self.store_batch_bytes = max(1, int(store_batch_bytes))

        self._rpc_pool = StorageRpcPool(max_message_bytes=max_message_bytes)
        self._sessions: Dict[str, TargetReplicationSession] = {}
        self._sessions_lock = threading.Lock()

    def close(self) -> None:
        with self._sessions_lock:
            self._sessions.clear()
        self._rpc_pool.close()

    def _get_session(self, member) -> TargetReplicationSession:
        with self._sessions_lock:
            session = self._sessions.get(member.address)
            if session is not None:
                return session

            session = TargetReplicationSession(
                node_id=member.node_id,
                address=member.address,
                rpc_pool=self._rpc_pool,
                rpc_timeout_s=self.rpc_timeout_s,
                probe_batch_hashes=self.probe_batch_hashes,
                store_batch_items=self.store_batch_items,
                store_batch_bytes=self.store_batch_bytes,
            )
            self._sessions[member.address] = session
            return session

    def _plan(self, chunk_hashes: Sequence[str]):
        chunk_targets: Dict[str, List] = {}
        target_chunks: Dict[str, List[str]] = defaultdict(list)
        target_member: Dict[str, object] = {}

        for chunk_hash in chunk_hashes:
            remote_targets = self.cluster.hrw_remote_targets(
                chunk_hash,
                rf=self.rf,
                salt=self.cluster_token,
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
        items_to_send: List[tuple[str, bytes]] = []

        for chunk_hash in ordered_hashes:
            if chunk_hash not in missing_hashes:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=member.node_id,
                    address=member.address,
                    success=True,
                    already_present=True,
                    message="already present on remote target",
                )
                continue

            try:
                chunk_data = self.repo.get_compressed(chunk_hash)
            except Exception as exc:
                acks[chunk_hash] = TargetAck(
                    chunk_hash=chunk_hash,
                    node_id=member.node_id,
                    address=member.address,
                    success=False,
                    already_present=False,
                    message=f"local read failed: {exc}",
                )
                continue

            items_to_send.append((chunk_hash, chunk_data))

        if items_to_send:
            try:
                acks.update(session.store_missing_chunks(items_to_send))
            except Exception as exc:
                return TargetExecutionResult(
                    node_id=member.node_id,
                    address=member.address,
                    acks=acks,
                    transport_error=f"store batch failed: {exc}",
                )

        return TargetExecutionResult(
            node_id=member.node_id,
            address=member.address,
            acks=acks,
        )

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

        with ThreadPoolExecutor(max_workers=self.target_parallelism, thread_name_prefix="target-batch") as executor:
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

                if target_result.transport_error:
                    message = f"{target_result.node_id[:8]}@{target_result.address}: {target_result.transport_error}"
                    for chunk_hash in planned_hashes:
                        accumulators[chunk_hash].errors.append(message)
                    continue

                for chunk_hash in planned_hashes:
                    ack = target_result.acks.get(chunk_hash)
                    if ack is None:
                        accumulators[chunk_hash].errors.append(
                            f"{target_result.node_id[:8]}@{target_result.address}: missing ack"
                        )
                        continue

                    if ack.success:
                        if ack.already_present:
                            accumulators[chunk_hash].already_present_remote_copies += 1
                        else:
                            accumulators[chunk_hash].stored_remote_copies += 1
                    else:
                        accumulators[chunk_hash].errors.append(
                            f"{ack.node_id[:8]}@{ack.address}: {ack.message}"
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


def _iter_hash_batches(chunk_hashes: Sequence[str], batch_size: int) -> Iterator[List[str]]:
    batch_size = max(1, int(batch_size))
    current: List[str] = []
    for chunk_hash in chunk_hashes:
        current.append(chunk_hash)
        if len(current) >= batch_size:
            yield current
            current = []
    if current:
        yield current


def _iter_store_batches(
    items: Sequence[tuple[str, bytes]],
    max_items: int,
    max_bytes: int,
) -> Iterator[List[tuple[str, bytes]]]:
    max_items = max(1, int(max_items))
    max_bytes = max(1, int(max_bytes))

    batch: List[tuple[str, bytes]] = []
    batch_bytes = 0

    for chunk_hash, chunk_data in items:
        chunk_bytes = len(chunk_data)

        if batch and (len(batch) >= max_items or (batch_bytes + chunk_bytes) > max_bytes):
            yield batch
            batch = []
            batch_bytes = 0

        batch.append((chunk_hash, chunk_data))
        batch_bytes += chunk_bytes

        if chunk_bytes >= max_bytes:
            yield batch
            batch = []
            batch_bytes = 0

    if batch:
        yield batch


def _normalize_batch_results(
    *,
    node_id: str,
    address: str,
    batch: Sequence[tuple[str, bytes]],
    response,
) -> Dict[str, TargetAck]:
    out: Dict[str, TargetAck] = {}
    expected = {chunk_hash for chunk_hash, _ in batch}

    for result in response.results:
        chunk_hash = result.chunk_hash
        if chunk_hash not in expected:
            continue
        out[chunk_hash] = TargetAck(
            chunk_hash=chunk_hash,
            node_id=node_id,
            address=address,
            success=bool(result.success),
            already_present=bool(result.already_present),
            message=result.message or "",
        )

    for chunk_hash, _ in batch:
        if chunk_hash not in out:
            out[chunk_hash] = TargetAck(
                chunk_hash=chunk_hash,
                node_id=node_id,
                address=address,
                success=False,
                already_present=False,
                message="batch response omitted chunk result",
            )

    return out
