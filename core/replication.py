from __future__ import annotations

import os
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List

import grpc

from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc


DEFAULT_RPC_TIMEOUT_S = float(os.getenv("REPLICATION_RPC_TIMEOUT_S", "10.0"))
DEFAULT_CHUNK_WORKERS = int(os.getenv("REPLICATION_CHUNK_WORKERS", "8"))
DEFAULT_TARGET_WORKERS = int(os.getenv("REPLICATION_TARGET_WORKERS", "4"))
DEFAULT_TARGET_INFLIGHT = int(os.getenv("REPLICATION_TARGET_INFLIGHT", "32"))
DEFAULT_MAX_PENDING_CHUNKS = int(os.getenv("REPLICATION_MAX_PENDING_CHUNKS", "128"))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


@dataclass(frozen=True)
class StoreAck:
    node_id: str
    address: str
    success: bool
    message: str


@dataclass(frozen=True)
class ChunkReplicationOutcome:
    chunk_hash: str
    success: bool
    protected_remote_copies: int
    error: str | None = None


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
    Sesión de replicación hacia un único nodo destino.
    """

    def __init__(
        self,
        *,
        node_id: str,
        address: str,
        rpc_pool: StorageRpcPool,
        rpc_timeout_s: float,
        max_inflight: int,
        max_workers: int,
    ):
        self.node_id = node_id
        self.address = address
        self._rpc_pool = rpc_pool
        self._rpc_timeout_s = float(rpc_timeout_s)
        self._semaphore = threading.BoundedSemaphore(max(1, int(max_inflight)))
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix=f"rep-{node_id[:8]}",
        )

    def submit_store(self, *, chunk_hash: str, chunk_data: bytes) -> Future:
        self._semaphore.acquire()
        fut = self._executor.submit(self._store_one, chunk_hash, chunk_data)
        fut.add_done_callback(lambda _f: self._semaphore.release())
        return fut

    def _store_one(self, chunk_hash: str, chunk_data: bytes) -> StoreAck:
        stub = self._rpc_pool.get_stub(self.address)
        req = p2p_storage_pb2.StoreRequest(chunk_hash=chunk_hash, chunk_data=chunk_data)

        try:
            resp = stub.StoreChunk(req, timeout=self._rpc_timeout_s)
            return StoreAck(
                node_id=self.node_id,
                address=self.address,
                success=bool(resp.success),
                message=resp.message or "",
            )
        except grpc.RpcError as e:
            return StoreAck(
                node_id=self.node_id,
                address=self.address,
                success=False,
                message=f"RPC {e.code().name}: {e.details()}",
            )

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)


class ReplicationCoordinator:
    """
    Coordina la protección RF de chunks para una época de placement concreta.
    """

    def __init__(
        self,
        *,
        repo,
        cluster,
        rf: int,
        cluster_token: str,
        rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
        chunk_workers: int = DEFAULT_CHUNK_WORKERS,
        target_workers: int = DEFAULT_TARGET_WORKERS,
        target_inflight: int = DEFAULT_TARGET_INFLIGHT,
        max_pending_chunks: int = DEFAULT_MAX_PENDING_CHUNKS,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ):
        self.repo = repo
        self.cluster = cluster
        self.rf = max(1, int(rf))
        self.cluster_token = cluster_token
        self.rpc_timeout_s = float(rpc_timeout_s)
        self.chunk_workers = max(1, int(chunk_workers))
        self.target_workers = max(1, int(target_workers))
        self.target_inflight = max(1, int(target_inflight))
        self.max_pending_chunks = max(1, int(max_pending_chunks))

        self._rpc_pool = StorageRpcPool(max_message_bytes=max_message_bytes)
        self._sessions: Dict[str, TargetReplicationSession] = {}
        self._sessions_lock = threading.Lock()

    def close(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.close()
        self._rpc_pool.close()

    def _get_session(self, member) -> TargetReplicationSession:
        with self._sessions_lock:
            sess = self._sessions.get(member.address)
            if sess is not None:
                return sess
            sess = TargetReplicationSession(
                node_id=member.node_id,
                address=member.address,
                rpc_pool=self._rpc_pool,
                rpc_timeout_s=self.rpc_timeout_s,
                max_inflight=self.target_inflight,
                max_workers=self.target_workers,
            )
            self._sessions[member.address] = sess
            return sess

    def _replicate_one(self, chunk_hash: str) -> ChunkReplicationOutcome:
        remote_targets = self.cluster.hrw_remote_targets(chunk_hash, rf=self.rf, salt=self.cluster_token)

        if not remote_targets:
            return ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                success=True,
                protected_remote_copies=0,
                error=None,
            )

        try:
            chunk_data = self.repo.get_compressed(chunk_hash)
        except Exception as e:
            return ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                success=False,
                protected_remote_copies=0,
                error=f"Could not read local chunk: {e}",
            )

        futures: List[Future] = []
        for member in remote_targets:
            session = self._get_session(member)
            futures.append(session.submit_store(chunk_hash=chunk_hash, chunk_data=chunk_data))

        ok_count = 0
        failures: List[str] = []
        for fut in futures:
            ack: StoreAck = fut.result()
            if ack.success:
                ok_count += 1
            else:
                failures.append(f"{ack.node_id[:8]}@{ack.address}: {ack.message}")

        if ok_count == len(remote_targets):
            return ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                success=True,
                protected_remote_copies=ok_count,
                error=None,
            )

        return ChunkReplicationOutcome(
            chunk_hash=chunk_hash,
            success=False,
            protected_remote_copies=ok_count,
            error="; ".join(failures)[:2000],
        )

    def replicate_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[ChunkReplicationOutcome]:
        executor = ThreadPoolExecutor(
            max_workers=self.chunk_workers,
            thread_name_prefix="chunk-repl",
        )
        pending: Dict[Future, str] = {}
        it = iter(chunk_hashes)

        def fill() -> None:
            while len(pending) < self.max_pending_chunks:
                try:
                    chunk_hash = next(it)
                except StopIteration:
                    break
                fut = executor.submit(self._replicate_one, chunk_hash)
                pending[fut] = chunk_hash

        try:
            fill()
            while pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                for fut in done:
                    pending.pop(fut, None)
                    yield fut.result()
                fill()
        finally:
            executor.shutdown(wait=True, cancel_futures=False)
