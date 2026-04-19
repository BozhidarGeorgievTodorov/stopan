from __future__ import annotations

import os
import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

from stopan.protos import p2p_storage_pb2
from stopan.replication.outcomes import (
    ChunkAccumulator,
    ChunkReplicationOutcome,
    StreamingReplicationError,
    TargetAck,
    TargetExecutionResult,
)
from stopan.replication.rpc_pool import StorageRpcPool
from stopan.replication.streaming import TargetStreamingSession


DEFAULT_PROBE_TIMEOUT_S = float(os.getenv("REPLICATION_PROBE_TIMEOUT_S", "10.0"))
DEFAULT_STREAM_TIMEOUT_S = float(os.getenv("REPLICATION_STREAM_TIMEOUT_S", "60.0"))
DEFAULT_TARGET_PARALLELISM = int(os.getenv("REPLICATION_TARGET_PARALLELISM", "4"))
DEFAULT_PROBE_BATCH_HASHES = int(os.getenv("REPLICATION_PROBE_BATCH_HASHES", "2048"))
DEFAULT_STREAM_INFLIGHT = int(os.getenv("REPLICATION_STREAM_INFLIGHT", "64"))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


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
        self._sessions: dict[str, TargetStreamingSession] = {}
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
        chunk_targets: dict[str, list] = {}
        target_chunks: dict[str, list[str]] = defaultdict(list)
        target_members: dict[str, object] = {}
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
                target_members[member.address] = member

        return chunk_targets, target_chunks, target_members

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

        acks: dict[str, TargetAck] = {}
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

        return TargetExecutionResult(
            node_id=member.node_id,
            address=member.address,
            acks=acks,
            transport_error=None,
        )

    def replicate_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[ChunkReplicationOutcome]:
        ordered_hashes = list(chunk_hashes)
        chunk_targets, target_chunks, target_members = self._plan(ordered_hashes)

        accumulators: dict[str, ChunkAccumulator] = {
            chunk_hash: ChunkAccumulator(required_remote_copies=len(chunk_targets[chunk_hash]))
            for chunk_hash in ordered_hashes
        }

        chunks_without_remote_targets = {
            chunk_hash
            for chunk_hash, targets in chunk_targets.items()
            if not targets
        }

        with ThreadPoolExecutor(
            max_workers=self.target_parallelism,
            thread_name_prefix="target-stream",
        ) as executor:
            future_map = {
                executor.submit(self._execute_target_plan, target_members[address], hashes): address
                for address, hashes in target_chunks.items()
            }

            for future in as_completed(future_map):
                address = future_map[future]

                try:
                    target_result = future.result()
                except Exception as exc:
                    member = target_members[address]
                    target_result = TargetExecutionResult(
                        node_id=member.node_id,
                        address=member.address,
                        acks={},
                        transport_error=f"target execution crashed: {exc}",
                    )

                planned_hashes = target_chunks[address]
                transport_message = None
                if target_result.transport_error:
                    transport_message = (
                        f"{target_result.node_id[:8]}@{target_result.address}: "
                        f"{target_result.transport_error}"
                    )

                for chunk_hash in planned_hashes:
                    ack = target_result.acks.get(chunk_hash)

                    if ack is None:
                        if transport_message is not None:
                            accumulators[chunk_hash].errors.append(transport_message)
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

            if chunk_hash in chunks_without_remote_targets:
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
