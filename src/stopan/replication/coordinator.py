"""
Coordinación de replicación remota de chunks.

El coordinator planifica targets remotos mediante HRW, consulta qué chunks faltan
en cada nodo con ProbeMissingChunks y envía únicamente los chunks ausentes usando
streaming gRPC.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

from stopan.config.defaults import (
    DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    DEFAULT_REPLICATION_PROBE_BATCH_HASHES,
    DEFAULT_REPLICATION_PROBE_TIMEOUT_S,
    DEFAULT_REPLICATION_STREAM_INFLIGHT,
    DEFAULT_REPLICATION_STREAM_TIMEOUT_S,
    DEFAULT_REPLICATION_TARGET_PARALLELISM,
)
from stopan.protos import p2p_storage_pb2
from stopan.replication.outcomes import (
    ChunkAccumulator,
    ChunkReplicationOutcome,
    StreamingReplicationError,
    TargetAck,
    TargetExecutionResult,
)
from stopan.replication.streaming import TargetStreamingSession
from stopan.rpc.p2p_storage_pool import P2PStorageStubPool


class StreamingReplicationCoordinator:
    """
    Coordina la protección RF remota usando probes por lote y streaming de chunks.

    RF representa copias remotas requeridas. El origin_node_id se excluye del
    placement porque la copia local del CAS no cuenta como réplica P2P.
    """

    def __init__(
        self,
        *,
        repo,
        cluster,
        rf: int,
        cluster_token: str,
        origin_node_id: str,
        probe_timeout_s: float = DEFAULT_REPLICATION_PROBE_TIMEOUT_S,
        stream_timeout_s: float = DEFAULT_REPLICATION_STREAM_TIMEOUT_S,
        target_parallelism: int = DEFAULT_REPLICATION_TARGET_PARALLELISM,
        probe_batch_hashes: int = DEFAULT_REPLICATION_PROBE_BATCH_HASHES,
        stream_inflight: int = DEFAULT_REPLICATION_STREAM_INFLIGHT,
        max_message_bytes: int = DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    ):
        origin_node_id = str(origin_node_id).strip()
        if not origin_node_id:
            raise ValueError("StreamingReplicationCoordinator requiere origin_node_id no vacío.")

        self.repo = repo
        self.cluster = cluster
        self.rf = int(rf)
        if self.rf < 1:
            raise ValueError("StreamingReplicationCoordinator requiere rf >= 1.")
        self.cluster_token = str(cluster_token or "")
        self.origin_node_id = origin_node_id
        self.probe_timeout_s = float(probe_timeout_s)
        self.stream_timeout_s = float(stream_timeout_s)
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self.stream_inflight = max(1, int(stream_inflight))

        self._rpc_pool = P2PStorageStubPool(max_message_bytes=max_message_bytes)
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
        """
        Calcula targets remotos por chunk y agrupa chunks por target.

        Cada chunk se asigna mediante HRW excluyendo origin_node_id. La agrupación
        inversa permite consultar y enviar lotes por nodo remoto.
        """

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
        """
        Ejecuta el plan de un target remoto.

        Primero consulta qué chunks faltan en el nodo. Después envía solo los chunks
        ausentes y conserva ACKs parciales si falla el stream.
        """

        session = self._get_session(member)
        ordered_hashes = list(dict.fromkeys(chunk_hashes))

        try:
            missing_hashes = session.probe_missing_hashes(ordered_hashes)
        except Exception as exc:
            return TargetExecutionResult(
                node_id=member.node_id,
                address=member.address,
                acks={},
                transport_error=f"probe falló: {exc}",
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
                    detail="ya presente en target remoto",
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
                    transport_error=f"replicación por stream falló: {exc}",
                )

        return TargetExecutionResult(
            node_id=member.node_id,
            address=member.address,
            acks=acks,
            transport_error=None,
        )

    def replicate_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[ChunkReplicationOutcome]:
        """
        Replica chunks en sus targets remotos y produce un outcome por chunk.

        Los outcomes conservan resultados parciales: un chunk puede quedar DEGRADED
        si algunos targets confirman almacenamiento y otros fallan.
        """

        ordered_hashes = list(dict.fromkeys(chunk_hashes))
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

        executor = ThreadPoolExecutor(
            max_workers=self.target_parallelism,
            thread_name_prefix="target-stream",
        )
        future_map = {}

        try:
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
                        transport_error=f"ejecución del target falló: {exc}",
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
                                f"{target_result.node_id[:8]}@{target_result.address}: falta ACK"
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

        except BaseException:
            for future in future_map:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True, cancel_futures=False)

        for chunk_hash in ordered_hashes:
            accumulator = accumulators[chunk_hash]

            if chunk_hash in chunks_without_remote_targets:
                yield ChunkReplicationOutcome(
                    chunk_hash=chunk_hash,
                    success=False,
                    required_remote_copies=self.rf,
                    protected_remote_copies=0,
                    stored_remote_copies=0,
                    already_present_remote_copies=0,
                    error="no se planificaron targets remotos para el chunk",
                )
                continue

            yield ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                success=accumulator.success,
                required_remote_copies=accumulator.required_remote_copies,
                protected_remote_copies=accumulator.protected_remote_copies,
                stored_remote_copies=accumulator.stored_remote_copies,
                already_present_remote_copies=accumulator.already_present_remote_copies,
                error="; ".join(accumulator.errors) if accumulator.errors else None,
            )
