"""
Ejecución del push de replicación clásica.

Este módulo contiene la concurrencia por target, la agregación de ACKs y los
outcomes por chunk. La orquestación de CLI/metadata vive en pusher.py y la red
gRPC vive en remote_client.py.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field

from stopan.common.sequences import ordered_unique
from stopan.protection.concurrency import iter_completed_keyed_tasks
from stopan.protos import p2p_storage_pb2

from .placement import plan_chunk_replication_targets
from .remote_client import RemoteChunkClientPool, StreamingReplicationError, TargetAck


@dataclass(frozen=True, slots=True)
class TargetExecutionResult:
    """Resultado de ejecutar probe/stream contra un target remoto."""

    node_id: str
    address: str
    acks: dict[str, TargetAck]
    transport_error: str | None = None


@dataclass(frozen=True, slots=True)
class ChunkReplicationOutcome:
    """Resultado agregado de replicar un chunk en sus targets remotos."""

    chunk_hash: str
    required_remote_copies: int
    protected_remote_copies: int
    stored_remote_copies: int
    already_present_remote_copies: int
    error: str | None = None


@dataclass(slots=True)
class ChunkAccumulator:
    required_remote_copies: int
    stored_remote_copies: int = 0
    already_present_remote_copies: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def protected_remote_copies(self) -> int:
        return self.stored_remote_copies + self.already_present_remote_copies


def push_chunk_replicas(
    *,
    repo,
    cluster,
    chunk_hashes: Iterable[str],
    required_remote_copies: int,
    cluster_token: str,
    origin_node_id: str,
    remote_client: RemoteChunkClientPool,
    target_parallelism: int,
) -> Iterator[ChunkReplicationOutcome]:
    """
    Replica chunks en sus targets remotos y produce un outcome por chunk.

    Los outcomes conservan resultados parciales: un chunk puede quedar DEGRADED
    si algunos targets confirman almacenamiento y otros fallan.
    """

    ordered_hashes = ordered_unique(chunk_hashes)
    plan = plan_chunk_replication_targets(
        cluster=cluster,
        chunk_hashes=ordered_hashes,
        required_remote_copies=required_remote_copies,
        cluster_token=cluster_token,
        origin_node_id=origin_node_id,
    )

    accumulators: dict[str, ChunkAccumulator] = {
        chunk_hash: ChunkAccumulator(required_remote_copies=len(plan.chunk_targets[chunk_hash]))
        for chunk_hash in ordered_hashes
    }

    chunks_without_remote_targets = {
        chunk_hash
        for chunk_hash, targets in plan.chunk_targets.items()
        if not targets
    }

    if plan.target_chunks:
        tasks = {
            address: (
                lambda address=address, hashes=hashes: _execute_target_plan(
                    remote_client=remote_client,
                    repo=repo,
                    member=plan.target_members[address],
                    chunk_hashes=hashes,
                )
            )
            for address, hashes in plan.target_chunks.items()
        }

        for completed in iter_completed_keyed_tasks(
            tasks=tasks,
            max_workers=target_parallelism,
            thread_name_prefix="target-stream",
        ):
            address = completed.key
            if completed.error is not None:
                member = plan.target_members[address]
                target_result = TargetExecutionResult(
                    node_id=member.node_id,
                    address=member.address,
                    acks={},
                    transport_error=f"ejecución del target falló: {completed.error}",
                )
            else:
                target_result = completed.result

            _apply_target_result(
                accumulators=accumulators,
                planned_hashes=plan.target_chunks[address],
                target_result=target_result,
            )

    for chunk_hash in ordered_hashes:
        accumulator = accumulators[chunk_hash]

        if chunk_hash in chunks_without_remote_targets:
            yield ChunkReplicationOutcome(
                chunk_hash=chunk_hash,
                required_remote_copies=required_remote_copies,
                protected_remote_copies=0,
                stored_remote_copies=0,
                already_present_remote_copies=0,
                error="no se planificaron targets remotos para el chunk",
            )
            continue

        yield ChunkReplicationOutcome(
            chunk_hash=chunk_hash,
            required_remote_copies=accumulator.required_remote_copies,
            protected_remote_copies=accumulator.protected_remote_copies,
            stored_remote_copies=accumulator.stored_remote_copies,
            already_present_remote_copies=accumulator.already_present_remote_copies,
            error="; ".join(accumulator.errors) if accumulator.errors else None,
        )


def _execute_target_plan(
    *,
    remote_client: RemoteChunkClientPool,
    repo,
    member,
    chunk_hashes: Sequence[str],
) -> TargetExecutionResult:
    ordered_hashes = ordered_unique(chunk_hashes)

    try:
        missing_hashes = remote_client.probe_missing_hashes(
            address=member.address,
            chunk_hashes=ordered_hashes,
        )
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
            acks.update(
                remote_client.replicate_missing_hashes(
                    node_id=member.node_id,
                    address=member.address,
                    chunk_hashes=to_send,
                    repo=repo,
                )
            )
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


def _apply_target_result(
    *,
    accumulators: dict[str, ChunkAccumulator],
    planned_hashes: Sequence[str],
    target_result: TargetExecutionResult,
) -> None:
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
