"""
Push de chunks pendientes hacia nodos remotos.

Este módulo coordina la protección P2P de chunks completos: resuelve membership,
calcula placement remoto excluyendo el nodo origen, replica chunks mediante
streaming y actualiza chunk_protection con el resultado.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.cas.repository import CASRepository
from stopan.errors import StopanConfigValueError
from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_after_successful_metadata_change,
)
from stopan.protection.policy import ProtectionState
from stopan.protection.remote_context import (
    DEFAULT_REMOTE_PROTECTION_MISSING_SEED_MESSAGE,
    resolve_remote_protection_context,
)
from stopan.protection.scope import (
    DEFAULT_PROTECTION_SCOPE,
    describe_protection_scope,
    scoped_replication_push_chunks,
)

from .push_execution import push_chunk_replicas
from .remote_client import RemoteChunkClientPool
from .states import replication_push_state


PUSH_DETAIL_LIMIT = 20


@dataclass(frozen=True)
class PushStats:
    """Contadores agregados de una ejecución de push."""

    attempted: int = 0
    protected: int = 0
    degraded: int = 0
    failed: int = 0
    stored_remote: int = 0
    already_present_remote: int = 0
    processed_bytes: int = 0
    insufficient_remote_targets: bool = False
    desired_rf: int = 0
    remote_candidates: int = 0
    interrupted: bool = False


def push_to_network(
    *,
    membership_seed: str | None,
    rf: int,
    limit: int | None,
    target_parallelism: int,
    probe_batch_hashes: int,
    stream_inflight: int,
    probe_timeout_s: float,
    stream_timeout_s: float,
    max_message_bytes: int,
    commit_every: int,
    strict_rf: bool,
    db_file: str,
    local_shard_dir: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
    scope: str | None = None,
    snapshot_id: int | None = None,
) -> PushStats:
    """
    Replica chunks pendientes en nodos remotos según las copias requeridas.

    En Stopan, rf representa copias remotas requeridas. RF=0 es un no-op
    remoto; el nodo origen se excluye del placement y la copia local del CAS no
    cuenta como réplica P2P.
    """
    desired_rf = int(rf)
    if desired_rf < 1:
        raise StopanConfigValueError("push requiere rf >= 1.")

    required_remote_copies = desired_rf
    commit_every = max(1, int(commit_every))
    self_addr = str(self_addr or "").strip()
    cluster_token = str(cluster_token or "")

    repo = CASRepository(local_shard_dir)
    db = MetadataDB(db_file)
    remote_client: RemoteChunkClientPool | None = None
    metadata_changed = False
    processed_bytes = 0

    try:
        remote_context = resolve_remote_protection_context(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message=DEFAULT_REMOTE_PROTECTION_MISSING_SEED_MESSAGE,
            missing_origin_message=(
                "Falta node.advertise_addr. Push necesita identificar el nodo origen en membership."
            ),
        )
        resolved_seed = remote_context.seed
        cluster = remote_context.cluster
        origin_node_id = remote_context.origin_node_id
        current_epoch = remote_context.placement_epoch(
            remote_targets=required_remote_copies,
        )

        remote_candidate_node_ids = remote_context.remote_candidate_node_ids
        remote_candidate_count = remote_context.remote_candidate_count

        if strict_rf and remote_candidate_count < required_remote_copies:
            print("Copias remotas estrictas: no hay suficientes targets remotos elegibles.")
            print(f"   remote_copies={desired_rf} required_remote_copies={required_remote_copies} remote_candidates={remote_candidate_count}")
            print(f"   candidates={[node_id[:8] for node_id in remote_candidate_node_ids]}")
            print("   No se modifica chunk_protection; reintenta cuando el cluster recupere capacidad.")
            return PushStats(
                insufficient_remote_targets=True,
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
            )

        scope_label = describe_protection_scope(scope, snapshot_id=snapshot_id)
        if (scope or DEFAULT_PROTECTION_SCOPE) == DEFAULT_PROTECTION_SCOPE and snapshot_id is None:
            db.mark_stale_protection(desired_rf=desired_rf, current_epoch=current_epoch)
            metadata_changed = True

        pending_chunks = scoped_replication_push_chunks(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            desired_rf=desired_rf,
            current_epoch=current_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print(f"No hay chunks pendientes de protección para scope={scope_label} y política actual.")
            return PushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
            )

        processed_bytes = _sum_chunk_sizes(db, pending_chunks)

        remote_client = RemoteChunkClientPool(
            probe_timeout_s=probe_timeout_s,
            probe_batch_hashes=probe_batch_hashes,
            stream_timeout_s=stream_timeout_s,
            stream_inflight=stream_inflight,
            max_message_bytes=max_message_bytes,
        )

        print(f"Push: {len(pending_chunks)} chunks pendientes de protección scope={scope_label}")
        print(f"Membership seed: {resolved_seed}")
        print(f"Eligible members: {[f'{member.node_id[:8]}@{member.address}' for member in cluster.members]}")
        print(f"Self: {origin_node_id[:8]}@{self_addr}")
        print(f"Remote candidates: {remote_candidate_count}")
        print(f"Copias remotas deseadas: {desired_rf}")
        print(f"Copias remotas requeridas: {required_remote_copies}")
        print(f"Copias remotas estrictas: {bool(strict_rf)}")
        print(f"placement_epoch={current_epoch[:12] if current_epoch else '-'}")
        print(
            f"Pipeline: target_parallelism={target_parallelism} "
            f"probe_batch_hashes={probe_batch_hashes} "
            f"stream_inflight={stream_inflight} "
            f"probe_timeout_s={probe_timeout_s} "
            f"stream_timeout_s={stream_timeout_s}"
        )

        attempted = 0
        protected = 0
        degraded = 0
        failed = 0
        stored_remote = 0
        already_present_remote = 0
        detail_lines = 0
        suppressed_degraded = 0
        suppressed_failed = 0

        try:
            for outcome in push_chunk_replicas(
                repo=repo,
                cluster=cluster,
                chunk_hashes=pending_chunks,
                required_remote_copies=required_remote_copies,
                cluster_token=cluster_token,
                origin_node_id=origin_node_id,
                remote_client=remote_client,
                target_parallelism=target_parallelism,
            ):
                attempted += 1
                stored_remote += outcome.stored_remote_copies
                already_present_remote += outcome.already_present_remote_copies

                state = replication_push_state(
                    protected_remote_copies=outcome.protected_remote_copies,
                    required_remote_copies=required_remote_copies,
                )

                if state == ProtectionState.PLACED:
                    db.mark_chunk_placed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                    )
                    protected += 1

                elif state == ProtectionState.DEGRADED:
                    error = (
                        f"placement parcial: protected_remote_copies="
                        f"{outcome.protected_remote_copies}/{required_remote_copies} "
                        f"(planned_targets={outcome.required_remote_copies})"
                    )
                    if outcome.error:
                        error = f"{error}; {outcome.error}"

                    db.mark_chunk_push_degraded(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                        error=error,
                    )
                    degraded += 1
                    if detail_lines < PUSH_DETAIL_LIMIT:
                        print(
                            f"   {outcome.chunk_hash[:8]} degradado: "
                            f"protected={outcome.protected_remote_copies}/{required_remote_copies} "
                            f"planned={outcome.required_remote_copies} "
                            f"error={error}"
                        )
                        detail_lines += 1
                    else:
                        suppressed_degraded += 1

                else:
                    error = outcome.error or "replicación fallida sin copias remotas protegidas"
                    db.mark_chunk_failed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=0,
                        placement_epoch=current_epoch,
                        error=error,
                    )
                    failed += 1
                    if detail_lines < PUSH_DETAIL_LIMIT:
                        print(
                            f"   {outcome.chunk_hash[:8]} fallido: "
                            f"protected=0/{required_remote_copies} "
                            f"planned={outcome.required_remote_copies} "
                            f"error={error}"
                        )
                        detail_lines += 1
                    else:
                        suppressed_failed += 1

                metadata_changed = True

                if attempted % commit_every == 0:
                    db.commit()
                    print(
                        f"   progreso {attempted}/{len(pending_chunks)} | "
                        f"protected={protected} | degraded={degraded} | failed={failed} | "
                        f"stored_remote={stored_remote} | "
                        f"already_present_remote={already_present_remote}"
                    )

            _print_suppressed_details(suppressed_degraded, suppressed_failed)

            return PushStats(
                attempted=attempted,
                protected=protected,
                degraded=degraded,
                failed=failed,
                stored_remote=stored_remote,
                already_present_remote=already_present_remote,
                processed_bytes=processed_bytes,
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
            )

        except KeyboardInterrupt:
            print("\nPush interrumpido por el usuario.")
            db.commit()
            _print_suppressed_details(suppressed_degraded, suppressed_failed)

            return PushStats(
                attempted=attempted,
                protected=protected,
                degraded=degraded,
                failed=failed,
                stored_remote=stored_remote,
                already_present_remote=already_present_remote,
                processed_bytes=processed_bytes,
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
                interrupted=True,
            )

    finally:
        if remote_client is not None:
            remote_client.close()
        db.commit()
        db.close()

        export_after_successful_metadata_change(
            metadata_changed=metadata_changed,
            db_file=db_file,
            settings=metadata_object_graph_auto_export,
            context_label="PUSH",
        )


def _sum_chunk_sizes(db: MetadataDB, chunk_hashes: list[str]) -> int:
    total = 0
    batch_size = 500
    for offset in range(0, len(chunk_hashes), batch_size):
        batch = chunk_hashes[offset:offset + batch_size]
        placeholders = ", ".join("?" for _ in batch)
        row = db.conn.execute(
            f"SELECT COALESCE(SUM(size), 0) AS total_size FROM chunks WHERE hash IN ({placeholders})",
            tuple(batch),
        ).fetchone()
        total += int(row["total_size"] or 0)
    return total


def _print_suppressed_details(suppressed_degraded: int, suppressed_failed: int) -> None:
    if suppressed_degraded == 0 and suppressed_failed == 0:
        return
    print(
        "   detalles omitidos: "
        f"degradados={suppressed_degraded} "
        f"fallidos={suppressed_failed} "
        "(consulta metadata para inspección completa)"
    )
