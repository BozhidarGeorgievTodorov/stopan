"""
Push de chunks pendientes hacia nodos remotos.

Este módulo coordina la protección P2P de chunks completos: resuelve membership,
calcula placement remoto excluyendo el nodo origen, replica chunks mediante
streaming y actualiza chunk_protection con el resultado.
"""

from __future__ import annotations

from dataclasses import dataclass
import sys

from stopan.cas.repository import CASRepository
from stopan.errors import StopanConfigRuntimeError, StopanConfigValueError
from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_metadata_object_graph_after_metadata_change,
)
from stopan.placement.cluster_resolver import require_cluster_view
from stopan.protection.policy import normalize_remote_rf
from stopan.replication.coordinator import StreamingReplicationCoordinator


@dataclass(frozen=True)
class PushStats:
    """Contadores agregados de una ejecución de push."""

    attempted: int = 0
    protected: int = 0
    degraded: int = 0
    failed: int = 0
    stored_remote: int = 0
    already_present_remote: int = 0
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

    if not self_addr:
        raise StopanConfigRuntimeError(
            "Falta node.advertise_addr. Push necesita identificar el nodo origen en membership."
        )

    repo = CASRepository(local_shard_dir)
    db = MetadataDB(db_file)
    coordinator: StreamingReplicationCoordinator | None = None
    metadata_changed = False

    try:
        resolved = require_cluster_view(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message=(
                "Falta membership seed. Usa '--membership-seed' o define cluster.seeds en node.yaml."
            ),
        )
        resolved_seed = resolved.seed
        cluster = resolved.cluster
        origin_node_id = cluster.self_node_id
        if not origin_node_id:
            raise StopanConfigRuntimeError(
                "No pude resolver origin_node_id desde membership. "
                "Asegúrate de que node.advertise_addr coincide con un miembro elegible."
            )

        current_epoch = (
            cluster.placement_epoch_excluding(
                desired_rf=required_remote_copies,
                cluster_token=cluster_token,
                excluded_node_ids={origin_node_id},
            )
            if required_remote_copies > 0
            else None
        )

        remote_candidate_node_ids = cluster.candidate_node_ids_excluding({origin_node_id})
        remote_candidate_count = len(remote_candidate_node_ids)

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

        db.mark_stale_protection(desired_rf=desired_rf, current_epoch=current_epoch)
        metadata_changed = True

        pending_chunks = db.get_pending_protection_chunks(
            desired_rf=desired_rf,
            current_epoch=current_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print("No hay chunks pendientes de protección para la política actual.")
            return PushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
            )

        coordinator = StreamingReplicationCoordinator(
            repo=repo,
            cluster=cluster,
            rf=required_remote_copies,
            cluster_token=cluster_token,
            origin_node_id=origin_node_id,
            probe_timeout_s=probe_timeout_s,
            stream_timeout_s=stream_timeout_s,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            stream_inflight=stream_inflight,
            max_message_bytes=max_message_bytes,
        )

        print(f"Push: {len(pending_chunks)} chunks pendientes de protección")
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

        try:
            for outcome in coordinator.replicate_chunks(pending_chunks):
                attempted += 1
                stored_remote += outcome.stored_remote_copies
                already_present_remote += outcome.already_present_remote_copies

                if outcome.protected_remote_copies >= required_remote_copies:
                    db.mark_chunk_placed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                    )
                    protected += 1

                elif outcome.protected_remote_copies > 0:
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
                    print(
                        f"   {outcome.chunk_hash[:8]} degradado: "
                        f"protected={outcome.protected_remote_copies}/{required_remote_copies} "
                        f"planned={outcome.required_remote_copies} "
                        f"error={error}"
                    )

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
                    print(
                        f"   {outcome.chunk_hash[:8]} fallido: "
                        f"protected=0/{required_remote_copies} "
                        f"planned={outcome.required_remote_copies} "
                        f"error={error}"
                    )

                metadata_changed = True

                if attempted % commit_every == 0:
                    db.commit()
                    print(
                        f"   progreso {attempted}/{len(pending_chunks)} | "
                        f"protected={protected} | degraded={degraded} | failed={failed} | "
                        f"stored_remote={stored_remote} | "
                        f"already_present_remote={already_present_remote}"
                    )

            return PushStats(
                attempted=attempted,
                protected=protected,
                degraded=degraded,
                failed=failed,
                stored_remote=stored_remote,
                already_present_remote=already_present_remote,
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
            )

        except KeyboardInterrupt:
            print("\nPush interrumpido por el usuario.")
            db.commit()
            return PushStats(
                attempted=attempted,
                protected=protected,
                degraded=degraded,
                failed=failed,
                stored_remote=stored_remote,
                already_present_remote=already_present_remote,
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
                interrupted=True,
            )

    finally:
        if coordinator is not None:
            coordinator.close()
        db.commit()
        db.close()

        if (
            metadata_changed
            and metadata_object_graph_auto_export is not None
            and metadata_object_graph_auto_export.enabled
            and sys.exc_info()[0] is None
        ):
            export_metadata_object_graph_after_metadata_change(
                db_file=db_file,
                settings=metadata_object_graph_auto_export,
                context_label="PUSH",
            )
