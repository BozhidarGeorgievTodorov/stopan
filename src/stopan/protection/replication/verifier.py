"""
Verificación de protección remota de chunks.

El verifier reevalúa el placement HRW vigente y usa ProbeMissingChunks para
confirmar copias remotas sin descargar blobs completos.
"""

from __future__ import annotations

from stopan.metadata.database import MetadataDB, VerificationCandidate
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_after_successful_metadata_change,
)
from stopan.protection.concurrency import iter_completed_keyed_tasks
from stopan.protection.policy import ProtectionState, normalize_remote_rf
from stopan.protection.scope import describe_protection_scope, scoped_replication_verification_candidates
from stopan.protection.remote_context import (
    RemoteProtectionContext,
    resolve_remote_protection_context,
)

from .placement import plan_chunk_replication_targets
from .remote_client import ProbeExecutionResult, RemoteChunkClientPool
from .states import replication_verify_state
from .verify_models import (
    VerificationAccumulator,
    VerificationOutcome,
    VerificationStats,
)


class ChunkProtectionVerifier:
    """
    Auditor canónico de protección remota.

    Contrato:
      - no transfiere blobs;
      - solo usa ProbeMissingChunks;
      - reevalúa el placement HRW vigente para cada chunk;
      - convierte evidencia PLACED/DEGRADED en VERIFIED o DEGRADED.
    """

    def __init__(
        self,
        *,
        db: MetadataDB,
        remote_context: RemoteProtectionContext,
        probe_timeout_s: float,
        target_parallelism: int,
        probe_batch_hashes: int,
        max_message_bytes: int,
    ):
        self.db = db
        self.remote_context = remote_context
        self.cluster = remote_context.cluster
        self.cluster_token = remote_context.cluster_token
        self.origin_node_id = remote_context.origin_node_id
        self.self_addr = remote_context.self_addr
        self._excluded_node_ids = remote_context.excluded_node_ids

        self.probe_timeout_s = float(probe_timeout_s)
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self._client_pool = RemoteChunkClientPool(
            probe_timeout_s=probe_timeout_s,
            probe_batch_hashes=probe_batch_hashes,
            max_message_bytes=max_message_bytes,
        )
        self._epoch_cache: dict[int, str] = {}

    def close(self) -> None:
        self._client_pool.close()

    def verify(self, candidates: list[VerificationCandidate]) -> VerificationStats:
        candidates = [
            candidate
            for candidate in candidates
            if normalize_remote_rf(candidate.desired_rf, field_name="candidate.desired_rf") > 0
        ]

        if not candidates:
            print("No hay chunks candidatos con protección remota que verificar.")
            return VerificationStats()

        print(f"Verify: {len(candidates)} chunks candidatos")
        print(f"Miembros elegibles: {[f'{member.node_id[:8]}@{member.address}' for member in self.cluster.members]}")
        if self.cluster.self_node_id:
            print(f"Nodo local: {self.cluster.self_node_id[:8]}@{self.self_addr}")

        remote_candidate_count = len(
            self.cluster.candidate_node_ids_excluding(self._excluded_node_ids)
        )
        print(f"Origin excluido de protección: {self.origin_node_id[:8]}")
        print(f"Candidatos remotos: {remote_candidate_count}")
        print(
            f"Pipeline: target_parallelism={self.target_parallelism} "
            f"probe_batch_hashes={self.probe_batch_hashes} "
            f"probe_timeout_s={self.probe_timeout_s}"
        )

        outcomes = self._verify_candidates(candidates)
        verified = 0
        degraded = 0
        rpc_failures = 0

        for outcome in outcomes:
            state = replication_verify_state(
                verified_remote_copies=outcome.verified_remote_copies,
                required_remote_copies=outcome.required_remote_copies,
            )

            if state == ProtectionState.VERIFIED:
                self.db.mark_chunk_verified(
                    outcome.chunk_hash,
                    desired_rf=outcome.desired_rf,
                    protected_remote_copies=outcome.verified_remote_copies,
                    placement_epoch=outcome.placement_epoch,
                )
                verified += 1
                continue

            self.db.mark_chunk_degraded(
                outcome.chunk_hash,
                desired_rf=outcome.desired_rf,
                protected_remote_copies=outcome.verified_remote_copies,
                placement_epoch=outcome.placement_epoch,
                error=outcome.error or "verificación fallida",
            )
            degraded += 1
            if outcome.error and "RPC" in outcome.error:
                rpc_failures += 1
            print(
                f"   {outcome.chunk_hash[:8]} degradado: "
                f"verified={outcome.verified_remote_copies}/{outcome.required_remote_copies} "
                f"error={outcome.error}"
            )

        return VerificationStats(
            candidates=len(candidates),
            verified=verified,
            degraded=degraded,
            rpc_failures=rpc_failures,
        )

    def _verify_candidates(self, candidates: list[VerificationCandidate]) -> list[VerificationOutcome]:
        accumulators, target_chunks, target_members = self._plan(candidates)

        if target_chunks:
            tasks = {
                address: (
                    lambda address=address, hashes=hashes: self._execute_target_probe(
                        target_members[address],
                        hashes,
                    )
                )
                for address, hashes in target_chunks.items()
            }

            for completed in iter_completed_keyed_tasks(
                tasks=tasks,
                max_workers=min(self.target_parallelism, len(target_chunks)),
                thread_name_prefix="verify-probe",
            ):
                address = completed.key
                planned_hashes = target_chunks[address]
                member = target_members[address]

                if completed.error is not None:
                    result = ProbeExecutionResult(
                        node_id=member.node_id,
                        address=member.address,
                        requested_hashes=tuple(planned_hashes),
                        present_hashes=frozenset(),
                        transport_error=f"probe del target falló: {completed.error}",
                    )
                else:
                    result = completed.result

                if result.transport_error:
                    message = f"RPC {result.node_id[:8]}@{result.address}: {result.transport_error}"
                    for chunk_hash in result.requested_hashes:
                        accumulators[chunk_hash].errors.append(message)
                    continue

                for chunk_hash in result.requested_hashes:
                    if chunk_hash in result.present_hashes:
                        accumulators[chunk_hash].verified_remote_copies += 1

        outcomes: list[VerificationOutcome] = []
        for candidate in candidates:
            accumulator = accumulators[candidate.chunk_hash]
            outcomes.append(
                VerificationOutcome(
                    chunk_hash=candidate.chunk_hash,
                    desired_rf=accumulator.desired_rf,
                    placement_epoch=accumulator.placement_epoch,
                    required_remote_copies=accumulator.required_remote_copies,
                    verified_remote_copies=accumulator.verified_remote_copies,
                    error=accumulator.error_summary,
                )
            )

        return outcomes

    def _plan(self, candidates: list[VerificationCandidate]):
        """
        Agrupa las consultas por target remoto usando el planner canónico de replicación.

        Los candidatos pueden tener distinto desired_rf, por eso se planifican por
        grupos de RF y luego se fusionan los lotes por target.
        """

        accumulators: dict[str, VerificationAccumulator] = {}
        target_chunks: dict[str, list[str]] = {}
        target_members: dict[str, object] = {}

        candidates_by_rf: dict[int, list[VerificationCandidate]] = {}
        for candidate in candidates:
            desired_rf = normalize_remote_rf(candidate.desired_rf, field_name="candidate.desired_rf")
            candidates_by_rf.setdefault(desired_rf, []).append(candidate)

        for desired_rf, group in candidates_by_rf.items():
            required_remote_copies = desired_rf
            placement_epoch = self._placement_epoch(required_remote_copies)
            plan = plan_chunk_replication_targets(
                cluster=self.cluster,
                chunk_hashes=[candidate.chunk_hash for candidate in group],
                required_remote_copies=required_remote_copies,
                cluster_token=self.cluster_token,
                origin_node_id=self.origin_node_id,
            )

            for candidate in group:
                remote_targets = plan.chunk_targets[candidate.chunk_hash]
                accumulator = VerificationAccumulator(
                    desired_rf=desired_rf,
                    placement_epoch=placement_epoch,
                    required_remote_copies=required_remote_copies,
                )

                if len(remote_targets) < required_remote_copies:
                    accumulator.errors.append(
                        "targets remotos elegibles insuficientes: "
                        f"planned={len(remote_targets)} "
                        f"required_remote_copies={required_remote_copies} "
                        f"desired_rf={desired_rf}"
                    )

                accumulators[candidate.chunk_hash] = accumulator

            for address, hashes in plan.target_chunks.items():
                target_chunks.setdefault(address, []).extend(hashes)
                target_members[address] = plan.target_members[address]

        return accumulators, target_chunks, target_members

    def _placement_epoch(self, required_remote_copies: int) -> str:
        required_remote_copies = normalize_remote_rf(
            required_remote_copies,
            field_name="required_remote_copies",
        )
        if required_remote_copies == 0:
            return ""
        cached = self._epoch_cache.get(required_remote_copies)
        if cached is not None:
            return cached

        epoch = self.remote_context.placement_epoch(
            remote_targets=required_remote_copies,
        ) or ""
        self._epoch_cache[required_remote_copies] = epoch
        return epoch

    def _execute_target_probe(self, member, chunk_hashes: list[str]) -> ProbeExecutionResult:
        return self._client_pool.probe_target(
            node_id=member.node_id,
            address=member.address,
            chunk_hashes=chunk_hashes,
        )


def verify_remote_protection(
    *,
    membership_seed: str | None,
    db_file: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    include_verified: bool,
    limit: int | None,
    scope: str | None,
    snapshot_id: int | None,
    target_parallelism: int,
    probe_batch_hashes: int,
    probe_timeout_s: float,
    max_message_bytes: int,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
) -> VerificationStats:
    """
    Ejecuta una verificación remota de chunk_protection.

    Resuelve membership, obtiene candidatos desde metadata, audita los targets
    esperados mediante ProbeMissingChunks y actualiza cada chunk como VERIFIED
    o DEGRADED.
    """

    db = MetadataDB(db_file)
    verifier: ChunkProtectionVerifier | None = None
    metadata_changed = False

    try:
        remote_context = resolve_remote_protection_context(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message=(
                "Falta membership seed. Usa '--membership-seed' o configura cluster.seeds."
            ),
        )
        resolved_seed = remote_context.seed

        candidates = scoped_replication_verification_candidates(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            include_verified=include_verified,
            limit=limit,
        )

        verifier = ChunkProtectionVerifier(
            db=db,
            remote_context=remote_context,
            probe_timeout_s=probe_timeout_s,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            max_message_bytes=max_message_bytes,
        )

        print(f"Membership seed: {resolved_seed}")
        print(f"Verify scope: {describe_protection_scope(scope, snapshot_id=snapshot_id)}")
        if include_verified:
            print("Modo reverify: incluyendo chunks ya VERIFIED.")

        stats = verifier.verify(candidates)
        metadata_changed = bool(candidates)
        return stats

    finally:
        if verifier is not None:
            verifier.close()
        db.commit()
        db.close()

        export_after_successful_metadata_change(
            metadata_changed=metadata_changed,
            db_file=db_file,
            settings=metadata_object_graph_auto_export,
            context_label="VERIFY",
        )
