from __future__ import annotations

from dataclasses import dataclass

from stopan.common.batching import iter_batches
from stopan.metadata.database import (
    ErasureDataPackRecord,
    ErasureDataPackVerificationUpdate,
    MetadataDB,
    MetadataDBAccessMode,
)
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_after_successful_metadata_change,
)
from stopan.protection.ec.remote_client import (
    RemoteDataPackShardClientPool,
    RemoteDataPackShardRef,
)
from stopan.protection.ec.shard_targets import group_erasure_shard_refs_by_address
from stopan.protection.ec.states import data_pack_state
from stopan.protection.concurrency import iter_completed_keyed_tasks
from stopan.protection.policy import ProtectionState
from stopan.protection.scope import describe_protection_scope, scoped_erasure_verification_candidates
from stopan.protection.remote_context import resolve_remote_protection_context


@dataclass(frozen=True)
class ErasureVerificationStats:
    candidates: int = 0
    verified: int = 0
    degraded: int = 0
    failed: int = 0
    rpc_failed_calls: int = 0
    rpc_failed_targets: int = 0
    rpc_unverified_assignments: int = 0


@dataclass(frozen=True)
class _AddressProbeOutcome:
    completed_keys: frozenset[tuple[str, int, str]]
    missing_keys: frozenset[tuple[str, int, str]]
    error: str | None = None
    rpc_failed_calls: int = 0


@dataclass
class _PackVerificationAccumulator:
    pack: ErasureDataPackRecord
    total_shards: int
    verified_shard_indexes: set[int]
    errors: list[str]
    metadata_complete: bool

    @property
    def error_summary(self) -> str | None:
        if not self.errors:
            return None
        return "; ".join(self.errors)[:1800]


class ErasureDataPackVerifier:
    def __init__(
        self,
        *,
        db: MetadataDB,
        client_pool: RemoteDataPackShardClientPool,
        target_parallelism: int,
        probe_batch_hashes: int,
        cluster,
    ):
        self.db = db
        self.client_pool = client_pool
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self.cluster = cluster

    def verify(self, candidates: list[ErasureDataPackRecord]) -> ErasureVerificationStats:
        if not candidates:
            print("No hay data packs EC candidatos para verificación.")
            return ErasureVerificationStats()

        print(f"Verify EC: {len(candidates)} data packs candidatos")

        (
            outcomes,
            rpc_failed_calls,
            rpc_failed_targets,
            rpc_unverified_assignments,
        ) = self._verify_candidates(candidates)
        verified = 0
        degraded = 0
        failed = 0
        updates: list[ErasureDataPackVerificationUpdate] = []

        for pack, outcome in zip(candidates, outcomes, strict=True):
            updates.append(
                ErasureDataPackVerificationUpdate(
                    pack_hash=pack.pack_hash,
                    protection_state=outcome.protection_state,
                    verified_shard_indexes=tuple(sorted(outcome.verified_shard_indexes)),
                    error=outcome.error,
                )
            )

            if outcome.protection_state == ProtectionState.VERIFIED:
                verified += 1
            elif outcome.protection_state == ProtectionState.DEGRADED:
                degraded += 1
                print(
                    f"   pack={pack.pack_hash[:8]} DEGRADED: "
                    f"shards={len(outcome.verified_shard_indexes)}/"
                    f"{pack.data_shards + pack.parity_shards} "
                    f"error={outcome.error}"
                )
            else:
                failed += 1
                print(
                    f"   pack={pack.pack_hash[:8]} FAILED: "
                    f"shards={len(outcome.verified_shard_indexes)}/"
                    f"{pack.data_shards + pack.parity_shards} "
                    f"error={outcome.error}"
                )

        self.db.apply_erasure_data_pack_verifications(updates)
        return ErasureVerificationStats(
            candidates=len(candidates),
            verified=verified,
            degraded=degraded,
            failed=failed,
            rpc_failed_calls=rpc_failed_calls,
            rpc_failed_targets=rpc_failed_targets,
            rpc_unverified_assignments=rpc_unverified_assignments,
        )

    def _verify_candidates(
        self,
        candidates: list[ErasureDataPackRecord],
    ) -> tuple[list["_PackVerificationOutcome"], int, int, int]:
        shard_rows_by_pack = self.db.get_erasure_pack_shards_many(
            pack.pack_hash for pack in candidates
        )
        accumulators: dict[str, _PackVerificationAccumulator] = {}
        refs_by_address: dict[str, list[RemoteDataPackShardRef]] = {}

        for pack in candidates:
            shard_rows = shard_rows_by_pack.get(pack.pack_hash, [])
            total_shards = pack.data_shards + pack.parity_shards
            metadata_complete = len(shard_rows) >= total_shards
            accumulator = _PackVerificationAccumulator(
                pack=pack,
                total_shards=total_shards,
                verified_shard_indexes=set(),
                errors=[],
                metadata_complete=metadata_complete,
            )
            accumulators[pack.pack_hash] = accumulator

            if not metadata_complete:
                accumulator.verified_shard_indexes.update(
                    row.shard_index for row in shard_rows
                )
                accumulator.errors.append(
                    "metadata EC incompleta: "
                    f"registered_shards={len(shard_rows)} "
                    f"total_shards={total_shards}"
                )
                continue

            target_groups = group_erasure_shard_refs_by_address(
                shard_rows=shard_rows,
                node_addresses=self.cluster.node_addresses,
                short_node_ids_in_errors=True,
            )
            accumulator.errors.extend(target_groups.offline_errors)
            for address, refs in target_groups.refs_by_address.items():
                refs_by_address.setdefault(address, []).extend(refs)

        (
            rpc_failed_calls,
            rpc_failed_targets,
            rpc_unverified_assignments,
        ) = self._probe_global_targets(
            refs_by_address=refs_by_address,
            accumulators=accumulators,
        )

        outcomes: list[_PackVerificationOutcome] = []
        for pack in candidates:
            accumulator = accumulators[pack.pack_hash]
            if not accumulator.metadata_complete:
                state = ProtectionState.FAILED
                error = accumulator.error_summary
            elif len(accumulator.verified_shard_indexes) >= accumulator.total_shards:
                state = ProtectionState.VERIFIED
                error = None
            else:
                state = data_pack_state(
                    protected_shards=len(accumulator.verified_shard_indexes),
                    data_shards=pack.data_shards,
                    total_shards=accumulator.total_shards,
                )
                error = accumulator.error_summary or (
                    "verified_shards="
                    f"{len(accumulator.verified_shard_indexes)}/"
                    f"{accumulator.total_shards}"
                )

            outcomes.append(
                _PackVerificationOutcome(
                    protection_state=state,
                    verified_shard_indexes=frozenset(
                        accumulator.verified_shard_indexes
                    ),
                    error=error,
                )
            )

        return (
            outcomes,
            rpc_failed_calls,
            rpc_failed_targets,
            rpc_unverified_assignments,
        )

    def _probe_global_targets(
        self,
        *,
        refs_by_address: dict[str, list[RemoteDataPackShardRef]],
        accumulators: dict[str, _PackVerificationAccumulator],
    ) -> tuple[int, int, int]:
        if not refs_by_address:
            return 0, 0, 0

        tasks = {
            address: (
                lambda address=address, refs=refs: self._probe_address(address, refs)
            )
            for address, refs in refs_by_address.items()
        }
        rpc_failed_calls = 0
        rpc_failed_targets = 0
        rpc_unverified_assignments = 0

        for completed in iter_completed_keyed_tasks(
            tasks=tasks,
            max_workers=min(self.target_parallelism, len(tasks)),
            thread_name_prefix="ec-verify",
        ):
            address = completed.key
            planned_refs = refs_by_address[address]

            if completed.error is not None:
                outcome = _AddressProbeOutcome(
                    completed_keys=frozenset(),
                    missing_keys=frozenset(),
                    error=str(completed.error),
                    rpc_failed_calls=1,
                )
            else:
                outcome = completed.result

            missing_by_pack: dict[str, list[int]] = {}
            unprobed_refs: list[RemoteDataPackShardRef] = []
            for ref in planned_refs:
                key = ref.identity_key
                if key not in outcome.completed_keys:
                    unprobed_refs.append(ref)
                elif key in outcome.missing_keys:
                    missing_by_pack.setdefault(ref.pack_hash, []).append(ref.shard_index)
                else:
                    accumulators[ref.pack_hash].verified_shard_indexes.add(
                        ref.shard_index
                    )

            for pack_hash, indexes in missing_by_pack.items():
                accumulators[pack_hash].errors.append(
                    f"{address}: missing_shards={sorted(indexes)}"
                )

            if outcome.error is not None:
                rpc_failed_calls += max(1, int(outcome.rpc_failed_calls))
                rpc_failed_targets += 1
                rpc_unverified_assignments += len(unprobed_refs)

                message = f"RPC {address}: {outcome.error}"
                for pack_hash in dict.fromkeys(ref.pack_hash for ref in unprobed_refs):
                    accumulators[pack_hash].errors.append(message)

        return (
            rpc_failed_calls,
            rpc_failed_targets,
            rpc_unverified_assignments,
        )

    def _probe_address(
        self,
        address: str,
        refs: list[RemoteDataPackShardRef],
    ) -> _AddressProbeOutcome:
        completed_keys: set[tuple[str, int, str]] = set()
        missing_keys: set[tuple[str, int, str]] = set()

        for batch in iter_batches(refs, self.probe_batch_hashes):
            try:
                missing = self.client_pool.probe_missing_shards(
                    addr=address,
                    refs=batch,
                )
            except Exception as exc:
                return _AddressProbeOutcome(
                    completed_keys=frozenset(completed_keys),
                    missing_keys=frozenset(missing_keys),
                    error=str(exc),
                    rpc_failed_calls=1,
                )

            completed_keys.update(ref.identity_key for ref in batch)
            missing_keys.update(ref.identity_key for ref in missing)

        return _AddressProbeOutcome(
            completed_keys=frozenset(completed_keys),
            missing_keys=frozenset(missing_keys),
        )


@dataclass(frozen=True)
class _PackVerificationOutcome:
    protection_state: ProtectionState
    verified_shard_indexes: frozenset[int]
    error: str | None


def verify_erasure_data_packs(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    db_file: str,
    include_verified: bool,
    limit: int | None,
    scope: str | None,
    snapshot_id: int | None,
    pack_hash: str | None,
    target_parallelism: int,
    probe_batch_hashes: int,
    probe_timeout_s: float,
    max_message_bytes: int,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
) -> ErasureVerificationStats:
    db = MetadataDB(db_file, access_mode=MetadataDBAccessMode.READ_WRITE)
    client_pool = RemoteDataPackShardClientPool(
        cluster_token=cluster_token,
        timeout_s=probe_timeout_s,
        max_message_bytes=max_message_bytes,
    )
    metadata_changed = False

    try:
        remote_context = resolve_remote_protection_context(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message="Falta membership seed. Usa '--membership-seed'.",
        )
        cluster = remote_context.cluster
        candidates = scoped_erasure_verification_candidates(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            pack_hash=pack_hash,
            include_verified=include_verified,
            limit=limit,
        )
        verifier = ErasureDataPackVerifier(
            db=db,
            client_pool=client_pool,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            cluster=cluster,
        )

        print(f"Verify EC scope: {pack_hash if pack_hash else describe_protection_scope(scope, snapshot_id=snapshot_id)}")
        if include_verified:
            print("Modo reverify EC: incluyendo data packs ya VERIFIED.")

        stats = verifier.verify(candidates)
        metadata_changed = bool(candidates)
        return stats

    finally:
        client_pool.close()
        db.close()

        export_after_successful_metadata_change(
            metadata_changed=metadata_changed,
            db_file=db_file,
            settings=metadata_object_graph_auto_export,
            context_label="VERIFY_EC",
        )
