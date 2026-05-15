from __future__ import annotations

import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from stopan.placement.cluster_resolver import require_cluster_view
from stopan.metadata.database import MetadataDB, ErasureDataPackRecord
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_metadata_object_graph_after_metadata_change,
)
from stopan.protection.ec.remote_client import (
    RemoteDataPackShardClientPool,
    RemoteDataPackShardRef,
)
from stopan.protection.policy import ProtectionState


@dataclass(frozen=True)
class ErasureVerificationStats:
    candidates: int = 0
    verified: int = 0
    degraded: int = 0
    failed: int = 0
    rpc_failures: int = 0


@dataclass(frozen=True)
class _ShardProbeOutcome:
    address: str
    requested_indexes: frozenset[int]
    present_indexes: frozenset[int]
    error: str | None = None


class ErasureDataPackVerifier:
    def __init__(
        self,
        *,
        db: MetadataDB,
        client_pool: RemoteDataPackShardClientPool,
        target_parallelism: int,
        cluster,
    ):
        self.db = db
        self.client_pool = client_pool
        self.target_parallelism = max(1, int(target_parallelism))
        self.cluster = cluster

    def verify(self, candidates: list[ErasureDataPackRecord]) -> ErasureVerificationStats:
        if not candidates:
            print("No hay data packs EC candidatos para verificación.")
            return ErasureVerificationStats()

        print(f"Verify EC: {len(candidates)} data packs candidatos")

        verified = 0
        degraded = 0
        failed = 0
        rpc_failures = 0

        for pack in candidates:
            outcome = self._verify_pack(pack)
            if outcome.error and "RPC" in outcome.error:
                rpc_failures += 1

            self.db.mark_erasure_data_pack_verification(
                pack.pack_hash,
                protection_state=outcome.protection_state,
                verified_shard_indexes=outcome.verified_shard_indexes,
                error=outcome.error,
            )

            if outcome.protection_state == ProtectionState.VERIFIED:
                verified += 1
            elif outcome.protection_state == ProtectionState.DEGRADED:
                degraded += 1
                print(
                    f"   pack={pack.pack_hash[:8]} DEGRADED: "
                    f"shards={len(outcome.verified_shard_indexes)}/{pack.data_shards + pack.parity_shards} "
                    f"error={outcome.error}"
                )
            else:
                failed += 1
                print(
                    f"   pack={pack.pack_hash[:8]} FAILED: "
                    f"shards={len(outcome.verified_shard_indexes)}/{pack.data_shards + pack.parity_shards} "
                    f"error={outcome.error}"
                )

        return ErasureVerificationStats(
            candidates=len(candidates),
            verified=verified,
            degraded=degraded,
            failed=failed,
            rpc_failures=rpc_failures,
        )

    def _verify_pack(self, pack: ErasureDataPackRecord) -> "_PackVerificationOutcome":
        shard_rows = self.db.get_erasure_pack_shards(pack.pack_hash)
        total_shards = pack.data_shards + pack.parity_shards

        if len(shard_rows) < total_shards:
            verified_indexes = {row.shard_index for row in shard_rows}
            return _PackVerificationOutcome(
                protection_state=ProtectionState.FAILED,
                verified_shard_indexes=frozenset(verified_indexes),
                error=(
                    "metadata EC incompleta: "
                    f"registered_shards={len(shard_rows)} total_shards={total_shards}"
                ),
            )

        refs_by_addr: dict[str, list[RemoteDataPackShardRef]] = defaultdict(list)
        verified_indexes: set[int] = set()
        errors: list[str] = []
        address_by_node_id = self.cluster.node_addresses

        for row in shard_rows:
            target_address = address_by_node_id.get(row.node_id)
            if target_address is None:
                errors.append(
                    f"shard={row.shard_index}: nodo {row.node_id[:8]} offline/ilocalizable"
                )
                continue

            refs_by_addr[target_address].append(
                RemoteDataPackShardRef(
                    pack_hash=row.pack_hash,
                    shard_index=row.shard_index,
                    shard_hash=row.shard_hash,
                )
            )

        outcomes = self._probe_targets(refs_by_addr)

        for outcome in outcomes:
            verified_indexes.update(outcome.present_indexes)
            if outcome.error:
                errors.append(outcome.error)

        if len(verified_indexes) >= total_shards:
            return _PackVerificationOutcome(
                protection_state=ProtectionState.VERIFIED,
                verified_shard_indexes=frozenset(verified_indexes),
                error=None,
            )

        error_summary = "; ".join(errors)[:1800] if errors else (
            f"verified_shards={len(verified_indexes)}/{total_shards}"
        )
        if len(verified_indexes) >= pack.data_shards:
            return _PackVerificationOutcome(
                protection_state=ProtectionState.DEGRADED,
                verified_shard_indexes=frozenset(verified_indexes),
                error=error_summary,
            )

        return _PackVerificationOutcome(
            protection_state=ProtectionState.FAILED,
            verified_shard_indexes=frozenset(verified_indexes),
            error=error_summary,
        )

    def _probe_targets(
        self,
        refs_by_addr: dict[str, list[RemoteDataPackShardRef]],
    ) -> list[_ShardProbeOutcome]:
        max_workers = min(self.target_parallelism, len(refs_by_addr))
        if max_workers <= 0:
            return []

        outcomes: list[_ShardProbeOutcome] = []

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ec-verify") as executor:
            future_map = {
                executor.submit(self._probe_address, address, refs): (address, refs)
                for address, refs in refs_by_addr.items()
            }

            for future in as_completed(future_map):
                address, refs = future_map[future]
                try:
                    outcomes.append(future.result())
                except Exception as exc:
                    outcomes.append(
                        _ShardProbeOutcome(
                            address=address,
                            requested_indexes=frozenset(ref.shard_index for ref in refs),
                            present_indexes=frozenset(),
                            error=f"RPC {address}: {exc}",
                        )
                    )

        return outcomes

    def _probe_address(
        self,
        address: str,
        refs: list[RemoteDataPackShardRef],
    ) -> _ShardProbeOutcome:
        missing = self.client_pool.probe_missing_shards(addr=address, refs=refs)
        missing_indexes = {ref.shard_index for ref in missing}
        requested_indexes = frozenset(ref.shard_index for ref in refs)
        present_indexes = frozenset(index for index in requested_indexes if index not in missing_indexes)

        error = None
        if missing_indexes:
            error = f"{address}: missing_shards={sorted(missing_indexes)}"

        return _ShardProbeOutcome(
            address=address,
            requested_indexes=requested_indexes,
            present_indexes=present_indexes,
            error=error,
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
    target_parallelism: int,
    probe_timeout_s: float,
    max_message_bytes: int,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
) -> ErasureVerificationStats:
    db = MetadataDB(db_file)
    client_pool = RemoteDataPackShardClientPool(
        timeout_s=probe_timeout_s,
        max_message_bytes=max_message_bytes,
    )
    metadata_changed = False

    resolved = require_cluster_view(
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        timeout_s=membership_timeout_s,
        max_message_bytes=max_message_bytes,
        missing_seed_message="Falta membership seed. Usa '--membership-seed'.",
    )
    cluster = resolved.cluster

    try:
        candidates = db.get_erasure_verification_candidates(
            include_verified=include_verified,
            limit=limit,
        )
        verifier = ErasureDataPackVerifier(
            db=db,
            client_pool=client_pool,
            target_parallelism=target_parallelism,
            cluster=cluster,
        )

        if include_verified:
            print("Modo reverify EC: incluyendo data packs ya VERIFIED.")

        stats = verifier.verify(candidates)
        metadata_changed = bool(candidates)
        return stats

    finally:
        client_pool.close()
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
                context_label="VERIFY_EC",
            )
