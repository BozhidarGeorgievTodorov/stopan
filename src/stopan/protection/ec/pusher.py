from __future__ import annotations

from dataclasses import dataclass

from stopan.cas.repository import CASRepository
from stopan.metadata.database import ErasureDataPackRecord, MetadataDB
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_after_successful_metadata_change,
)
from stopan.protection.ec.metadata_adapter import spec_from_erasure_metadata
from stopan.protection.ec.models import ErasureCodingError, ErasureSpec
from stopan.protection.ec.packer import DataPackBuilder, build_data_pack
from stopan.protection.ec.placement import plan_data_pack_shard_placement
from stopan.protection.ec.push_execution import (
    push_data_pack_shards,
    refresh_data_pack_push_metadata,
    register_data_pack_metadata,
)
from stopan.protection.ec.remote_client import RemoteDataPackShardClientPool
from stopan.protection.ec.states import data_pack_state
from stopan.protection.policy import ProtectionState
from stopan.protection.remote_context import (
    DEFAULT_REMOTE_PROTECTION_MISSING_SEED_MESSAGE,
    resolve_remote_protection_context,
)
from stopan.protection.scope import (
    describe_protection_scope,
    scoped_erasure_push_new_chunks,
    scoped_erasure_push_retry_packs,
)


@dataclass(frozen=True)
class ErasurePushStats:
    attempted_chunks: int = 0
    packed_chunks: int = 0
    placed_chunks: int = 0
    degraded_chunks: int = 0
    failed_chunks: int = 0
    missing_local_chunks: int = 0
    attempted_packs: int = 0
    placed_packs: int = 0
    degraded_packs: int = 0
    failed_packs: int = 0
    stored_shards: int = 0
    already_present_shards: int = 0
    failed_shards: int = 0
    insufficient_remote_targets: bool = False
    remote_candidates: int = 0
    required_remote_targets: int = 0
    interrupted: bool = False


def push_erasure_data_packs_to_network(
    *,
    membership_seed: str | None,
    limit: int | None,
    ec_k: int,
    ec_m: int,
    ec_pack_size_bytes: int,
    stream_timeout_s: float,
    max_message_bytes: int,
    commit_every: int,
    db_file: str,
    local_shard_dir: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
    scope: str | None = None,
    snapshot_id: int | None = None,
) -> ErasurePushStats:
    spec = ErasureSpec(data_shards=int(ec_k), parity_shards=int(ec_m))
    pack_size = max(int(ec_pack_size_bytes), 1)
    commit_every = max(int(commit_every), 1)
    self_addr = str(self_addr or "").strip()
    cluster_token = str(cluster_token or "")

    repo = CASRepository(local_shard_dir)
    db = MetadataDB(db_file)
    pool: RemoteDataPackShardClientPool | None = None
    metadata_changed = False

    try:
        remote_context = resolve_remote_protection_context(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message=DEFAULT_REMOTE_PROTECTION_MISSING_SEED_MESSAGE,
            missing_origin_message=(
                "Falta node.advertise_addr. Push EC necesita identificar el nodo origen."
            ),
        )
        cluster = remote_context.cluster
        origin_node_id = remote_context.origin_node_id
        remote_candidate_count = remote_context.remote_candidate_count
        if remote_candidate_count < spec.total_shards:
            print("Push EC no iniciado: no hay suficientes nodos remotos elegibles.")
            print(
                f"   necesarios={spec.total_shards} disponibles={remote_candidate_count} "
                f"ec_k={spec.data_shards} ec_m={spec.parity_shards}"
            )
            return ErasurePushStats(
                insufficient_remote_targets=True,
                remote_candidates=remote_candidate_count,
                required_remote_targets=spec.total_shards,
            )

        placement_epoch = remote_context.placement_epoch(remote_targets=spec.total_shards)

        scope_label = describe_protection_scope(scope, snapshot_id=snapshot_id)
        retry_packs = scoped_erasure_push_retry_packs(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            current_epoch=placement_epoch,
            limit=limit,
        )
        pending_chunks = scoped_erasure_push_new_chunks(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            limit=limit,
        )
        if not retry_packs and not pending_chunks:
            print(f"No hay chunks ni data packs pendientes de protección EC para scope={scope_label}.")
            return ErasurePushStats(
                remote_candidates=remote_candidate_count,
                required_remote_targets=spec.total_shards,
            )

        print(
            f"Push EC: {len(retry_packs)} data packs pendientes de reintento y "
            f"{len(pending_chunks)} chunks nuevos pendientes scope={scope_label}"
        )
        print(f"Membership seed: {remote_context.seed}")
        print(f"Eligible members: {[f'{member.node_id[:8]}@{member.address}' for member in cluster.members]}")
        print(f"Self: {origin_node_id[:8]}@{self_addr}")
        print(
            f"EC: data_shards={spec.data_shards} parity_shards={spec.parity_shards} "
            f"total_shards={spec.total_shards} pack_size={pack_size}"
        )
        print(f"Remote candidates: {remote_candidate_count}")
        print(f"placement_epoch={placement_epoch[:12]}")

        pool = RemoteDataPackShardClientPool(
            timeout_s=stream_timeout_s,
            max_message_bytes=max_message_bytes,
        )
        builder = DataPackBuilder(spec=spec, target_size_bytes=pack_size)

        stats = _MutableErasurePushStats(
            attempted_chunks=len(pending_chunks),
            remote_candidates=remote_candidate_count,
            required_remote_targets=spec.total_shards,
        )

        for record in retry_packs:
            pack_chunk_count, pack_metadata_changed = _retry_existing_pack(
                record=record,
                db=db,
                repo=repo,
                pool=pool,
                cluster=cluster,
                origin_node_id=origin_node_id,
                cluster_token=cluster_token,
                remote_candidate_count=remote_candidate_count,
                stats=stats,
            )
            stats.attempted_chunks += pack_chunk_count
            metadata_changed = metadata_changed or pack_metadata_changed
            if pack_metadata_changed and stats.attempted_packs % commit_every == 0:
                db.commit()
                _print_progress(stats)

        for chunk_hash in pending_chunks:
            try:
                data = repo.get(chunk_hash)
            except FileNotFoundError:
                stats.missing_local_chunks += 1
                stats.failed_chunks += 1
                print(f"   {chunk_hash[:8]} omitido: no existe en CAS local")
                continue

            try:
                must_flush = builder.add_chunk(chunk_hash=chunk_hash, data=data)
            except ErasureCodingError as exc:
                stats.failed_chunks += 1
                print(f"   {chunk_hash[:8]} omitido: {exc}")
                continue

            stats.packed_chunks += 1
            if must_flush:
                _flush_and_push_pack(
                    builder=builder,
                    db=db,
                    pool=pool,
                    cluster=cluster,
                    origin_node_id=origin_node_id,
                    cluster_token=cluster_token,
                    placement_epoch=placement_epoch,
                    stats=stats,
                )
                metadata_changed = True
                if stats.attempted_packs % commit_every == 0:
                    db.commit()
                    _print_progress(stats)

        _flush_and_push_pack(
            builder=builder,
            db=db,
            pool=pool,
            cluster=cluster,
            origin_node_id=origin_node_id,
            cluster_token=cluster_token,
            placement_epoch=placement_epoch,
            stats=stats,
        )
        metadata_changed = metadata_changed or stats.attempted_packs > 0
        db.commit()

        return stats.freeze()

    except KeyboardInterrupt:
        print("\nPush EC interrumpido por el usuario.")
        db.commit()
        if "stats" in locals():
            return stats.freeze(interrupted=True)
        return ErasurePushStats(interrupted=True)

    finally:
        if pool is not None:
            pool.close()
        db.commit()
        db.close()

        export_after_successful_metadata_change(
            metadata_changed=metadata_changed,
            db_file=db_file,
            settings=metadata_object_graph_auto_export,
            context_label="PUSH_EC",
        )


def _flush_and_push_pack(
    *,
    builder: DataPackBuilder,
    db: MetadataDB,
    pool: RemoteDataPackShardClientPool,
    cluster,
    origin_node_id: str,
    cluster_token: str,
    placement_epoch: str,
    stats: "_MutableErasurePushStats",
) -> None:
    pack = builder.flush()
    if pack is None:
        return

    _push_pack(
        pack=pack,
        db=db,
        pool=pool,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
        placement_epoch=placement_epoch,
        stats=stats,
        refresh_existing=False,
    )


def _retry_existing_pack(
    *,
    record: ErasureDataPackRecord,
    db: MetadataDB,
    repo: CASRepository,
    pool: RemoteDataPackShardClientPool,
    cluster,
    origin_node_id: str,
    cluster_token: str,
    remote_candidate_count: int,
    stats: "_MutableErasurePushStats",
) -> tuple[int, bool]:
    chunks = db.get_erasure_pack_chunks(record.pack_hash)
    chunk_count = len(chunks)
    if not chunks:
        print(f"   pack={record.pack_hash[:8]} omitido: no tiene chunks registrados")
        return 0, False

    record_spec = spec_from_erasure_metadata(record)
    if remote_candidate_count < record_spec.total_shards:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        print(
            f"   pack={record.pack_hash[:8]} omitido: no hay suficientes nodos remotos "
            f"para reintento EC ({remote_candidate_count}/{record_spec.total_shards})"
        )
        return chunk_count, False

    record_placement_epoch = cluster.placement_epoch_excluding(
        desired_rf=record_spec.total_shards,
        cluster_token=cluster_token,
        excluded_node_ids={origin_node_id},
    )

    materialized_chunks: list[tuple[str, bytes]] = []
    missing_local = False
    for chunk in chunks:
        try:
            data = repo.get(chunk.chunk_hash)
        except FileNotFoundError:
            stats.missing_local_chunks += 1
            stats.failed_chunks += 1
            missing_local = True
            print(
                f"   pack={record.pack_hash[:8]} chunk={chunk.chunk_hash[:8]} "
                "omitido: no existe en CAS local"
            )
            continue
        materialized_chunks.append((chunk.chunk_hash, data))

    if missing_local:
        stats.failed_packs += 1
        return chunk_count, False

    try:
        pack = build_data_pack(chunks=materialized_chunks, spec=record_spec)
    except ErasureCodingError as exc:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        print(f"   pack={record.pack_hash[:8]} omitido: {exc}")
        return chunk_count, False

    if pack.pack_hash != record.pack_hash:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        print(
            f"   pack={record.pack_hash[:8]} omitido: el pack reconstruido no coincide "
            f"con metadata ({pack.pack_hash[:8]})"
        )
        return chunk_count, False

    stats.packed_chunks += len(pack.entries)
    _push_pack(
        pack=pack,
        db=db,
        pool=pool,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
        placement_epoch=record_placement_epoch,
        stats=stats,
        refresh_existing=True,
    )
    return chunk_count, True


def _push_pack(
    *,
    pack,
    db: MetadataDB,
    pool: RemoteDataPackShardClientPool,
    cluster,
    origin_node_id: str,
    cluster_token: str,
    placement_epoch: str,
    stats: "_MutableErasurePushStats",
    refresh_existing: bool,
) -> None:
    stats.attempted_packs += 1
    placements = plan_data_pack_shard_placement(
        pack_hash=pack.pack_hash,
        spec=pack.spec,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
    )

    push_result = push_data_pack_shards(
        pack=pack,
        placements=placements,
        pool=pool,
    )
    stats.stored_shards += push_result.stored_shards
    stats.already_present_shards += push_result.already_present_shards
    stats.failed_shards += push_result.failed_shards

    for address, result in push_result.failed_results:
        print(
            f"   pack={pack.pack_hash[:8]} shard={result.ref.shard_index} "
            f"falló en {address}: {result.detail}"
        )

    protection_state = data_pack_state(
        protected_shards=len(push_result.successful_indexes),
        data_shards=pack.spec.data_shards,
        total_shards=pack.spec.total_shards,
    )
    metadata_writer = (
        refresh_data_pack_push_metadata
        if refresh_existing
        else register_data_pack_metadata
    )
    metadata_writer(
        db=db,
        pack=pack,
        placements=placements,
        protection_state=protection_state,
        placement_epoch=placement_epoch,
    )

    _apply_pack_state_to_stats(
        pack_hash=pack.pack_hash,
        chunk_count=len(pack.entries),
        protected_shards=len(push_result.successful_indexes),
        total_shards=pack.spec.total_shards,
        protection_state=protection_state,
        stats=stats,
    )


def _apply_pack_state_to_stats(
    *,
    pack_hash: str,
    chunk_count: int,
    protected_shards: int,
    total_shards: int,
    protection_state: ProtectionState,
    stats: "_MutableErasurePushStats",
) -> None:
    if protection_state == ProtectionState.PLACED:
        stats.placed_packs += 1
        stats.placed_chunks += chunk_count
    elif protection_state == ProtectionState.DEGRADED:
        stats.degraded_packs += 1
        stats.degraded_chunks += chunk_count
        print(
            f"   pack={pack_hash[:8]} DEGRADED: "
            f"shards={protected_shards}/{total_shards}"
        )
    else:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        print(
            f"   pack={pack_hash[:8]} FAILED: "
            f"shards={protected_shards}/{total_shards}"
        )


def _print_progress(stats: "_MutableErasurePushStats") -> None:
    print(
        f"   progress packs={stats.attempted_packs} chunks={stats.packed_chunks} | "
        f"placed_packs={stats.placed_packs} degraded_packs={stats.degraded_packs} "
        f"failed_packs={stats.failed_packs} | stored_shards={stats.stored_shards} "
        f"already_present_shards={stats.already_present_shards}"
    )


@dataclass
class _MutableErasurePushStats:
    attempted_chunks: int = 0
    packed_chunks: int = 0
    placed_chunks: int = 0
    degraded_chunks: int = 0
    failed_chunks: int = 0
    missing_local_chunks: int = 0
    attempted_packs: int = 0
    placed_packs: int = 0
    degraded_packs: int = 0
    failed_packs: int = 0
    stored_shards: int = 0
    already_present_shards: int = 0
    failed_shards: int = 0
    remote_candidates: int = 0
    required_remote_targets: int = 0

    def freeze(self, *, interrupted: bool = False) -> ErasurePushStats:
        return ErasurePushStats(
            attempted_chunks=self.attempted_chunks,
            packed_chunks=self.packed_chunks,
            placed_chunks=self.placed_chunks,
            degraded_chunks=self.degraded_chunks,
            failed_chunks=self.failed_chunks,
            missing_local_chunks=self.missing_local_chunks,
            attempted_packs=self.attempted_packs,
            placed_packs=self.placed_packs,
            degraded_packs=self.degraded_packs,
            failed_packs=self.failed_packs,
            stored_shards=self.stored_shards,
            already_present_shards=self.already_present_shards,
            failed_shards=self.failed_shards,
            remote_candidates=self.remote_candidates,
            required_remote_targets=self.required_remote_targets,
            interrupted=interrupted,
        )
