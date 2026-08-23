from __future__ import annotations

from dataclasses import dataclass

from stopan.cas.repository import CASRepository, CASRepositoryError
from stopan.metadata.database import (
    ErasureDataPackPushErrorUpdate,
    ErasureDataPackPushUpdate,
    ErasureDataPackRecord,
    MetadataDB,
    MetadataDBAccessMode,
)
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_after_successful_metadata_change,
)
from stopan.protection.ec.metadata_adapter import spec_from_erasure_metadata
from stopan.protection.ec.models import (
    ErasureCodingConfigError,
    ErasureCodingError,
    ErasureSpec,
)
from stopan.protection.ec.packer import (
    DataPackBuilder,
    _build_data_pack_from_validated_chunks,
)
from stopan.protection.ec.placement import plan_data_pack_shard_placement
from stopan.protection.ec.push_execution import (
    build_data_pack_push_update,
    push_data_pack_shards,
    register_data_pack_metadata,
    validate_data_pack_delivery_limits,
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
    processed_bytes: int = 0
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
    target_parallelism: int,
    stream_timeout_s: float,
    max_message_bytes: int,
    max_shard_size: int,
    commit_every: int,
    db_file: str,
    local_chunk_dir: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
    scope: str | None = None,
    snapshot_id: int | None = None,
) -> ErasurePushStats:
    spec = ErasureSpec(data_shards=int(ec_k), parity_shards=int(ec_m))
    pack_size = max(int(ec_pack_size_bytes), 1)
    target_parallelism = int(target_parallelism)
    max_shard_size = int(max_shard_size)
    if target_parallelism < 1:
        raise ErasureCodingConfigError("ec target_parallelism debe ser >= 1")
    if max_shard_size < 1:
        raise ErasureCodingConfigError("storage.max_chunk_size debe ser >= 1")
    commit_every = max(int(commit_every), 1)
    self_addr = str(self_addr or "").strip()
    cluster_token = str(cluster_token or "")

    repo = CASRepository(local_chunk_dir)
    db = MetadataDB(db_file, access_mode=MetadataDBAccessMode.READ_WRITE)
    pool: RemoteDataPackShardClientPool | None = None
    metadata_changed = False
    pending_updates: list[ErasureDataPackPushUpdate] = []
    pending_error_updates: list[ErasureDataPackPushErrorUpdate] = []

    def flush_push_updates() -> None:
        nonlocal metadata_changed
        if not pending_updates and not pending_error_updates:
            return
        db.apply_erasure_data_pack_push_result_batch(
            pending_updates,
            pending_error_updates,
        )
        pending_updates.clear()
        pending_error_updates.clear()
        metadata_changed = True

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
        new_pack_placement_epoch = remote_context.placement_epoch(remote_targets=spec.total_shards)
        existing_epochs = {
            remote_targets: remote_context.placement_epoch(remote_targets=remote_targets)
            for remote_targets in db.erasure_data_pack_remote_targets()
        }

        scope_label = describe_protection_scope(scope, snapshot_id=snapshot_id)
        retry_packs = scoped_erasure_push_retry_packs(
            db,
            scope=scope,
            snapshot_id=snapshot_id,
            current_epochs=existing_epochs,
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
            f"total_shards={spec.total_shards} pack_size={pack_size} "
            f"target_parallelism={target_parallelism}"
        )
        print(f"Remote candidates: {remote_candidate_count}")
        print(f"new_pack_placement_epoch={new_pack_placement_epoch[:12]}")

        required_remote_targets = max(
            [
                *(pack.data_shards + pack.parity_shards for pack in retry_packs),
                *([spec.total_shards] if pending_chunks else []),
            ],
            default=0,
        )
        if remote_candidate_count < required_remote_targets:
            print("Push EC no iniciado: no hay suficientes nodos remotos elegibles.")
            print(
                f"   necesarios={required_remote_targets} "
                f"disponibles={remote_candidate_count}"
            )
            return ErasurePushStats(
                insufficient_remote_targets=True,
                remote_candidates=remote_candidate_count,
                required_remote_targets=required_remote_targets,
            )

        pool = RemoteDataPackShardClientPool(
            cluster_token=cluster_token,
            timeout_s=stream_timeout_s,
            max_message_bytes=max_message_bytes,
        )
        builder = (
            DataPackBuilder(spec=spec, target_size_bytes=pack_size)
            if pending_chunks
            else None
        )

        stats = _MutableErasurePushStats(
            attempted_chunks=len(pending_chunks),
            remote_candidates=remote_candidate_count,
            required_remote_targets=required_remote_targets,
        )

        for record in retry_packs:
            pack_chunk_count, retry_update = _retry_existing_pack(
                record=record,
                db=db,
                repo=repo,
                pool=pool,
                cluster=cluster,
                origin_node_id=origin_node_id,
                cluster_token=cluster_token,
                target_parallelism=target_parallelism,
                max_shard_size=max_shard_size,
                stats=stats,
            )
            stats.attempted_chunks += pack_chunk_count
            if isinstance(retry_update, ErasureDataPackPushErrorUpdate):
                pending_error_updates.append(retry_update)
            elif retry_update is not None:
                pending_updates.append(retry_update)
            if len(pending_updates) + len(pending_error_updates) >= commit_every:
                flush_push_updates()

        if builder is not None:
            for chunk_hash in pending_chunks:
                try:
                    data = repo.get(chunk_hash)
                except FileNotFoundError:
                    stats.missing_local_chunks += 1
                    stats.failed_chunks += 1
                    continue
                except CASRepositoryError:
                    stats.failed_chunks += 1
                    continue

                if builder.would_exceed_target(data_size=len(data)):
                    push_update = _flush_and_push_pack(
                        builder=builder,
                        db=db,
                        pool=pool,
                        cluster=cluster,
                        origin_node_id=origin_node_id,
                        cluster_token=cluster_token,
                        placement_epoch=new_pack_placement_epoch,
                        target_parallelism=target_parallelism,
                        max_shard_size=max_shard_size,
                        stats=stats,
                    )
                    if push_update is not None:
                        metadata_changed = True
                        pending_updates.append(push_update)
                        if len(pending_updates) >= commit_every:
                            flush_push_updates()

                try:
                    must_flush = builder._add_prevalidated_chunk(chunk_hash=chunk_hash, data=data)
                except ErasureCodingError:
                    stats.failed_chunks += 1
                    continue

                stats.packed_chunks += 1
                stats.processed_bytes += len(data)
                if must_flush:
                    push_update = _flush_and_push_pack(
                        builder=builder,
                        db=db,
                        pool=pool,
                        cluster=cluster,
                        origin_node_id=origin_node_id,
                        cluster_token=cluster_token,
                        placement_epoch=new_pack_placement_epoch,
                        target_parallelism=target_parallelism,
                        max_shard_size=max_shard_size,
                        stats=stats,
                    )
                    if push_update is not None:
                        metadata_changed = True
                        pending_updates.append(push_update)
                        if len(pending_updates) >= commit_every:
                            flush_push_updates()

        if builder is not None:
            push_update = _flush_and_push_pack(
                builder=builder,
                db=db,
                pool=pool,
                cluster=cluster,
                origin_node_id=origin_node_id,
                cluster_token=cluster_token,
                placement_epoch=new_pack_placement_epoch,
                target_parallelism=target_parallelism,
                max_shard_size=max_shard_size,
                stats=stats,
            )
            if push_update is not None:
                metadata_changed = True
                pending_updates.append(push_update)

        flush_push_updates()

        return stats.freeze()

    except KeyboardInterrupt:
        print("\nPush EC interrumpido por el usuario.")
        flush_push_updates()
        if "stats" in locals():
            return stats.freeze(interrupted=True)
        return ErasurePushStats(interrupted=True)

    except BaseException:
        try:
            flush_push_updates()
        except Exception:
            pass
        raise

    finally:
        if pool is not None:
            pool.close()
        if "stats" in locals() and stats.attempted_packs > 0:
            metadata_changed = True
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
    target_parallelism: int,
    max_shard_size: int,
    stats: "_MutableErasurePushStats",
) -> ErasureDataPackPushUpdate | None:
    pack = builder.flush()
    if pack is None:
        return None

    return _push_pack(
        pack=pack,
        db=db,
        pool=pool,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
        placement_epoch=placement_epoch,
        target_parallelism=target_parallelism,
        max_shard_size=max_shard_size,
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
    target_parallelism: int,
    max_shard_size: int,
    stats: "_MutableErasurePushStats",
) -> tuple[int, ErasureDataPackPushUpdate | ErasureDataPackPushErrorUpdate | None]:
    chunks = db.get_erasure_pack_chunks(record.pack_hash)
    chunk_count = len(chunks)
    if not chunks:
        stats.failed_packs += 1
        return (
            0,
            _pack_push_error(
                record.pack_hash,
                "reintento EC imposible: el paquete no tiene chunks registrados",
            ),
        )

    record_spec = spec_from_erasure_metadata(record)
    record_placement_epoch = cluster.placement_epoch_excluding(
        desired_rf=record_spec.total_shards,
        cluster_token=cluster_token,
        excluded_node_ids={origin_node_id},
    )

    materialized_chunks: list[tuple[str, bytes]] = []
    local_errors: list[str] = []
    for chunk in chunks:
        try:
            data = repo.get(chunk.chunk_hash)
        except FileNotFoundError:
            stats.missing_local_chunks += 1
            stats.failed_chunks += 1
            local_errors.append(f"chunk local ausente: {chunk.chunk_hash}")
            continue
        except CASRepositoryError as exc:
            stats.failed_chunks += 1
            local_errors.append(f"chunk local ilegible {chunk.chunk_hash}: {exc}")
            continue
        materialized_chunks.append((chunk.chunk_hash, data))

    if local_errors:
        stats.failed_packs += 1
        return (
            chunk_count,
            _pack_push_error(record.pack_hash, "; ".join(local_errors)),
        )

    try:
        pack = _build_data_pack_from_validated_chunks(
            chunks=tuple(materialized_chunks),
            spec=record_spec,
        )
    except ErasureCodingError as exc:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        return (
            chunk_count,
            _pack_push_error(
                record.pack_hash,
                f"reintento EC: no se pudo reconstruir el paquete: {exc}",
            ),
        )

    if pack.pack_hash != record.pack_hash:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        return (
            chunk_count,
            _pack_push_error(
                record.pack_hash,
                "reintento EC: la reconstrucción no coincide con el pack_hash registrado",
            ),
        )

    stats.packed_chunks += len(pack.entries)
    stats.processed_bytes += sum(len(data) for _, data in materialized_chunks)
    push_update = _push_pack(
        pack=pack,
        db=db,
        pool=pool,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
        placement_epoch=record_placement_epoch,
        target_parallelism=target_parallelism,
        max_shard_size=max_shard_size,
        stats=stats,
        refresh_existing=True,
    )
    return chunk_count, push_update


def _pack_push_error(pack_hash: str, error: str) -> ErasureDataPackPushErrorUpdate:
    return ErasureDataPackPushErrorUpdate(
        pack_hash=pack_hash,
        error=str(error)[:1800],
    )


def _push_pack(
    *,
    pack,
    db: MetadataDB,
    pool: RemoteDataPackShardClientPool,
    cluster,
    origin_node_id: str,
    cluster_token: str,
    placement_epoch: str,
    target_parallelism: int,
    max_shard_size: int,
    stats: "_MutableErasurePushStats",
    refresh_existing: bool,
) -> ErasureDataPackPushUpdate:
    validate_data_pack_delivery_limits(
        pack=pack,
        max_shard_size=max_shard_size,
        max_message_bytes=pool.max_message_bytes,
    )
    stats.attempted_packs += 1
    placements = plan_data_pack_shard_placement(
        pack_hash=pack.pack_hash,
        spec=pack.spec,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
    )

    if not refresh_existing:
        register_data_pack_metadata(
            db=db,
            pack=pack,
            placements=placements,
            protection_state=ProtectionState.PENDING,
            placement_epoch=placement_epoch,
        )

    push_result = push_data_pack_shards(
        pack=pack,
        placements=placements,
        pool=pool,
        target_parallelism=target_parallelism,
    )
    stats.stored_shards += push_result.stored_shards
    stats.already_present_shards += push_result.already_present_shards
    stats.failed_shards += push_result.failed_shards

    protection_state = data_pack_state(
        protected_shards=len(push_result.successful_indexes),
        data_shards=pack.spec.data_shards,
        total_shards=pack.spec.total_shards,
    )
    push_update = build_data_pack_push_update(
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
    return push_update


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
    else:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count



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
    processed_bytes: int = 0
    remote_candidates: int = 0
    required_remote_targets: int = 0
    insufficient_remote_targets: bool = False

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
            processed_bytes=self.processed_bytes,
            remote_candidates=self.remote_candidates,
            required_remote_targets=self.required_remote_targets,
            insufficient_remote_targets=self.insufficient_remote_targets,
            interrupted=interrupted,
        )
