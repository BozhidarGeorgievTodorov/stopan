from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import sys

from stopan.cas.repository import CASRepository
from stopan.errors import StopanConfigRuntimeError
from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_metadata_object_graph_after_metadata_change,
)
from stopan.placement.cluster_resolver import require_cluster_view
from stopan.protection.ec.models import DataPackShard, ErasureCodingError, ErasureSpec
from stopan.protection.ec.packer import DataPackBuilder
from stopan.protection.ec.placement import plan_data_pack_shard_placement
from stopan.protection.ec.remote_client import (
    RemoteDataPackShardClientPool,
    RemoteDataPackShardPayload,
    RemoteDataPackShardRef,
)
from stopan.protection.policy import ProtectionState


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
) -> ErasurePushStats:
    spec = ErasureSpec(data_shards=int(ec_k), parity_shards=int(ec_m))
    pack_size = max(int(ec_pack_size_bytes), 1)
    commit_every = max(int(commit_every), 1)
    self_addr = str(self_addr or "").strip()
    cluster_token = str(cluster_token or "")

    if not self_addr:
        raise StopanConfigRuntimeError(
            "Falta node.advertise_addr. Push EC necesita identificar el nodo origen."
        )

    repo = CASRepository(local_shard_dir)
    db = MetadataDB(db_file)
    pool: RemoteDataPackShardClientPool | None = None
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
        cluster = resolved.cluster
        origin_node_id = cluster.self_node_id
        if not origin_node_id:
            raise StopanConfigRuntimeError(
                "No pude resolver origin_node_id desde membership. "
                "Asegúrate de que node.advertise_addr coincide con un miembro elegible."
            )

        remote_candidate_node_ids = cluster.candidate_node_ids_excluding({origin_node_id})
        remote_candidate_count = len(remote_candidate_node_ids)
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

        placement_epoch = cluster.placement_epoch_excluding(
            desired_rf=spec.total_shards,
            cluster_token=cluster_token,
            excluded_node_ids={origin_node_id},
        )

        pending_chunks = db.get_erasure_unprotected_chunks(limit=limit)
        if not pending_chunks:
            print("No hay chunks pendientes de protección EC.")
            return ErasurePushStats(
                remote_candidates=remote_candidate_count,
                required_remote_targets=spec.total_shards,
            )

        print(f"Push EC: {len(pending_chunks)} chunks pendientes de protection packs")
        print(f"Membership seed: {resolved.seed}")
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

        if (
            metadata_changed
            and metadata_object_graph_auto_export is not None
            and metadata_object_graph_auto_export.enabled
            and sys.exc_info()[0] is None
        ):
            export_metadata_object_graph_after_metadata_change(
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

    stats.attempted_packs += 1
    placements = plan_data_pack_shard_placement(
        pack_hash=pack.pack_hash,
        spec=pack.spec,
        cluster=cluster,
        origin_node_id=origin_node_id,
        cluster_token=cluster_token,
    )
    placement_by_index = {item.shard_index: item for item in placements}
    payloads_by_address: dict[str, list[RemoteDataPackShardPayload]] = defaultdict(list)

    for shard in pack.shards:
        placement = placement_by_index[shard.shard_index]
        ref = RemoteDataPackShardRef(
            pack_hash=shard.pack_hash,
            shard_index=shard.shard_index,
            shard_hash=shard.shard_hash,
        )
        payloads_by_address[placement.address].append(
            RemoteDataPackShardPayload(ref=ref, data=shard.data)
        )

    successful_indexes: set[int] = set()

    for address, payloads in sorted(payloads_by_address.items()):
        refs = [item.ref for item in payloads]
        missing_refs = pool.probe_missing_shards(addr=address, refs=refs)
        missing_keys = {(ref.pack_hash, ref.shard_index, ref.shard_hash) for ref in missing_refs}

        already_present_here = len(refs) - len(missing_refs)
        stats.already_present_shards += already_present_here
        for ref in refs:
            key = (ref.pack_hash, ref.shard_index, ref.shard_hash)
            if key not in missing_keys:
                successful_indexes.add(ref.shard_index)

        if not missing_refs:
            continue

        payloads_to_send = [
            item for item in payloads
            if (item.ref.pack_hash, item.ref.shard_index, item.ref.shard_hash) in missing_keys
        ]

        for result in pool.replicate_shards(addr=address, shards=payloads_to_send):
            if result.is_success(pool.store_status_stored, pool.store_status_already_present):
                successful_indexes.add(result.ref.shard_index)
                if result.status == pool.store_status_stored:
                    stats.stored_shards += 1
                else:
                    stats.already_present_shards += 1
            else:
                stats.failed_shards += 1
                print(
                    f"   pack={pack.pack_hash[:8]} shard={result.ref.shard_index} "
                    f"falló en {address}: {result.detail}"
                )

    protection_state = _pack_state(
        protected_shards=len(successful_indexes),
        data_shards=pack.spec.data_shards,
        total_shards=pack.spec.total_shards,
    )
    _register_pack_metadata(
        db=db,
        pack=pack,
        placements=placements,
        protection_state=protection_state,
        placement_epoch=placement_epoch,
    )

    chunk_count = len(pack.entries)
    if protection_state == ProtectionState.PLACED:
        stats.placed_packs += 1
        stats.placed_chunks += chunk_count
    elif protection_state == ProtectionState.DEGRADED:
        stats.degraded_packs += 1
        stats.degraded_chunks += chunk_count
        print(
            f"   pack={pack.pack_hash[:8]} DEGRADED: "
            f"shards={len(successful_indexes)}/{pack.spec.total_shards}"
        )
    else:
        stats.failed_packs += 1
        stats.failed_chunks += chunk_count
        print(
            f"   pack={pack.pack_hash[:8]} FAILED: "
            f"shards={len(successful_indexes)}/{pack.spec.total_shards}"
        )


def _register_pack_metadata(
    *,
    db: MetadataDB,
    pack,
    placements,
    protection_state: ProtectionState,
    placement_epoch: str,
) -> None:
    placement_by_index = {item.shard_index: item for item in placements}
    db.register_erasure_data_pack(
        pack_hash=pack.pack_hash,
        codec=pack.spec.codec,
        data_shards=pack.spec.data_shards,
        parity_shards=pack.spec.parity_shards,
        payload_size=pack.payload_size,
        padded_size=pack.padded_size,
        shard_size=pack.shard_size,
        chunks=[
            (entry.chunk_hash, entry.offset, entry.length, entry.ordinal)
            for entry in pack.entries
        ],
        shards=[
            (
                shard.shard_index,
                shard.shard_hash,
                placement_by_index[shard.shard_index].node_id,
                len(shard.data),
            )
            for shard in pack.shards
        ],
        protection_state=protection_state,
        placement_epoch=placement_epoch,
    )


def _pack_state(
    *,
    protected_shards: int,
    data_shards: int,
    total_shards: int,
) -> ProtectionState:
    if protected_shards >= total_shards:
        return ProtectionState.PLACED
    if protected_shards >= data_shards:
        return ProtectionState.DEGRADED
    return ProtectionState.FAILED


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
