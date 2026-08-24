"""
Placement determinista de shards EC sobre nodos remotos.

Cada shard de un data pack debe colocarse en un nodo distinto. El planner usa
HRW sobre pack_hash y excluye el nodo origen para conservar la semántica P2P
remota del proyecto.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.cluster.view import ClusterMember, ClusterView

from .models import (
    ErasureCodingConfigError,
    ErasureCodingError,
    ErasureCodingNetworkError,
    ErasureSpec,
    require_hash64,
    require_non_negative_int,
)


@dataclass(frozen=True, slots=True)
class DataPackShardPlacement:
    pack_hash: str
    shard_index: int
    node_id: str
    address: str

    def __post_init__(self) -> None:
        pack_hash = require_hash64("pack_hash", self.pack_hash)
        shard_index = require_non_negative_int("shard_index", self.shard_index)
        node_id = str(self.node_id or "").strip()
        address = str(self.address or "").strip()

        if not node_id:
            raise ErasureCodingNetworkError("node_id no puede estar vacío")
        if not address:
            raise ErasureCodingNetworkError("address no puede estar vacío")

        object.__setattr__(self, "pack_hash", pack_hash)
        object.__setattr__(self, "shard_index", shard_index)
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "address", address)


def plan_data_pack_shard_placement(
    *,
    pack_hash: str,
    spec: ErasureSpec,
    cluster: ClusterView,
    origin_node_id: str,
    cluster_token: str,
) -> tuple[DataPackShardPlacement, ...]:
    pack_hash = require_hash64("pack_hash", pack_hash)
    if not isinstance(spec, ErasureSpec):
        raise ErasureCodingError("spec debe ser ErasureSpec")
    if not isinstance(cluster, ClusterView):
        raise ErasureCodingError("cluster debe ser ClusterView")

    origin_node_id = str(origin_node_id or "").strip()
    if not origin_node_id:
        raise ErasureCodingConfigError("origin_node_id no puede estar vacío")
    cluster_self_node_id = str(cluster.self_node_id or "").strip()
    if origin_node_id not in cluster.node_ids and origin_node_id != cluster_self_node_id:
        raise ErasureCodingConfigError(
            "origin_node_id no pertenece a la vista de membership ni coincide con "
            "ClusterView.self_node_id; usa la identidad canónica del nodo origen "
            "al colocar shards EC"
        )

    members = cluster.hrw_targets_excluding(
        pack_hash,
        rf=spec.total_shards,
        salt=str(cluster_token or ""),
        excluded_node_ids={origin_node_id},
    )
    if len(members) < spec.total_shards:
        raise ErasureCodingNetworkError(
            "no hay suficientes nodos remotos para colocar shards EC: "
            f"necesarios={spec.total_shards} disponibles={len(members)}"
        )

    placements = tuple(
        _placement_for_member(
            pack_hash=pack_hash,
            shard_index=shard_index,
            member=member,
        )
        for shard_index, member in enumerate(members)
    )
    _require_distinct_nodes(placements)
    return placements


def _placement_for_member(
    *,
    pack_hash: str,
    shard_index: int,
    member: ClusterMember,
) -> DataPackShardPlacement:
    return DataPackShardPlacement(
        pack_hash=pack_hash,
        shard_index=shard_index,
        node_id=member.node_id,
        address=member.address,
    )


def _require_distinct_nodes(placements: tuple[DataPackShardPlacement, ...]) -> None:
    node_ids = [item.node_id for item in placements]
    if len(node_ids) != len(set(node_ids)):
        raise ErasureCodingError("los shards EC de un pack deben ir a nodos distintos")

