"""
Placement HRW para replicación clásica de chunks completos.

La replicación clásica coloca copias completas en nodos remotos. Este módulo
solo calcula targets y agrupaciones por dirección; no abre red ni toca metadata.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from stopan.common.sequences import ordered_unique
from stopan.errors import StopanConfigValueError
from stopan.cluster.view import ClusterMember, ClusterView
from stopan.placement.hrw import hrw_top_k_node_ids
from stopan.protection.policy import normalize_remote_rf


@dataclass(frozen=True, slots=True)
class ReplicationTargetPlan:
    chunk_targets: dict[str, tuple[ClusterMember, ...]]
    target_chunks: dict[str, tuple[str, ...]]
    target_members: dict[str, ClusterMember]


def select_remote_chunk_targets(
    *,
    cluster: ClusterView,
    chunk_hash: str,
    required_remote_copies: int,
    cluster_token: str,
    origin_node_id: str,
) -> tuple[ClusterMember, ...]:
    required_remote_copies = normalize_remote_rf(
        required_remote_copies,
        field_name="required_remote_copies",
    )
    if required_remote_copies == 0:
        return ()

    return tuple(
        cluster.hrw_targets_excluding(
            chunk_hash,
            rf=required_remote_copies,
            salt=str(cluster_token or ""),
            excluded_node_ids=_excluded_origin(origin_node_id),
        )
    )


def plan_chunk_replication_targets(
    *,
    cluster: ClusterView,
    chunk_hashes: Sequence[str] | Iterable[str],
    required_remote_copies: int,
    cluster_token: str,
    origin_node_id: str,
) -> ReplicationTargetPlan:
    ordered_hashes = ordered_unique(chunk_hashes)
    required_remote_copies = normalize_remote_rf(
        required_remote_copies,
        field_name="required_remote_copies",
    )
    excluded = _excluded_origin(origin_node_id)
    candidates = tuple(
        member
        for member in cluster.members
        if member.node_id and member.node_id not in excluded
    )
    k = min(required_remote_copies, len(candidates))
    candidate_node_ids = tuple(member.node_id for member in candidates)
    member_by_id = {member.node_id: member for member in candidates}
    salt = str(cluster_token or "")

    chunk_targets: dict[str, tuple[ClusterMember, ...]] = {}
    target_chunks: dict[str, list[str]] = defaultdict(list)
    target_members: dict[str, ClusterMember] = {}

    for chunk_hash in ordered_hashes:
        if k <= 0:
            remote_targets = ()
        else:
            remote_targets = tuple(
                member_by_id[node_id]
                for node_id in hrw_top_k_node_ids(
                    chunk_hash,
                    candidate_node_ids,
                    k=k,
                    salt=salt,
                )
            )
        chunk_targets[chunk_hash] = remote_targets

        for member in remote_targets:
            target_chunks[member.address].append(chunk_hash)
            target_members[member.address] = member

    return ReplicationTargetPlan(
        chunk_targets=chunk_targets,
        target_chunks={address: tuple(hashes) for address, hashes in target_chunks.items()},
        target_members=target_members,
    )


def _excluded_origin(origin_node_id: str) -> frozenset[str]:
    origin_node_id = str(origin_node_id or "").strip()
    if not origin_node_id:
        raise StopanConfigValueError("origin_node_id no puede estar vacío para placement remoto.")
    return frozenset({origin_node_id})
