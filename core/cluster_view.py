from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

import grpc

from protos import membership_pb2
from protos import membership_pb2_grpc

from core.placement import hrw_top_k_node_ids
from core.protection import compute_placement_epoch


DEFAULT_MEMBERSHIP_TIMEOUT_S = float(os.getenv("MEMBERSHIP_TIMEOUT_S", "2.0"))
GRPC_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


@dataclass(frozen=True)
class ClusterMember:
    node_id: str
    address: str


@dataclass(frozen=True)
class ClusterView:
    """
    Vista canónica de los miembros elegibles para placement.
    """

    self_node_id: str | None
    members: tuple[ClusterMember, ...]

    @property
    def node_ids(self) -> list[str]:
        return [member.node_id for member in self.members]

    @property
    def node_addresses(self) -> dict[str, str]:
        return {member.node_id: member.address for member in self.members}

    def _candidate_members_excluding(
        self,
        excluded_node_ids: Iterable[str],
    ) -> list[ClusterMember]:
        excluded = {str(node_id) for node_id in excluded_node_ids if node_id}
        return [
            member
            for member in self.members
            if member.node_id and member.node_id not in excluded
        ]

    def candidate_node_ids_excluding(self, excluded_node_ids: Iterable[str]) -> list[str]:
        return [
            member.node_id
            for member in self._candidate_members_excluding(excluded_node_ids)
        ]

    def hrw_targets(self, chunk_hash: str, *, rf: int, salt: str) -> list[ClusterMember]:
        if not self.members:
            return []

        k = min(max(int(rf), 1), len(self.members))
        ranked_node_ids = hrw_top_k_node_ids(chunk_hash, self.node_ids, k=k, salt=salt)
        address_map = self.node_addresses

        return [
            ClusterMember(node_id=node_id, address=address_map[node_id])
            for node_id in ranked_node_ids
            if node_id in address_map
        ]

    def hrw_targets_excluding(
        self,
        chunk_hash: str,
        *,
        rf: int,
        salt: str,
        excluded_node_ids: Iterable[str],
    ) -> list[ClusterMember]:
        candidates = self._candidate_members_excluding(excluded_node_ids)
        if not candidates:
            return []

        k = min(max(int(rf), 1), len(candidates))
        candidate_node_ids = [member.node_id for member in candidates]
        ranked_node_ids = hrw_top_k_node_ids(
            chunk_hash,
            candidate_node_ids,
            k=k,
            salt=salt,
        )
        address_map = {member.node_id: member.address for member in candidates}

        return [
            ClusterMember(node_id=node_id, address=address_map[node_id])
            for node_id in ranked_node_ids
            if node_id in address_map
        ]

    def hrw_remote_targets(self, chunk_hash: str, *, rf: int, salt: str) -> list[ClusterMember]:
        """
        Targets remotos respecto al self actual.
        """
        excluded = {self.self_node_id} if self.self_node_id else set()
        return self.hrw_targets_excluding(
            chunk_hash,
            rf=rf,
            salt=salt,
            excluded_node_ids=excluded,
        )

    def placement_epoch(self, *, desired_rf: int, cluster_token: str) -> str:
        return compute_placement_epoch(
            cluster_token=cluster_token,
            desired_rf=desired_rf,
            eligible_node_ids=self.node_ids,
        )

    def placement_epoch_excluding(
        self,
        *,
        desired_rf: int,
        cluster_token: str,
        excluded_node_ids: Iterable[str],
    ) -> str:
        return compute_placement_epoch(
            cluster_token=cluster_token,
            desired_rf=desired_rf,
            eligible_node_ids=self.candidate_node_ids_excluding(excluded_node_ids),
        )


class ClusterMembershipClient:
    """
    Cliente de membership para obtener una vista de cluster apta para placement.
    """

    def __init__(
        self,
        seed_addr: str,
        *,
        self_addr: str = "",
        timeout_s: float = DEFAULT_MEMBERSHIP_TIMEOUT_S,
    ):
        seed_addr = seed_addr.strip()
        if not seed_addr:
            raise ValueError("ClusterMembershipClient requires a non-empty seed_addr")

        self.seed_addr = seed_addr
        self.self_addr = self_addr.strip()
        self.timeout_s = float(timeout_s)

    def get_cluster_view(self) -> ClusterView:
        channel = grpc.insecure_channel(
            self.seed_addr,
            options=[
                ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_BYTES),
                ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES),
            ],
        )

        try:
            stub = membership_pb2_grpc.MembershipStub(channel)
            response = stub.GetMembers(
                membership_pb2.GetMembersRequest(),
                timeout=self.timeout_s,
            )
        finally:
            channel.close()

        members_by_node_id: dict[str, ClusterMember] = {}
        for member in response.members:
            if not member.node_id or not member.address:
                continue
            members_by_node_id[member.node_id] = ClusterMember(
                node_id=member.node_id,
                address=member.address,
            )

        members = tuple(members_by_node_id.values())

        self_node_id = None
        if self.self_addr:
            for member in members:
                if member.address == self.self_addr:
                    self_node_id = member.node_id
                    break

        return ClusterView(self_node_id=self_node_id, members=members)