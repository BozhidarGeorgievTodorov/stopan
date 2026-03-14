from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import grpc

from protos import membership_pb2
from protos import membership_pb2_grpc

from core.placement import hrw_top_k_node_ids
from core.protection import compute_placement_epoch


DEFAULT_MEMBERSHIP_TIMEOUT_S = float(os.getenv("MEMBERSHIP_TIMEOUT_S", "2.0"))


@dataclass(frozen=True)
class ClusterMember:
    node_id: str
    address: str


@dataclass(frozen=True)
class ClusterView:
    self_node_id: Optional[str]
    members: List[ClusterMember]

    @property
    def node_ids(self) -> List[str]:
        return [member.node_id for member in self.members]

    @property
    def node_addr(self) -> Dict[str, str]:
        return {member.node_id: member.address for member in self.members}

    def hrw_targets(self, chunk_hash: str, *, rf: int, salt: str) -> List[ClusterMember]:
        if not self.members:
            return []

        k = min(max(rf, 1), len(self.members))
        ranked_ids = hrw_top_k_node_ids(chunk_hash, self.node_ids, k=k, salt=salt)
        addr_map = self.node_addr
        return [
            ClusterMember(node_id=node_id, address=addr_map[node_id])
            for node_id in ranked_ids
            if node_id in addr_map
        ]

    def hrw_remote_targets(self, chunk_hash: str, *, rf: int, salt: str) -> List[ClusterMember]:
        targets = self.hrw_targets(chunk_hash, rf=rf, salt=salt)
        if not self.self_node_id:
            return targets
        return [member for member in targets if member.node_id != self.self_node_id]

    def placement_epoch(self, *, desired_rf: int, cluster_token: str) -> str:
        return compute_placement_epoch(
            cluster_token=cluster_token,
            desired_rf=desired_rf,
            eligible_node_ids=self.node_ids,
        )


class ClusterMembershipClient:
    """
    Cliente para obtener una vista de nodos elegibles desde membership.
    """

    def __init__(self, seed_addr: str, *, self_addr: str = "", timeout_s: float = DEFAULT_MEMBERSHIP_TIMEOUT_S):
        self.seed_addr = seed_addr
        self.self_addr = self_addr.strip()
        self.timeout_s = timeout_s

    def get_cluster_view(self) -> ClusterView:
        channel = grpc.insecure_channel(self.seed_addr)
        try:
            stub = membership_pb2_grpc.MembershipStub(channel)
            response = stub.GetMembers(membership_pb2.GetMembersRequest(), timeout=self.timeout_s)
        finally:
            channel.close()

        members = [
            ClusterMember(node_id=member.node_id, address=member.address)
            for member in response.members
            if member.node_id and member.address
        ]

        self_node_id = None
        if self.self_addr:
            for member in members:
                if member.address == self.self_addr:
                    self_node_id = member.node_id
                    break

        return ClusterView(self_node_id=self_node_id, members=members)
