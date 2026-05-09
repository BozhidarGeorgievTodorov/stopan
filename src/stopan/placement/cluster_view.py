"""
Modelo de vista de cluster y selección de targets de placement.

ClusterView representa los miembros elegibles devueltos por membership y ofrece
operaciones estables para seleccionar nodos mediante HRW y calcular placement_epoch.
"""
from __future__ import annotations


from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

import grpc
from stopan.protos import membership_pb2
from stopan.protos import membership_pb2_grpc

from .hrw import hrw_top_k_node_ids
from stopan.placement.epoch import compute_placement_epoch
from stopan.rpc.errors import format_rpc_error
from stopan.rpc.options import grpc_channel_options


@dataclass(frozen=True)
class ClusterMember:
    """Miembro elegible del cluster usado como candidato de placement."""
    node_id: str
    address: str


@dataclass(frozen=True)
class ClusterView:
    """
    Vista canónica de los miembros elegibles para placement.

    Contrato:
      - el servidor de membership ya filtra miembros no elegibles.
      - este objeto no reinterpreta estados SWIM.
    """
    self_node_id: str | None
    members: tuple[ClusterMember, ...]

    @cached_property
    def node_ids(self) -> list[str]:
        return [member.node_id for member in self.members]

    @cached_property
    def node_addresses(self) -> dict[str, str]:
        return {member.node_id: member.address for member in self.members}

    def _candidate_members_excluding(self, excluded_node_ids: Iterable[str]) -> list[ClusterMember]:
        """Devuelve candidatos de placement excluyendo nodos concretos."""
        excluded = {node_id for node_id in excluded_node_ids if node_id}
        return [
            member for member in self.members
            if member.node_id and member.node_id not in excluded
        ]

    def candidate_node_ids_excluding(self, excluded_node_ids: Iterable[str]) -> list[str]:
        return [member.node_id for member in self._candidate_members_excluding(excluded_node_ids)]

    def hrw_targets(self, chunk_hash: str, *, rf: int, salt: str) -> list[ClusterMember]:
        return self.hrw_targets_excluding(chunk_hash, rf=rf, salt=salt, excluded_node_ids=set())

    def hrw_targets_excluding(
        self,
        chunk_hash: str,
        *,
        rf: int,
        salt: str,
        excluded_node_ids: Iterable[str],
    ) -> list[ClusterMember]:
        """
        Selecciona hasta rf candidatos mediante HRW excluyendo nodos concretos.

        Se usa para placement remoto. El nodo origen no debe contar como réplica P2P del chunk.
        """
        candidates = self._candidate_members_excluding(excluded_node_ids)
        if not candidates:
            return []

        k = min(max(int(rf), 0), len(candidates))
        if k <= 0:
            return []
        candidate_node_ids = [member.node_id for member in candidates]
        ranked_node_ids = hrw_top_k_node_ids(
            chunk_hash,
            candidate_node_ids,
            k=k,
            salt=salt,
        )
        address_map = self.node_addresses

        return [
            ClusterMember(node_id=node_id, address=address_map[node_id])
            for node_id in ranked_node_ids
            if node_id in address_map
        ]

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
        """
        Calcula un placement_epoch estable excluyendo nodos concretos.

        El backup fast-remote y restore usan esta variante para que la copia local
        del origin_node_id no cuente como protección remota.
        """
        return compute_placement_epoch(
            cluster_token=cluster_token,
            desired_rf=desired_rf,
            eligible_node_ids=self.candidate_node_ids_excluding(excluded_node_ids),
        )


class ClusterMembershipClient:
    """
    Cliente de membership para obtener una vista de cluster apta para placement.

    La vista devuelta se normaliza eliminando miembros sin node_id o address y
    deduplicando por node_id.
    """

    def __init__(
        self,
        seed_addr: str,
        *,
        self_addr: str,
        cluster_token: str,
        timeout_s: float,
        max_message_bytes: int,
    ):
        seed_addr = seed_addr.strip()
        if not seed_addr:
            raise ValueError("ClusterMembershipClient requiere un seed_addr no vacío")

        self.seed_addr = seed_addr
        self.self_addr = str(self_addr or "").strip()
        self.cluster_token = str(cluster_token or "").strip()
        self.timeout_s = float(timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)

    def get_cluster_view(self) -> ClusterView:
        # El context manager cierra el canal gRPC al terminar la petición.
        try:
            with grpc.insecure_channel(
                self.seed_addr,
                options=grpc_channel_options(self.max_message_bytes),
            ) as channel:
                stub = membership_pb2_grpc.MembershipStub(channel)
                response = stub.GetMembers(
                    membership_pb2.GetMembersRequest(cluster_token=self.cluster_token),
                    timeout=self.timeout_s,
                )
        except grpc.RpcError as exc:
            raise RuntimeError(
                f"No se pudo obtener la vista del clúster desde seed={self.seed_addr}: "
                f"{format_rpc_error(exc)}"
            ) from exc

        members_by_node_id: dict[str, ClusterMember] = {}
        for member in response.members:
            if not member.node_id or not member.address:
                continue
            members_by_node_id[member.node_id] = ClusterMember(
                node_id=member.node_id,
                address=member.address,
            )

        members = tuple(members_by_node_id.values())

        self_node_id = next(
            (m.node_id for m in members if m.address == self.self_addr),
            None
        )

        return ClusterView(self_node_id=self_node_id, members=members)
