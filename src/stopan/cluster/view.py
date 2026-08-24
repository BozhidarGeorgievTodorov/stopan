"""Vista canónica del cluster usada por placement y flujos cliente."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

from stopan.placement.epoch import compute_placement_epoch
from stopan.placement.hrw import hrw_top_k_node_ids


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
      - el servidor de membership ya filtra miembros no elegibles;
      - este objeto no reinterpreta estados SWIM;
      - self_node_id identifica al ejecutor y puede no figurar entre members si
        una operación admitida conserva su identidad durante el drenaje;
      - placement consume esta vista, pero membership sigue perteneciendo al nodo.
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
        """Selecciona hasta rf candidatos mediante HRW excluyendo nodos concretos."""
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
