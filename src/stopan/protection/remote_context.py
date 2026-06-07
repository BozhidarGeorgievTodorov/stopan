"""Contexto común para operaciones de protección remota."""

from __future__ import annotations

from dataclasses import dataclass

from stopan.errors import StopanConfigRuntimeError
from stopan.cluster.resolver import require_cluster_view
from stopan.cluster.view import ClusterView


DEFAULT_REMOTE_PROTECTION_MISSING_SEED_MESSAGE = (
    "Falta membership seed. Usa '--membership-seed' o define cluster.seeds en stopan.yaml."
)


@dataclass(frozen=True, slots=True)
class RemoteProtectionContext:
    seed: str
    cluster: ClusterView
    origin_node_id: str
    self_addr: str
    cluster_token: str
    remote_candidate_node_ids: tuple[str, ...]

    @property
    def remote_candidate_count(self) -> int:
        return len(self.remote_candidate_node_ids)

    @property
    def excluded_node_ids(self) -> frozenset[str]:
        return frozenset({self.origin_node_id})

    def placement_epoch(self, *, remote_targets: int) -> str:
        return self.cluster.placement_epoch_excluding(
            desired_rf=int(remote_targets),
            cluster_token=self.cluster_token,
            excluded_node_ids=self.excluded_node_ids,
        )


def resolve_remote_protection_context(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    missing_seed_message: str,
    missing_origin_message: str | None = None,
) -> RemoteProtectionContext:
    self_addr = str(self_addr or "").strip()
    cluster_token = str(cluster_token or "")

    if not self_addr:
        raise StopanConfigRuntimeError(
            missing_origin_message
            or "Falta node.advertise_addr. La protección remota necesita identificar el nodo origen."
        )

    resolved = require_cluster_view(
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        timeout_s=timeout_s,
        max_message_bytes=max_message_bytes,
        missing_seed_message=missing_seed_message,
    )
    cluster = resolved.cluster
    origin_node_id = str(cluster.self_node_id or "").strip()
    if not origin_node_id:
        raise StopanConfigRuntimeError(
            "No pude resolver origin_node_id desde membership. "
            "Asegúrate de que node.advertise_addr coincide con un miembro elegible."
        )

    remote_candidate_node_ids = tuple(
        cluster.candidate_node_ids_excluding({origin_node_id})
    )
    return RemoteProtectionContext(
        seed=resolved.seed,
        cluster=cluster,
        origin_node_id=origin_node_id,
        self_addr=self_addr,
        cluster_token=cluster_token,
        remote_candidate_node_ids=remote_candidate_node_ids,
    )
