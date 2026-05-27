"""
Resolución de vistas de cluster para operaciones con red.

El módulo separa dos modos de uso:
  - require_cluster_view: operaciones que necesitan membership obligatoriamente;
  - try_cluster_view: flujos offline-first que pueden continuar sin red.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.cluster.membership_client import ClusterMembershipClient
from stopan.cluster.view import ClusterView
from stopan.errors import StopanNetworkError, StopanUsageError


@dataclass(frozen=True, slots=True)
class ClusterViewResolution:
    seed: str
    cluster: ClusterView


def require_cluster_view(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    missing_seed_message: str | None = None,
    empty_cluster_message: str | None = None,
) -> ClusterViewResolution:
    """
    Resuelve una vista de cluster para operaciones que requieren membership.

    Es el resolver estricto usado por flujos como push, verify o recover.
    La ausencia de seed, errores de membership o una vista sin miembros se tratan como fallos operativos.
    """
    seed = str(membership_seed or "").strip()
    if not seed:
        raise StopanUsageError(
            missing_seed_message
            or "Falta membership seed. Usa '--membership-seed' o define cluster.seeds."
        )

    cluster = ClusterMembershipClient(
        seed,
        self_addr=str(self_addr or ""),
        cluster_token=str(cluster_token or ""),
        timeout_s=float(timeout_s),
        max_message_bytes=int(max_message_bytes),
    ).get_cluster_view()

    if not cluster.members:
        raise StopanNetworkError(empty_cluster_message or f"No se pudo obtener miembros elegibles desde seed={seed}")

    return ClusterViewResolution(seed=seed, cluster=cluster)


def try_cluster_view(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
) -> ClusterViewResolution | None:
    """
    Intenta resolver una vista de cluster para flujos tolerantes a modo offline.

    Devuelve None cuando no hay seed, membership no responde o la vista resultante
    no contiene miembros. Se usa donde el fallback local forma parte del contrato.
    """
    seed = str(membership_seed or "").strip()
    if not seed:
        return None

    try:
        cluster = ClusterMembershipClient(
            seed,
            self_addr=str(self_addr or ""),
            cluster_token=str(cluster_token or ""),
            timeout_s=float(timeout_s),
            max_message_bytes=int(max_message_bytes),
        ).get_cluster_view()
    except Exception:
        return None

    if not cluster.members:
        return None

    return ClusterViewResolution(seed=seed, cluster=cluster)
