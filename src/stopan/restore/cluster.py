"""
Resolución lazy de cluster para restore.

El restore puede ejecutarse en modo local sin contactar con membership. Este
módulo retrasa la resolución del cluster hasta que falta un chunk local y se
necesita consultar nodos remotos.
"""

from __future__ import annotations

import threading

from stopan.errors import StopanConfigValueError
from stopan.cluster.resolver import require_cluster_view
from stopan.protection.policy import normalize_remote_rf


def resolve_membership_seed(explicit_seed: str | None = None) -> str | None:
    """Normaliza un seed explícito de membership para restore."""
    if explicit_seed:
        return explicit_seed.strip()
    return None


class LazyClusterResolver:
    """
    Resuelve membership solo cuando hace falta recuperar un chunk remoto.

    Contrato:
      - rf representa copias remotas consultables;
      - origin_node_id define qué nodo queda excluido del placement remoto;
      - self_node_id solo identifica al ejecutor actual del restore.
    """

    def __init__(
        self,
        *,
        membership_seed: str | None,
        rf: int,
        origin_node_id: str,
        self_addr: str,
        cluster_token: str,
        membership_timeout_s: float,
        max_message_bytes: int,
    ):
        self.membership_seed = resolve_membership_seed(membership_seed)
        self.rf = normalize_remote_rf(rf)
        self.self_addr = str(self_addr or "").strip()
        self.cluster_token = str(cluster_token or "")
        self.membership_timeout_s = float(membership_timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)

        self.origin_node_id = str(origin_node_id).strip()
        if not self.origin_node_id:
            raise StopanConfigValueError("LazyClusterResolver requiere origin_node_id no vacío.")

        self._cluster = None
        self._announced = False
        self._lock = threading.Lock()

    def get_cluster(self):
        """
        Devuelve la vista de cluster, resolviéndola una sola vez bajo demanda.

        Si no hay seed o membership falla, propaga el error porque solo se llama
        cuando el restore ya necesita recuperación remota.
        """

        cluster = self._cluster
        if cluster is not None:
            return cluster

        with self._lock:
            if self._cluster is not None:
                return self._cluster

            resolved = require_cluster_view(
                membership_seed=self.membership_seed,
                self_addr=self.self_addr,
                cluster_token=self.cluster_token,
                timeout_s=self.membership_timeout_s,
                max_message_bytes=self.max_message_bytes,
                missing_seed_message=(
                    "Falta membership seed para recuperación remota. "
                    "El restore puede ejecutarse sin red, pero para recuperar chunks ausentes "
                    "necesita '--membership-seed' o cluster.seeds en la configuración."
                ),
            )

            self.membership_seed = resolved.seed
            self._cluster = resolved.cluster
            return resolved.cluster

    def announce_once(self) -> None:
        """Imprime el resumen de recuperación remota activada."""
        cluster = self.get_cluster()

        should_announce = False
        with self._lock:
            if not self._announced:
                self._announced = True
                should_announce = True

        if not should_announce:
            return

        print(
            "Activando recuperación remota. "
            f"Miembros elegibles: {[f'{member.node_id[:8]}@{member.address}' for member in cluster.members]}"
        )
        if cluster.self_node_id:
            print(f"Nodo local: {cluster.self_node_id[:8]}@{self.self_addr}")

        remote_candidate_count = len(
            cluster.candidate_node_ids_excluding({self.origin_node_id})
        )
        print(f"Origin excluido de protección: {self.origin_node_id[:8]}")
        print(f"Copias remotas consultables: {min(self.rf, remote_candidate_count)}/{remote_candidate_count}")
