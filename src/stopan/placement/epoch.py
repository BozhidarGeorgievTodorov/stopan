"""
Cálculo estable del placement_epoch.

El placement_epoch identifica el contexto de placement usado para decidir si una
evidencia remota sigue siendo válida. Cambia cuando cambia el cluster_token, el
número de copias remotas requeridas o el conjunto de nodos elegibles.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable


def compute_placement_epoch(
    *,
    cluster_token: str,
    desired_rf: int,
    eligible_node_ids: Iterable[str],
) -> str:
    """
    Calcula un identificador estable del contexto de placement vigente.

    El epoch cambia si cambia cualquiera de estos elementos:
      - cluster_token;
      - desired_rf, entendido como número de copias remotas requeridas;
      - conjunto de nodos elegibles para placement.
    """
    normalized_node_ids = tuple(sorted(str(node_id) for node_id in eligible_node_ids if node_id))
    material = (str(cluster_token), int(desired_rf), normalized_node_ids)
    return hashlib.sha256(repr(material).encode("utf-8")).hexdigest()