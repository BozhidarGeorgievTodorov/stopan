"""
Cálculo canónico del placement_epoch.

El placement_epoch identifica el contexto de placement usado para decidir si una
evidencia remota sigue siendo válida. Cambia cuando cambia la credencial del
clúster, el número de copias remotas requeridas, el conjunto de nodos elegibles
o el contrato de colocación.
"""

from __future__ import annotations

from collections.abc import Iterable

from stopan.common.hashes import blake3_hex_digest
from stopan.common.json import canonical_json_bytes
from stopan.placement.hrw import HRW_PLACEMENT_SCHEME


PLACEMENT_EPOCH_FORMAT = "stopan.placement-epoch.v1"


def compute_placement_epoch(
    *,
    cluster_token: str,
    desired_rf: int,
    eligible_node_ids: Iterable[str],
) -> str:
    """
    Calcula un identificador canónico del contexto de placement vigente.

    El epoch cambia si cambia cualquiera de estos elementos:
      - cluster_token
      - desired_rf
      - conjunto de nodos elegibles para placement
      - contrato de colocación identificado por HRW_PLACEMENT_SCHEME.

    El orden de entrada y las repeticiones de node_id no afectan al resultado.
    """
    normalized_node_ids = sorted(
        {str(node_id) for node_id in eligible_node_ids if node_id}
    )
    material = {
        "format": PLACEMENT_EPOCH_FORMAT,
        "placement_scheme": HRW_PLACEMENT_SCHEME,
        "cluster_token": str(cluster_token),
        "desired_rf": int(desired_rf),
        "eligible_node_ids": normalized_node_ids,
    }
    return blake3_hex_digest(canonical_json_bytes(material))
