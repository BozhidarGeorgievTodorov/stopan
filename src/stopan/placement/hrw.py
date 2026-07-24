"""
Selección determinista de nodos mediante HRW / Rendezvous Hashing.

HRW ordena candidatos de forma estable para cada chunk_hash y salt. Se usa para
elegir targets remotos sin mantener estado adicional de asignación.
"""

from __future__ import annotations

import heapq


# Identifica el contrato de colocación usado por placement_epoch. Debe cambiar si
# cambia el algoritmo, el hash o la serialización del mensaje de puntuación.
HRW_PLACEMENT_SCHEME = "hrw-blake3-v1"


def _hrw_score(chunk_hash: str, node_id: str, *, salt: str) -> int:
    """
    Devuelve el score HRW determinista para (chunk_hash, node_id, salt).

    Convención estable del mensaje:
        "{salt}|{node_id}|{chunk_hash}"

    Importante:
      - cualquier cambio en este formato exige actualizar HRW_PLACEMENT_SCHEME
      - score mayor = mejor candidato.
    """
    import blake3

    message = f"{salt}|{node_id}|{chunk_hash}".encode("utf-8")
    digest = blake3.blake3(message).digest()
    return int.from_bytes(digest, "big")


def hrw_top_k_node_ids(chunk_hash: str, node_ids, k: int, *, salt: str = "") -> list[str]:
    """
    Devuelve los k mejores node_id según HRW.

    Contrato:
      - si k <= 0, devuelve []
      - si k > número de nodos disponibles, devuelve todos los nodos rankeados
    """
    if k <= 0:
        return []

    heap: list[tuple[int, str]] = []

    for raw_node_id in node_ids:
        node_id = str(raw_node_id)
        if not node_id:
            continue

        item = (_hrw_score(chunk_hash, node_id, salt=salt), node_id)

        if len(heap) < k:
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)

    heap.sort(reverse=True)
    return [node_id for _, node_id in heap]
