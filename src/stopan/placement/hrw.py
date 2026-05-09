"""
Selección determinista de nodos mediante HRW / Rendezvous Hashing.

HRW ordena candidatos de forma estable para cada chunk_hash y salt. Se usa para
elegir targets remotos sin mantener estado adicional de asignación.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable

import blake3


def _hrw_score(chunk_hash: str, node_id: str, *, salt: str) -> int:
    """
    Devuelve el score HRW determinista para (chunk_hash, node_id, salt).

    Convención estable del mensaje:
        "{salt}|{node_id}|{chunk_hash}"

    Importante:
      - no cambiar este formato una vez congelado, porque define el placement.
      - score mayor = mejor candidato.
    """
    message = f"{salt}|{node_id}|{chunk_hash}".encode("utf-8")
    digest = blake3.blake3(message).digest()
    return int.from_bytes(digest, "big")


def hrw_rank_node_ids(
    chunk_hash: str,
    node_ids: Iterable[str],
    *,
    salt: str = "",
) -> list[str]:
    """
    Devuelve todos los node_id ordenados por preferencia HRW (mejor primero).

    Desempate:
      - si dos nodos obtienen el mismo score, gana el node_id lexicográficamente mayor,
        porque el sort se hace en orden inverso sobre (score, node_id).
    """
    scored = [
        (_hrw_score(chunk_hash, node_id, salt=salt), node_id)
        for node_id in node_ids
        if node_id
    ]
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [node_id for _, node_id in scored]


def hrw_top_k_node_ids(chunk_hash: str, node_ids, k: int, *, salt: str = "") -> list[str]:
    """
    Devuelve los k mejores node_id según HRW.

    Contrato:
      - si k <= 0, devuelve [].
      - si k > número de nodos disponibles, devuelve todos los nodos rankeados.
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