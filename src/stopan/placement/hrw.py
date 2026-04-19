from __future__ import annotations

import heapq
from collections.abc import Iterable

import blake3


def _hrw_score(chunk_hash: str, node_id: str, *, salt: str) -> int:
    message = f"{salt}|{node_id}|{chunk_hash}".encode("utf-8")
    digest = blake3.blake3(message).digest()
    return int.from_bytes(digest, "big")


def hrw_rank_node_ids(
    chunk_hash: str,
    node_ids: Iterable[str],
    *,
    salt: str = "",
) -> list[str]:
    normalized_node_ids = [str(node_id) for node_id in node_ids if node_id]
    scored = [
        (_hrw_score(chunk_hash, node_id, salt=salt), node_id)
        for node_id in normalized_node_ids
    ]
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [node_id for _, node_id in scored]


def hrw_top_k_node_ids(
    chunk_hash: str,
    node_ids: Iterable[str],
    k: int,
    *,
    salt: str = "",
) -> list[str]:
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
