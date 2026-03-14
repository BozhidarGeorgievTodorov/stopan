from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import blake3


@dataclass(frozen=True)
class Member:
    node_id: str
    address: str


def _score_hrw_int(chunk_hash: str, node_id: str, *, salt: str) -> int:
    material = f"{salt}|{node_id}|{chunk_hash}".encode("utf-8")
    digest = blake3.blake3(material).digest()
    return int.from_bytes(digest, "big")


def hrw_rank_node_ids(chunk_hash: str, node_ids: Iterable[str], *, salt: str = "") -> List[str]:
    scored = [(_score_hrw_int(chunk_hash, node_id, salt=salt), node_id) for node_id in node_ids]
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [node_id for _, node_id in scored]


def hrw_top_k_node_ids(chunk_hash: str, node_ids: Iterable[str], k: int, *, salt: str = "") -> List[str]:
    if k <= 0:
        return []

    heap: List[Tuple[int, str]] = []
    for node_id in node_ids:
        key = (_score_hrw_int(chunk_hash, node_id, salt=salt), node_id)
        if len(heap) < k:
            heapq.heappush(heap, key)
        elif key > heap[0]:
            heapq.heapreplace(heap, key)

    heap.sort(reverse=True)
    return [node_id for _, node_id in heap]


def members_to_maps(members: Iterable[Member]) -> Dict[str, str]:
    return {member.node_id: member.address for member in members if member.node_id and member.address}
