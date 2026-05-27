from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from stopan.metadata.database import ErasureDataPackShardRecord
from stopan.protection.ec.remote_client import RemoteDataPackShardRef


@dataclass(frozen=True)
class RemoteShardTargetGroups:
    refs_by_address: dict[str, list[RemoteDataPackShardRef]]
    offline_errors: tuple[str, ...]


def group_erasure_shard_refs_by_address(
    *,
    shard_rows: Sequence[ErasureDataPackShardRecord],
    node_addresses: Mapping[str, str],
    short_node_ids_in_errors: bool,
) -> RemoteShardTargetGroups:
    refs_by_address: dict[str, list[RemoteDataPackShardRef]] = {}
    offline_errors: list[str] = []

    for row in shard_rows:
        target_address = node_addresses.get(row.node_id)
        if target_address is None:
            node_label = row.node_id[:8] if short_node_ids_in_errors else row.node_id
            offline_errors.append(
                f"shard={row.shard_index}: nodo {node_label} offline/ilocalizable"
            )
            continue

        refs_by_address.setdefault(target_address, []).append(
            RemoteDataPackShardRef(
                pack_hash=row.pack_hash,
                shard_index=row.shard_index,
                shard_hash=row.shard_hash,
            )
        )

    return RemoteShardTargetGroups(
        refs_by_address=refs_by_address,
        offline_errors=tuple(offline_errors),
    )
