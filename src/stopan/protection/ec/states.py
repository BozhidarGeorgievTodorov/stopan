from __future__ import annotations

from stopan.protection.policy import ProtectionState, protection_state_from_thresholds


def data_pack_state(
    *,
    protected_shards: int,
    data_shards: int,
    total_shards: int,
) -> ProtectionState:
    protected_shards = int(protected_shards)
    data_shards = int(data_shards)
    total_shards = int(total_shards)

    return protection_state_from_thresholds(
        confirmed=protected_shards,
        success_threshold=total_shards,
        degraded_threshold=data_shards,
        success_state=ProtectionState.PLACED,
    )
