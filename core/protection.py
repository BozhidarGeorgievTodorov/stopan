from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable, Optional


DEFAULT_DESIRED_RF = 3
PROTECTED_STATES = {"PLACED", "VERIFIED"}


class ProtectionState(StrEnum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    DEGRADED = "DEGRADED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ProtectionRecord:
    chunk_hash: str
    desired_rf: int
    protection_state: str
    protected_remote_copies: int
    placement_epoch: Optional[str]
    last_push_at: Optional[float]
    last_verify_at: Optional[float]
    last_error: Optional[str]

    @property
    def is_protected(self) -> bool:
        return self.protection_state in PROTECTED_STATES


def compute_placement_epoch(*, cluster_token: str, desired_rf: int, eligible_node_ids: Iterable[str]) -> str:
    material = (
        cluster_token,
        int(desired_rf),
        tuple(sorted(str(node_id) for node_id in eligible_node_ids if node_id)),
    )
    return hashlib.sha256(repr(material).encode("utf-8")).hexdigest()


def required_remote_copies_for_planner(required_rf: int) -> int:
    """
    Para permitir que el backup salte un chunk remoto, exigimos que la protección
    registrada cubra al menos las copias remotas esperadas para ese RF.
    """
    return max(0, int(required_rf) - 1)


def is_record_sufficient(
    record: Optional[ProtectionRecord],
    *,
    required_rf: int,
    current_epoch: Optional[str] = None,
) -> bool:
    if record is None:
        return False

    if record.protection_state not in PROTECTED_STATES:
        return False

    if int(record.desired_rf) < int(required_rf):
        return False

    if current_epoch is not None:
        if not record.placement_epoch or record.placement_epoch != current_epoch:
            return False

    if int(record.protected_remote_copies) < required_remote_copies_for_planner(required_rf):
        return False

    return True
