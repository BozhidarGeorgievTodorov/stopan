from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable


DEFAULT_DESIRED_RF = 3


class ProtectionState(StrEnum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    DEGRADED = "DEGRADED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"

    @property
    def counts_as_protected(self) -> bool:
        return self in {ProtectionState.PLACED, ProtectionState.VERIFIED}


@dataclass(frozen=True)
class ProtectionRecord:
    chunk_hash: str
    desired_rf: int
    protection_state: ProtectionState
    protected_remote_copies: int
    placement_epoch: str | None
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None

    @property
    def is_protected(self) -> bool:
        return self.protection_state.counts_as_protected


def compute_placement_epoch(
    *,
    cluster_token: str,
    desired_rf: int,
    eligible_node_ids: Iterable[str],
) -> str:
    """
    Calcula un identificador estable del contexto de placement vigente.
    """
    normalized_node_ids = tuple(sorted(str(node_id) for node_id in eligible_node_ids if node_id))
    material = (str(cluster_token), max(int(desired_rf), 1), normalized_node_ids)
    return hashlib.sha256(repr(material).encode("utf-8")).hexdigest()


def required_remote_copies_for_remote_skip(required_rf: int) -> int:
    """
    Criterio conservador para permitir fast-path/skip remoto.

    El planner no sabe si el nodo local pertenece al top-k HRW de un chunk concreto.
    Por eso, si RF > 1, exigimos al menos RF-1 copias remotas protegidas.
    Para RF=1, no exigimos copias remotas.
    """
    return max(0, max(int(required_rf), 1) - 1)


def is_record_sufficient(
    record: ProtectionRecord | None,
    *,
    required_rf: int,
    current_epoch: str | None = None,
) -> bool:
    """
    Devuelve True si la evidencia de protección del chunk es suficiente
    para el contexto actual de planificación.
    """
    if record is None:
        return False

    required_rf = max(int(required_rf), 1)

    if not record.is_protected:
        return False

    if int(record.desired_rf) < required_rf:
        return False

    if current_epoch is not None and record.placement_epoch != current_epoch:
        return False

    if int(record.protected_remote_copies) < required_remote_copies_for_remote_skip(required_rf):
        return False

    return True