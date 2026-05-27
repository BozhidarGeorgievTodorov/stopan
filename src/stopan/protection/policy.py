"""
Modelo común de protección remota de chunks.

Este módulo define los estados persistidos en chunk_protection y las reglas para
decidir si una evidencia de protección remota sigue siendo suficiente para el
placement actual.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from stopan.errors import StopanConfigValueError


class ProtectionState(StrEnum):
    """
    Estado persistido de protección remota de un chunk.

    Solo PLACED y VERIFIED cuentan como evidencia suficiente para saltos rápidos
    o para considerar el chunk protegido.
    """

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
    """
    Vista en memoria de una fila de chunk_protection.

    desired_rf y protected_remote_copies se interpretan siempre como copias
    remotas, no como copias totales incluyendo el CAS local.
    """

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


def normalize_remote_rf(value: int, *, field_name: str = "rf") -> int:
    """
    Normaliza un RF remoto.

    RF representa copias remotas requeridas. RF=0 es válido y
    significa que no se solicita protección remota. La copia local del CAS de
    backup nunca cuenta como réplica P2P.
    """
    remote_rf = int(value)
    if remote_rf < 0:
        raise StopanConfigValueError(f"{field_name} debe ser >= 0")
    return remote_rf


def is_record_sufficient(
    record: ProtectionRecord | None,
    *,
    required_rf: int,
    current_epoch: str | None = None,
) -> bool:
    """
    Devuelve True si una fila de chunk_protection permite confiar en el chunk.

    La evidencia solo es suficiente si el estado cuenta como protegido, el RF
    registrado cubre las copias remotas requeridas, el número de copias
    verificadas/protegidas es suficiente y, cuando se proporciona, el
    placement_epoch coincide con el contexto actual.
    """
    required_remote_copies = normalize_remote_rf(required_rf, field_name="required_rf")

    if required_remote_copies == 0:
        return True

    if record is None:
        return False

    if not record.is_protected:
        return False

    if int(record.desired_rf) < required_remote_copies:
        return False

    if current_epoch is not None and record.placement_epoch != current_epoch:
        return False

    if int(record.protected_remote_copies) < required_remote_copies:
        return False

    return True


def protection_state_from_thresholds(
    *,
    confirmed: int,
    success_threshold: int,
    degraded_threshold: int,
    success_state: ProtectionState,
    degraded_state: ProtectionState = ProtectionState.DEGRADED,
    failed_state: ProtectionState = ProtectionState.FAILED,
) -> ProtectionState:
    confirmed = int(confirmed)
    success_threshold = int(success_threshold)
    degraded_threshold = int(degraded_threshold)

    if confirmed >= success_threshold:
        return success_state
    if confirmed >= degraded_threshold:
        return degraded_state
    return failed_state

