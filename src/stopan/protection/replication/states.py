"""
Decisiones de estado para protección por replicación clásica.

Este módulo concentra la semántica de PLACED, DEGRADED, FAILED y VERIFIED para
chunks completos. No actualiza metadata ni decide placement.
"""

from __future__ import annotations

from stopan.protection.policy import ProtectionState, protection_state_from_thresholds


def replication_push_state(
    *,
    protected_remote_copies: int,
    required_remote_copies: int,
) -> ProtectionState:
    protected_remote_copies = int(protected_remote_copies)
    required_remote_copies = int(required_remote_copies)

    return protection_state_from_thresholds(
        confirmed=protected_remote_copies,
        success_threshold=required_remote_copies,
        degraded_threshold=1,
        success_state=ProtectionState.PLACED,
    )


def replication_verify_state(
    *,
    verified_remote_copies: int,
    required_remote_copies: int,
) -> ProtectionState:
    verified_remote_copies = int(verified_remote_copies)
    required_remote_copies = int(required_remote_copies)

    return protection_state_from_thresholds(
        confirmed=verified_remote_copies,
        success_threshold=required_remote_copies,
        degraded_threshold=0,
        success_state=ProtectionState.VERIFIED,
        failed_state=ProtectionState.DEGRADED,
    )
