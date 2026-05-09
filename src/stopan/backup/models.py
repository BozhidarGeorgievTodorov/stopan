from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WorkerStats:
    """Contadores agregados que devuelve cada worker de backup."""
    chunks_total: int = 0
    processed: int = 0
    skipped: int = 0
    skipped_local: int = 0
    skipped_remote: int = 0
    written: int = 0


@dataclass(frozen=True, slots=True)
class BackupPolicy:
    """
    Política efectiva de fast-path usada por un backup.

    safe_mode y la disponibilidad de membership ya vienen resueltos antes de
    construir esta estructura.
    """
    desired_rf: int
    fast_local_enabled: bool
    fast_remote_enabled: bool
    placement_epoch: str | None
