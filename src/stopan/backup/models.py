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


@dataclass(slots=True)
class BackupRunStats:
    """Contadores acumulados por el coordinador de backup."""
    files: int = 0
    size: int = 0
    chunks_total: int = 0
    processed: int = 0
    written: int = 0
    skipped: int = 0
    skipped_local: int = 0
    skipped_remote: int = 0

    def add_worker_file(self, *, file_size: int, stats: WorkerStats) -> None:
        self.files += 1
        self.size += file_size
        self.chunks_total += stats.chunks_total
        self.processed += stats.processed
        self.written += stats.written
        self.skipped += stats.skipped
        self.skipped_local += stats.skipped_local
        self.skipped_remote += stats.skipped_remote

    def add_reused_file(self, *, file_size: int) -> None:
        self.files += 1
        self.size += file_size


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
