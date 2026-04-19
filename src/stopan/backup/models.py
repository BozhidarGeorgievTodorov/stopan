from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerStats:
    chunks_total: int = 0
    chunks_processed: int = 0
    chunks_skipped: int = 0
    chunks_skipped_local: int = 0
    chunks_skipped_remote: int = 0
    chunks_written: int = 0


@dataclass(frozen=True)
class BackupPolicy:
    desired_rf: int
    fast_local_enabled: bool
    fast_remote_enabled: bool
    placement_epoch: str | None
    