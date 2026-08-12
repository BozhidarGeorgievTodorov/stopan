from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LocalFileGarbageCollectionResult:
    target: str
    root_dir: Path
    dry_run: bool
    enabled: bool
    max_age_seconds: int | None
    cutoff_unix: float | None
    files_seen: int = 0
    files_collectable: int = 0
    files_deleted: int = 0
    files_retained_by_policy: int = 0
    files_skipped_by_age: int = 0
    bytes_collectable: int = 0
    bytes_deleted: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)
