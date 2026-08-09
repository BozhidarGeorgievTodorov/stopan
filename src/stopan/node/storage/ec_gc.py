from __future__ import annotations

from pathlib import Path

from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult


def collect_ec_shards(
    *,
    target: str,
    root_dir: str | Path,
    max_age_seconds: int | None,
    dry_run: bool,
) -> LocalFileGarbageCollectionResult:
    root = Path(root_dir).expanduser().resolve()
    return collect_files_by_age(
        target=target,
        root_dir=root,
        files=root.rglob("*.stec") if root.exists() else (),
        max_age_seconds=max_age_seconds,
        dry_run=dry_run,
    )
