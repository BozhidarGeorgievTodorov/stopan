from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from pathlib import Path

from stopan.common.fs import fsync_dir
from stopan.gc.models import LocalFileGarbageCollectionResult

PathPredicate = Callable[[Path], bool]


def collect_files_by_age(
    *,
    target: str,
    root_dir: str | Path,
    files: Iterable[Path],
    max_age_seconds: int | None,
    dry_run: bool,
) -> LocalFileGarbageCollectionResult:
    root = Path(root_dir).expanduser().resolve()
    max_age = None if max_age_seconds is None else max(int(max_age_seconds), 0)
    enabled = max_age is not None and max_age > 0
    cutoff = time.time() - max_age if enabled else None

    files_seen = 0
    files_collectable = 0
    files_deleted = 0
    files_skipped_by_age = 0
    bytes_collectable = 0
    bytes_deleted = 0
    errors: list[str] = []
    touched_dirs: set[Path] = set()

    if not root.exists():
        return LocalFileGarbageCollectionResult(
            target=target,
            root_dir=root,
            dry_run=bool(dry_run),
            enabled=enabled,
            max_age_seconds=max_age,
            cutoff_unix=cutoff,
        )
    if not root.is_dir():
        return LocalFileGarbageCollectionResult(
            target=target,
            root_dir=root,
            dry_run=bool(dry_run),
            enabled=enabled,
            max_age_seconds=max_age,
            cutoff_unix=cutoff,
            errors=(f"root_dir no es un directorio: {root}",),
        )

    for path in sorted(files):
        if not path.is_file():
            continue
        files_seen += 1

        if not enabled:
            continue

        try:
            stat_result = path.stat()
        except OSError as exc:
            errors.append(f"stat falló {path}: {exc}")
            continue

        if cutoff is not None and stat_result.st_mtime > cutoff:
            files_skipped_by_age += 1
            continue

        files_collectable += 1
        bytes_collectable += stat_result.st_size
        if dry_run:
            continue

        try:
            size = stat_result.st_size
            path.unlink()
            files_deleted += 1
            bytes_deleted += size
            touched_dirs.add(path.parent)
        except OSError as exc:
            errors.append(f"borrado falló {path}: {exc}")

    _fsync_touched_dirs(touched_dirs, errors)
    return LocalFileGarbageCollectionResult(
        target=target,
        root_dir=root,
        dry_run=bool(dry_run),
        enabled=enabled,
        max_age_seconds=max_age,
        cutoff_unix=cutoff,
        files_seen=files_seen,
        files_collectable=files_collectable,
        files_deleted=files_deleted,
        files_skipped_by_age=files_skipped_by_age,
        bytes_collectable=bytes_collectable,
        bytes_deleted=bytes_deleted,
        errors=tuple(errors),
    )


def _fsync_touched_dirs(touched_dirs: set[Path], errors: list[str]) -> None:
    for directory in sorted(touched_dirs):
        try:
            fsync_dir(directory)
        except OSError as exc:
            errors.append(f"fsync de directorio falló {directory}: {exc}")
