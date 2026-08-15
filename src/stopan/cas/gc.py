from __future__ import annotations

import stat
from collections.abc import Callable, Collection
from pathlib import Path

from stopan.common.hashes import is_valid_blake3_hex
from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult
from stopan.gc.path_safety import is_filesystem_redirection


def collect_cas_chunks(
    *,
    target: str,
    root_dir: str | Path,
    max_age_seconds: int | None,
    dry_run: bool,
    reachable_hashes: Collection[str] | None = None,
    candidate_reporter: Callable[[Path], None] | None = None,
) -> LocalFileGarbageCollectionResult:
    root = Path(root_dir).expanduser().resolve()
    reachable = frozenset(reachable_hashes or ())
    return collect_files_by_age(
        target=target,
        root_dir=root,
        files=_iter_cas_chunk_files(root),
        max_age_seconds=max_age_seconds,
        dry_run=dry_run,
        retain_file=(lambda path: path.name in reachable) if reachable_hashes is not None else None,
        sort_files=False,
        candidate_reporter=candidate_reporter,
    )


def _iter_cas_chunk_files(root: Path):
    if not root.exists() or not root.is_dir():
        return
    for first_level in root.iterdir():
        try:
            first_stat = first_level.lstat()
        except OSError:
            continue
        if (
            is_filesystem_redirection(first_stat)
            or not stat.S_ISDIR(first_stat.st_mode)
            or len(first_level.name) != 2
        ):
            continue
        for second_level in first_level.iterdir():
            try:
                second_stat = second_level.lstat()
            except OSError:
                continue
            if (
                is_filesystem_redirection(second_stat)
                or not stat.S_ISDIR(second_stat.st_mode)
                or len(second_level.name) != 2
            ):
                continue
            for path in second_level.iterdir():
                chunk_hash = path.name
                if (
                    is_valid_blake3_hex(chunk_hash)
                    and first_level.name == chunk_hash[:2]
                    and second_level.name == chunk_hash[2:4]
                ):
                    yield path
