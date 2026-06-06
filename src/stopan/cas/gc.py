from __future__ import annotations

from pathlib import Path

from stopan.common.hashes import is_valid_blake3_hex
from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult


def collect_cas_chunks(
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
        files=_iter_cas_chunk_files(root),
        max_age_seconds=max_age_seconds,
        dry_run=dry_run,
    )


def _iter_cas_chunk_files(root: Path):
    if not root.exists() or not root.is_dir():
        return
    for first_level in root.iterdir():
        if not first_level.is_dir() or len(first_level.name) != 2:
            continue
        for second_level in first_level.iterdir():
            if not second_level.is_dir() or len(second_level.name) != 2:
                continue
            for path in second_level.iterdir():
                if path.is_file() and is_valid_blake3_hex(path.name):
                    yield path
