from __future__ import annotations

import re
import stat
from collections.abc import Callable
from pathlib import Path

from stopan.common.hashes import is_valid_blake3_hex
from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult
from stopan.gc.path_safety import is_filesystem_redirection


_SHARD_FILE_RE = re.compile(r"^(?P<index>[0-9]+)-(?P<shard_hash>[0-9a-f]{64})\.stec$")


def collect_ec_shards(
    *,
    target: str,
    root_dir: str | Path,
    max_age_seconds: int | None,
    dry_run: bool,
    candidate_reporter: Callable[[Path], None] | None = None,
) -> LocalFileGarbageCollectionResult:
    root = Path(root_dir).expanduser().resolve()
    return collect_files_by_age(
        target=target,
        root_dir=root,
        files=_iter_ec_shard_files(root),
        max_age_seconds=max_age_seconds,
        dry_run=dry_run,
        sort_files=False,
        candidate_reporter=candidate_reporter,
    )


def _iter_ec_shard_files(root: Path):
    if not root.exists() or not root.is_dir():
        return

    for prefix_dir in root.iterdir():
        try:
            prefix_stat = prefix_dir.lstat()
        except OSError:
            continue
        if (
            is_filesystem_redirection(prefix_stat)
            or not stat.S_ISDIR(prefix_stat.st_mode)
            or len(prefix_dir.name) != 2
        ):
            continue

        for pack_dir in prefix_dir.iterdir():
            try:
                pack_stat = pack_dir.lstat()
            except OSError:
                continue
            pack_hash = pack_dir.name
            if (
                is_filesystem_redirection(pack_stat)
                or not stat.S_ISDIR(pack_stat.st_mode)
                or not is_valid_blake3_hex(pack_hash)
                or prefix_dir.name != pack_hash[:2]
            ):
                continue

            for path in pack_dir.iterdir():
                match = _SHARD_FILE_RE.fullmatch(path.name)
                if match is None:
                    continue
                shard_hash = match.group("shard_hash")
                if not is_valid_blake3_hex(shard_hash):
                    continue
                shard_index = int(match.group("index"))
                if path.name != f"{shard_index:03d}-{shard_hash}.stec":
                    continue
                yield path
