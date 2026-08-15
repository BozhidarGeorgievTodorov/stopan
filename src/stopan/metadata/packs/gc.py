from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

from stopan.config.model import StopanConfig
from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult
from stopan.metadata.packs.distributed_store import MetadataPackStore
from stopan.metadata.packs.distributed_store.models import PruneMetadataPackStoreResult

_SECONDS_PER_DAY = 24 * 60 * 60
_RECOVERED_PACK_RE = re.compile(r"^recovered-(?P<pack_hash>[0-9a-f]{64})\.stopanmetapack$")


def collect_received_metadata_packs(
    *,
    cfg: StopanConfig,
    pack_store: str | Path | None,
    max_age_days: int | None,
    dry_run: bool,
    candidate_reporter: Callable[[Path], None] | None = None,
) -> PruneMetadataPackStoreResult:
    root_dir = pack_store or cfg.metadata.custody_pack_store_dir
    store = MetadataPackStore(
        root_dir,
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        max_packs_per_owner=int(cfg.metadata.max_distributed_packs_per_owner),
        max_total_bytes_per_owner=int(cfg.metadata.max_distributed_pack_bytes_per_owner),
        max_total_store_bytes=int(cfg.metadata.max_distributed_pack_store_bytes),
        max_age_days=int(cfg.gc.received_metadata_pack_max_age_days if max_age_days is None else max_age_days),
    )
    return store.prune_to_limits(
        dry_run=bool(dry_run),
        candidate_reporter=candidate_reporter,
    )


def collect_recovered_metadata_packs(
    *,
    cfg: StopanConfig,
    object_store: str | Path | None,
    pack_dir: str | Path | None,
    max_age_days: int | None,
    dry_run: bool,
    candidate_reporter: Callable[[Path], None] | None = None,
) -> LocalFileGarbageCollectionResult:
    root = Path(pack_dir or cfg.metadata.recovered_pack_dir).expanduser().resolve()
    days = int(cfg.gc.recovered_metadata_pack_max_age_days if max_age_days is None else max_age_days)
    return collect_files_by_age(
        target="recovered-metadata-packs",
        root_dir=root,
        files=_iter_recovered_pack_files(root),
        max_age_seconds=None if days == 0 else days * _SECONDS_PER_DAY,
        dry_run=bool(dry_run),
        sort_files=False,
        candidate_reporter=candidate_reporter,
    )


def _iter_recovered_pack_files(root: Path):
    if not root.exists() or not root.is_dir():
        return
    for path in root.iterdir():
        if _RECOVERED_PACK_RE.fullmatch(path.name):
            yield path
