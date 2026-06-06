from __future__ import annotations

from pathlib import Path

from stopan.config.model import StopanConfig
from stopan.gc.local_files import collect_files_by_age
from stopan.gc.models import LocalFileGarbageCollectionResult
from stopan.metadata.packs.distributed_store import MetadataPackStore
from stopan.metadata.packs.distributed_store.models import PruneMetadataPackStoreResult

_SECONDS_PER_DAY = 24 * 60 * 60


def collect_received_metadata_packs(
    *,
    cfg: StopanConfig,
    pack_store: str | Path | None,
    max_age_days: int | None,
    dry_run: bool,
) -> PruneMetadataPackStoreResult:
    root_dir = pack_store or cfg.metadata.distributed_pack_store_dir
    store = MetadataPackStore(
        root_dir,
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        max_packs_per_owner=int(cfg.metadata.max_distributed_packs_per_owner),
        max_total_bytes_per_owner=int(cfg.metadata.max_distributed_pack_bytes_per_owner),
        max_total_store_bytes=int(cfg.metadata.max_distributed_pack_store_bytes),
        max_age_days=int(cfg.gc.received_metadata_pack_max_age_days if max_age_days is None else max_age_days),
    )
    return store.prune_to_limits(dry_run=bool(dry_run))


def collect_recovered_metadata_packs(
    *,
    cfg: StopanConfig,
    object_store: str | Path | None,
    pack_dir: str | Path | None,
    max_age_days: int | None,
    dry_run: bool,
) -> LocalFileGarbageCollectionResult:
    object_store_dir = Path(object_store or cfg.metadata.object_store_dir)
    root = Path(pack_dir or (object_store_dir / "recovered_packs")).expanduser().resolve()
    days = int(cfg.gc.recovered_metadata_pack_max_age_days if max_age_days is None else max_age_days)
    return collect_files_by_age(
        target="recovered-metadata-packs",
        root_dir=root,
        files=root.glob("*.stopanmetapack") if root.exists() else (),
        max_age_seconds=days * _SECONDS_PER_DAY,
        dry_run=bool(dry_run),
    )
