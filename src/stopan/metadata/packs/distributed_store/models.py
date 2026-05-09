"""
Modelos del almacén local de metadata packs distribuidos.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class MetadataPackStoreError(RuntimeError):
    pass


class MetadataPackSignatureError(MetadataPackStoreError):
    pass


class MetadataPackQuotaError(MetadataPackStoreError):
    pass


class MetadataPackCorruptionError(MetadataPackStoreError):
    pass


@dataclass(frozen=True, slots=True)
class StoredMetadataPackRecord:
    owner_id: str
    pack_hash: str
    size_bytes: int
    stored_at_unix: float
    path: Path
    public_key_b64: str = ""
    signature_b64: str = ""

    @property
    def signed(self) -> bool:
        return bool(self.public_key_b64 and self.signature_b64)


@dataclass(frozen=True, slots=True)
class StoreMetadataPackResult:
    owner_id: str
    pack_hash: str
    path: Path
    size_bytes: int
    stored: bool
    already_present: bool
    public_key_b64: str = ""
    signature_b64: str = ""
    pruned_packs: int = 0
    pruned_bytes: int = 0


@dataclass(frozen=True, slots=True)
class PruneMetadataPackStoreResult:
    root_dir: Path
    dry_run: bool
    max_age_days: int
    cutoff_unix: float | None
    packs_seen: int
    owners_seen: int
    expired_packs: int
    quota_packs: int
    pruned_packs: int
    pruned_bytes: int
