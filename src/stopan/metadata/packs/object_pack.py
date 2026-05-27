"""
Servicio de exportación, inspección e importación de metadata object packs.

El servicio trabaja sobre el object store local y usa packs cifrados para mover
estado de metadata entre nodos o para recuperación posterior.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from stopan.common.fs import atomic_write_bytes, ensure_private_dir
from stopan.errors import StopanUsageError
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.graph.walk import collect_reachable_object_bytes
from stopan.metadata.objects.store import MetadataObjectStore, object_store_lock
from stopan.metadata.packs.crypto import decrypt_pack_payload, encrypt_pack_payload
from stopan.metadata.packs.format import (
    MetadataObjectPackHeader,
    OBJECT_PACK_FILE_SUFFIX,
    read_pack_header,
)
from stopan.metadata.packs.payload import pack_payload, parse_pack_payload, parse_pack_payload_summary


@dataclass(frozen=True, slots=True)
class MetadataObjectPackInspection:
    header: MetadataObjectPackHeader
    decrypted: "MetadataObjectPackSummary | None" = None


@dataclass(frozen=True, slots=True)
class MetadataObjectPackSummary:
    vault_generation: int
    pack_created_at_unix: float
    catalog_hash: str
    state_digest: str
    object_count: int
    total_canonical_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int
    compressed_bytes: int
    plaintext_bytes: int


@dataclass(frozen=True, slots=True)
class MetadataObjectPackExportStats:
    objects_packed: int
    total_canonical_bytes: int
    plaintext_bytes: int
    compressed_bytes: int
    ciphertext_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataObjectPackExportResult:
    path: Path
    pack_hash: str
    vault_generation: int
    pack_created_at_unix: float
    catalog_hash: str
    state_digest: str
    stats: MetadataObjectPackExportStats


@dataclass(frozen=True, slots=True)
class MetadataObjectPackImportStats:
    objects_total: int
    objects_written: int
    objects_reused: int
    total_canonical_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataObjectPackImportResult:
    path: Path
    pack_hash: str
    vault_generation: int
    pack_created_at_unix: float
    object_store_dir: Path
    catalog_hash: str
    state_digest: str
    stats: MetadataObjectPackImportStats


class MetadataObjectPackService:
    def __init__(self, *, scrypt_cost: ScryptCost):
        self.scrypt_cost = scrypt_cost

    def export_latest_pack(
        self,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes,
        identity_file: str | Path,
        out_path: str | Path | None = None,
        pack_dir: str | Path | None = None,
    ) -> MetadataObjectPackExportResult:
        if not identity_file:
            raise StopanUsageError("export_latest_pack requiere identity_file")

        with object_store_lock(object_store_dir):
            store = MetadataObjectStore.open_existing(object_store_dir, passphrase=passphrase)
            latest = store.read_latest_pointer()
            objects = collect_reachable_object_bytes(
                catalog_hash=latest.catalog_hash,
                read_object_bytes=lambda object_hash: store.get_object_bytes(object_hash=object_hash),
            )
            pack_created_at_unix = time.time()
            vault_generation = time.time_ns()
            plaintext = pack_payload(
                latest=latest,
                objects=objects,
                vault_generation=vault_generation,
                pack_created_at_unix=pack_created_at_unix,
            )
            pack_bytes, pack_hash, compressed_bytes, ciphertext_bytes = encrypt_pack_payload(
                plaintext,
                identity_file=identity_file,
            )

            if out_path is None:
                base_dir = (
                    Path(pack_dir).expanduser().resolve()
                    if pack_dir is not None
                    else Path(object_store_dir).expanduser().resolve() / "packs"
                )
                ensure_private_dir(base_dir)
                path = base_dir / f"pack-{pack_hash}{OBJECT_PACK_FILE_SUFFIX}"
            else:
                path = Path(out_path).expanduser().resolve()
                ensure_private_dir(path.parent)

            atomic_write_bytes(path, pack_bytes, mode=0o600)

        return MetadataObjectPackExportResult(
            path=path,
            pack_hash=pack_hash,
            vault_generation=vault_generation,
            pack_created_at_unix=pack_created_at_unix,
            catalog_hash=latest.catalog_hash,
            state_digest=latest.state_digest,
            stats=MetadataObjectPackExportStats(
                objects_packed=len(objects),
                total_canonical_bytes=latest.total_canonical_bytes,
                plaintext_bytes=len(plaintext),
                compressed_bytes=compressed_bytes,
                ciphertext_bytes=ciphertext_bytes,
                snapshot_count=latest.snapshot_count,
                known_chunk_count=latest.known_chunk_count,
                protection_record_count=latest.protection_record_count,
            ),
        )

    def inspect_pack(
        self,
        path: str | Path,
        *,
        identity_file: str | Path | None = None,
        passphrase: str | bytes | None = None,
        decrypt: bool = False,
    ) -> MetadataObjectPackInspection:
        pack_path = Path(path).expanduser().resolve()
        header = read_pack_header(pack_path)
        if not decrypt:
            return MetadataObjectPackInspection(header=header, decrypted=None)
        if passphrase is None:
            raise StopanUsageError("inspect_pack decrypt=True requiere passphrase")
        if identity_file is None:
            raise StopanUsageError("inspect_pack decrypt=True requiere identity_file")

        payload, _header, compressed_bytes, plaintext_bytes = decrypt_pack_payload(
            pack_path,
            identity_file=identity_file,
            passphrase=passphrase,
        )
        latest, vault_generation, pack_created_at_unix = parse_pack_payload_summary(payload)
        return MetadataObjectPackInspection(
            header=header,
            decrypted=MetadataObjectPackSummary(
                vault_generation=vault_generation,
                pack_created_at_unix=pack_created_at_unix,
                catalog_hash=latest.catalog_hash,
                state_digest=latest.state_digest,
                object_count=latest.object_count,
                total_canonical_bytes=latest.total_canonical_bytes,
                snapshot_count=latest.snapshot_count,
                known_chunk_count=latest.known_chunk_count,
                protection_record_count=latest.protection_record_count,
                compressed_bytes=compressed_bytes,
                plaintext_bytes=plaintext_bytes,
            ),
        )

    def import_pack(
        self,
        path: str | Path,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes,
        identity_file: str | Path,
    ) -> MetadataObjectPackImportResult:
        pack_path = Path(path).expanduser().resolve()
        payload, header, _compressed_bytes, _plaintext_bytes = decrypt_pack_payload(
            pack_path,
            identity_file=identity_file,
            passphrase=passphrase,
        )
        latest, objects, vault_generation, pack_created_at_unix = parse_pack_payload(payload)

        with object_store_lock(object_store_dir):
            store = MetadataObjectStore.open_or_create(
                object_store_dir,
                passphrase=passphrase,
                scrypt_cost=self.scrypt_cost,
            )
            written, reused = store.put_objects_batch(objects)
            store.write_latest_pointer(latest)

        return MetadataObjectPackImportResult(
            path=pack_path,
            pack_hash=header.pack_hash,
            vault_generation=vault_generation,
            pack_created_at_unix=pack_created_at_unix,
            object_store_dir=store.root_dir,
            catalog_hash=latest.catalog_hash,
            state_digest=latest.state_digest,
            stats=MetadataObjectPackImportStats(
                objects_total=len(objects),
                objects_written=written,
                objects_reused=reused,
                total_canonical_bytes=latest.total_canonical_bytes,
                snapshot_count=latest.snapshot_count,
                known_chunk_count=latest.known_chunk_count,
                protection_record_count=latest.protection_record_count,
            ),
        )
