"""
Servicio de exportación, inspección e importación de metadata object packs.

El servicio trabaja sobre el object store local y usa packs cifrados para mover
estado de metadata entre nodos o para recuperación posterior.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stopan.common.fs import atomic_write_bytes, ensure_private_dir
from stopan.errors import StopanUsageError
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.graph.walk import collect_reachable_object_bytes
from stopan.metadata.objects.store import (
    LatestMetadataPointer,
    MetadataObjectStore,
    object_store_lock,
)
from stopan.metadata.packs.crypto import decrypt_pack_payload, encrypt_pack_payload
from stopan.metadata.packs.format import (
    MetadataObjectPackError,
    MetadataObjectPackHeader,
    OBJECT_PACK_FILE_SUFFIX,
    read_pack_header,
)
from stopan.metadata.packs.payload import (
    pack_payload,
    parse_pack_payload,
    parse_pack_payload_summary,
    require_positive_int,
    require_vault_id,
    validate_pack_payload,
)


_PACK_GENERATION_DIR = "pack-generations"


_PackPayloadSummaryParser = Callable[
    [dict[str, Any]],
    tuple[LatestMetadataPointer, str, int, float],
]


def _pack_generation_path(object_store_dir: str | Path, vault_id: str) -> Path:
    vault = require_vault_id("vault_id", vault_id)
    return Path(object_store_dir).expanduser().resolve() / _PACK_GENERATION_DIR / f"{vault}.txt"


def _read_pack_generation(object_store_dir: str | Path, vault_id: str) -> int:
    path = _pack_generation_path(object_store_dir, vault_id)
    if not path.exists():
        return 0
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise MetadataObjectPackError(f"No se pudo leer la generación de metadata packs {path}: {exc}") from exc

    try:
        generation = int(text)
    except ValueError as exc:
        raise MetadataObjectPackError(f"Generación de metadata packs inválida en {path}: {text!r}") from exc
    if generation < 0:
        raise MetadataObjectPackError(f"Generación de metadata packs negativa en {path}: {generation}")
    return generation


def _write_pack_generation(object_store_dir: str | Path, vault_id: str, generation: int) -> None:
    if generation < 0:
        raise MetadataObjectPackError(f"Generación de metadata packs negativa: {generation}")
    path = _pack_generation_path(object_store_dir, vault_id)
    ensure_private_dir(path.parent)
    atomic_write_bytes(path, f"{int(generation)}\n".encode("utf-8"), mode=0o600)


def _next_pack_generation(object_store_dir: str | Path, vault_id: str) -> int:
    generation = _read_pack_generation(object_store_dir, vault_id) + 1
    _write_pack_generation(object_store_dir, vault_id, generation)
    return generation


def _remember_pack_generation(object_store_dir: str | Path, vault_id: str, generation: int) -> None:
    generation = int(generation)
    if generation > _read_pack_generation(object_store_dir, vault_id):
        _write_pack_generation(object_store_dir, vault_id, generation)


@dataclass(frozen=True, slots=True)
class MetadataObjectPackInspection:
    header: MetadataObjectPackHeader
    decrypted: "MetadataObjectPackSummary | None" = None


@dataclass(frozen=True, slots=True)
class MetadataObjectPackSummary:
    vault_id: str
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
    vault_id: str
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
    vault_id: str
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
            vault_generation = _next_pack_generation(object_store_dir, latest.vault_id)
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
                if pack_dir is None:
                    raise StopanUsageError("export_latest_pack requiere pack_dir cuando no se proporciona out_path")
                base_dir = Path(pack_dir).expanduser().resolve()
                ensure_private_dir(base_dir)
                path = base_dir / f"pack-{pack_hash}{OBJECT_PACK_FILE_SUFFIX}"
            else:
                path = Path(out_path).expanduser().resolve()
                ensure_private_dir(path.parent)

            atomic_write_bytes(path, pack_bytes, mode=0o600)

        return MetadataObjectPackExportResult(
            path=path,
            pack_hash=pack_hash,
            vault_id=latest.vault_id,
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

    def find_reusable_latest_pack(
        self,
        *,
        object_store_dir: str | Path,
        pack_dir: str | Path,
        identity_file: str | Path,
        passphrase: str | bytes,
    ) -> tuple[Path, MetadataObjectPackSummary] | None:
        """Selecciona y registra una representación reutilizable del latest vigente.

        La lectura de latest, la selección y el avance del marcador de generación
        comparten el lock del object store para que una exportación concurrente no
        pueda reservar una generación inferior entre esas operaciones.
        """

        resolved_pack_dir = Path(pack_dir).expanduser().resolve()
        if not resolved_pack_dir.exists():
            return None
        if not resolved_pack_dir.is_dir():
            raise StopanUsageError(f"No es un directorio de metadata packs: {resolved_pack_dir}")

        with object_store_lock(object_store_dir):
            store = MetadataObjectStore.open_existing(
                object_store_dir,
                passphrase=passphrase,
            )
            latest = store.read_latest_pointer()
            candidates: list[tuple[int, str, Path, MetadataObjectPackSummary]] = []

            for path in sorted(resolved_pack_dir.glob(f"*{OBJECT_PACK_FILE_SUFFIX}")):
                try:
                    inspection = self.validate_pack(
                        path,
                        identity_file=identity_file,
                        passphrase=passphrase,
                    )
                    summary = inspection.decrypted
                    if summary is None:
                        continue
                    if summary.vault_id != latest.vault_id:
                        continue
                    if summary.catalog_hash != latest.catalog_hash:
                        continue
                    if summary.state_digest != latest.state_digest:
                        continue
                    candidates.append(
                        (
                            int(summary.vault_generation),
                            str(inspection.header.pack_hash),
                            path,
                            summary,
                        )
                    )
                except Exception:
                    # Un pack ilegible, de otra identidad, corrupto o antiguo no
                    # impide reutilizar otra representación válida del mismo estado.
                    continue

            if not candidates:
                return None

            generation, _pack_hash, path, summary = max(
                candidates,
                key=lambda item: (item[0], item[1]),
            )
            _remember_pack_generation(
                object_store_dir,
                latest.vault_id,
                generation,
            )
            return path, summary

    def inspect_pack_header(self, path: str | Path) -> MetadataObjectPackInspection:
        pack_path = Path(path).expanduser().resolve()
        return MetadataObjectPackInspection(header=read_pack_header(pack_path), decrypted=None)

    def inspect_pack_summary(
        self,
        path: str | Path,
        *,
        identity_file: str | Path,
        passphrase: str | bytes,
    ) -> MetadataObjectPackInspection:
        return self._inspect_decrypted_pack(
            path,
            identity_file=identity_file,
            passphrase=passphrase,
            payload_parser=parse_pack_payload_summary,
        )

    def validate_pack(
        self,
        path: str | Path,
        *,
        identity_file: str | Path,
        passphrase: str | bytes,
    ) -> MetadataObjectPackInspection:
        return self._inspect_decrypted_pack(
            path,
            identity_file=identity_file,
            passphrase=passphrase,
            payload_parser=validate_pack_payload,
        )

    def _inspect_decrypted_pack(
        self,
        path: str | Path,
        *,
        identity_file: str | Path,
        passphrase: str | bytes,
        payload_parser: _PackPayloadSummaryParser,
    ) -> MetadataObjectPackInspection:
        if not identity_file:
            raise StopanUsageError("la inspección descifrada requiere identity_file")
        if passphrase is None:
            raise StopanUsageError("la inspección descifrada requiere passphrase")

        pack_path = Path(path).expanduser().resolve()
        payload, header, compressed_bytes, plaintext_bytes = decrypt_pack_payload(
            pack_path,
            identity_file=identity_file,
            passphrase=passphrase,
        )
        latest, vault_id, vault_generation, pack_created_at_unix = payload_parser(payload)
        return MetadataObjectPackInspection(
            header=header,
            decrypted=MetadataObjectPackSummary(
                vault_id=vault_id,
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
        latest, objects, vault_id, vault_generation, pack_created_at_unix = parse_pack_payload(payload)

        with object_store_lock(object_store_dir):
            store = MetadataObjectStore.open_or_create(
                object_store_dir,
                passphrase=passphrase,
                scrypt_cost=self.scrypt_cost,
            )
            written, reused = store.put_objects_batch(objects)
            store.write_latest_pointer(latest)
            _remember_pack_generation(object_store_dir, vault_id, vault_generation)

        return MetadataObjectPackImportResult(
            path=pack_path,
            pack_hash=header.pack_hash,
            vault_id=vault_id,
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
