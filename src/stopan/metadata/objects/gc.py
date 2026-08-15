"""
Garbage collection del metadata object store.

El GC es mark-and-sweep: parte del latest catalog, marca objetos alcanzables y
considera collectable lo demás solo tras el grace period configurado. En dry-run
calcula candidatos sin borrar archivos.
"""

from __future__ import annotations

import os
import re
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from stopan.common.fs import fsync_dir
from stopan.errors import StopanDataError
from stopan.gc.path_safety import (
    GarbageCollectionPathError,
    is_filesystem_redirection,
    same_file_identity,
    validate_gc_regular_file,
)
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.graph.walk import collect_reachable_object_hashes
from stopan.metadata.objects.store import MetadataObjectStore, object_store_lock
from stopan.metadata.packs.object_pack import MetadataObjectPackService


_HASH64_RE = re.compile(r"^[0-9a-f]{64}$")
_STORAGE_FILE_RE = re.compile(r"^[0-9a-f]{64}\.stobj$")
_GENERATED_PACK_RE = re.compile(r"^pack-(?P<pack_hash>[0-9a-f]{64})\.stopanmetapack$")

PathReporter = Callable[[Path], None]


class MetadataObjectGarbageCollectionError(StopanDataError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataObjectGarbageCollectionResult:
    root_dir: Path
    catalog_hash: str
    object_grace_seconds: int
    pack_grace_seconds: int
    dry_run: bool

    live_objects: int
    object_files_seen: int
    object_files_live: int
    object_files_collectable: int
    object_files_deleted: int
    object_files_skipped_by_grace: int
    object_files_malformed: int
    object_bytes_collectable: int
    object_bytes_deleted: int

    pack_dir: Path | None
    pack_files_seen: int
    pack_files_latest: int
    pack_files_collectable: int
    pack_files_deleted: int
    pack_files_skipped_by_grace: int
    pack_files_unreadable: int
    pack_bytes_collectable: int
    pack_bytes_deleted: int

    errors: tuple[str, ...] = field(default_factory=tuple)


class MetadataObjectGarbageCollector:
    """
    GC mark-and-sweep del metadata object store cifrado.

    Política:
      - latest.json es la raíz;
      - los objetos alcanzables desde latest.catalog_hash están vivos;
      - objetos no alcanzables solo se borran tras object_grace_seconds;
      - los packs locales son artefactos de export/cache;
      - de los packs legibles del estado actual se conserva solo el más nuevo;
      - packs no actuales solo se borran tras pack_grace_seconds;
      - packs ilegibles se reportan y no se tocan;
      - el lock del object store se mantiene durante toda la operación.
    """

    def __init__(self, *, scrypt_cost: ScryptCost):
        self.scrypt_cost = scrypt_cost

    def collect(
        self,
        *,
        object_store_dir: str | Path,
        identity_file: str | Path,
        passphrase: str | bytes,
        object_grace_seconds: int,
        pack_grace_seconds: int,
        dry_run: bool = True,
        include_objects: bool = True,
        include_packs: bool = True,
        pack_dir: str | Path | None = None,
        candidate_reporter: PathReporter | None = None,
    ) -> MetadataObjectGarbageCollectionResult:
        root = Path(object_store_dir).expanduser().resolve()
        object_grace_seconds = _require_non_negative_int(
            "object_grace_seconds", object_grace_seconds
        )
        pack_grace_seconds = _require_non_negative_int(
            "pack_grace_seconds", pack_grace_seconds
        )
        now = time.time()
        object_cutoff = now - object_grace_seconds
        pack_cutoff = now - pack_grace_seconds
        errors: list[str] = []

        with object_store_lock(root):
            store = MetadataObjectStore.open_existing(root, passphrase=passphrase)
            latest = store.read_latest_pointer()
            live_hashes = collect_reachable_object_hashes(
                catalog_hash=latest.catalog_hash,
                read_object_bytes=lambda object_hash: store.get_object_bytes(
                    object_hash=object_hash
                ),
            )
            live_storage_ids = {store.object_storage_id(object_hash) for object_hash in live_hashes}

            object_stats = _sweep_objects(
                root=root,
                live_storage_ids=live_storage_ids,
                cutoff=object_cutoff,
                dry_run=bool(dry_run),
                enabled=bool(include_objects),
                errors=errors,
                candidate_reporter=candidate_reporter,
            )

            resolved_pack_dir = (
                Path(pack_dir).expanduser().resolve()
                if pack_dir is not None
                else None
            )
            if include_packs and resolved_pack_dir is None:
                raise MetadataObjectGarbageCollectionError(
                    "GC de metadata packs requiere pack_dir cuando include_packs está activo"
                )
            pack_stats = _sweep_packs(
                pack_dir=resolved_pack_dir,
                latest_vault_id=latest.vault_id,
                latest_catalog_hash=latest.catalog_hash,
                latest_state_digest=latest.state_digest,
                identity_file=identity_file,
                passphrase=passphrase,
                scrypt_cost=self.scrypt_cost,
                cutoff=pack_cutoff,
                dry_run=bool(dry_run),
                enabled=bool(include_packs),
                errors=errors,
                candidate_reporter=candidate_reporter,
            )

        return MetadataObjectGarbageCollectionResult(
            root_dir=root,
            catalog_hash=latest.catalog_hash,
            object_grace_seconds=object_grace_seconds,
            pack_grace_seconds=pack_grace_seconds,
            dry_run=bool(dry_run),
            live_objects=len(live_hashes),
            object_files_seen=object_stats.files_seen,
            object_files_live=object_stats.files_live,
            object_files_collectable=object_stats.files_collectable,
            object_files_deleted=object_stats.files_deleted,
            object_files_skipped_by_grace=object_stats.files_skipped_by_grace,
            object_files_malformed=object_stats.files_malformed,
            object_bytes_collectable=object_stats.bytes_collectable,
            object_bytes_deleted=object_stats.bytes_deleted,
            pack_dir=resolved_pack_dir,
            pack_files_seen=pack_stats.files_seen,
            pack_files_latest=pack_stats.files_latest,
            pack_files_collectable=pack_stats.files_collectable,
            pack_files_deleted=pack_stats.files_deleted,
            pack_files_skipped_by_grace=pack_stats.files_skipped_by_grace,
            pack_files_unreadable=pack_stats.files_unreadable,
            pack_bytes_collectable=pack_stats.bytes_collectable,
            pack_bytes_deleted=pack_stats.bytes_deleted,
            errors=tuple(errors),
        )


@dataclass(slots=True)
class _ObjectSweepStats:
    files_seen: int = 0
    files_live: int = 0
    files_collectable: int = 0
    files_deleted: int = 0
    files_skipped_by_grace: int = 0
    files_malformed: int = 0
    bytes_collectable: int = 0
    bytes_deleted: int = 0


@dataclass(slots=True)
class _PackSweepStats:
    files_seen: int = 0
    files_latest: int = 0
    files_collectable: int = 0
    files_deleted: int = 0
    files_skipped_by_grace: int = 0
    files_unreadable: int = 0
    bytes_collectable: int = 0
    bytes_deleted: int = 0


@dataclass(frozen=True, slots=True)
class _ReadablePack:
    path: Path
    stat_result: os.stat_result
    pack_hash: str
    vault_id: str
    catalog_hash: str
    state_digest: str
    vault_generation: int
    pack_created_at_unix: float


def _sweep_objects(
    *,
    root: Path,
    live_storage_ids: set[str],
    cutoff: float,
    dry_run: bool,
    enabled: bool,
    errors: list[str],
    candidate_reporter: PathReporter | None,
) -> _ObjectSweepStats:
    stats = _ObjectSweepStats()
    objects_dir = root / "objects"
    if not enabled or not objects_dir.exists():
        return stats

    try:
        objects_stat = objects_dir.lstat()
    except OSError as exc:
        errors.append(f"no se pudo inspeccionar objects_dir {objects_dir}: {exc}")
        return stats
    if is_filesystem_redirection(objects_stat) or not stat.S_ISDIR(objects_stat.st_mode):
        errors.append(f"objects_dir no es un directorio real: {objects_dir}")
        return stats

    touched_dirs: set[Path] = set()
    for path in _iter_object_files(objects_dir):
        stats.files_seen += 1
        storage_id = path.stem
        if (
            not _STORAGE_FILE_RE.fullmatch(path.name)
            or path.parent.name != storage_id[:2]
        ):
            stats.files_malformed += 1
            continue

        if storage_id in live_storage_ids:
            stats.files_live += 1
            continue

        try:
            stat_result = validate_gc_regular_file(root_dir=root, path=path)
        except GarbageCollectionPathError as exc:
            errors.append(str(exc))
            continue

        if stat_result.st_mtime > cutoff:
            stats.files_skipped_by_grace += 1
            continue

        stats.files_collectable += 1
        stats.bytes_collectable += stat_result.st_size
        if candidate_reporter is not None:
            candidate_reporter(path)
        if dry_run:
            continue

        try:
            current_stat = validate_gc_regular_file(root_dir=root, path=path)
            if not same_file_identity(stat_result, current_stat):
                errors.append(f"object cambió antes del borrado, omitido {path}")
                continue
            size = current_stat.st_size
            path.unlink()
            stats.files_deleted += 1
            stats.bytes_deleted += size
            touched_dirs.add(path.parent)
        except (OSError, GarbageCollectionPathError) as exc:
            errors.append(f"borrado de object falló {path}: {exc}")

    _fsync_touched_dirs(touched_dirs, errors)
    return stats


def _sweep_packs(
    *,
    pack_dir: Path | None,
    latest_vault_id: str,
    latest_catalog_hash: str,
    latest_state_digest: str,
    identity_file: str | Path,
    passphrase: str | bytes,
    scrypt_cost: ScryptCost,
    cutoff: float,
    dry_run: bool,
    enabled: bool,
    errors: list[str],
    candidate_reporter: PathReporter | None,
) -> _PackSweepStats:
    stats = _PackSweepStats()
    if not enabled:
        return stats
    if pack_dir is None:
        raise MetadataObjectGarbageCollectionError(
            "GC de metadata packs requiere pack_dir cuando está activo"
        )
    if not pack_dir.exists():
        return stats
    if not pack_dir.is_dir():
        errors.append(f"pack_dir no es un directorio: {pack_dir}")
        return stats

    latest_vault_id = _require_vault_id("latest_vault_id", latest_vault_id)
    latest_catalog_hash = _require_hash64("latest_catalog_hash", latest_catalog_hash)
    latest_state_digest = _require_hash64("latest_state_digest", latest_state_digest)
    service = MetadataObjectPackService(scrypt_cost=scrypt_cost)
    readable_packs: list[_ReadablePack] = []

    for path in sorted(_iter_generated_pack_files(pack_dir)):
        stats.files_seen += 1
        filename_match = _GENERATED_PACK_RE.fullmatch(path.name)
        if filename_match is None:
            continue
        filename_pack_hash = filename_match.group("pack_hash")
        try:
            stat_result = validate_gc_regular_file(root_dir=pack_dir, path=path)
        except GarbageCollectionPathError as exc:
            errors.append(str(exc))
            continue

        try:
            inspection = service.inspect_pack_summary(
                path,
                identity_file=identity_file,
                passphrase=passphrase,
            )
            if inspection.decrypted is None:
                raise MetadataObjectGarbageCollectionError("la inspección del pack no devolvió resumen descifrado")
            summary = inspection.decrypted
            inspected_pack_hash = _require_hash64("pack_hash", inspection.header.pack_hash)
            if inspected_pack_hash != filename_pack_hash:
                raise MetadataObjectGarbageCollectionError(
                    "el hash del nombre no coincide con el contenido del pack"
                )
            readable_packs.append(
                _ReadablePack(
                    path=path,
                    stat_result=stat_result,
                    pack_hash=inspected_pack_hash,
                    vault_id=_require_vault_id("pack.vault_id", summary.vault_id),
                    catalog_hash=_require_hash64("pack.catalog_hash", summary.catalog_hash),
                    state_digest=_require_hash64("pack.state_digest", summary.state_digest),
                    vault_generation=int(summary.vault_generation),
                    pack_created_at_unix=float(summary.pack_created_at_unix),
                )
            )
        except Exception as exc:
            stats.files_unreadable += 1
            errors.append(f"pack ilegible, omitido {path}: {exc}")
            continue

    current_candidates = [
        pack
        for pack in readable_packs
        if (
            pack.vault_id == latest_vault_id
            and pack.catalog_hash == latest_catalog_hash
            and pack.state_digest == latest_state_digest
        )
    ]
    latest_path: Path | None = None
    if current_candidates:
        latest_pack = max(
            current_candidates,
            key=lambda pack: (
                int(pack.vault_generation),
                pack.pack_hash,
            ),
        )
        latest_path = latest_pack.path

    touched_dirs: set[Path] = set()
    for pack in readable_packs:
        if latest_path is not None and pack.path == latest_path:
            stats.files_latest += 1
            continue

        if pack.stat_result.st_mtime > cutoff:
            stats.files_skipped_by_grace += 1
            continue

        stats.files_collectable += 1
        stats.bytes_collectable += pack.stat_result.st_size
        if candidate_reporter is not None:
            candidate_reporter(pack.path)
        if dry_run:
            continue

        try:
            current_stat = validate_gc_regular_file(root_dir=pack_dir, path=pack.path)
            if not same_file_identity(pack.stat_result, current_stat):
                errors.append(f"pack cambió antes del borrado, omitido {pack.path}")
                continue
            size = current_stat.st_size
            pack.path.unlink()
            stats.files_deleted += 1
            stats.bytes_deleted += size
            touched_dirs.add(pack.path.parent)
        except (OSError, GarbageCollectionPathError) as exc:
            errors.append(f"borrado de pack falló {pack.path}: {exc}")

    _fsync_touched_dirs(touched_dirs, errors)
    return stats


def _iter_object_files(objects_dir: Path):
    for prefix_dir in objects_dir.iterdir():
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
        for path in prefix_dir.iterdir():
            if path.name.endswith(".stobj"):
                yield path


def _iter_generated_pack_files(pack_dir: Path):
    for path in pack_dir.iterdir():
        if _GENERATED_PACK_RE.fullmatch(path.name):
            yield path


def _fsync_touched_dirs(touched_dirs: set[Path], errors: list[str]) -> None:
    for directory in sorted(touched_dirs):
        try:
            fsync_dir(directory, strict=True)
        except OSError as exc:
            errors.append(f"fsync de directorio falló {directory}: {exc}")


def _require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataObjectGarbageCollectionError(f"{name} debe ser un entero")
    if value < 0:
        raise MetadataObjectGarbageCollectionError(f"{name} debe ser >= 0")
    return value


def _require_vault_id(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectGarbageCollectionError(f"{name} debe ser string")
    text = value.strip()
    if len(text) != 32 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectGarbageCollectionError(
            f"{name} debe tener 32 caracteres hexadecimales lowercase"
        )
    return text


def _require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectGarbageCollectionError(f"{name} debe ser string")
    if not _HASH64_RE.fullmatch(value):
        raise MetadataObjectGarbageCollectionError(
            f"{name} debe tener 64 caracteres hexadecimales lowercase"
        )
    return value
