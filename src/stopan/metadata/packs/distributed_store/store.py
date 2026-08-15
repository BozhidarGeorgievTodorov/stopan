"""
Almacén local de metadata packs distribuidos.

Guarda packs cifrados direccionados por BLAKE3, exige sidecar firmado y aplica
cuotas locales por owner, tamaño total y antigüedad.
"""

from __future__ import annotations

import os
import logging
import stat
import threading
import time
from collections.abc import Callable
from pathlib import Path

from stopan.common.fs import atomic_copy_file, atomic_write_bytes, ensure_private_dir, fsync_dir
from stopan.common.hashes import is_valid_blake3_hex
from stopan.errors import StopanConfigValueError
from stopan.gc.path_safety import (
    GarbageCollectionPathError,
    is_filesystem_redirection,
    validate_gc_regular_file,
)
from stopan.metadata.identity import validate_owner_id, verify_metadata_pack_signature
from stopan.metadata.packs.format import OBJECT_PACK_FILE_SUFFIX
from stopan.metadata.packs.hashes import calculate_pack_hash, calculate_pack_hash_file, validate_pack_hash

from .models import (
    MetadataPackCorruptionError,
    MetadataPackQuotaError,
    MetadataPackSignatureError,
    MetadataPackStoreError,
    PruneMetadataPackStoreResult,
    StoreMetadataPackResult,
    StoredMetadataPackRecord,
)
from .paths import pack_path_for, signature_path_for
from .signatures import (
    SIGNATURE_RECORD_FORMAT,
    SIGNATURE_RECORD_VERSION,
    read_signature_record,
    write_signature_record,
)


_SECONDS_PER_DAY = 86_400
_LOGGER = logging.getLogger(__name__)
PathReporter = Callable[[Path], None]


class MetadataPackStore:
    """
    Almacén local de metadata packs cifrados recibidos o preparados para P2P.

    Contrato de seguridad:
      - owner_id y pack_hash se validan antes de construir rutas;
      - los packs son content-addressed: BLAKE3(pack_data) debe ser pack_hash;
      - los packs distribuidos requieren sidecar con firma Ed25519 válida;
      - las escrituras son atómicas y privadas;
      - cuotas y retención por antigüedad se aplican sin descifrar packs.
    """

    SIGNATURE_FORMAT = SIGNATURE_RECORD_FORMAT
    SIGNATURE_VERSION = SIGNATURE_RECORD_VERSION

    def __init__(
        self,
        root_dir: str | Path,
        *,
        max_pack_bytes: int,
        max_packs_per_owner: int,
        max_total_bytes_per_owner: int,
        max_total_store_bytes: int,
        max_age_days: int,
    ):
        self.max_pack_bytes = _require_int("max_pack_bytes", max_pack_bytes, min_value=1)
        self.max_packs_per_owner = _require_int("max_packs_per_owner", max_packs_per_owner, min_value=1)
        self.max_total_bytes_per_owner = _require_int(
            "max_total_bytes_per_owner",
            max_total_bytes_per_owner,
            min_value=1,
        )
        self.max_total_store_bytes = _require_int("max_total_store_bytes", max_total_store_bytes, min_value=1)
        self.max_age_days = _require_int("max_age_days", max_age_days, min_value=0)

        if self.max_total_bytes_per_owner < self.max_pack_bytes:
            raise StopanConfigValueError("max_total_bytes_per_owner debe ser >= max_pack_bytes")
        if self.max_total_store_bytes < self.max_pack_bytes:
            raise StopanConfigValueError("max_total_store_bytes debe ser >= max_pack_bytes")

        self.root_dir = Path(root_dir).expanduser().resolve()
        self._lock = threading.RLock()
        ensure_private_dir(self.root_dir)

    def pack_path(self, *, owner_id: str, pack_hash: str) -> Path:
        return pack_path_for(root_dir=self.root_dir, owner_id=owner_id, pack_hash=pack_hash)

    def signature_path(self, *, owner_id: str, pack_hash: str) -> Path:
        return signature_path_for(root_dir=self.root_dir, owner_id=owner_id, pack_hash=pack_hash)

    def _write_signature_record(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        public_key_b64: str,
        signature_b64: str,
    ) -> None:
        write_signature_record(
            self.signature_path(owner_id=owner_id, pack_hash=pack_hash),
            owner_id=owner_id,
            pack_hash=pack_hash,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
        )

    def _read_signature_record(self, *, owner_id: str, pack_hash: str) -> tuple[str, str]:
        return read_signature_record(
            self.signature_path(owner_id=owner_id, pack_hash=pack_hash),
            owner_id=owner_id,
            pack_hash=pack_hash,
        )

    def _pack_records_for_owner_unlocked(
        self,
        *,
        owner_id: str,
        errors: list[str] | None = None,
    ) -> list[StoredMetadataPackRecord]:
        owner = validate_owner_id(owner_id)
        records: list[StoredMetadataPackRecord] = []
        for path in self._iter_pack_paths_unlocked(owner_id=owner):
            record = self._record_from_path_unlocked(path, errors=errors)
            if record is not None and record.owner_id == owner:
                records.append(record)
        return records

    def _all_pack_records_unlocked(
        self,
        *,
        errors: list[str] | None = None,
    ) -> list[StoredMetadataPackRecord]:
        records: list[StoredMetadataPackRecord] = []
        for path in self._iter_pack_paths_unlocked():
            record = self._record_from_path_unlocked(path, errors=errors)
            if record is not None:
                records.append(record)
        return records

    def _iter_pack_paths_unlocked(self, *, owner_id: str | None = None):
        if not self.root_dir.exists() or not self.root_dir.is_dir():
            return

        if owner_id is not None:
            owner = validate_owner_id(owner_id)
            owner_prefix_dir = self.root_dir / owner[:2]
            owner_dir = owner_prefix_dir / owner
            try:
                owner_prefix_stat = owner_prefix_dir.lstat()
                owner_stat = owner_dir.lstat()
            except FileNotFoundError:
                return
            except OSError:
                return
            if (
                is_filesystem_redirection(owner_prefix_stat)
                or not stat.S_ISDIR(owner_prefix_stat.st_mode)
                or is_filesystem_redirection(owner_stat)
                or not stat.S_ISDIR(owner_stat.st_mode)
            ):
                return
            yield from self._iter_owner_pack_paths_unlocked(owner_dir=owner_dir, owner=owner)
            return

        for owner_prefix_dir in sorted(self.root_dir.iterdir()):
            try:
                owner_prefix_stat = owner_prefix_dir.lstat()
            except OSError:
                continue
            if (
                is_filesystem_redirection(owner_prefix_stat)
                or not stat.S_ISDIR(owner_prefix_stat.st_mode)
                or len(owner_prefix_dir.name) != 2
            ):
                continue

            for owner_dir in sorted(owner_prefix_dir.iterdir()):
                try:
                    owner_stat = owner_dir.lstat()
                except OSError:
                    continue
                if is_filesystem_redirection(owner_stat) or not stat.S_ISDIR(owner_stat.st_mode):
                    continue
                if not is_valid_blake3_hex(owner_dir.name):
                    continue
                owner = owner_dir.name
                if owner_prefix_dir.name != owner[:2]:
                    continue
                yield from self._iter_owner_pack_paths_unlocked(owner_dir=owner_dir, owner=owner)

    def _iter_owner_pack_paths_unlocked(self, *, owner_dir: Path, owner: str):
        for pack_prefix_dir in sorted(owner_dir.iterdir()):
            try:
                pack_prefix_stat = pack_prefix_dir.lstat()
            except OSError:
                continue
            if (
                is_filesystem_redirection(pack_prefix_stat)
                or not stat.S_ISDIR(pack_prefix_stat.st_mode)
                or len(pack_prefix_dir.name) != 2
            ):
                continue

            for path in sorted(pack_prefix_dir.iterdir()):
                if not path.name.endswith(OBJECT_PACK_FILE_SUFFIX):
                    continue
                pack_hash = path.name.removesuffix(OBJECT_PACK_FILE_SUFFIX)
                if not is_valid_blake3_hex(pack_hash):
                    continue
                if pack_prefix_dir.name != pack_hash[:2]:
                    continue
                # ``owner`` forma parte del contrato de este nivel del layout.
                if owner_dir.name != owner:
                    continue
                yield path

    def _record_from_path_unlocked(
        self,
        path: Path,
        *,
        errors: list[str] | None = None,
    ) -> StoredMetadataPackRecord | None:
        try:
            pack_hash = validate_pack_hash(path.name.removesuffix(OBJECT_PACK_FILE_SUFFIX))
            owner_id = validate_owner_id(path.parent.parent.name)
            expected_path = self.pack_path(owner_id=owner_id, pack_hash=pack_hash)
            if path != expected_path:
                return None
            st = validate_gc_regular_file(root_dir=self.root_dir, path=path)
            calculated = calculate_pack_hash_file(path)
            if calculated != pack_hash:
                return None
            public_key_b64, signature_b64 = self._read_signature_record(owner_id=owner_id, pack_hash=pack_hash)
            return StoredMetadataPackRecord(
                owner_id=owner_id,
                pack_hash=pack_hash,
                size_bytes=int(st.st_size),
                stored_at_unix=float(st.st_mtime),
                path=path,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            )
        except (OSError, GarbageCollectionPathError, TypeError, ValueError) as exc:
            if errors is not None:
                errors.append(f"metadata pack omitido {path}: {exc}")
            return None

    def _delete_record_unlocked(
        self,
        record: StoredMetadataPackRecord,
        *,
        errors: list[str] | None = None,
    ) -> int:
        try:
            stat_result = validate_gc_regular_file(root_dir=self.root_dir, path=record.path)
            deleted_bytes = int(stat_result.st_size)
            record.path.unlink()
        except FileNotFoundError:
            return 0
        except (OSError, GarbageCollectionPathError) as exc:
            raise MetadataPackStoreError(
                f"no se pudo borrar metadata pack {record.path}: {exc}"
            ) from exc

        signature_path = self.signature_path(
            owner_id=record.owner_id,
            pack_hash=record.pack_hash,
        )
        try:
            signature_path.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            message = f"no se pudo comprobar la firma de metadata pack {signature_path}: {exc}"
            if errors is not None:
                errors.append(message)
            else:
                _LOGGER.warning(message)
        else:
            try:
                validate_gc_regular_file(root_dir=self.root_dir, path=signature_path)
                signature_path.unlink()
            except (OSError, GarbageCollectionPathError) as exc:
                message = f"no se pudo borrar la firma de metadata pack {signature_path}: {exc}"
                if errors is not None:
                    errors.append(message)
                else:
                    _LOGGER.warning(message)

        try:
            fsync_dir(record.path.parent, strict=True)
        except OSError as exc:
            message = f"fsync de directorio falló {record.path.parent}: {exc}"
            if errors is not None:
                errors.append(message)
            else:
                _LOGGER.warning(message)
        return deleted_bytes

    def _prune_records_unlocked(
        self,
        *,
        candidates: list[StoredMetadataPackRecord],
        bytes_needed: int = 0,
        count_needed: int = 0,
        protected_pack_hash: str,
        dry_run: bool = False,
        errors: list[str] | None = None,
        candidate_reporter: PathReporter | None = None,
    ) -> tuple[int, int]:
        pruned_count = 0
        pruned_bytes = 0
        remaining_bytes = max(0, int(bytes_needed))
        remaining_count = max(0, int(count_needed))

        ordered = sorted(
            [record for record in candidates if record.pack_hash != protected_pack_hash],
            key=lambda record: (record.stored_at_unix, record.pack_hash),
        )
        for record in ordered:
            if remaining_bytes <= 0 and remaining_count <= 0:
                break
            if candidate_reporter is not None:
                candidate_reporter(record.path)
            if dry_run:
                deleted = int(record.size_bytes)
            else:
                try:
                    deleted = self._delete_record_unlocked(record, errors=errors)
                except MetadataPackStoreError as exc:
                    if errors is None:
                        raise
                    errors.append(str(exc))
                    continue
                if deleted <= 0:
                    continue
            pruned_count += 1
            pruned_bytes += deleted
            remaining_bytes = max(0, remaining_bytes - deleted)
            remaining_count = max(0, remaining_count - 1)

        if remaining_bytes > 0 or remaining_count > 0:
            raise MetadataPackQuotaError(
                "cuota de metadata packs excedida y no hay suficientes packs antiguos para podar"
            )

        return pruned_count, pruned_bytes

    def _prune_expired_records_unlocked(
        self,
        *,
        records: list[StoredMetadataPackRecord],
        protected_pack_hash: str = "",
        dry_run: bool = False,
        now_unix: float | None = None,
        errors: list[str] | None = None,
        candidate_reporter: PathReporter | None = None,
    ) -> tuple[int, int, float | None]:
        if self.max_age_days <= 0:
            return 0, 0, None

        now = time.time() if now_unix is None else float(now_unix)
        cutoff = now - (self.max_age_days * _SECONDS_PER_DAY)
        expired = sorted(
            [
                record
                for record in records
                if record.pack_hash != protected_pack_hash and record.stored_at_unix < cutoff
            ],
            key=lambda record: (record.stored_at_unix, record.pack_hash),
        )

        pruned_count = 0
        pruned_bytes = 0
        for record in expired:
            if candidate_reporter is not None:
                candidate_reporter(record.path)
            if dry_run:
                deleted = int(record.size_bytes)
            else:
                try:
                    deleted = self._delete_record_unlocked(record, errors=errors)
                except MetadataPackStoreError as exc:
                    if errors is None:
                        raise
                    errors.append(str(exc))
                    continue
                if deleted <= 0:
                    continue
            pruned_count += 1
            pruned_bytes += deleted

        return pruned_count, pruned_bytes, cutoff

    def _validate_incoming_quota_limits_unlocked(self, *, incoming_size: int) -> None:
        if incoming_size > self.max_pack_bytes:
            raise MetadataPackStoreError(
                f"metadata pack demasiado grande: {incoming_size} bytes > {self.max_pack_bytes}"
            )
        if incoming_size > self.max_total_bytes_per_owner:
            raise MetadataPackQuotaError(
                "metadata pack excede la cuota total del owner: "
                f"{incoming_size} bytes > {self.max_total_bytes_per_owner}"
            )
        if incoming_size > self.max_total_store_bytes:
            raise MetadataPackQuotaError(
                f"metadata pack excede la cuota total del store: {incoming_size} bytes > {self.max_total_store_bytes}"
            )

    def _enforce_post_store_quotas_unlocked(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        incoming_size: int,
    ) -> tuple[int, int]:
        self._validate_incoming_quota_limits_unlocked(incoming_size=incoming_size)

        pruned_count = 0
        pruned_bytes = 0

        expired_count, expired_bytes, _ = self._prune_expired_records_unlocked(
            records=self._all_pack_records_unlocked(),
            protected_pack_hash=pack_hash,
            dry_run=False,
        )
        pruned_count += expired_count
        pruned_bytes += expired_bytes

        owner_records = [
            record
            for record in self._pack_records_for_owner_unlocked(owner_id=owner_id)
            if record.pack_hash != pack_hash
        ]
        owner_bytes = sum(record.size_bytes for record in owner_records)
        owner_count_after = len(owner_records) + 1
        owner_bytes_after = owner_bytes + incoming_size

        owner_count_excess = max(0, owner_count_after - self.max_packs_per_owner)
        owner_bytes_excess = max(0, owner_bytes_after - self.max_total_bytes_per_owner)
        if owner_count_excess or owner_bytes_excess:
            count, bytes_deleted = self._prune_records_unlocked(
                candidates=owner_records,
                bytes_needed=owner_bytes_excess,
                count_needed=owner_count_excess,
                protected_pack_hash=pack_hash,
            )
            pruned_count += count
            pruned_bytes += bytes_deleted

        global_records = [
            record
            for record in self._all_pack_records_unlocked()
            if record.pack_hash != pack_hash
        ]
        global_bytes = sum(record.size_bytes for record in global_records)
        global_bytes_excess = max(0, global_bytes + incoming_size - self.max_total_store_bytes)
        if global_bytes_excess:
            count, bytes_deleted = self._prune_records_unlocked(
                candidates=global_records,
                bytes_needed=global_bytes_excess,
                protected_pack_hash=pack_hash,
            )
            pruned_count += count
            pruned_bytes += bytes_deleted

        return pruned_count, pruned_bytes

    def prune_to_limits(
        self,
        *,
        dry_run: bool = False,
        candidate_reporter: PathReporter | None = None,
    ) -> PruneMetadataPackStoreResult:
        with self._lock:
            errors: list[str] = []
            initial_records = self._all_pack_records_unlocked(errors=errors)
            owners_seen = len({record.owner_id for record in initial_records})
            pruned_count = 0
            pruned_bytes = 0

            expired_count, expired_bytes, cutoff = self._prune_expired_records_unlocked(
                records=initial_records,
                dry_run=bool(dry_run),
                errors=errors,
                candidate_reporter=candidate_reporter,
            )
            pruned_count += expired_count
            pruned_bytes += expired_bytes

            quota_pruned_count = 0
            quota_pruned_bytes = 0
            records_after_expiry = (
                initial_records
                if dry_run
                else self._all_pack_records_unlocked(errors=errors)
            )
            if dry_run and self.max_age_days > 0 and cutoff is not None:
                records_after_expiry = [record for record in initial_records if record.stored_at_unix >= cutoff]

            for owner_id in sorted({record.owner_id for record in records_after_expiry}):
                owner_records = [record for record in records_after_expiry if record.owner_id == owner_id]
                owner_count_excess = max(0, len(owner_records) - self.max_packs_per_owner)
                owner_bytes = sum(record.size_bytes for record in owner_records)
                owner_bytes_excess = max(0, owner_bytes - self.max_total_bytes_per_owner)

                if owner_count_excess or owner_bytes_excess:
                    count, bytes_deleted = self._prune_records_unlocked(
                        candidates=owner_records,
                        bytes_needed=owner_bytes_excess,
                        count_needed=owner_count_excess,
                        protected_pack_hash="",
                        dry_run=bool(dry_run),
                        errors=errors,
                        candidate_reporter=candidate_reporter,
                    )
                    quota_pruned_count += count
                    quota_pruned_bytes += bytes_deleted
                    pruned_count += count
                    pruned_bytes += bytes_deleted

                    if dry_run:
                        deleted_keys = {
                            (record.owner_id, record.pack_hash)
                            for record in sorted(
                                owner_records,
                                key=lambda record: (record.stored_at_unix, record.pack_hash),
                            )[:count]
                        }
                        records_after_expiry = [
                            record
                            for record in records_after_expiry
                            if (record.owner_id, record.pack_hash) not in deleted_keys
                        ]

            global_records = (
                records_after_expiry
                if dry_run
                else self._all_pack_records_unlocked(errors=errors)
            )
            global_bytes_excess = max(0, sum(record.size_bytes for record in global_records) - self.max_total_store_bytes)
            if global_bytes_excess:
                count, bytes_deleted = self._prune_records_unlocked(
                    candidates=global_records,
                    bytes_needed=global_bytes_excess,
                    protected_pack_hash="",
                    dry_run=bool(dry_run),
                    errors=errors,
                    candidate_reporter=candidate_reporter,
                )
                quota_pruned_count += count
                quota_pruned_bytes += bytes_deleted
                pruned_count += count
                pruned_bytes += bytes_deleted

            return PruneMetadataPackStoreResult(
                root_dir=self.root_dir,
                dry_run=bool(dry_run),
                max_age_days=self.max_age_days,
                cutoff_unix=cutoff,
                packs_seen=len(initial_records),
                owners_seen=owners_seen,
                expired_packs=expired_count,
                quota_packs=quota_pruned_count,
                pruned_packs=pruned_count,
                pruned_bytes=pruned_bytes,
                errors=tuple(errors),
            )

    def put_pack_bytes(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        data: bytes,
        public_key_b64: str,
        signature_b64: str,
    ) -> StoreMetadataPackResult:
        owner = validate_owner_id(owner_id)
        pack = validate_pack_hash(pack_hash)

        if not isinstance(data, bytes):
            raise MetadataPackStoreError(f"data debe ser bytes; recibido {type(data).__name__}")
        if not data:
            raise MetadataPackStoreError("metadata pack vacío rejected")
        if len(data) > self.max_pack_bytes:
            raise MetadataPackStoreError(
                f"metadata pack demasiado grande: {len(data)} bytes > {self.max_pack_bytes}"
            )

        calculated = calculate_pack_hash(data)
        if calculated != pack:
            raise MetadataPackStoreError(
                f"pack_hash no coincide: esperado={pack} calculado={calculated}"
            )

        if not public_key_b64 or not signature_b64:
            raise MetadataPackSignatureError("public_key_b64 y signature_b64 son obligatorios para packs distribuidos")
        if not verify_metadata_pack_signature(
            owner_id=owner,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
            pack_hash=pack,
        ):
            raise MetadataPackSignatureError("la firma del metadata pack es inválida")

        with self._lock:
            path = self.pack_path(owner_id=owner, pack_hash=pack)
            if path.exists():
                try:
                    existing_hash = calculate_pack_hash_file(path)
                except OSError as exc:
                    raise MetadataPackStoreError(
                        f"No se pudo leer el metadata pack existente {path}: {exc}"
                    ) from exc
                if existing_hash != pack:
                    raise MetadataPackCorruptionError(
                        f"metadata pack existente corrupto: esperado={pack} calculado={existing_hash}"
                    )

                self._write_signature_record(
                    owner_id=owner,
                    pack_hash=pack,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                )
                try:
                    os.utime(path, None)
                except OSError:
                    pass
                existing_size = int(path.stat().st_size)
                pruned_count, pruned_bytes = self._enforce_post_store_quotas_unlocked(
                    owner_id=owner,
                    pack_hash=pack,
                    incoming_size=existing_size,
                )
                existing_public, existing_signature = self._read_signature_record(owner_id=owner, pack_hash=pack)
                return StoreMetadataPackResult(
                    owner_id=owner,
                    pack_hash=pack,
                    path=path,
                    size_bytes=existing_size,
                    stored=False,
                    already_present=True,
                    public_key_b64=existing_public,
                    signature_b64=existing_signature,
                    pruned_packs=pruned_count,
                    pruned_bytes=pruned_bytes,
                )

            self._validate_incoming_quota_limits_unlocked(incoming_size=len(data))

            ensure_private_dir(path.parent)
            pack_written = False
            try:
                atomic_write_bytes(path, data, mode=0o600)
                pack_written = True
                self._write_signature_record(
                    owner_id=owner,
                    pack_hash=pack,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                )
            except Exception:
                if pack_written:
                    try:
                        path.unlink()
                    except OSError:
                        pass
                raise

            pruned_count, pruned_bytes = self._enforce_post_store_quotas_unlocked(
                owner_id=owner,
                pack_hash=pack,
                incoming_size=len(data),
            )

            return StoreMetadataPackResult(
                owner_id=owner,
                pack_hash=pack,
                path=path,
                size_bytes=len(data),
                stored=True,
                already_present=False,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
                pruned_packs=pruned_count,
                pruned_bytes=pruned_bytes,
            )

    def put_pack_file(
        self,
        *,
        owner_id: str,
        path: str | Path,
        expected_pack_hash: str | None = None,
        expected_size_bytes: int | None = None,
        public_key_b64: str,
        signature_b64: str,
    ) -> StoreMetadataPackResult:
        owner = validate_owner_id(owner_id)
        source_path = Path(path).expanduser().resolve()

        try:
            source_stat = source_path.stat()
        except OSError as exc:
            raise MetadataPackStoreError(
                f"No se pudo inspeccionar el metadata pack {source_path}: {exc}"
            ) from exc
        if not source_path.is_file():
            raise MetadataPackStoreError(f"metadata pack no es un archivo regular: {source_path}")

        size_bytes = int(source_stat.st_size)
        if size_bytes <= 0:
            raise MetadataPackStoreError("metadata pack vacío rechazado")
        if size_bytes > self.max_pack_bytes:
            raise MetadataPackStoreError(
                f"metadata pack demasiado grande: {size_bytes} bytes > {self.max_pack_bytes}"
            )
        if expected_size_bytes is not None and int(expected_size_bytes) != size_bytes:
            raise MetadataPackStoreError(
                "el tamaño del metadata pack no coincide con el anunciado: "
                f"esperado={int(expected_size_bytes)} recibido={size_bytes}"
            )

        try:
            pack_hash = calculate_pack_hash_file(source_path)
        except OSError as exc:
            raise MetadataPackStoreError(
                f"No se pudo leer el metadata pack {source_path}: {exc}"
            ) from exc
        if expected_pack_hash is not None:
            expected = validate_pack_hash(expected_pack_hash)
            if expected != pack_hash:
                raise MetadataPackStoreError(
                    "expected pack_hash no coincide con el archivo: "
                    f"esperado={expected} calculado={pack_hash}"
                )

        if not public_key_b64 or not signature_b64:
            raise MetadataPackSignatureError(
                "public_key_b64 y signature_b64 son obligatorios para packs distribuidos"
            )
        if not verify_metadata_pack_signature(
            owner_id=owner,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
            pack_hash=pack_hash,
        ):
            raise MetadataPackSignatureError("la firma del metadata pack es inválida")

        with self._lock:
            destination = self.pack_path(owner_id=owner, pack_hash=pack_hash)
            if destination.exists():
                try:
                    existing_hash = calculate_pack_hash_file(destination)
                except OSError as exc:
                    raise MetadataPackStoreError(
                        f"No se pudo leer el metadata pack existente {destination}: {exc}"
                    ) from exc
                if existing_hash != pack_hash:
                    raise MetadataPackCorruptionError(
                        "metadata pack existente corrupto: "
                        f"esperado={pack_hash} calculado={existing_hash}"
                    )

                self._write_signature_record(
                    owner_id=owner,
                    pack_hash=pack_hash,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                )
                try:
                    os.utime(destination, None)
                except OSError:
                    pass
                existing_size = int(destination.stat().st_size)
                pruned_count, pruned_bytes = self._enforce_post_store_quotas_unlocked(
                    owner_id=owner,
                    pack_hash=pack_hash,
                    incoming_size=existing_size,
                )
                existing_public, existing_signature = self._read_signature_record(
                    owner_id=owner,
                    pack_hash=pack_hash,
                )
                return StoreMetadataPackResult(
                    owner_id=owner,
                    pack_hash=pack_hash,
                    path=destination,
                    size_bytes=existing_size,
                    stored=False,
                    already_present=True,
                    public_key_b64=existing_public,
                    signature_b64=existing_signature,
                    pruned_packs=pruned_count,
                    pruned_bytes=pruned_bytes,
                )

            self._validate_incoming_quota_limits_unlocked(incoming_size=size_bytes)

            ensure_private_dir(destination.parent)
            pack_written = False
            try:
                atomic_copy_file(source_path, destination, mode=0o600)
                pack_written = True
                self._write_signature_record(
                    owner_id=owner,
                    pack_hash=pack_hash,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                )
            except Exception:
                if pack_written:
                    try:
                        destination.unlink()
                    except OSError:
                        pass
                raise

            pruned_count, pruned_bytes = self._enforce_post_store_quotas_unlocked(
                owner_id=owner,
                pack_hash=pack_hash,
                incoming_size=size_bytes,
            )

            return StoreMetadataPackResult(
                owner_id=owner,
                pack_hash=pack_hash,
                path=destination,
                size_bytes=size_bytes,
                stored=True,
                already_present=False,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
                pruned_packs=pruned_count,
                pruned_bytes=pruned_bytes,
            )

    def get_pack_bytes(self, *, owner_id: str, pack_hash: str) -> bytes:
        path = self.pack_path(owner_id=owner_id, pack_hash=pack_hash)
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise MetadataPackStoreError(f"No se pudo leer el metadata pack {path}: {exc}") from exc

        calculated = calculate_pack_hash(data)
        expected = validate_pack_hash(pack_hash)
        if calculated != expected:
            raise MetadataPackCorruptionError(
                f"hash de metadata pack no coincide: esperado={expected} calculado={calculated}"
            )
        return data

    def get_signature_record(self, *, owner_id: str, pack_hash: str) -> tuple[str, str]:
        return self._read_signature_record(owner_id=owner_id, pack_hash=pack_hash)

    def retrieve_pack_to_file(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        out_path: str | Path,
    ) -> Path:
        expected = validate_pack_hash(pack_hash)
        source = self.pack_path(owner_id=owner_id, pack_hash=expected)
        try:
            calculated = calculate_pack_hash_file(source)
        except OSError as exc:
            raise MetadataPackStoreError(
                f"No se pudo leer el metadata pack {source}: {exc}"
            ) from exc
        if calculated != expected:
            raise MetadataPackCorruptionError(
                f"hash de metadata pack no coincide: esperado={expected} calculado={calculated}"
            )

        out = Path(out_path).expanduser().resolve()
        atomic_copy_file(source, out, mode=0o600)
        return out

    def list_packs(self, *, owner_id: str) -> list[StoredMetadataPackRecord]:
        with self._lock:
            return self._pack_records_for_owner_unlocked(owner_id=owner_id)


def _require_int(name: str, value: object, *, min_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StopanConfigValueError(f"{name} debe ser un entero >= {min_value}; recibido {value!r}")
    if value < min_value:
        raise StopanConfigValueError(f"{name} debe ser >= {min_value}; recibido {value!r}")
    return value
