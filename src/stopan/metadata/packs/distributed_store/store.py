"""
Almacén local de metadata packs distribuidos.

Guarda packs cifrados direccionados por BLAKE3, exige sidecar firmado y aplica
cuotas locales por owner, tamaño total y antigüedad.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from stopan.common.fs import atomic_write_bytes, ensure_private_dir, fsync_dir
from stopan.errors import StopanConfigValueError
from stopan.metadata.identity import validate_owner_id, verify_metadata_pack_signature
from stopan.metadata.packs.format import OBJECT_PACK_FILE_SUFFIX
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash

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

    def _pack_records_for_owner_unlocked(self, *, owner_id: str) -> list[StoredMetadataPackRecord]:
        owner = validate_owner_id(owner_id)
        owner_dir = self.root_dir / owner[:2] / owner
        if not owner_dir.exists():
            return []

        records: list[StoredMetadataPackRecord] = []
        for path in sorted(owner_dir.glob(f"*/*{OBJECT_PACK_FILE_SUFFIX}")):
            record = self._record_from_path_unlocked(path)
            if record is not None and record.owner_id == owner:
                records.append(record)
        return records

    def _all_pack_records_unlocked(self) -> list[StoredMetadataPackRecord]:
        if not self.root_dir.exists():
            return []

        records: list[StoredMetadataPackRecord] = []
        for path in sorted(self.root_dir.glob(f"*/*/*/*{OBJECT_PACK_FILE_SUFFIX}")):
            record = self._record_from_path_unlocked(path)
            if record is not None:
                records.append(record)
        return records

    def _record_from_path_unlocked(self, path: Path) -> StoredMetadataPackRecord | None:
        try:
            pack_hash = validate_pack_hash(path.name.removesuffix(OBJECT_PACK_FILE_SUFFIX))
            owner_id = validate_owner_id(path.parent.parent.name)
            st = path.stat()
            calculated = calculate_pack_hash(path.read_bytes())
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
        except (OSError, TypeError, ValueError):
            return None

    def _delete_record_unlocked(self, record: StoredMetadataPackRecord) -> int:
        try:
            deleted_bytes = int(record.path.stat().st_size)
        except OSError:
            deleted_bytes = int(record.size_bytes)

        for path in (
            self.signature_path(owner_id=record.owner_id, pack_hash=record.pack_hash),
            record.path,
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

        try:
            fsync_dir(record.path.parent)
        except OSError:
            pass
        return deleted_bytes

    def _prune_records_unlocked(
        self,
        *,
        candidates: list[StoredMetadataPackRecord],
        bytes_needed: int = 0,
        count_needed: int = 0,
        protected_pack_hash: str,
        dry_run: bool = False,
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
            deleted = int(record.size_bytes) if dry_run else self._delete_record_unlocked(record)
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
            deleted = int(record.size_bytes) if dry_run else self._delete_record_unlocked(record)
            pruned_count += 1
            pruned_bytes += deleted

        return pruned_count, pruned_bytes, cutoff

    def _enforce_pre_store_quotas_unlocked(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        incoming_size: int,
    ) -> tuple[int, int]:
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

    def prune_to_limits(self, *, dry_run: bool = False) -> PruneMetadataPackStoreResult:
        with self._lock:
            initial_records = self._all_pack_records_unlocked()
            owners_seen = len({record.owner_id for record in initial_records})
            pruned_count = 0
            pruned_bytes = 0

            expired_count, expired_bytes, cutoff = self._prune_expired_records_unlocked(
                records=initial_records,
                dry_run=bool(dry_run),
            )
            pruned_count += expired_count
            pruned_bytes += expired_bytes

            quota_pruned_count = 0
            quota_pruned_bytes = 0
            records_after_expiry = initial_records if dry_run else self._all_pack_records_unlocked()
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

            global_records = records_after_expiry if dry_run else self._all_pack_records_unlocked()
            global_bytes_excess = max(0, sum(record.size_bytes for record in global_records) - self.max_total_store_bytes)
            if global_bytes_excess:
                count, bytes_deleted = self._prune_records_unlocked(
                    candidates=global_records,
                    bytes_needed=global_bytes_excess,
                    protected_pack_hash="",
                    dry_run=bool(dry_run),
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
            raise TypeError(f"data debe ser bytes; recibido {type(data).__name__}")
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
                    existing_data = path.read_bytes()
                except OSError as exc:
                    raise MetadataPackStoreError(f"No se pudo leer el metadata pack existente {path}: {exc}") from exc
                existing_hash = calculate_pack_hash(existing_data)
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
                prune_result = self.prune_to_limits(dry_run=False)
                existing_public, existing_signature = self._read_signature_record(owner_id=owner, pack_hash=pack)
                return StoreMetadataPackResult(
                    owner_id=owner,
                    pack_hash=pack,
                    path=path,
                    size_bytes=path.stat().st_size,
                    stored=False,
                    already_present=True,
                    public_key_b64=existing_public,
                    signature_b64=existing_signature,
                    pruned_packs=prune_result.pruned_packs,
                    pruned_bytes=prune_result.pruned_bytes,
                )

            pruned_count, pruned_bytes = self._enforce_pre_store_quotas_unlocked(
                owner_id=owner,
                pack_hash=pack,
                incoming_size=len(data),
            )

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
        public_key_b64: str,
        signature_b64: str,
    ) -> StoreMetadataPackResult:
        pack_path = Path(path).expanduser().resolve()
        try:
            data = pack_path.read_bytes()
        except OSError as exc:
            raise MetadataPackStoreError(f"No se pudo leer el metadata pack {pack_path}: {exc}") from exc

        pack_hash = calculate_pack_hash(data)
        if expected_pack_hash is not None and validate_pack_hash(expected_pack_hash) != pack_hash:
            raise MetadataPackStoreError(
                f"expected pack_hash no coincide con el archivo: esperado={expected_pack_hash} calculado={pack_hash}"
            )
        return self.put_pack_bytes(
            owner_id=owner_id,
            pack_hash=pack_hash,
            data=data,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
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
        data = self.get_pack_bytes(owner_id=owner_id, pack_hash=pack_hash)
        out = Path(out_path).expanduser().resolve()
        ensure_private_dir(out.parent)
        atomic_write_bytes(out, data, mode=0o600)
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
