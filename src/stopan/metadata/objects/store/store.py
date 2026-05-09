"""
Metadata object store local cifrado.

Guarda objetos de metadata deduplicados, cifrados con ChaCha20-Poly1305 y
direccionados por un storage_id derivado del hash lógico mediante clave privada.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import blake3

from stopan.common.encoding import b64decode, b64encode
from stopan.common.fs import atomic_write_bytes, ensure_private_dir, fsync_dir
from stopan.common.json import canonical_json_bytes, load_json_file
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.codec import EncodedMetadataObject
from stopan.metadata.objects.graph import MetadataObjectGraph
from stopan.metadata.objects.models import (
    METADATA_OBJECT_FORMAT,
    METADATA_OBJECT_VERSION,
    MetadataObjectType,
)
from stopan.metadata.objects.store.crypto import (
    derive_master_key,
    derive_subkey,
    require_cryptography,
    storage_id,
)
from stopan.metadata.objects.store.errors import (
    MetadataObjectStoreAuthenticationError,
    MetadataObjectStoreError,
)
from stopan.metadata.objects.store.format import (
    ENCRYPTED_OBJECT_FORMAT,
    ENCRYPTED_OBJECT_VERSION,
    LATEST_POINTER_FORMAT,
    LATEST_POINTER_VERSION,
    OBJECT_STORE_AEAD_CHACHA20_POLY1305,
    ObjectStoreHeader,
    header_payload,
    inspect_object_store_header,
    parse_header,
)
from stopan.metadata.objects.store.lock import MetadataObjectStoreLock, object_store_lock


def _require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectStoreError(f"{name} debe ser string")
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise MetadataObjectStoreError(f"{name} debe ser hex lowercase de 64 caracteres")
    return value


def _require_int(name: str, value: object, *, min_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataObjectStoreError(f"{name} debe ser entero")
    if value < min_value:
        raise MetadataObjectStoreError(f"{name} debe ser >= {min_value}")
    return value


@dataclass(frozen=True, slots=True)
class LatestMetadataPointer:
    catalog_hash: str
    state_digest: str
    object_count: int
    total_canonical_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int

    def __post_init__(self) -> None:
        _require_hash64("latest.catalog_hash", self.catalog_hash)
        _require_hash64("latest.state_digest", self.state_digest)
        _require_int("latest.object_count", self.object_count, min_value=0)
        _require_int(
            "latest.total_canonical_bytes",
            self.total_canonical_bytes,
            min_value=0,
        )
        _require_int("latest.snapshot_count", self.snapshot_count, min_value=0)
        _require_int("latest.known_chunk_count", self.known_chunk_count, min_value=0)
        _require_int("latest.protection_record_count", self.protection_record_count, min_value=0)


@dataclass(frozen=True, slots=True)
class MetadataObjectStoreWriteResult:
    root_dir: Path
    catalog_hash: str
    state_digest: str
    objects_total: int
    objects_written: int
    objects_reused: int
    total_canonical_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataObjectStoreInspection:
    header: ObjectStoreHeader
    latest: LatestMetadataPointer | None = None


class MetadataObjectStore:
    """Object store local cifrado para objetos de metadata deduplicados."""

    def __init__(self, *, root_dir: Path, salt: bytes, cost: ScryptCost, master_key: bytes):
        self.root_dir = root_dir
        self.salt = salt
        self.cost = cost
        self._aead_key = derive_subkey(
            master_key,
            info=b"stopan.metadata.object_store.aead.v1",
        )
        self._id_key = derive_subkey(
            master_key,
            info=b"stopan.metadata.object_store.object_id.v1",
        )

    @classmethod
    def open_or_create(
        cls,
        root_dir: str | Path,
        *,
        passphrase: str | bytes,
        scrypt_cost: ScryptCost,
    ) -> "MetadataObjectStore":
        root = Path(root_dir).expanduser().resolve()
        ensure_private_dir(root)
        ensure_private_dir(root / "objects")

        header_path = root / "store.json"
        if header_path.exists():
            salt, cost = parse_header(root, load_json_file(header_path))
        else:
            salt = os.urandom(16)
            cost = scrypt_cost
            atomic_write_bytes(header_path, canonical_json_bytes(header_payload(salt=salt, cost=cost)), mode=0o600)
            fsync_dir(root)

        master_key = derive_master_key(passphrase, salt=salt, cost=cost)
        return cls(root_dir=root, salt=salt, cost=cost, master_key=master_key)

    @classmethod
    def open_existing(
        cls,
        root_dir: str | Path,
        *,
        passphrase: str | bytes,
    ) -> "MetadataObjectStore":
        root = Path(root_dir).expanduser().resolve()
        header_path = root / "store.json"
        if not header_path.exists():
            raise FileNotFoundError(f"No existe object store en {root}: falta store.json")
        salt, cost = parse_header(root, load_json_file(header_path))
        master_key = derive_master_key(passphrase, salt=salt, cost=cost)
        return cls(root_dir=root, salt=salt, cost=cost, master_key=master_key)

    def inspect(self, *, decrypt_latest: bool = False) -> MetadataObjectStoreInspection:
        header = inspect_object_store_header(self.root_dir)
        latest = self.read_latest_pointer() if decrypt_latest and header.has_latest else None
        return MetadataObjectStoreInspection(header=header, latest=latest)

    def object_storage_id(self, object_hash: str) -> str:
        return storage_id(object_hash, id_key=self._id_key)

    def object_path_for_hash(self, object_hash: str) -> Path:
        storage_id_value = self.object_storage_id(object_hash)
        return self.root_dir / "objects" / storage_id_value[:2] / f"{storage_id_value}.stobj"

    def has_object(self, object_hash: str) -> bool:
        return self.object_path_for_hash(object_hash).exists()

    def put_graph(self, graph: MetadataObjectGraph) -> MetadataObjectStoreWriteResult:
        written, reused = self.put_objects_batch(list(graph.objects.values()))

        self.write_latest_pointer(
            LatestMetadataPointer(
                catalog_hash=graph.catalog_hash,
                state_digest=graph.state_digest,
                object_count=graph.object_count,
                total_canonical_bytes=graph.total_canonical_bytes,
                snapshot_count=graph.snapshot_count,
                known_chunk_count=graph.known_chunk_count,
                protection_record_count=graph.protection_record_count,
            )
        )

        return MetadataObjectStoreWriteResult(
            root_dir=self.root_dir,
            catalog_hash=graph.catalog_hash,
            state_digest=graph.state_digest,
            objects_total=graph.object_count,
            objects_written=written,
            objects_reused=reused,
            total_canonical_bytes=graph.total_canonical_bytes,
            snapshot_count=graph.snapshot_count,
            known_chunk_count=graph.known_chunk_count,
            protection_record_count=graph.protection_record_count,
        )

    def _validate_encoded_object(self, encoded: EncodedMetadataObject) -> None:
        if not isinstance(encoded, EncodedMetadataObject):
            raise MetadataObjectStoreError("encoded debe ser EncodedMetadataObject")
        if not isinstance(encoded.canonical_bytes, bytes):
            raise MetadataObjectStoreError("encoded.canonical_bytes debe ser bytes")
        calculated = blake3.blake3(encoded.canonical_bytes).hexdigest()
        if calculated != encoded.object_hash:
            raise MetadataObjectStoreError(
                f"encoded.object_hash no coincide: esperado={encoded.object_hash} "
                f"calculado={calculated}"
            )

    def _put_object_no_parent_fsync(self, encoded: EncodedMetadataObject) -> tuple[bool, Path]:
        self._validate_encoded_object(encoded)
        storage_id_value = self.object_storage_id(encoded.object_hash)
        path = self.root_dir / "objects" / storage_id_value[:2] / f"{storage_id_value}.stobj"
        if path.exists():
            return False, path.parent

        ensure_private_dir(path.parent)
        crypto = require_cryptography()
        nonce = os.urandom(12)
        ad = self._object_associated_data(storage_id_value)
        ciphertext = crypto.ChaCha20Poly1305(self._aead_key).encrypt(
            nonce,
            encoded.canonical_bytes,
            ad,
        )

        payload = {
            "format": ENCRYPTED_OBJECT_FORMAT,
            "version": ENCRYPTED_OBJECT_VERSION,
            "storage_id": storage_id_value,
            "aead": {
                "name": OBJECT_STORE_AEAD_CHACHA20_POLY1305,
                "nonce": b64encode(nonce),
            },
            "ciphertext": b64encode(ciphertext),
        }

        atomic_write_bytes(
            path,
            canonical_json_bytes(payload),
            mode=0o600,
            sync_parent_dir=False,
        )
        return True, path.parent

    def put_object(self, encoded: EncodedMetadataObject) -> bool:
        written, parent = self._put_object_no_parent_fsync(encoded)
        if written:
            fsync_dir(parent)
        return written

    def put_objects_batch(self, objects: list[EncodedMetadataObject]) -> tuple[int, int]:
        written = 0
        reused = 0
        touched_dirs: set[Path] = set()

        for encoded in sorted(objects, key=lambda item: item.object_hash):
            did_write, parent_dir = self._put_object_no_parent_fsync(encoded)
            if did_write:
                written += 1
                touched_dirs.add(parent_dir)
            else:
                reused += 1

        for directory in sorted(touched_dirs):
            fsync_dir(directory)

        return written, reused

    def get_object_bytes(
        self,
        *,
        object_hash: str,
        expected_type: MetadataObjectType | None = None,
    ) -> bytes:
        storage_id_value = self.object_storage_id(object_hash)
        path = self.root_dir / "objects" / storage_id_value[:2] / f"{storage_id_value}.stobj"
        if not path.exists():
            raise FileNotFoundError(f"metadata object no encontrado: {object_hash}")

        raw = load_json_file(path)
        if raw.get("format") != ENCRYPTED_OBJECT_FORMAT:
            raise MetadataObjectStoreError(f"objeto cifrado inválido: {path}")
        if raw.get("version") != ENCRYPTED_OBJECT_VERSION:
            raise MetadataObjectStoreError(
                f"versión de objeto cifrado no soportada: {raw.get('version')!r}"
            )
        if raw.get("storage_id") != storage_id_value:
            raise MetadataObjectStoreError(f"storage_id inconsistente en {path}")

        aead = raw.get("aead")
        if (
            not isinstance(aead, dict)
            or aead.get("name") != OBJECT_STORE_AEAD_CHACHA20_POLY1305
        ):
            raise MetadataObjectStoreError(f"AEAD inválido en {path}")
        nonce = b64decode("object.aead.nonce", aead.get("nonce"))
        ciphertext = b64decode("object.ciphertext", raw.get("ciphertext"))

        crypto = require_cryptography()
        try:
            plaintext = crypto.ChaCha20Poly1305(self._aead_key).decrypt(
                nonce,
                ciphertext,
                self._object_associated_data(storage_id_value),
            )
        except crypto.InvalidTag as exc:
            raise MetadataObjectStoreAuthenticationError(
                f"No se pudo autenticar metadata object {object_hash}: "
                "passphrase incorrecta o fichero manipulado."
            ) from exc

        calculated_hash = blake3.blake3(plaintext).hexdigest()
        if calculated_hash != object_hash:
            raise MetadataObjectStoreAuthenticationError(
                f"hash de metadata object no coincide: esperado={object_hash} "
                f"calculado={calculated_hash}"
            )

        if expected_type is not None:
            envelope = json.loads(plaintext.decode("utf-8"))
            if not isinstance(envelope, dict):
                raise MetadataObjectStoreError(
                    f"metadata object plaintext inválido: {object_hash}"
                )
            if (
                envelope.get("format") != METADATA_OBJECT_FORMAT
                or envelope.get("version") != METADATA_OBJECT_VERSION
            ):
                raise MetadataObjectStoreError(
                    f"metadata object envelope no soportado: {object_hash}"
                )
            if envelope.get("object_type") != expected_type.value:
                raise MetadataObjectStoreError(
                    f"tipo de metadata object no coincide para {object_hash}: "
                    f"esperado={expected_type.value} recibido={envelope.get('object_type')!r}"
                )

        return plaintext

    def write_latest_pointer(self, latest: LatestMetadataPointer) -> None:
        if not isinstance(latest, LatestMetadataPointer):
            raise MetadataObjectStoreError("latest debe ser LatestMetadataPointer")

        plaintext = canonical_json_bytes(
            {
                "format": LATEST_POINTER_FORMAT,
                "version": LATEST_POINTER_VERSION,
                "catalog_hash": latest.catalog_hash,
                "state_digest": latest.state_digest,
                "object_count": latest.object_count,
                "total_canonical_bytes": latest.total_canonical_bytes,
                "snapshot_count": latest.snapshot_count,
                "known_chunk_count": latest.known_chunk_count,
                "protection_record_count": latest.protection_record_count,
            }
        )
        crypto = require_cryptography()
        nonce = os.urandom(12)
        ciphertext = crypto.ChaCha20Poly1305(self._aead_key).encrypt(
            nonce,
            plaintext,
            self._latest_associated_data(),
        )
        payload = {
            "format": f"{LATEST_POINTER_FORMAT}.encrypted",
            "version": LATEST_POINTER_VERSION,
            "aead": {
                "name": OBJECT_STORE_AEAD_CHACHA20_POLY1305,
                "nonce": b64encode(nonce),
            },
            "ciphertext": b64encode(ciphertext),
        }
        atomic_write_bytes(
            self.root_dir / "latest.json",
            canonical_json_bytes(payload),
            mode=0o600,
        )

    def read_latest_pointer(self) -> LatestMetadataPointer:
        path = self.root_dir / "latest.json"
        if not path.exists():
            raise FileNotFoundError(f"metadata object store sin latest pointer: {self.root_dir}")

        raw = load_json_file(path)
        if raw.get("format") != f"{LATEST_POINTER_FORMAT}.encrypted":
            raise MetadataObjectStoreError(f"latest pointer inválido en {path}")
        if raw.get("version") != LATEST_POINTER_VERSION:
            raise MetadataObjectStoreError(
                f"versión de latest pointer no soportada: {raw.get('version')!r}"
            )
        aead = raw.get("aead")
        if (
            not isinstance(aead, dict)
            or aead.get("name") != OBJECT_STORE_AEAD_CHACHA20_POLY1305
        ):
            raise MetadataObjectStoreError(f"latest pointer AEAD inválido en {path}")

        nonce = b64decode("latest.aead.nonce", aead.get("nonce"))
        ciphertext = b64decode("latest.ciphertext", raw.get("ciphertext"))

        crypto = require_cryptography()
        try:
            plaintext = crypto.ChaCha20Poly1305(self._aead_key).decrypt(
                nonce,
                ciphertext,
                self._latest_associated_data(),
            )
        except crypto.InvalidTag as exc:
            raise MetadataObjectStoreAuthenticationError(
                "No se pudo autenticar latest pointer: passphrase incorrecta o fichero manipulado."
            ) from exc

        decoded = json.loads(plaintext.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise MetadataObjectStoreError("latest pointer plaintext inválido")
        if decoded.get("format") != LATEST_POINTER_FORMAT:
            raise MetadataObjectStoreError("formato de latest pointer inválido")
        if decoded.get("version") != LATEST_POINTER_VERSION:
            raise MetadataObjectStoreError(
                f"versión de latest pointer no soportada: {decoded.get('version')!r}"
            )

        return LatestMetadataPointer(
            catalog_hash=str(decoded["catalog_hash"]),
            state_digest=str(decoded["state_digest"]),
            object_count=int(decoded["object_count"]),
            total_canonical_bytes=int(decoded["total_canonical_bytes"]),
            snapshot_count=int(decoded["snapshot_count"]),
            known_chunk_count=int(decoded["known_chunk_count"]),
            protection_record_count=int(decoded["protection_record_count"]),
        )

    def _object_associated_data(self, storage_id_value: str) -> bytes:
        return canonical_json_bytes(
            {
                "format": ENCRYPTED_OBJECT_FORMAT,
                "version": ENCRYPTED_OBJECT_VERSION,
                "storage_id": storage_id_value,
            }
        )

    def _latest_associated_data(self) -> bytes:
        return canonical_json_bytes(
            {
                "format": f"{LATEST_POINTER_FORMAT}.encrypted",
                "version": LATEST_POINTER_VERSION,
            }
        )
