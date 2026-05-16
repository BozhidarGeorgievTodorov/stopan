"""
Formato persistido del metadata object store.

Define el header del store, el formato de objetos cifrados y el latest pointer.
La validación del header fija algoritmos y parámetros KDF antes de abrir el store.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stopan.common.encoding import b64decode, b64encode
from stopan.common.json import load_json_file
from stopan.metadata.identity.models import AEAD_CHACHA20_POLY1305, KDF_SCRYPT
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.store.errors import MetadataObjectStoreError, MetadataObjectStoreMissingError


OBJECT_STORE_FORMAT = "stopan.metadata_object_store"
OBJECT_STORE_VERSION = 1
ENCRYPTED_OBJECT_FORMAT = "stopan.encrypted_metadata_object"
ENCRYPTED_OBJECT_VERSION = 1
LATEST_POINTER_FORMAT = "stopan.metadata_latest_pointer"
LATEST_POINTER_VERSION = 1
OBJECT_ID_DERIVATION_BLAKE3_KEYED = "hkdf-sha256+scrypt-passphrase+blake3-keyed"
OBJECT_STORE_AEAD_CHACHA20_POLY1305 = AEAD_CHACHA20_POLY1305


@dataclass(frozen=True, slots=True)
class ObjectStoreHeader:
    root_dir: Path
    scrypt_n: int
    scrypt_r: int
    scrypt_p: int
    key_length: int
    salt: bytes
    object_count_on_disk: int
    has_latest: bool


def header_payload(*, salt: bytes, cost: ScryptCost) -> dict[str, Any]:
    return {
        "format": OBJECT_STORE_FORMAT,
        "version": OBJECT_STORE_VERSION,
        "kdf": {
            "name": KDF_SCRYPT,
            "salt": b64encode(salt),
            "n": int(cost.n),
            "r": int(cost.r),
            "p": int(cost.p),
            "key_length": int(cost.key_length),
        },
        "object_encryption": {
            "aead": OBJECT_STORE_AEAD_CHACHA20_POLY1305,
            "object_id": OBJECT_ID_DERIVATION_BLAKE3_KEYED,
        },
    }


def parse_header(root_dir: Path, raw: dict[str, Any]) -> tuple[bytes, ScryptCost]:
    if raw.get("format") != OBJECT_STORE_FORMAT:
        raise MetadataObjectStoreError(f"object store format inválido en {root_dir}")
    if raw.get("version") != OBJECT_STORE_VERSION:
        raise MetadataObjectStoreError(
            f"versión de object store no soportada en {root_dir}: {raw.get('version')!r}"
        )

    kdf = raw.get("kdf")
    if not isinstance(kdf, dict):
        raise MetadataObjectStoreError("object store header sin kdf válido")
    if kdf.get("name") != KDF_SCRYPT:
        raise MetadataObjectStoreError(f"KDF no soportado: {kdf.get('name')!r}")

    object_encryption = raw.get("object_encryption")
    if not isinstance(object_encryption, dict):
        raise MetadataObjectStoreError("object store header sin object_encryption válido")
    if object_encryption.get("aead") != OBJECT_STORE_AEAD_CHACHA20_POLY1305:
        raise MetadataObjectStoreError(
            f"object store AEAD no soportado: {object_encryption.get('aead')!r}"
        )
    if object_encryption.get("object_id") != OBJECT_ID_DERIVATION_BLAKE3_KEYED:
        raise MetadataObjectStoreError(
            f"object_id de object store no soportado: {object_encryption.get('object_id')!r}"
        )

    salt = b64decode("store.kdf.salt", kdf.get("salt"))
    cost = ScryptCost(
        n=int(kdf.get("n")),
        r=int(kdf.get("r")),
        p=int(kdf.get("p")),
        key_length=int(kdf.get("key_length")),
    )
    return salt, cost


def object_files(root_dir: Path) -> list[Path]:
    objects_dir = root_dir / "objects"
    if not objects_dir.exists():
        return []
    return sorted(objects_dir.glob("*/*.stobj"))


def inspect_object_store_header(root_dir: str | Path) -> ObjectStoreHeader:
    root = Path(root_dir).expanduser().resolve()
    header_path = root / "store.json"
    if not header_path.exists():
        raise MetadataObjectStoreMissingError(f"No existe object store en {root}: falta store.json")
    salt, cost = parse_header(root, load_json_file(header_path))
    return ObjectStoreHeader(
        root_dir=root,
        scrypt_n=cost.n,
        scrypt_r=cost.r,
        scrypt_p=cost.p,
        key_length=cost.key_length,
        salt=salt,
        object_count_on_disk=len(object_files(root)),
        has_latest=(root / "latest.json").exists(),
    )
