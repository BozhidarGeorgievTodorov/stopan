"""
Primitivas criptográficas del metadata object store.

El store deriva una master key desde passphrase+scrypt y separa subclaves para
cifrado AEAD y derivación privada de storage_id.
"""

from __future__ import annotations

from dataclasses import dataclass

import blake3

from stopan.errors import StopanDependencyError
from stopan.metadata.crypto.kdf import derive_master_key as _derive_master_key
from stopan.metadata.crypto.kdf import derive_subkey as _derive_subkey
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.store.errors import MetadataObjectStoreError


@dataclass(frozen=True, slots=True)
class ObjectStoreCryptoPrimitives:
    ChaCha20Poly1305: type
    InvalidTag: type[Exception]


def require_cryptography() -> ObjectStoreCryptoPrimitives:
    try:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
    except ImportError as exc:
        raise StopanDependencyError(
            "Falta dependencia criptográfica: instala 'cryptography' para usar metadata object store."
        ) from exc

    return ObjectStoreCryptoPrimitives(
        ChaCha20Poly1305=ChaCha20Poly1305,
        InvalidTag=InvalidTag,
    )


def derive_master_key(passphrase: str | bytes, *, salt: bytes, cost: ScryptCost) -> bytes:
    return _derive_master_key(passphrase, salt=salt, cost=cost)


def derive_subkey(master_key: bytes, *, info: bytes) -> bytes:
    return _derive_subkey(master_key, info=info)


def storage_id(object_hash: str, *, id_key: bytes) -> str:
    if not isinstance(object_hash, str):
        raise MetadataObjectStoreError("object_hash debe ser string")
    if len(object_hash) != 64 or any(char not in "0123456789abcdef" for char in object_hash):
        raise MetadataObjectStoreError("object_hash debe ser hex lowercase de 64 caracteres")
    if not isinstance(id_key, bytes) or len(id_key) != 32:
        raise MetadataObjectStoreError("id_key debe contener exactamente 32 bytes")
    return blake3.blake3(object_hash.encode("ascii"), key=id_key).hexdigest()
