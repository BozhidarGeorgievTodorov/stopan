"""
Primitivas criptográficas del metadata object store.

El store deriva una master key desde passphrase+scrypt y separa subclaves para
cifrado AEAD y derivación privada de storage_id.
"""

from __future__ import annotations

from dataclasses import dataclass

import blake3

from stopan.common.secrets import passphrase_bytes
from stopan.metadata.identity.passphrase import ScryptCost


@dataclass(frozen=True, slots=True)
class ObjectStoreCryptoPrimitives:
    ChaCha20Poly1305: type
    Scrypt: type
    HKDF: type
    hashes: object
    InvalidTag: type[Exception]


def require_cryptography() -> ObjectStoreCryptoPrimitives:
    try:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError as exc:
        raise RuntimeError(
            "Falta dependencia criptográfica: instala 'cryptography' para usar metadata object store."
        ) from exc

    return ObjectStoreCryptoPrimitives(
        ChaCha20Poly1305=ChaCha20Poly1305,
        Scrypt=Scrypt,
        HKDF=HKDF,
        hashes=hashes,
        InvalidTag=InvalidTag,
    )


def derive_master_key(passphrase: str | bytes, *, salt: bytes, cost: ScryptCost) -> bytes:
    crypto = require_cryptography()
    kdf = crypto.Scrypt(
        salt=salt,
        length=cost.key_length,
        n=cost.n,
        r=cost.r,
        p=cost.p,
    )
    return kdf.derive(passphrase_bytes(passphrase))


def derive_subkey(master_key: bytes, *, info: bytes) -> bytes:
    crypto = require_cryptography()
    return crypto.HKDF(
        algorithm=crypto.hashes.SHA256(),
        length=32,
        salt=None,
        info=info,
    ).derive(master_key)


def storage_id(object_hash: str, *, id_key: bytes) -> str:
    if not isinstance(object_hash, str):
        raise TypeError("object_hash debe ser string")
    if len(object_hash) != 64 or any(char not in "0123456789abcdef" for char in object_hash):
        raise ValueError("object_hash debe ser hex lowercase de 64 caracteres")
    if not isinstance(id_key, bytes) or len(id_key) != 32:
        raise ValueError("id_key debe contener exactamente 32 bytes")
    return blake3.blake3(object_hash.encode("ascii"), key=id_key).hexdigest()
