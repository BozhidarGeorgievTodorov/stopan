"""KDF comunes para metadata.

Centraliza la derivación Scrypt y HKDF-SHA256 usada por identidad,
object store y metadata packs. Los módulos de dominio conservan sus
propias validaciones y excepciones alrededor de estas primitivas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from stopan.common.secrets import passphrase_bytes
from stopan.errors import StopanDependencyError


@dataclass(frozen=True, slots=True)
class MetadataKdfPrimitives:
    Scrypt: type
    HKDF: type
    hashes: object


class ScryptCostLike(Protocol):
    n: int
    r: int
    p: int
    key_length: int


def require_kdf_primitives() -> MetadataKdfPrimitives:
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError as exc:
        raise StopanDependencyError("Falta la dependencia 'cryptography' para derivar claves de metadata.") from exc

    return MetadataKdfPrimitives(Scrypt=Scrypt, HKDF=HKDF, hashes=hashes)


def derive_master_key(passphrase: str | bytes, *, salt: bytes, cost: ScryptCostLike) -> bytes:
    crypto = require_kdf_primitives()
    kdf = crypto.Scrypt(
        salt=salt,
        length=cost.key_length,
        n=cost.n,
        r=cost.r,
        p=cost.p,
    )
    return kdf.derive(passphrase_bytes(passphrase))


def derive_hkdf_sha256_key(secret: bytes, *, info: bytes, length: int = 32) -> bytes:
    crypto = require_kdf_primitives()
    return crypto.HKDF(
        algorithm=crypto.hashes.SHA256(),
        length=length,
        salt=None,
        info=info,
    ).derive(secret)


def derive_subkey(master_key: bytes, *, info: bytes) -> bytes:
    return derive_hkdf_sha256_key(master_key, info=info, length=32)
