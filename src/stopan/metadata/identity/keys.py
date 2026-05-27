"""
Primitivas criptográficas para identidad de metadata.

Centraliza la validación de owner_id, derivación de claves con scrypt/HKDF y
cifrado autenticado de claves privadas con ChaCha20-Poly1305.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import blake3

from stopan.common.encoding import b64decode, b64encode
from stopan.errors import StopanDependencyError
from stopan.common.json import canonical_json_bytes
from stopan.metadata.identity.models import (
    AEAD_CHACHA20_POLY1305,
    IDENTITY_ALGORITHM_ED25519_X25519,
    KDF_SCRYPT,
    METADATA_IDENTITY_FORMAT,
    METADATA_IDENTITY_VERSION,
    PRIVATE_KEY_ROLE_ENCRYPTION,
    PRIVATE_KEY_ROLE_SIGNING,
    MetadataIdentity,
    MetadataIdentityAuthenticationError,
    MetadataIdentityError,
)
from stopan.metadata.crypto.kdf import derive_master_key as _derive_master_key
from stopan.metadata.crypto.kdf import derive_subkey as _derive_subkey
from stopan.metadata.identity.passphrase import ScryptCost


_HEX64_ALPHABET = set("0123456789abcdef")
_PRIVATE_KEY_ROLES = {PRIVATE_KEY_ROLE_SIGNING, PRIVATE_KEY_ROLE_ENCRYPTION}


@dataclass(frozen=True, slots=True)
class CryptographyPrimitives:
    Ed25519PrivateKey: Any
    Ed25519PublicKey: Any
    serialization: Any
    ChaCha20Poly1305: Any
    InvalidTag: type[Exception]
    InvalidSignature: type[Exception]


@dataclass(frozen=True, slots=True)
class X25519Primitives:
    X25519PrivateKey: Any
    X25519PublicKey: Any


def require_cryptography() -> CryptographyPrimitives:
    try:
        from cryptography.exceptions import InvalidSignature, InvalidTag
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
    except ImportError as exc:
        raise StopanDependencyError("Falta la dependencia 'cryptography' para usar identidad de metadata.") from exc

    return CryptographyPrimitives(
        Ed25519PrivateKey=Ed25519PrivateKey,
        Ed25519PublicKey=Ed25519PublicKey,
        serialization=serialization,
        ChaCha20Poly1305=ChaCha20Poly1305,
        InvalidTag=InvalidTag,
        InvalidSignature=InvalidSignature,
    )


def require_x25519() -> X25519Primitives:
    try:
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
    except ImportError as exc:
        raise StopanDependencyError("La dependencia 'cryptography' no tiene soporte X25519 para identidad de metadata.") from exc
    return X25519Primitives(
        X25519PrivateKey=X25519PrivateKey,
        X25519PublicKey=X25519PublicKey,
    )


def validate_hex64_lower(value: str, *, name: str) -> str:
    if not isinstance(value, str):
        raise MetadataIdentityError(f"{name} debe ser string")
    text = value.strip()
    if len(text) != 64 or any(char not in _HEX64_ALPHABET for char in text):
        raise MetadataIdentityError(f"{name} debe tener 64 caracteres hexadecimales lowercase")
    return text


def validate_owner_id(value: str, *, name: str = "owner_id") -> str:
    return validate_hex64_lower(value, name=name)


def require_raw_key(name: str, value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise MetadataIdentityError(f"{name} debe contener exactamente 32 bytes raw")
    return value


def derive_master_key(passphrase: str | bytes, *, salt: bytes, cost: ScryptCost) -> bytes:
    return _derive_master_key(passphrase, salt=salt, cost=cost)


def derive_subkey(master_key: bytes, *, info: bytes) -> bytes:
    return _derive_subkey(master_key, info=info)


def require_private_key_role(key_role: str) -> str:
    key_role = str(key_role)
    if key_role not in _PRIVATE_KEY_ROLES:
        raise MetadataIdentityError(f"Rol de clave privada de metadata inválido: {key_role!r}")
    return key_role


def identity_private_key_associated_data(
    *,
    owner_id: str,
    signing_public_key_b64: str,
    encryption_public_key_b64: str,
    key_role: str,
) -> bytes:
    key_role = require_private_key_role(key_role)
    return canonical_json_bytes(
        {
            "format": METADATA_IDENTITY_FORMAT,
            "version": METADATA_IDENTITY_VERSION,
            "algorithm": IDENTITY_ALGORITHM_ED25519_X25519,
            "owner_id": validate_owner_id(owner_id),
            "signing_public_key_b64": str(signing_public_key_b64),
            "encryption_public_key_b64": str(encryption_public_key_b64),
            "private_key_role": key_role,
            "private_key": "encrypted",
        }
    )


def subkey_info_for_role(key_role: str) -> bytes:
    key_role = require_private_key_role(key_role)
    if key_role == PRIVATE_KEY_ROLE_SIGNING:
        return b"stopan.metadata.identity.signing_private_key.aead.v1"
    return b"stopan.metadata.identity.encryption_private_key.aead.v1"


def owner_id_from_public_key(public_key_raw: bytes) -> str:
    if not isinstance(public_key_raw, bytes) or len(public_key_raw) != 32:
        raise MetadataIdentityError("public_key_raw debe contener 32 bytes raw de clave pública Ed25519")
    return blake3.blake3(public_key_raw).hexdigest()


def encrypted_private_key_record(
    *,
    raw_private_key: bytes,
    passphrase: str | bytes,
    scrypt_cost: ScryptCost,
    owner_id: str,
    signing_public_key_b64: str,
    encryption_public_key_b64: str,
    key_role: str,
) -> dict[str, Any]:
    key_role = require_private_key_role(key_role)
    raw_private_key = require_raw_key(f"{key_role}_private_key", raw_private_key)
    salt = os.urandom(16)
    master_key = derive_master_key(passphrase, salt=salt, cost=scrypt_cost)
    key = derive_subkey(master_key, info=subkey_info_for_role(key_role))

    nonce = os.urandom(12)
    crypto = require_cryptography()
    ciphertext = crypto.ChaCha20Poly1305(key).encrypt(
        nonce,
        raw_private_key,
        identity_private_key_associated_data(
            owner_id=owner_id,
            signing_public_key_b64=signing_public_key_b64,
            encryption_public_key_b64=encryption_public_key_b64,
            key_role=key_role,
        ),
    )

    return {
        "encrypted": True,
        "role": key_role,
        "kdf": {
            "name": KDF_SCRYPT,
            "salt": b64encode(salt),
            "n": int(scrypt_cost.n),
            "r": int(scrypt_cost.r),
            "p": int(scrypt_cost.p),
            "key_length": int(scrypt_cost.key_length),
        },
        "aead": {
            "name": AEAD_CHACHA20_POLY1305,
            "nonce": b64encode(nonce),
        },
        "ciphertext": b64encode(ciphertext),
    }


def decrypt_private_key_record(
    *,
    identity: MetadataIdentity,
    record: dict[str, Any],
    key_role: str,
    passphrase: str | bytes,
) -> bytes:
    key_role = require_private_key_role(key_role)

    if not isinstance(record, dict) or record.get("encrypted") is not True:
        raise MetadataIdentityError(
            "La identidad de metadata no tiene un registro cifrado válido para "
            f"{key_role}_private_key"
        )
    if record.get("role") != key_role:
        raise MetadataIdentityError(
            "Rol de clave privada de metadata incoherente: "
            f"{record.get('role')!r}"
        )

    kdf = record.get("kdf")
    if not isinstance(kdf, dict) or kdf.get("name") != KDF_SCRYPT:
        raise MetadataIdentityError(f"KDF de identidad de metadata no soportada: {kdf!r}")
    salt = b64decode(f"identity.{key_role}_private_key.kdf.salt", kdf.get("salt"))
    cost = ScryptCost(
        n=int(kdf.get("n")),
        r=int(kdf.get("r")),
        p=int(kdf.get("p")),
        key_length=int(kdf.get("key_length")),
    )

    aead = record.get("aead")
    if not isinstance(aead, dict) or aead.get("name") != AEAD_CHACHA20_POLY1305:
        raise MetadataIdentityError(f"AEAD de identidad de metadata no soportado: {aead!r}")
    nonce = b64decode(f"identity.{key_role}_private_key.aead.nonce", aead.get("nonce"))
    ciphertext = b64decode(f"identity.{key_role}_private_key.ciphertext", record.get("ciphertext"))

    master_key = derive_master_key(passphrase, salt=salt, cost=cost)
    key = derive_subkey(master_key, info=subkey_info_for_role(key_role))

    crypto = require_cryptography()
    try:
        private_key_raw = crypto.ChaCha20Poly1305(key).decrypt(
            nonce,
            ciphertext,
            identity_private_key_associated_data(
                owner_id=identity.owner_id,
                signing_public_key_b64=identity.signing_public_key_b64,
                encryption_public_key_b64=identity.encryption_public_key_b64,
                key_role=key_role,
            ),
        )
    except crypto.InvalidTag as exc:
        raise MetadataIdentityAuthenticationError(
            "No se pudo autenticar o descifrar la clave privada de identidad de metadata. "
            "La passphrase es incorrecta o el archivo de identidad fue modificado."
        ) from exc

    return require_raw_key(f"{key_role}_private_key", private_key_raw)
