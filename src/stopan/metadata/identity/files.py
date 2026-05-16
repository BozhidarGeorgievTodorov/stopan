"""
Creación y carga de archivos de identidad de metadata.

El archivo contiene claves públicas, owner_id y claves privadas cifradas. Al
cargar una identidad privada se verifica que cada clave privada descifrada
corresponde a su clave pública persistida.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from stopan.common.encoding import b64decode, b64encode
from stopan.common.fs import atomic_write_bytes
from stopan.common.json import canonical_json_bytes, load_json_file
from stopan.errors import StopanUsageError
from stopan.metadata.identity.keys import (
    decrypt_private_key_record,
    encrypted_private_key_record,
    owner_id_from_public_key,
    require_cryptography,
    require_raw_key,
    require_x25519,
    validate_owner_id,
)
from stopan.metadata.identity.models import (
    IDENTITY_ALGORITHM_ED25519_X25519,
    METADATA_IDENTITY_FORMAT,
    METADATA_IDENTITY_VERSION,
    PRIVATE_KEY_ROLE_ENCRYPTION,
    PRIVATE_KEY_ROLE_SIGNING,
    MetadataIdentity,
    MetadataIdentityAuthenticationError,
    MetadataIdentityError,
    MetadataPrivateIdentity,
)
from stopan.metadata.identity.passphrase import ScryptCost


def create_metadata_identity_file(
    path: str | Path,
    *,
    passphrase: str | bytes,
    scrypt_cost: ScryptCost,
    force: bool = False,
) -> MetadataIdentity:
    identity_path = Path(path).expanduser().resolve()
    if identity_path.exists() and not force:
        raise StopanUsageError(f"El archivo de identidad de metadata ya existe: {identity_path}")

    crypto = require_cryptography()
    x25519 = require_x25519()

    signing_private_key = crypto.Ed25519PrivateKey.generate()
    signing_public_key = signing_private_key.public_key()
    encryption_private_key = x25519.X25519PrivateKey.generate()
    encryption_public_key = encryption_private_key.public_key()

    signing_private_key_raw = signing_private_key.private_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PrivateFormat.Raw,
        encryption_algorithm=crypto.serialization.NoEncryption(),
    )
    signing_public_key_raw = signing_public_key.public_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PublicFormat.Raw,
    )
    encryption_private_key_raw = encryption_private_key.private_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PrivateFormat.Raw,
        encryption_algorithm=crypto.serialization.NoEncryption(),
    )
    encryption_public_key_raw = encryption_public_key.public_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PublicFormat.Raw,
    )

    owner_id = owner_id_from_public_key(signing_public_key_raw)
    signing_public_key_b64 = b64encode(signing_public_key_raw)
    encryption_public_key_b64 = b64encode(encryption_public_key_raw)
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    signing_record = encrypted_private_key_record(
        raw_private_key=signing_private_key_raw,
        passphrase=passphrase,
        scrypt_cost=scrypt_cost,
        owner_id=owner_id,
        signing_public_key_b64=signing_public_key_b64,
        encryption_public_key_b64=encryption_public_key_b64,
        key_role=PRIVATE_KEY_ROLE_SIGNING,
    )
    encryption_record = encrypted_private_key_record(
        raw_private_key=encryption_private_key_raw,
        passphrase=passphrase,
        scrypt_cost=scrypt_cost,
        owner_id=owner_id,
        signing_public_key_b64=signing_public_key_b64,
        encryption_public_key_b64=encryption_public_key_b64,
        key_role=PRIVATE_KEY_ROLE_ENCRYPTION,
    )

    payload = {
        "format": METADATA_IDENTITY_FORMAT,
        "version": METADATA_IDENTITY_VERSION,
        "algorithm": IDENTITY_ALGORITHM_ED25519_X25519,
        "owner_id": owner_id,
        "signing_public_key_b64": signing_public_key_b64,
        "encryption_public_key_b64": encryption_public_key_b64,
        "created_at": created_at,
        "signing_private_key": signing_record,
        "encryption_private_key": encryption_record,
    }
    atomic_write_bytes(identity_path, canonical_json_bytes(payload), mode=0o600)
    return MetadataIdentity(
        owner_id=owner_id,
        created_at=created_at,
        signing_public_key_b64=signing_public_key_b64,
        encryption_public_key_b64=encryption_public_key_b64,
        path=identity_path,
        private_keys_encrypted=True,
    )


def load_identity_json(path: str | Path) -> tuple[Path, dict[str, Any]]:
    identity_path = Path(path).expanduser().resolve()
    try:
        raw = load_json_file(identity_path)
    except Exception as exc:
        raise MetadataIdentityError(f"No se pudo leer el archivo de identidad de metadata {identity_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise MetadataIdentityError(f"El archivo de identidad de metadata debe contener un objeto JSON: {identity_path}")
    if raw.get("format") != METADATA_IDENTITY_FORMAT:
        raise MetadataIdentityError(f"Formato de identidad de metadata inválido en {identity_path}")
    if raw.get("version") != METADATA_IDENTITY_VERSION:
        raise MetadataIdentityError(
            f"Versión de identidad de metadata no soportada en {identity_path}: {raw.get('version')!r}. "
            "Crea una identidad nueva con 'stopan metadata identity-create'."
        )
    if raw.get("algorithm") != IDENTITY_ALGORITHM_ED25519_X25519:
        raise MetadataIdentityError(f"Algoritmo de identidad de metadata no soportado: {raw.get('algorithm')!r}")
    return identity_path, raw


def _metadata_identity_from_raw(identity_path: Path, raw: dict[str, Any]) -> MetadataIdentity:
    signing_public_key_raw = require_raw_key(
        "identity.signing_public_key_b64",
        b64decode("identity.signing_public_key_b64", raw.get("signing_public_key_b64")),
    )
    encryption_public_key_raw = require_raw_key(
        "identity.encryption_public_key_b64",
        b64decode("identity.encryption_public_key_b64", raw.get("encryption_public_key_b64")),
    )

    calculated_owner_id = owner_id_from_public_key(signing_public_key_raw)
    owner_id = validate_owner_id(str(raw.get("owner_id", "")))
    if owner_id != calculated_owner_id:
        raise MetadataIdentityError(
            f"owner_id no coincide con signing_public_key en {identity_path}: "
            f"owner_id={owner_id} calculated={calculated_owner_id}"
        )

    signing_private_key = raw.get("signing_private_key")
    encryption_private_key = raw.get("encryption_private_key")
    if not isinstance(signing_private_key, dict) or signing_private_key.get("encrypted") is not True:
        raise MetadataIdentityError(f"La identidad de metadata no tiene signing_private_key cifrada válida: {identity_path}")
    if not isinstance(encryption_private_key, dict) or encryption_private_key.get("encrypted") is not True:
        raise MetadataIdentityError(f"La identidad de metadata no tiene encryption_private_key cifrada válida: {identity_path}")

    created_at = str(raw.get("created_at", "")).strip()
    if not created_at:
        raise MetadataIdentityError(f"La identidad de metadata tiene created_at vacío: {identity_path}")

    return MetadataIdentity(
        owner_id=owner_id,
        created_at=created_at,
        signing_public_key_b64=b64encode(signing_public_key_raw),
        encryption_public_key_b64=b64encode(encryption_public_key_raw),
        algorithm=IDENTITY_ALGORITHM_ED25519_X25519,
        path=identity_path,
        private_keys_encrypted=True,
    )


def load_metadata_identity_file(path: str | Path) -> MetadataIdentity:
    identity_path, raw = load_identity_json(path)
    return _metadata_identity_from_raw(identity_path, raw)


def load_metadata_private_identity_file(
    path: str | Path,
    *,
    passphrase: str | bytes,
) -> MetadataPrivateIdentity:
    identity_path, raw = load_identity_json(path)
    identity = _metadata_identity_from_raw(identity_path, raw)

    signing_private_key_raw = decrypt_private_key_record(
        identity=identity,
        record=raw.get("signing_private_key"),
        key_role=PRIVATE_KEY_ROLE_SIGNING,
        passphrase=passphrase,
    )
    encryption_private_key_raw = decrypt_private_key_record(
        identity=identity,
        record=raw.get("encryption_private_key"),
        key_role=PRIVATE_KEY_ROLE_ENCRYPTION,
        passphrase=passphrase,
    )

    crypto = require_cryptography()
    signing_private_obj = crypto.Ed25519PrivateKey.from_private_bytes(signing_private_key_raw)
    derived_signing_public = signing_private_obj.public_key().public_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PublicFormat.Raw,
    )
    expected_signing_public = b64decode("identity.signing_public_key_b64", identity.signing_public_key_b64)
    if derived_signing_public != expected_signing_public:
        raise MetadataIdentityAuthenticationError(
            "signing_private_key descifrada no coincide con identity.signing_public_key_b64."
        )

    x25519 = require_x25519()
    encryption_private_obj = x25519.X25519PrivateKey.from_private_bytes(encryption_private_key_raw)
    derived_encryption_public = encryption_private_obj.public_key().public_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PublicFormat.Raw,
    )
    expected_encryption_public = b64decode("identity.encryption_public_key_b64", identity.encryption_public_key_b64)
    if derived_encryption_public != expected_encryption_public:
        raise MetadataIdentityAuthenticationError(
            "encryption_private_key descifrada no coincide con identity.encryption_public_key_b64."
        )

    return MetadataPrivateIdentity(
        identity=identity,
        signing_private_key_raw=signing_private_key_raw,
        encryption_private_key_raw=encryption_private_key_raw,
    )
