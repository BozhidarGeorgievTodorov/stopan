"""
Firmas de identidad y metadata packs.

Las firmas de packs se calculan sobre un payload canónico que liga owner_id y
pack_hash, no sobre el contenido cifrado completo.
"""

from __future__ import annotations

from pathlib import Path

from stopan.common.encoding import b64decode, b64encode
from stopan.common.json import canonical_json_bytes
from stopan.metadata.identity.files import (
    load_metadata_identity_file,
    load_metadata_private_identity_file,
)
from stopan.metadata.identity.keys import (
    owner_id_from_public_key,
    require_cryptography,
    require_raw_key,
    validate_owner_id,
)
from stopan.metadata.identity.models import (
    IDENTITY_ALGORITHM_ED25519,
    MetadataIdentityError,
)
from stopan.metadata.packs.hashes import validate_pack_hash


METADATA_PACK_SIGNATURE_FORMAT = "stopan.metadata_pack_signature"
METADATA_PACK_SIGNATURE_VERSION = 1


def sign_metadata_bytes(
    *,
    identity_file: str | Path,
    passphrase: str | bytes,
    data: bytes,
) -> bytes:
    private_identity = load_metadata_private_identity_file(identity_file, passphrase=passphrase)
    return private_identity.sign(data)


def verify_metadata_signature(
    *,
    owner_id: str,
    public_key_b64: str,
    signature: bytes,
    data: bytes,
) -> bool:
    owner_id = validate_owner_id(owner_id)
    public_key_raw = require_raw_key("public_key_b64", b64decode("public_key_b64", public_key_b64))
    if owner_id_from_public_key(public_key_raw) != owner_id:
        return False

    crypto = require_cryptography()
    try:
        crypto.Ed25519PublicKey.from_public_bytes(public_key_raw).verify(signature, data)
        return True
    except crypto.InvalidSignature:
        return False


def resolve_owner_id(
    *,
    explicit_owner_id: str | None = None,
    explicit_identity_file: str | Path | None = None,
    config_owner_id: str | None = None,
    config_identity_file: str | Path | None = None,
) -> str:
    if explicit_owner_id:
        return validate_owner_id(explicit_owner_id, name="--owner-id")
    if config_owner_id:
        return validate_owner_id(config_owner_id, name="metadata.owner_id")

    identity_file = explicit_identity_file or config_identity_file
    if identity_file:
        return load_metadata_identity_file(identity_file).owner_id

    raise MetadataIdentityError(
        "No hay owner_id configurado. Usa --owner-id, metadata.owner_id o metadata.identity_file."
    )


def metadata_pack_signature_payload(*, owner_id: str, pack_hash: str) -> bytes:
    return canonical_json_bytes(
        {
            "format": METADATA_PACK_SIGNATURE_FORMAT,
            "version": METADATA_PACK_SIGNATURE_VERSION,
            "algorithm": IDENTITY_ALGORITHM_ED25519,
            "owner_id": validate_owner_id(owner_id),
            "pack_hash": validate_pack_hash(pack_hash),
        }
    )


def sign_metadata_pack_hash(
    *,
    identity_file: str | Path,
    passphrase: str | bytes,
    pack_hash: str,
    expected_owner_id: str | None = None,
) -> tuple[str, str, str]:
    private_identity = load_metadata_private_identity_file(identity_file, passphrase=passphrase)
    owner_id = private_identity.identity.owner_id
    if expected_owner_id is not None and validate_owner_id(expected_owner_id) != owner_id:
        raise MetadataIdentityError(
            "owner_id explícito/configurado no coincide con el archivo de identidad de metadata: "
            f"owner_id={expected_owner_id} identity_owner_id={owner_id}"
        )

    payload = metadata_pack_signature_payload(owner_id=owner_id, pack_hash=pack_hash)
    signature = private_identity.sign(payload)
    return owner_id, private_identity.identity.signing_public_key_b64, b64encode(signature)


def verify_metadata_pack_signature(
    *,
    owner_id: str,
    public_key_b64: str,
    signature_b64: str,
    pack_hash: str,
) -> bool:
    try:
        signature = b64decode("signature_b64", signature_b64)
        payload = metadata_pack_signature_payload(owner_id=owner_id, pack_hash=pack_hash)
        return verify_metadata_signature(
            owner_id=owner_id,
            public_key_b64=public_key_b64,
            signature=signature,
            data=payload,
        )
    except Exception:
        return False