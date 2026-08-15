"""
Cifrado y descifrado de metadata object packs.

El payload del pack se comprime y cifra con una data key aleatoria. Esa data key
se envuelve para cada recipient mediante X25519 + HKDF + ChaCha20-Poly1305.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import zstandard as zstd

from stopan.common.encoding import b64decode, b64encode
from stopan.common.json import canonical_json_bytes
from stopan.metadata.identity.files import (
    load_metadata_identity_file,
    load_metadata_private_identity_file,
)
from stopan.metadata.crypto.kdf import derive_hkdf_sha256_key
from stopan.metadata.identity.keys import require_cryptography, require_x25519, validate_owner_id
from stopan.metadata.packs.format import (
    MetadataObjectPackAuthenticationError,
    MetadataObjectPackError,
    MetadataObjectPackHeader,
    OBJECT_PACK_AEAD_CHACHA20_POLY1305,
    OBJECT_PACK_COMPRESSION_ZSTD,
    OBJECT_PACK_ENCRYPTION_X25519_CHACHA20POLY1305,
    OBJECT_PACK_FORMAT,
    OBJECT_PACK_VERSION,
    load_canonical_pack_outer,
    pack_header_from_outer,
    parse_pack_outer,
)
from stopan.metadata.packs.hashes import calculate_pack_hash


def _require_raw32(name: str, value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise MetadataObjectPackError(f"{name} debe contener exactamente 32 bytes raw")
    return value


def _hkdf_sha256(secret: bytes, *, info: bytes) -> bytes:
    return derive_hkdf_sha256_key(secret, info=info)


def recipient_wrap_associated_data(recipient: dict[str, Any]) -> bytes:
    return canonical_json_bytes(
        {
            "format": OBJECT_PACK_FORMAT,
            "version": OBJECT_PACK_VERSION,
            "purpose": "recipient_data_key_wrap",
            "owner_id": recipient.get("owner_id"),
            "encryption_public_key_b64": recipient.get("encryption_public_key_b64"),
            "ephemeral_public_key_b64": recipient.get("ephemeral_public_key_b64"),
            "aead": recipient.get("aead"),
        }
    )


def payload_associated_data(raw_without_ciphertext: dict[str, Any]) -> bytes:
    return canonical_json_bytes(
        {
            "format": OBJECT_PACK_FORMAT,
            "version": OBJECT_PACK_VERSION,
            "purpose": "pack_payload",
            "encryption": raw_without_ciphertext.get("encryption"),
            "compression": raw_without_ciphertext.get("compression"),
            "recipients": raw_without_ciphertext.get("recipients"),
            "payload_aead": raw_without_ciphertext.get("payload_aead"),
        }
    )


def _recipient_kek_info(*, owner_id: str) -> bytes:
    return canonical_json_bytes(
        {
            "format": OBJECT_PACK_FORMAT,
            "version": OBJECT_PACK_VERSION,
            "purpose": "x25519_recipient_kek",
            "owner_id": validate_owner_id(owner_id),
        }
    )


def build_recipient_for_identity(*, identity_file: str | Path, data_key: bytes) -> dict[str, Any]:
    identity = load_metadata_identity_file(identity_file)
    owner_id = validate_owner_id(identity.owner_id)
    recipient_public_raw = _require_raw32(
        "identity.encryption_public_key_b64",
        b64decode("identity.encryption_public_key_b64", identity.encryption_public_key_b64),
    )

    x25519 = require_x25519()
    crypto = require_cryptography()
    ephemeral_private = x25519.X25519PrivateKey.generate()
    ephemeral_public_raw = ephemeral_private.public_key().public_bytes(
        encoding=crypto.serialization.Encoding.Raw,
        format=crypto.serialization.PublicFormat.Raw,
    )
    recipient_public = x25519.X25519PublicKey.from_public_bytes(recipient_public_raw)
    shared_secret = ephemeral_private.exchange(recipient_public)
    kek = _hkdf_sha256(shared_secret, info=_recipient_kek_info(owner_id=owner_id))
    wrap_nonce = os.urandom(12)

    recipient = {
        "owner_id": owner_id,
        "encryption_public_key_b64": identity.encryption_public_key_b64,
        "ephemeral_public_key_b64": b64encode(ephemeral_public_raw),
        "aead": {
            "name": OBJECT_PACK_AEAD_CHACHA20_POLY1305,
            "nonce": b64encode(wrap_nonce),
        },
    }

    encrypted_data_key = crypto.ChaCha20Poly1305(kek).encrypt(
        wrap_nonce,
        _require_raw32("data_key", data_key),
        recipient_wrap_associated_data(recipient),
    )
    recipient["encrypted_data_key_b64"] = b64encode(encrypted_data_key)
    return recipient


def unwrap_data_key(
    *,
    raw: dict[str, Any],
    identity_file: str | Path,
    passphrase: str | bytes,
) -> bytes:
    private_identity = load_metadata_private_identity_file(identity_file, passphrase=passphrase)
    owner_id = private_identity.identity.owner_id
    encryption_public_key_b64 = private_identity.identity.encryption_public_key_b64

    recipients = raw.get("recipients")
    if not isinstance(recipients, list) or not recipients:
        raise MetadataObjectPackError("metadata pack sin recipients válidos")

    selected: dict[str, Any] | None = None
    for item in recipients:
        if not isinstance(item, dict):
            continue
        if item.get("owner_id") == owner_id and item.get("encryption_public_key_b64") == encryption_public_key_b64:
            selected = item
            break
    if selected is None:
        raise MetadataObjectPackError("metadata pack sin recipient para esta identidad")

    ephemeral_public_raw = _require_raw32(
        "recipient.ephemeral_public_key_b64",
        b64decode("recipient.ephemeral_public_key_b64", selected.get("ephemeral_public_key_b64")),
    )
    aead = selected.get("aead")
    if not isinstance(aead, dict) or aead.get("name") != OBJECT_PACK_AEAD_CHACHA20_POLY1305:
        raise MetadataObjectPackError(f"AEAD de recipient no soportado: {aead!r}")
    wrap_nonce = b64decode("recipient.aead.nonce", aead.get("nonce"))
    encrypted_data_key = b64decode("recipient.encrypted_data_key_b64", selected.get("encrypted_data_key_b64"))

    x25519 = require_x25519()
    private_key = x25519.X25519PrivateKey.from_private_bytes(
        _require_raw32("encryption_private_key", private_identity.encryption_private_key_raw)
    )
    shared_secret = private_key.exchange(x25519.X25519PublicKey.from_public_bytes(ephemeral_public_raw))
    kek = _hkdf_sha256(shared_secret, info=_recipient_kek_info(owner_id=owner_id))

    crypto = require_cryptography()
    try:
        return crypto.ChaCha20Poly1305(kek).decrypt(
            wrap_nonce,
            encrypted_data_key,
            recipient_wrap_associated_data(selected),
        )
    except crypto.InvalidTag as exc:
        raise MetadataObjectPackAuthenticationError(
            "No se pudo autenticar/descifrar la data key del metadata pack: identidad incorrecta, passphrase incorrecta o pack manipulado."
        ) from exc


def decrypt_pack_payload(
    path: str | Path,
    *,
    identity_file: str | Path,
    passphrase: str | bytes,
) -> tuple[dict[str, Any], MetadataObjectPackHeader, int, int]:
    pack_path = Path(path).expanduser().resolve()
    raw = load_canonical_pack_outer(pack_path)
    raw, payload_nonce, pack_hash = parse_pack_outer(pack_path, raw)
    data_key = unwrap_data_key(raw=raw, identity_file=identity_file, passphrase=passphrase)
    ciphertext = b64decode("pack.ciphertext", raw.get("ciphertext"))

    raw_without_ciphertext = dict(raw)
    raw_without_ciphertext.pop("ciphertext", None)

    crypto = require_cryptography()
    try:
        compressed = crypto.ChaCha20Poly1305(_require_raw32("data_key", data_key)).decrypt(
            payload_nonce,
            ciphertext,
            payload_associated_data(raw_without_ciphertext),
        )
    except crypto.InvalidTag as exc:
        raise MetadataObjectPackAuthenticationError(
            f"No se pudo autenticar el metadata object pack {pack_path}: identidad incorrecta, passphrase incorrecta o pack manipulado."
        ) from exc

    try:
        plaintext = zstd.ZstdDecompressor().decompress(compressed)
    except zstd.ZstdError as exc:
        raise MetadataObjectPackError(f"payload zstd de metadata pack corrupto: {pack_path}: {exc}") from exc

    try:
        decoded = json.loads(plaintext.decode("utf-8"))
    except Exception as exc:
        raise MetadataObjectPackError(f"JSON de payload de metadata pack inválido: {pack_path}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise MetadataObjectPackError(f"payload de metadata pack no es un objeto JSON: {pack_path}")
    if plaintext != canonical_json_bytes(decoded):
        raise MetadataObjectPackError(
            f"payload de metadata pack no usa la representación JSON canónica: {pack_path}"
        )

    header = pack_header_from_outer(pack_path, raw, pack_hash=pack_hash, ciphertext_bytes=len(ciphertext))
    return decoded, header, len(compressed), len(plaintext)


def encrypt_pack_payload(
    plaintext: bytes,
    *,
    identity_file: str | Path,
) -> tuple[bytes, str, int, int]:
    if not isinstance(plaintext, bytes):
        raise MetadataObjectPackError("metadata pack plaintext debe ser bytes")

    compressed = zstd.ZstdCompressor(level=3).compress(plaintext)
    data_key = os.urandom(32)
    payload_nonce = os.urandom(12)
    recipient = build_recipient_for_identity(identity_file=identity_file, data_key=data_key)

    outer_without_ciphertext = {
        "format": OBJECT_PACK_FORMAT,
        "version": OBJECT_PACK_VERSION,
        "encryption": OBJECT_PACK_ENCRYPTION_X25519_CHACHA20POLY1305,
        "compression": OBJECT_PACK_COMPRESSION_ZSTD,
        "recipients": [recipient],
        "payload_aead": {
            "name": OBJECT_PACK_AEAD_CHACHA20_POLY1305,
            "nonce": b64encode(payload_nonce),
        },
    }

    crypto = require_cryptography()
    ciphertext = crypto.ChaCha20Poly1305(data_key).encrypt(
        payload_nonce,
        compressed,
        payload_associated_data(outer_without_ciphertext),
    )

    outer = dict(outer_without_ciphertext)
    outer["ciphertext"] = b64encode(ciphertext)
    pack_bytes = canonical_json_bytes(outer)
    pack_hash = calculate_pack_hash(pack_bytes)
    return pack_bytes, pack_hash, len(compressed), len(ciphertext)
