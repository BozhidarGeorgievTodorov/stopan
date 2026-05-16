"""
Formato externo de metadata object packs.

Valida la envoltura JSON del pack, sus algoritmos declarados y el header visible
sin descifrar el payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stopan.errors import StopanDataError
from stopan.common.encoding import b64decode
from stopan.common.json import canonical_json_bytes, load_json_file
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash


OBJECT_PACK_FORMAT = "stopan.metadata_object_pack"
OBJECT_PACK_VERSION = 1
OBJECT_PACK_PAYLOAD_FORMAT = "stopan.metadata_object_pack_payload"
OBJECT_PACK_PAYLOAD_VERSION = 1
OBJECT_PACK_COMPRESSION_ZSTD = "zstd"
OBJECT_PACK_ENCRYPTION_X25519_CHACHA20POLY1305 = "x25519+chacha20poly1305"
OBJECT_PACK_AEAD_CHACHA20_POLY1305 = "chacha20poly1305"
OBJECT_PACK_FILE_SUFFIX = ".stopanmetapack"


class MetadataObjectPackError(StopanDataError, RuntimeError):
    pass


class MetadataObjectPackAuthenticationError(MetadataObjectPackError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataObjectPackHeader:
    path: Path
    pack_hash: str
    format: str
    version: int
    encryption: str
    aead_name: str
    compression: str
    recipient_count: int
    ciphertext_bytes: int


def parse_pack_outer(path: Path, raw: dict[str, Any]) -> tuple[dict[str, Any], bytes, str]:
    if raw.get("format") != OBJECT_PACK_FORMAT:
        raise MetadataObjectPackError(f"formato de metadata pack inválido: {path}")
    if raw.get("version") != OBJECT_PACK_VERSION:
        raise MetadataObjectPackError(
            f"versión de metadata pack no soportada in {path}: {raw.get('version')!r}; "
            "crea un pack nuevo con la versión actual de Stopan."
        )
    if raw.get("encryption") != OBJECT_PACK_ENCRYPTION_X25519_CHACHA20POLY1305:
        raise MetadataObjectPackError(
        f"cifrado de metadata pack no soportado: {raw.get('encryption')!r}"
    )
    if raw.get("compression") != OBJECT_PACK_COMPRESSION_ZSTD:
        raise MetadataObjectPackError(
        f"compresión de metadata pack no soportada: {raw.get('compression')!r}"
    )

    payload_aead = raw.get("payload_aead")
    if (
        not isinstance(payload_aead, dict)
        or payload_aead.get("name") != OBJECT_PACK_AEAD_CHACHA20_POLY1305
    ):
        raise MetadataObjectPackError(
            f"AEAD de payload de metadata pack no soportado: {payload_aead!r}"
        )

    payload_nonce = b64decode("pack.payload_aead.nonce", payload_aead.get("nonce"))
    b64decode("pack.ciphertext", raw.get("ciphertext"))
    pack_hash = validate_pack_hash(calculate_pack_hash(canonical_json_bytes(raw)))
    return raw, payload_nonce, pack_hash


def pack_header_from_outer(
    path: Path,
    raw: dict[str, Any],
    *,
    pack_hash: str,
    ciphertext_bytes: int,
) -> MetadataObjectPackHeader:
    payload_aead = raw.get("payload_aead")
    recipients = raw.get("recipients")
    if not isinstance(payload_aead, dict):
        raise MetadataObjectPackError(f"header de metadata pack sin payload_aead válido: {path}")
    if not isinstance(recipients, list):
        raise MetadataObjectPackError(f"header de metadata pack sin recipients válidos: {path}")

    return MetadataObjectPackHeader(
        path=path,
        pack_hash=validate_pack_hash(pack_hash),
        format=str(raw.get("format")),
        version=int(raw.get("version")),
        encryption=str(raw.get("encryption")),
        aead_name=str(payload_aead.get("name")),
        compression=str(raw.get("compression")),
        recipient_count=len(recipients),
        ciphertext_bytes=int(ciphertext_bytes),
    )


def read_pack_header(path: str | Path) -> MetadataObjectPackHeader:
    pack_path = Path(path).expanduser().resolve()
    raw = load_json_file(pack_path)
    _raw, _payload_nonce, pack_hash = parse_pack_outer(pack_path, raw)
    ciphertext = b64decode("pack.ciphertext", raw.get("ciphertext"))
    return pack_header_from_outer(
        pack_path,
        raw,
        pack_hash=pack_hash,
        ciphertext_bytes=len(ciphertext),
    )
