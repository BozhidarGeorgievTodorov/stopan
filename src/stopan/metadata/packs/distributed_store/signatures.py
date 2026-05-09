"""
Lectura y escritura de sidecars de firma para metadata packs distribuidos.
"""

from __future__ import annotations

from pathlib import Path

from stopan.common.fs import atomic_write_bytes, ensure_private_dir
from stopan.common.json import canonical_json_bytes, load_json_file
from stopan.metadata.identity import verify_metadata_pack_signature
from stopan.metadata.packs.hashes import validate_pack_hash

from .models import MetadataPackSignatureError


SIGNATURE_RECORD_FORMAT = "stopan.metadata_pack_signature_record"
SIGNATURE_RECORD_VERSION = 1


def write_signature_record(
    path: str | Path,
    *,
    owner_id: str,
    pack_hash: str,
    public_key_b64: str,
    signature_b64: str,
) -> None:
    pack = validate_pack_hash(pack_hash)
    if not public_key_b64 or not signature_b64:
        raise MetadataPackSignatureError("la firma del metadata pack es obligatoria")
    if not verify_metadata_pack_signature(
        owner_id=owner_id,
        public_key_b64=public_key_b64,
        signature_b64=signature_b64,
        pack_hash=pack,
    ):
        raise MetadataPackSignatureError("la firma del metadata pack es inválida")

    payload = {
        "format": SIGNATURE_RECORD_FORMAT,
        "version": SIGNATURE_RECORD_VERSION,
        "owner_id": owner_id,
        "pack_hash": pack,
        "public_key_b64": public_key_b64,
        "signature_b64": signature_b64,
    }
    record_path = Path(path).expanduser().resolve()
    ensure_private_dir(record_path.parent)
    atomic_write_bytes(record_path, canonical_json_bytes(payload), mode=0o600)


def read_signature_record(
    path: str | Path,
    *,
    owner_id: str,
    pack_hash: str,
) -> tuple[str, str]:
    try:
        pack = validate_pack_hash(pack_hash)
        raw = load_json_file(Path(path).expanduser().resolve())
    except Exception:
        return "", ""

    if not isinstance(raw, dict):
        return "", ""
    if raw.get("format") != SIGNATURE_RECORD_FORMAT or raw.get("version") != SIGNATURE_RECORD_VERSION:
        return "", ""
    if raw.get("owner_id") != owner_id or raw.get("pack_hash") != pack:
        return "", ""

    public_key_b64 = str(raw.get("public_key_b64") or "")
    signature_b64 = str(raw.get("signature_b64") or "")
    if not public_key_b64 or not signature_b64:
        return "", ""
    if not verify_metadata_pack_signature(
        owner_id=owner_id,
        public_key_b64=public_key_b64,
        signature_b64=signature_b64,
        pack_hash=pack,
    ):
        return "", ""
    return public_key_b64, signature_b64
