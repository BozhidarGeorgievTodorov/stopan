"""
Codificación canónica de metadata objects.

Cada objeto se normaliza a JSON canónico, se envuelve con formato/version/type y
se identifica por BLAKE3 de esos bytes canónicos.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from typing import Any

import blake3

from stopan.common.json import canonical_json_bytes
from stopan.metadata.objects.models import (
    METADATA_OBJECT_FORMAT,
    METADATA_OBJECT_VERSION,
    MetadataObjectError,
    MetadataObjectType,
    MetadataPlainObject,
)


@dataclass(frozen=True, slots=True)
class EncodedMetadataObject:
    object_type: MetadataObjectType
    object_hash: str
    canonical_bytes: bytes

    @property
    def size(self) -> int:
        return len(self.canonical_bytes)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    if is_dataclass(value):
        raw = asdict(value)
        return _normalize_value(raw)

    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise MetadataObjectError(
                    f"clave de metadata object debe ser str; recibido {type(key).__name__}"
                )
            out[key] = _normalize_value(item)
        return out

    if isinstance(value, (tuple, list)):
        return [_normalize_value(item) for item in value]

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    raise MetadataObjectError(f"tipo de valor de metadata object no soportado: {type(value).__name__}")


def object_payload(obj: MetadataPlainObject) -> dict[str, Any]:
    if not hasattr(obj, "object_type"):
        raise MetadataObjectError(f"no es un metadata object: {type(obj).__name__}")

    payload = _normalize_value(obj)
    if not isinstance(payload, dict):
        raise MetadataObjectError("payload de metadata object debe normalizarse a dict")

    # Solo el object_type del objeto raíz vive en el envelope. Los
    # ObjectRef.object_type anidados forman parte del payload y son necesarios
    # para validar imports.
    payload.pop("object_type", None)
    return payload


def object_envelope(obj: MetadataPlainObject) -> dict[str, Any]:
    object_type = getattr(obj, "object_type", None)
    if not isinstance(object_type, MetadataObjectType):
        raise MetadataObjectError("metadata object tiene object_type inválido")

    return {
        "format": METADATA_OBJECT_FORMAT,
        "version": METADATA_OBJECT_VERSION,
        "object_type": object_type.value,
        "payload": object_payload(obj),
    }


def canonical_object_bytes(obj: MetadataPlainObject) -> bytes:
    return canonical_json_bytes(object_envelope(obj))


def encode_metadata_object(obj: MetadataPlainObject) -> EncodedMetadataObject:
    canonical = canonical_object_bytes(obj)
    return EncodedMetadataObject(
        object_type=getattr(obj, "object_type"),
        object_hash=blake3.blake3(canonical).hexdigest(),
        canonical_bytes=canonical,
    )


def canonical_state_digest(
    catalog_hash: str,
    object_hashes: list[str] | tuple[str, ...],
) -> str:
    payload = {
        "catalog_hash": catalog_hash,
        "object_hashes": sorted(object_hashes),
    }
    return blake3.blake3(canonical_json_bytes(payload)).hexdigest()
