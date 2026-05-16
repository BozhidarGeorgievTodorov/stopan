"""
Recorrido y decodificación de metadata object graphs.

El recorrido parte del catalog y sigue ObjectRef anidados dentro del payload,
validando formato, versión, tipo y hash de cada objeto visitado.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import blake3

from stopan.errors import StopanDataError
from stopan.metadata.objects.models import (
    METADATA_OBJECT_FORMAT,
    METADATA_OBJECT_VERSION,
    MetadataObjectType,
)


class MetadataObjectGraphWalkError(StopanDataError, RuntimeError):
    pass


def _require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectGraphWalkError(f"{name} debe ser str; recibido {type(value).__name__}")

    text = value.strip()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectGraphWalkError(f"{name} debe tener 64 caracteres hexadecimales lowercase")

    return text


def decode_object_envelope(
    canonical_bytes: bytes,
    *,
    expected_hash: str | None = None,
) -> tuple[MetadataObjectType, dict[str, Any]]:
    if not isinstance(canonical_bytes, bytes):
        raise MetadataObjectGraphWalkError(
            "canonical_bytes debe ser bytes; "
            f"recibido {type(canonical_bytes).__name__}"
        )

    if expected_hash is not None:
        expected_hash = _require_hash64("expected_hash", expected_hash)

    calculated = blake3.blake3(canonical_bytes).hexdigest()
    if expected_hash is not None and calculated != expected_hash:
        raise MetadataObjectGraphWalkError(
            "hash de metadata object no coincide: "
            f"esperado={expected_hash} calculado={calculated}"
        )

    try:
        envelope = json.loads(canonical_bytes.decode("utf-8"))
    except Exception as exc:
        raise MetadataObjectGraphWalkError(
            f"JSON de metadata object inválido: {calculated}: {exc}"
        ) from exc

    if not isinstance(envelope, dict):
        raise MetadataObjectGraphWalkError(f"envelope de metadata object inválido: {calculated}")
    if envelope.get("format") != METADATA_OBJECT_FORMAT:
        raise MetadataObjectGraphWalkError(f"formato de metadata object inválido: {calculated}")
    if envelope.get("version") != METADATA_OBJECT_VERSION:
        raise MetadataObjectGraphWalkError(f"versión de metadata object no soportada: {calculated}")

    try:
        object_type = MetadataObjectType(str(envelope.get("object_type")))
    except ValueError as exc:
        raise MetadataObjectGraphWalkError(
            f"tipo de metadata object no soportado: {envelope.get('object_type')!r}"
        ) from exc

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise MetadataObjectGraphWalkError(f"payload de metadata object inválido: {calculated}")

    return object_type, payload


def iter_object_refs(value: Any) -> Iterator[tuple[MetadataObjectType, str]]:
    if isinstance(value, dict):
        has_object_type = "object_type" in value
        has_object_hash = "object_hash" in value

        if has_object_type or has_object_hash:
            if not has_object_type or not has_object_hash:
                raise MetadataObjectGraphWalkError(
                    "ObjectRef incompleto: object_type y object_hash son obligatorios"
                )

            object_type = value.get("object_type")
            object_hash = value.get("object_hash")

            if not isinstance(object_type, str):
                raise MetadataObjectGraphWalkError(
                    "object_ref.object_type debe ser str; "
                    f"recibido {type(object_type).__name__}"
                )

            try:
                resolved_type = MetadataObjectType(object_type)
            except ValueError as exc:
                raise MetadataObjectGraphWalkError(
                    f"object_ref.object_type no soportado: {object_type!r}"
                ) from exc

            yield resolved_type, _require_hash64("object_ref.object_hash", object_hash)

        for item in value.values():
            yield from iter_object_refs(item)

    elif isinstance(value, list):
        for item in value:
            yield from iter_object_refs(item)


def collect_reachable_object_hashes(
    *,
    catalog_hash: str,
    read_object_bytes: Callable[[str], bytes],
) -> set[str]:
    pending = [_require_hash64("catalog_hash", catalog_hash)]
    pending_set = set(pending)
    seen: set[str] = set()

    while pending:
        object_hash = pending.pop()
        pending_set.discard(object_hash)
        if object_hash in seen:
            continue

        canonical_bytes = read_object_bytes(object_hash)
        _object_type, payload = decode_object_envelope(
            canonical_bytes,
            expected_hash=object_hash,
        )

        seen.add(object_hash)

        for _ref_type, ref_hash in iter_object_refs(payload):
            if ref_hash not in seen and ref_hash not in pending_set:
                pending.append(ref_hash)
                pending_set.add(ref_hash)

    return seen


def collect_reachable_object_bytes(
    *,
    catalog_hash: str,
    read_object_bytes: Callable[[str], bytes],
) -> dict[str, bytes]:
    pending = [_require_hash64("catalog_hash", catalog_hash)]
    pending_set = set(pending)
    seen: set[str] = set()
    objects: dict[str, bytes] = {}

    while pending:
        object_hash = pending.pop()
        pending_set.discard(object_hash)
        if object_hash in seen:
            continue

        canonical_bytes = read_object_bytes(object_hash)
        _object_type, payload = decode_object_envelope(
            canonical_bytes,
            expected_hash=object_hash,
        )

        seen.add(object_hash)
        objects[object_hash] = canonical_bytes

        for _ref_type, ref_hash in iter_object_refs(payload):
            if ref_hash not in seen and ref_hash not in pending_set:
                pending.append(ref_hash)
                pending_set.add(ref_hash)

    return objects
