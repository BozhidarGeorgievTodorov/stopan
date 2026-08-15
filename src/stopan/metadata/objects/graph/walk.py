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
from stopan.common.json import canonical_json_bytes
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
    if canonical_json_bytes(envelope) != canonical_bytes:
        raise MetadataObjectGraphWalkError(
            f"metadata object no usa la representación JSON canónica: {calculated}"
        )
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


def _collect_reachable_objects(
    *,
    catalog_hash: str,
    read_object_bytes: Callable[[str], bytes],
    collect_bytes: bool,
) -> tuple[set[str], dict[str, bytes]]:
    root_hash = _require_hash64("catalog_hash", catalog_hash)
    pending: list[tuple[str, MetadataObjectType]] = [
        (root_hash, MetadataObjectType.CATALOG)
    ]
    expected_types: dict[str, MetadataObjectType] = {
        root_hash: MetadataObjectType.CATALOG
    }
    seen: set[str] = set()
    objects: dict[str, bytes] = {}

    while pending:
        object_hash, expected_type = pending.pop()
        if object_hash in seen:
            continue

        canonical_bytes = read_object_bytes(object_hash)
        actual_type, payload = decode_object_envelope(
            canonical_bytes,
            expected_hash=object_hash,
        )
        if actual_type != expected_type:
            raise MetadataObjectGraphWalkError(
                "tipo de metadata object no coincide para "
                f"{object_hash}: esperado={expected_type.value} "
                f"recibido={actual_type.value}"
            )

        seen.add(object_hash)
        if collect_bytes:
            objects[object_hash] = canonical_bytes

        for ref_type, ref_hash in iter_object_refs(payload):
            previous_type = expected_types.get(ref_hash)
            if previous_type is not None and previous_type != ref_type:
                raise MetadataObjectGraphWalkError(
                    "metadata object referenciado con tipos incompatibles: "
                    f"{ref_hash}: {previous_type.value} y {ref_type.value}"
                )
            expected_types.setdefault(ref_hash, ref_type)
            if ref_hash not in seen:
                pending.append((ref_hash, ref_type))

    return seen, objects


def collect_reachable_object_hashes(
    *,
    catalog_hash: str,
    read_object_bytes: Callable[[str], bytes],
) -> set[str]:
    seen, _objects = _collect_reachable_objects(
        catalog_hash=catalog_hash,
        read_object_bytes=read_object_bytes,
        collect_bytes=False,
    )
    return seen


def collect_reachable_object_bytes(
    *,
    catalog_hash: str,
    read_object_bytes: Callable[[str], bytes],
) -> dict[str, bytes]:
    _seen, objects = _collect_reachable_objects(
        catalog_hash=catalog_hash,
        read_object_bytes=read_object_bytes,
        collect_bytes=True,
    )
    return objects
