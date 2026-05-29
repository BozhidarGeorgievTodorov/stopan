"""
Construcción y validación del payload interno de metadata object packs.

El payload enlaza el latest pointer con los objetos alcanzables del grafo y
comprueba que tamaños, tipos y hashes coinciden antes de importar.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from stopan.common.encoding import b64decode, b64encode
from stopan.common.json import canonical_json_bytes
from stopan.metadata.objects.codec import EncodedMetadataObject
from stopan.metadata.objects.graph.walk import decode_object_envelope
from stopan.metadata.objects.models import MetadataObjectType
from stopan.metadata.objects.store import LatestMetadataPointer
from stopan.metadata.packs.format import (
    MetadataObjectPackError,
    OBJECT_PACK_PAYLOAD_FORMAT,
    OBJECT_PACK_PAYLOAD_VERSION,
)


_HEX64_ALPHABET = set("0123456789abcdef")


@dataclass(frozen=True, slots=True)
class ParsedPackPayloadBase:
    vault_id: str
    latest: LatestMetadataPointer
    entries: list[dict[str, Any]]
    vault_generation: int
    pack_created_at_unix: float


def require_metadata_hash(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectPackError(f"{name} debe ser string")
    text = value.strip()
    if len(text) != 64 or any(char not in _HEX64_ALPHABET for char in text):
        raise MetadataObjectPackError(f"{name} debe tener 64 caracteres hexadecimales lowercase")
    return text


def require_vault_id(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectPackError(f"{name} debe ser string")
    text = value.strip()
    if len(text) != 32 or any(char not in _HEX64_ALPHABET for char in text):
        raise MetadataObjectPackError(f"{name} debe tener 32 caracteres hexadecimales lowercase")
    return text


def require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise MetadataObjectPackError(f"{name} debe ser un entero >= 0")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise MetadataObjectPackError(f"{name} debe ser un entero >= 0") from exc
    if parsed < 0:
        raise MetadataObjectPackError(f"{name} debe ser >= 0. Recibido {parsed}")
    return parsed


def require_positive_int(name: str, value: object) -> int:
    parsed = require_non_negative_int(name, value)
    if parsed <= 0:
        raise MetadataObjectPackError(f"{name} debe ser > 0.. Recibido {parsed}")
    return parsed


def require_positive_float(name: str, value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise MetadataObjectPackError(f"{name} debe ser un número > 0") from exc
    if parsed <= 0.0:
        raise MetadataObjectPackError(f"{name} debe ser > 0. Recibido {parsed!r}")
    return parsed


def pack_payload(
    *,
    latest: LatestMetadataPointer,
    objects: dict[str, bytes],
    vault_generation: int,
    pack_created_at_unix: float,
) -> bytes:
    generation = require_positive_int("vault_generation", vault_generation)
    created_at = require_positive_float("pack_created_at_unix", pack_created_at_unix)

    entries: list[dict[str, Any]] = []
    total_canonical_bytes = 0
    for object_hash in sorted(objects):
        object_hash = require_metadata_hash("object_hash", object_hash)
        canonical_bytes = objects[object_hash]
        if not isinstance(canonical_bytes, bytes):
            raise MetadataObjectPackError(f"object {object_hash} canonical bytes debe ser bytes")
        object_type, _payload = decode_object_envelope(canonical_bytes, expected_hash=object_hash)
        total_canonical_bytes += len(canonical_bytes)
        entries.append(
            {
                "object_hash": object_hash,
                "object_type": object_type.value,
                "canonical_b64": b64encode(canonical_bytes),
                "canonical_bytes": len(canonical_bytes),
            }
        )

    if len(entries) != int(latest.object_count):
        raise MetadataObjectPackError(
            f"latest.object_count={latest.object_count} pero el recorrido alcanzable encontró {len(entries)} objects"
        )
    if total_canonical_bytes != int(latest.total_canonical_bytes):
        raise MetadataObjectPackError(
            f"latest.total_canonical_bytes={latest.total_canonical_bytes} pero el recorrido alcanzable suma {total_canonical_bytes}"
        )

    return canonical_json_bytes(
        {
            "format": OBJECT_PACK_PAYLOAD_FORMAT,
            "version": OBJECT_PACK_PAYLOAD_VERSION,
            "vault": {
                "id": require_vault_id("latest.vault_id", latest.vault_id),
                "generation": generation,
                "created_at_unix": created_at,
            },
            "latest": {
                "catalog_hash": require_metadata_hash("latest.catalog_hash", latest.catalog_hash),
                "state_digest": require_metadata_hash("latest.state_digest", latest.state_digest),
                "object_count": require_non_negative_int("latest.object_count", latest.object_count),
                "total_canonical_bytes": require_non_negative_int(
                    "latest.total_canonical_bytes",
                    latest.total_canonical_bytes,
                ),
                "snapshot_count": require_non_negative_int("latest.snapshot_count", latest.snapshot_count),
                "known_chunk_count": require_non_negative_int("latest.known_chunk_count", latest.known_chunk_count),
                "protection_record_count": require_non_negative_int(
                    "latest.protection_record_count",
                    latest.protection_record_count,
                ),
            },
            "objects": entries,
        }
    )


def parse_pack_payload_base(payload: dict[str, Any]) -> ParsedPackPayloadBase:
    if payload.get("format") != OBJECT_PACK_PAYLOAD_FORMAT:
        raise MetadataObjectPackError("formato de payload de metadata pack inválido")
    if payload.get("version") != OBJECT_PACK_PAYLOAD_VERSION:
        raise MetadataObjectPackError(f"versión de payload de metadata pack no soportada: {payload.get('version')!r}")

    vault_raw = payload.get("vault")
    if not isinstance(vault_raw, dict):
        raise MetadataObjectPackError("payload de metadata pack sin sección vault válida")

    vault_id = require_vault_id("vault.id", vault_raw.get("id"))
    vault_generation = require_positive_int("vault.generation", vault_raw.get("generation"))
    pack_created_at_unix = require_positive_float("vault.created_at_unix", vault_raw.get("created_at_unix"))

    latest_raw = payload.get("latest")
    if not isinstance(latest_raw, dict):
        raise MetadataObjectPackError("payload de metadata pack sin sección latest válida")

    latest = LatestMetadataPointer(
        vault_id=vault_id,
        catalog_hash=require_metadata_hash("latest.catalog_hash", latest_raw.get("catalog_hash")),
        state_digest=require_metadata_hash("latest.state_digest", latest_raw.get("state_digest")),
        object_count=require_non_negative_int("latest.object_count", latest_raw.get("object_count")),
        total_canonical_bytes=require_non_negative_int(
            "latest.total_canonical_bytes",
            latest_raw.get("total_canonical_bytes"),
        ),
        snapshot_count=require_non_negative_int("latest.snapshot_count", latest_raw.get("snapshot_count")),
        known_chunk_count=require_non_negative_int("latest.known_chunk_count", latest_raw.get("known_chunk_count")),
        protection_record_count=require_non_negative_int(
            "latest.protection_record_count",
            latest_raw.get("protection_record_count"),
        ),
    )

    entries = payload.get("objects")
    if not isinstance(entries, list):
        raise MetadataObjectPackError("objects del payload de metadata pack debe ser una lista")

    if len(entries) != latest.object_count:
        raise MetadataObjectPackError(
            f"pack latest.object_count={latest.object_count} pero contiene {len(entries)} objects"
        )

    return ParsedPackPayloadBase(
        vault_id=vault_id,
        latest=latest,
        entries=entries,
        vault_generation=vault_generation,
        pack_created_at_unix=pack_created_at_unix,
    )


def parse_pack_payload(payload: dict[str, Any]) -> tuple[LatestMetadataPointer, list[EncodedMetadataObject], str, int, float]:
    parsed = parse_pack_payload_base(payload)

    objects: list[EncodedMetadataObject] = []
    seen: set[str] = set()
    total_bytes = 0
    catalog_vault_id: str | None = None

    for index, entry in enumerate(parsed.entries):
        if not isinstance(entry, dict):
            raise MetadataObjectPackError(f"pack object[{index}] no es un objeto JSON")

        object_hash = require_metadata_hash(f"pack object[{index}].object_hash", entry.get("object_hash"))
        if object_hash in seen:
            raise MetadataObjectPackError(f"objeto de metadata pack duplicado: {object_hash}")
        seen.add(object_hash)

        try:
            expected_type = MetadataObjectType(str(entry.get("object_type")))
        except ValueError as exc:
            raise MetadataObjectPackError(f"tipo de objeto de metadata pack no soportado: {entry.get('object_type')!r}") from exc

        canonical = b64decode(f"pack object[{index}].canonical_b64", entry.get("canonical_b64"))
        actual_type, object_payload = decode_object_envelope(canonical, expected_hash=object_hash)

        if actual_type != expected_type:
            raise MetadataObjectPackError(
                f"tipo de objeto de metadata pack no coincide {object_hash}: esperado={expected_type.value} actual={actual_type.value}"
            )
        if actual_type == MetadataObjectType.CATALOG and object_hash == parsed.latest.catalog_hash:
            catalog_vault_id = require_vault_id("catalog.vault_id", object_payload.get("vault_id"))

        declared_size = require_non_negative_int(f"pack object[{index}].canonical_bytes", entry.get("canonical_bytes"))
        if declared_size != len(canonical):
            raise MetadataObjectPackError(
                f"tamaño de objeto de metadata pack no coincide {object_hash}: declarado={declared_size} actual={len(canonical)}"
            )

        total_bytes += len(canonical)
        objects.append(
            EncodedMetadataObject(
                object_type=actual_type,
                object_hash=object_hash,
                canonical_bytes=canonical,
            )
        )

    if total_bytes != parsed.latest.total_canonical_bytes:
        raise MetadataObjectPackError(
            f"pack latest.total_canonical_bytes={parsed.latest.total_canonical_bytes} pero contiene {total_bytes} bytes"
        )
    if catalog_vault_id is None:
        raise MetadataObjectPackError("metadata pack no contiene el catalog latest")
    if catalog_vault_id != parsed.vault_id:
        raise MetadataObjectPackError(
            f"catalog.vault_id={catalog_vault_id} no coincide con vault.id={parsed.vault_id}"
        )

    return parsed.latest, objects, parsed.vault_id, parsed.vault_generation, parsed.pack_created_at_unix


def parse_pack_payload_summary(payload: dict[str, Any]) -> tuple[LatestMetadataPointer, str, int, float]:
    parsed = parse_pack_payload_base(payload)

    seen: set[str] = set()
    total_declared_bytes = 0

    for index, entry in enumerate(parsed.entries):
        if not isinstance(entry, dict):
            raise MetadataObjectPackError(f"pack object[{index}] no es un objeto JSON")

        object_hash = require_metadata_hash(f"pack object[{index}].object_hash", entry.get("object_hash"))
        if object_hash in seen:
            raise MetadataObjectPackError(f"objeto de metadata pack duplicado: {object_hash}")
        seen.add(object_hash)

        try:
            MetadataObjectType(str(entry.get("object_type")))
        except ValueError as exc:
            raise MetadataObjectPackError(f"tipo de objeto de metadata pack no soportado: {entry.get('object_type')!r}") from exc

        total_declared_bytes += require_non_negative_int(
            f"pack object[{index}].canonical_bytes",
            entry.get("canonical_bytes"),
        )

    if total_declared_bytes != parsed.latest.total_canonical_bytes:
        raise MetadataObjectPackError(
            f"pack latest.total_canonical_bytes={parsed.latest.total_canonical_bytes} pero declara {total_declared_bytes} bytes"
        )

    return parsed.latest, parsed.vault_id, parsed.vault_generation, parsed.pack_created_at_unix
