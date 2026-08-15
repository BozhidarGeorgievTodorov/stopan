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
from stopan.metadata.objects.codec import EncodedMetadataObject, canonical_state_digest
from stopan.metadata.objects.graph.walk import decode_object_envelope, iter_object_refs
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


@dataclass(frozen=True, slots=True)
class _ValidatedPackObject:
    object_type: MetadataObjectType
    payload: dict[str, Any]
    canonical_bytes: int


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


def _validate_pack_payload_graph(
    parsed: ParsedPackPayloadBase,
    objects_by_hash: dict[str, _ValidatedPackObject],
) -> None:
    pending: list[tuple[str, MetadataObjectType]] = [
        (parsed.latest.catalog_hash, MetadataObjectType.CATALOG)
    ]
    expected_types: dict[str, MetadataObjectType] = {
        parsed.latest.catalog_hash: MetadataObjectType.CATALOG
    }
    reachable: set[str] = set()

    while pending:
        object_hash, expected_type = pending.pop()
        if object_hash in reachable:
            continue

        validated = objects_by_hash.get(object_hash)
        if validated is None:
            raise MetadataObjectPackError(
                f"metadata pack no contiene objeto referenciado: {object_hash}"
            )
        if validated.object_type != expected_type:
            raise MetadataObjectPackError(
                "tipo de metadata object no coincide para "
                f"{object_hash}: esperado={expected_type.value} "
                f"recibido={validated.object_type.value}"
            )

        reachable.add(object_hash)
        try:
            refs = tuple(iter_object_refs(validated.payload))
        except Exception as exc:
            raise MetadataObjectPackError(
                f"ObjectRef inválido en metadata object {object_hash}: {exc}"
            ) from exc

        for ref_type, ref_hash in refs:
            previous_type = expected_types.get(ref_hash)
            if previous_type is not None and previous_type != ref_type:
                raise MetadataObjectPackError(
                    "metadata object referenciado con tipos incompatibles: "
                    f"{ref_hash}: {previous_type.value} y {ref_type.value}"
                )
            expected_types.setdefault(ref_hash, ref_type)
            if ref_hash not in reachable:
                pending.append((ref_hash, ref_type))

    packaged = set(objects_by_hash)
    if reachable != packaged:
        extras = sorted(packaged - reachable)
        missing = sorted(reachable - packaged)
        detail: list[str] = []
        if missing:
            detail.append("faltan=" + ",".join(missing[:5]))
        if extras:
            detail.append("no_alcanzables=" + ",".join(extras[:5]))
        raise MetadataObjectPackError(
            "el conjunto de objetos del metadata pack no coincide con el grafo "
            "alcanzable desde latest.catalog_hash"
            + (": " + "; ".join(detail) if detail else "")
        )

    digest = canonical_state_digest(
        parsed.latest.catalog_hash,
        tuple(reachable),
    )
    if digest != parsed.latest.state_digest:
        raise MetadataObjectPackError(
            "pack latest.state_digest no coincide con el grafo alcanzable: "
            f"esperado={parsed.latest.state_digest} calculado={digest}"
        )

    if len(reachable) != int(parsed.latest.object_count):
        raise MetadataObjectPackError(
            "pack latest.object_count no coincide con el grafo alcanzable: "
            f"latest={parsed.latest.object_count} alcanzables={len(reachable)}"
        )

    reachable_bytes = sum(objects_by_hash[item].canonical_bytes for item in reachable)
    if reachable_bytes != int(parsed.latest.total_canonical_bytes):
        raise MetadataObjectPackError(
            "pack latest.total_canonical_bytes no coincide con el grafo alcanzable: "
            f"latest={parsed.latest.total_canonical_bytes} alcanzables={reachable_bytes}"
        )


def _validate_pack_payload_entries(
    parsed: ParsedPackPayloadBase,
    *,
    collect_objects: bool,
) -> list[EncodedMetadataObject] | None:
    objects: list[EncodedMetadataObject] | None = [] if collect_objects else None
    seen: set[str] = set()
    total_bytes = 0
    catalog_vault_id: str | None = None
    validated_by_hash: dict[str, _ValidatedPackObject] = {}

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
            raise MetadataObjectPackError(
                f"tipo de objeto de metadata pack no soportado: {entry.get('object_type')!r}"
            ) from exc

        canonical = b64decode(f"pack object[{index}].canonical_b64", entry.get("canonical_b64"))
        actual_type, object_payload = decode_object_envelope(canonical, expected_hash=object_hash)

        if actual_type != expected_type:
            raise MetadataObjectPackError(
                f"tipo de objeto de metadata pack no coincide {object_hash}: "
                f"esperado={expected_type.value} actual={actual_type.value}"
            )
        if actual_type == MetadataObjectType.CATALOG and object_hash == parsed.latest.catalog_hash:
            catalog_vault_id = require_vault_id("catalog.vault_id", object_payload.get("vault_id"))

        declared_size = require_non_negative_int(
            f"pack object[{index}].canonical_bytes",
            entry.get("canonical_bytes"),
        )
        if declared_size != len(canonical):
            raise MetadataObjectPackError(
                f"tamaño de objeto de metadata pack no coincide {object_hash}: "
                f"declarado={declared_size} actual={len(canonical)}"
            )

        total_bytes += len(canonical)
        validated_by_hash[object_hash] = _ValidatedPackObject(
            object_type=actual_type,
            payload=object_payload,
            canonical_bytes=len(canonical),
        )
        if objects is not None:
            objects.append(
                EncodedMetadataObject(
                    object_type=actual_type,
                    object_hash=object_hash,
                    canonical_bytes=canonical,
                )
            )

    if total_bytes != parsed.latest.total_canonical_bytes:
        raise MetadataObjectPackError(
            f"pack latest.total_canonical_bytes={parsed.latest.total_canonical_bytes} "
            f"pero contiene {total_bytes} bytes"
        )
    if catalog_vault_id is None:
        raise MetadataObjectPackError("metadata pack no contiene el catalog latest")
    if catalog_vault_id != parsed.vault_id:
        raise MetadataObjectPackError(
            f"catalog.vault_id={catalog_vault_id} no coincide con vault.id={parsed.vault_id}"
        )

    _validate_pack_payload_graph(parsed, validated_by_hash)
    return objects


def validate_pack_payload(
    payload: dict[str, Any],
) -> tuple[LatestMetadataPointer, str, int, float]:
    """Valida objetos y grafo completo sin materializar EncodedMetadataObject."""

    parsed = parse_pack_payload_base(payload)
    _validate_pack_payload_entries(parsed, collect_objects=False)
    return parsed.latest, parsed.vault_id, parsed.vault_generation, parsed.pack_created_at_unix


def parse_pack_payload(
    payload: dict[str, Any],
) -> tuple[LatestMetadataPointer, list[EncodedMetadataObject], str, int, float]:
    """Valida el payload y materializa sus objetos para importarlos."""

    parsed = parse_pack_payload_base(payload)
    objects = _validate_pack_payload_entries(parsed, collect_objects=True)
    if objects is None:
        raise AssertionError("collect_objects=True debe devolver los objetos validados")
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
