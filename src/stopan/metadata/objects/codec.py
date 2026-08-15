"""
Codificación canónica de metadata objects.

Cada objeto se normaliza a JSON canónico, se envuelve con formato/version/type y
se identifica por BLAKE3 de esos bytes canónicos.
"""

from __future__ import annotations

from dataclasses import MISSING, asdict, dataclass, fields, is_dataclass
from enum import Enum
import types
from typing import Any, Union, get_args, get_origin, get_type_hints

import blake3

from stopan.common.json import canonical_json_bytes
from stopan.metadata.objects.models import (
    METADATA_OBJECT_FORMAT,
    METADATA_OBJECT_VERSION,
    CatalogObject,
    ChunkListObject,
    ErasureDataPackIndexObject,
    ErasureDataPackShardObject,
    FileObject,
    KnownChunkIndexObject,
    KnownChunkShardObject,
    MetadataObjectError,
    MetadataObjectType,
    MetadataPlainObject,
    ProtectionIndexObject,
    ProtectionShardObject,
    RecipeObject,
    SnapshotIndexObject,
    SnapshotRootObject,
    TreeObject,
)


_OBJECT_CLASS_BY_TYPE = {
    MetadataObjectType.CATALOG: CatalogObject,
    MetadataObjectType.SNAPSHOT_INDEX: SnapshotIndexObject,
    MetadataObjectType.SNAPSHOT_ROOT: SnapshotRootObject,
    MetadataObjectType.TREE: TreeObject,
    MetadataObjectType.FILE: FileObject,
    MetadataObjectType.RECIPE: RecipeObject,
    MetadataObjectType.CHUNK_LIST: ChunkListObject,
    MetadataObjectType.KNOWN_CHUNK_INDEX: KnownChunkIndexObject,
    MetadataObjectType.KNOWN_CHUNK_SHARD: KnownChunkShardObject,
    MetadataObjectType.PROTECTION_INDEX: ProtectionIndexObject,
    MetadataObjectType.PROTECTION_SHARD: ProtectionShardObject,
    MetadataObjectType.ERASURE_DATA_PACK_INDEX: ErasureDataPackIndexObject,
    MetadataObjectType.ERASURE_DATA_PACK_SHARD: ErasureDataPackShardObject,
}


def _decode_model_value(value: Any, expected_type: Any, *, path: str) -> Any:
    if expected_type is Any:
        return value

    origin = get_origin(expected_type)
    args = get_args(expected_type)

    if origin in (types.UnionType, Union):
        if value is None and type(None) in args:
            return None
        errors: list[str] = []
        for candidate in args:
            if candidate is type(None):
                continue
            try:
                return _decode_model_value(value, candidate, path=path)
            except MetadataObjectError as exc:
                errors.append(str(exc))
        raise MetadataObjectError(
            f"{path} no coincide con ninguno de los tipos permitidos"
            + (": " + " | ".join(errors) if errors else "")
        )

    if origin is tuple:
        if not isinstance(value, list):
            raise MetadataObjectError(
                f"{path} debe ser lista JSON; recibido {type(value).__name__}"
            )
        if len(args) == 2 and args[1] is Ellipsis:
            item_type = args[0]
            return tuple(
                _decode_model_value(item, item_type, path=f"{path}[{index}]")
                for index, item in enumerate(value)
            )
        if len(value) != len(args):
            raise MetadataObjectError(
                f"{path} debe contener {len(args)} elementos; recibido {len(value)}"
            )
        return tuple(
            _decode_model_value(item, item_type, path=f"{path}[{index}]")
            for index, (item, item_type) in enumerate(zip(value, args, strict=True))
        )

    if isinstance(expected_type, type) and issubclass(expected_type, Enum):
        try:
            return expected_type(value)
        except (TypeError, ValueError) as exc:
            raise MetadataObjectError(
                f"{path} no es un valor válido de {expected_type.__name__}: {value!r}"
            ) from exc

    if isinstance(expected_type, type) and is_dataclass(expected_type):
        return _decode_model_dataclass(expected_type, value, path=path)

    return value


def _decode_model_dataclass(cls: type[Any], payload: Any, *, path: str) -> Any:
    if not isinstance(payload, dict):
        raise MetadataObjectError(
            f"{path} debe ser objeto JSON; recibido {type(payload).__name__}"
        )

    model_fields = tuple(field for field in fields(cls) if field.init)
    allowed = {field.name for field in model_fields}
    extras = sorted(set(payload) - allowed)
    if extras:
        raise MetadataObjectError(
            f"{path} contiene campos no soportados: {', '.join(extras)}"
        )

    hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for field in model_fields:
        if field.name not in payload:
            if field.default is MISSING and field.default_factory is MISSING:
                raise MetadataObjectError(f"{path}.{field.name} es obligatorio")
            continue
        kwargs[field.name] = _decode_model_value(
            payload[field.name],
            hints[field.name],
            path=f"{path}.{field.name}",
        )

    try:
        return cls(**kwargs)
    except MetadataObjectError:
        raise
    except (TypeError, ValueError) as exc:
        raise MetadataObjectError(f"{path} inválido: {exc}") from exc


def decode_metadata_object_payload(
    object_type: MetadataObjectType,
    payload: dict[str, Any],
) -> MetadataPlainObject:
    """Decodifica un payload JSON contra el modelo canónico de su tipo.

    La reconstrucción fuerza los invariantes de las dataclasses y exige que la
    representación normalizada vuelva a producir exactamente el mismo payload.
    Así se rechazan campos extra, omisiones de campos canónicos y coerciones de
    tipos que no formarían parte de un objeto emitido por el propio sistema.
    """

    cls = _OBJECT_CLASS_BY_TYPE.get(object_type)
    if cls is None:
        raise MetadataObjectError(f"tipo de metadata object no soportado: {object_type.value}")
    obj = _decode_model_dataclass(cls, payload, path=object_type.value)
    normalized = object_payload(obj)
    if normalized != payload:
        raise MetadataObjectError(
            f"payload de {object_type.value} no coincide con su representación canónica de modelo"
        )
    return obj


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
