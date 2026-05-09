from __future__ import annotations

from .errors import MetadataObjectImportError
from stopan.metadata.objects.models import MetadataObjectType
from stopan.protection.policy import ProtectionState


def require_str(name: str, value: object, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise MetadataObjectImportError(
            f"{name} debe ser string; recibido {type(value).__name__}"
        )
    if not allow_empty and not value:
        raise MetadataObjectImportError(f"{name} no puede estar vacío")
    return value


def require_int(name: str, value: object, *, min_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataObjectImportError(
            f"{name} debe ser entero; recibido {type(value).__name__}"
        )
    if value < min_value:
        raise MetadataObjectImportError(f"{name} debe ser >= {min_value}; recibido {value}")
    return value


def require_number(name: str, value: object, *, min_value: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetadataObjectImportError(
            f"{name} debe ser numérico; recibido {type(value).__name__}"
        )
    number = float(value)
    if min_value is not None and number < min_value:
        raise MetadataObjectImportError(f"{name} debe ser >= {min_value}; recibido {value}")
    return number


def require_hash64(name: str, value: object) -> str:
    text = require_str(name, value)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectImportError(f"{name} debe ser hex lowercase de 64 caracteres")
    return text


def require_ref(name: str, value: object, expected_type: MetadataObjectType) -> str:
    if not isinstance(value, dict):
        raise MetadataObjectImportError(f"{name} debe ser ObjectRef")
    if value.get("object_type") != expected_type.value:
        raise MetadataObjectImportError(
            f"{name} debe referenciar {expected_type.value}; "
            f"recibido {value.get('object_type')!r}"
        )
    return require_hash64(f"{name}.object_hash", value.get("object_hash"))


def optional_ref(name: str, value: object, expected_type: MetadataObjectType) -> str | None:
    if value is None:
        return None
    return require_ref(name, value, expected_type)


def require_protection_state(name: str, value: object) -> str:
    text = require_str(name, value)
    try:
        ProtectionState(text)
    except ValueError as exc:
        raise MetadataObjectImportError(
            f"{name} no es un estado de protección válido: {text!r}"
        ) from exc
    return text
