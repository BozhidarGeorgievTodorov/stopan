from __future__ import annotations

import blake3

from stopan.errors import StopanDataError


class MetadataPackHashTypeError(StopanDataError, TypeError):
    """Tipo inválido en hash de metadata pack, compatible con TypeError."""


class MetadataPackHashValueError(StopanDataError, ValueError):
    """Valor inválido en hash de metadata pack, compatible con ValueError."""


_HEX64_ALPHABET = set("0123456789abcdef")


def calculate_pack_hash(data: bytes) -> str:
    if not isinstance(data, bytes):
        raise MetadataPackHashTypeError("metadata pack data debe ser bytes")
    return blake3.blake3(data).hexdigest()


def validate_pack_hash(value: str, *, name: str = "pack_hash") -> str:
    if not isinstance(value, str):
        raise MetadataPackHashTypeError(f"{name} debe ser string")

    text = value.strip()
    if len(text) != 64 or any(char not in _HEX64_ALPHABET for char in text):
        raise MetadataPackHashValueError(f"{name} debe tener 64 caracteres hexadecimales lowercase")

    return text
