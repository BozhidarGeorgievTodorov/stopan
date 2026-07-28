from __future__ import annotations

from pathlib import Path

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


def calculate_pack_hash_file(path: str | Path, *, block_size: int = 1024 * 1024) -> str:
    pack_path = Path(path).expanduser().resolve()
    size = int(block_size)
    if size <= 0:
        raise MetadataPackHashValueError("block_size debe ser > 0")

    hasher = blake3.blake3()
    with pack_path.open("rb") as fh:
        while True:
            block = fh.read(size)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def validate_pack_hash(value: str, *, name: str = "pack_hash") -> str:
    if not isinstance(value, str):
        raise MetadataPackHashTypeError(f"{name} debe ser string")

    text = value.strip()
    if len(text) != 64 or any(char not in _HEX64_ALPHABET for char in text):
        raise MetadataPackHashValueError(f"{name} debe tener 64 caracteres hexadecimales lowercase")

    return text
