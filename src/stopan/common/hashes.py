from __future__ import annotations

BLAKE3_HEX_LENGTH = 64
_HEX_LOWER = frozenset("0123456789abcdef")


def is_valid_blake3_hex(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == BLAKE3_HEX_LENGTH
        and all(char in _HEX_LOWER for char in value)
    )


def blake3_hex_digest(data: bytes) -> str:
    import blake3

    return blake3.blake3(data).hexdigest()
