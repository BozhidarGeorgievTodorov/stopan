from __future__ import annotations

import base64


class EncodingError(RuntimeError):
    pass


def b64encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def b64decode(name: str, value: object) -> bytes:
    if not isinstance(value, str):
        raise EncodingError(f"{name} debe ser base64 string")
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except Exception as exc:
        raise EncodingError(f"{name} no es base64 válido") from exc
