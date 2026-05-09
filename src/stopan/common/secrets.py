from __future__ import annotations


def passphrase_bytes(passphrase: str | bytes) -> bytes:
    if isinstance(passphrase, str):
        data = passphrase.encode("utf-8")
    elif isinstance(passphrase, bytes):
        data = passphrase
    else:
        raise TypeError(
            f"passphrase debe ser str o bytes; recibido {type(passphrase).__name__}"
        )
    if not data:
        raise ValueError("passphrase no puede estar vacía")
    return data
