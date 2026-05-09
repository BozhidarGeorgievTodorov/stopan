"""
Lectura y validación de passphrase para identidades de metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from stopan.metadata.identity.models import MetadataIdentityError


_MAX_PASSPHRASE_FILE_BYTES = 1024 * 1024


class MetadataPassphraseError(MetadataIdentityError):
    pass


@dataclass(frozen=True, slots=True)
class ScryptCost:
    n: int
    r: int
    p: int
    key_length: int = 32

    def __post_init__(self) -> None:
        if type(self.n) is not int or self.n < 2:
            raise ValueError(f"scrypt.n debe ser un entero >= 2. Recibido {self.n!r}")
        if self.n & (self.n - 1) != 0:
            raise ValueError(f"scrypt.n debe ser potencia de 2. Recibido {self.n!r}")
        if type(self.r) is not int or self.r < 1:
            raise ValueError(f"scrypt.r debe ser un entero >= 1. Recibido {self.r!r}")
        if type(self.p) is not int or self.p < 1:
            raise ValueError(f"scrypt.p debe ser un entero >= 1. Recibido {self.p!r}")
        if type(self.key_length) is not int or self.key_length < 32:
            raise ValueError(
                f"scrypt.key_length debe ser un entero >= 32. "
                f"Recibido {self.key_length!r}"
            )


def read_passphrase_file(path: str | Path) -> str:
    file_path = Path(path).expanduser().resolve()

    if not file_path.is_file():
        raise MetadataPassphraseError(
            f"La ruta de passphrase no es un archivo regular: {file_path}"
        )

    try:
        if file_path.stat().st_size > _MAX_PASSPHRASE_FILE_BYTES:
            raise MetadataPassphraseError(
                f"El archivo de passphrase es demasiado grande: {file_path}"
            )
    except OSError as exc:
        raise MetadataPassphraseError(
            f"No se pudo inspeccionar el archivo de passphrase {file_path}: {exc}"
        ) from exc

    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            passphrase = handle.readline().strip()
    except OSError as exc:
        raise MetadataPassphraseError(
            f"No se pudo leer el archivo de passphrase {file_path}: {exc}"
        ) from exc

    if not passphrase:
        raise MetadataPassphraseError(f"El archivo de passphrase está vacío: {file_path}")

    return passphrase
