"""
Validación de identificadores canónicos de chunk.

Los hashes de chunk se usan como identificadores de contenido y como parte de
rutas internas del CAS. Por eso se aceptan únicamente hashes BLAKE3 en formato
hexadecimal canónico: 64 caracteres, minúsculas y sin separadores.
"""

from __future__ import annotations


_BLAKE3_HEX_LENGTH = 64
_HEX_LOWER = frozenset("0123456789abcdef")


def is_valid_chunk_hash(value: object) -> bool:
    """
    Devuelve True si value es un identificador canónico de chunk Stopan.

    Contrato:
      - BLAKE3 hexadecimal en minúsculas;
      - exactamente 64 caracteres;
      - sin separadores, rutas, prefijos ni whitespace.

    Esta validación es una barrera de seguridad para no usar entradas remotas
    como nombres de fichero dentro del CAS local.
    """
    if not isinstance(value, str):
        return False
    if len(value) != _BLAKE3_HEX_LENGTH:
        return False
    return all(char in _HEX_LOWER for char in value)


def require_valid_chunk_hash(value: object, *, field_name: str = "chunk_hash") -> str:
    """
    Devuelve value como hash de chunk válido o lanza ValueError.

    Esta función se usa en los límites de confianza del sistema cuando un hash
    recibido desde CLI, metadata, red o almacenamiento persistente debe pasar a
    formar parte de rutas internas o consultas sobre el CAS.
    """
    if not is_valid_chunk_hash(value):
        raise ValueError(
            f"{field_name} inválido: se esperaba BLAKE3 hexadecimal en minúsculas "
            f"de {_BLAKE3_HEX_LENGTH} caracteres"
        )
    assert isinstance(value, str)
    return value