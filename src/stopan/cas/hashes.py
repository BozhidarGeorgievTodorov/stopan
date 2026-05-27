"""
Validación de identificadores canónicos de chunk.

Los hashes de chunk se usan como identificadores de contenido y como parte de
rutas internas del CAS. Por eso se aceptan únicamente hashes BLAKE3 en formato
hexadecimal canónico: 64 caracteres, minúsculas y sin separadores.
"""

from __future__ import annotations

from stopan.common.hashes import is_valid_blake3_hex


def is_valid_chunk_hash(value: object) -> bool:
    """
    Devuelve True si value es un identificador canónico de chunk Stopan.

    Contrato:
      - BLAKE3 hexadecimal en minúsculas;
      - exactamente 64 caracteres;
      - sin separadores, rutas, prefijos ni whitespace.
    """
    return is_valid_blake3_hex(value)
