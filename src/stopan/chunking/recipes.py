"""
Hash canónico de recipes de snapshot.

Una recipe describe cómo reconstruir un archivo a partir de una secuencia
ordenada de chunks. Su hash identifica la estructura de esa secuencia, no el
contenido de los chunks, que ya está identificado por BLAKE3.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

RecipeChunk = tuple[int, str, int]


def compute_recipe_hash(chunks: Iterable[RecipeChunk]) -> str:
    """
    Calcula el hash canónico SHA-256 de una recipe.

    Contrato:
      - el hash depende del orden del chunk dentro del archivo.
      - el chunk_hash se serializa como bytes crudos desde su hexadecimal.
      - el tamaño del chunk se serializa en bytes.
      - los enteros se serializan en Big Endian para mantener el resultado estable
        entre arquitecturas.
    """
    digest = hashlib.sha256()
    for order, chunk_hash, size in chunks:
        digest.update(int(order).to_bytes(4, "big", signed=False))
        digest.update(bytes.fromhex(chunk_hash))
        digest.update(int(size).to_bytes(8, "big", signed=False))
    return digest.hexdigest()