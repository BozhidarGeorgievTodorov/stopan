"""
Chunking de archivos mediante Content-Defined Chunking.

Este módulo divide streams de archivo en chunks de tamaño variable mediante
FastCDC. Cada chunk se identifica por el BLAKE3 del contenido raw que se
entrega al CAS.
"""

from __future__ import annotations

import mmap
import os
from collections.abc import Iterator
from typing import BinaryIO

import blake3

from stopan.chunking import fastcdc
from stopan.errors import StopanConfigValueError


_AVG_CHUNK_SIZE = 65536
_MIN_CHUNK_SIZE = 16384      # // 4
_MAX_CHUNK_SIZE = 262144     # * 4


class FileChunker:
    """
    Divide archivos en chunks de tamaño variable usando CDC y mmap.

    Invariantes de diseño:
      - avg_chunk_size debe ser una potencia de dos para derivar la selectividad
        de las máscaras normalizadas de FastCDC.
      - Se utiliza mmap para recorrer archivos sin cargarlos completos en memoria.
      - La computación intensiva en CPU se delega a una extensión nativa.
    """

    __slots__ = (
        "avg_chunk_size",
        "min_chunk_size",
        "max_chunk_size",
    )

    def __init__(
        self,
        avg_chunk_size: int = _AVG_CHUNK_SIZE,
        min_chunk_size: int = _MIN_CHUNK_SIZE,
        max_chunk_size: int = _MAX_CHUNK_SIZE,
    ):
        avg_chunk_size = int(avg_chunk_size)
        min_chunk_size = int(min_chunk_size)
        max_chunk_size = int(max_chunk_size)

        if avg_chunk_size <= 0:
            raise StopanConfigValueError("avg_chunk_size debe ser > 0")
        if avg_chunk_size & (avg_chunk_size - 1) != 0:
            raise StopanConfigValueError("avg_chunk_size debe ser potencia de 2")
        if min_chunk_size <= 0:
            raise StopanConfigValueError("min_chunk_size debe ser > 0")
        if avg_chunk_size < min_chunk_size:
            raise StopanConfigValueError("avg_chunk_size debe ser >= min_chunk_size")
        if max_chunk_size < avg_chunk_size:
            raise StopanConfigValueError("max_chunk_size debe ser >= avg_chunk_size")

        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size

    def chunk_stream(self, file_stream: BinaryIO) -> Iterator[tuple[str, bytes]]:
        """Genera pares (chunk_hash, chunk_data) para un stream abierto."""
        yield from self._chunk_fastcdc(file_stream)

    def _chunk_fastcdc(self, file_stream: BinaryIO) -> Iterator[tuple[str, bytes]]:
        file_descriptor = file_stream.fileno()
        file_size = os.fstat(file_descriptor).st_size

        if file_size == 0:
            return

        # Ningún archivo de tamaño <= min_chunk_size puede contener un corte CDC
        # anterior a EOF. Evitamos mmap y el iterador nativo en esta ruta frecuente.
        if file_size <= self.min_chunk_size:
            file_stream.seek(0)
            chunk_data = file_stream.read(self.min_chunk_size + 1)
            if len(chunk_data) <= self.min_chunk_size:
                if chunk_data:
                    yield blake3.blake3(chunk_data).hexdigest(), chunk_data
                return

            # El archivo creció entre fstat() y read(). Se procesa por la ruta CDC
            # normal con su tamaño actual en lugar de tratarlo como un único chunk.
            file_size = os.fstat(file_descriptor).st_size

        with mmap.mmap(file_descriptor, length=0, access=mmap.ACCESS_READ) as mapped_file:
            boundaries = fastcdc.iter_boundaries(
                mapped_file,
                self.min_chunk_size,
                self.avg_chunk_size,
                self.max_chunk_size,
            )

            try:
                start = 0
                for end in boundaries:
                    end = int(end)
                    if end <= start:
                        raise RuntimeError(
                            "fastcdc devolvió puntos de corte no crecientes: "
                            f"start={start} end={end} file_size={file_size}"
                        )
                    if end > file_size:
                        raise RuntimeError(
                            "fastcdc devolvió un punto de corte más allá del EOF: "
                            f"end={end} file_size={file_size}"
                        )

                    chunk_data = mapped_file[start:end]
                    chunk_hash = blake3.blake3(chunk_data).hexdigest()
                    yield chunk_hash, chunk_data
                    start = end

                if start < file_size:
                    chunk_data = mapped_file[start:file_size]
                    chunk_hash = blake3.blake3(chunk_data).hexdigest()
                    yield chunk_hash, chunk_data
            finally:
                del boundaries
