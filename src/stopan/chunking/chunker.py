from __future__ import annotations

import mmap
import os

import blake3

from stopan.chunking import fast_rabin


AVG_CHUNK_SIZE = 1024 * 1024
MIN_CHUNK_SIZE = 512 * 1024
MAX_CHUNK_SIZE = 8 * 1024 * 1024


class FileChunker:
    """
    Divide archivos en bloques de tamaño variable usando Content-Defined Chunking.
    La búsqueda de puntos de corte se delega en la extensión nativa fast_rabin.
    """

    __slots__ = (
        "avg_chunk_size",
        "min_chunk_size",
        "max_chunk_size",
        "relaxed_mask",
        "strong_mask",
    )

    def __init__(
        self,
        avg_chunk_size: int = AVG_CHUNK_SIZE,
        min_chunk_size: int = MIN_CHUNK_SIZE,
        max_chunk_size: int = MAX_CHUNK_SIZE,
    ):
        avg_chunk_size = int(avg_chunk_size)
        min_chunk_size = int(min_chunk_size)
        max_chunk_size = int(max_chunk_size)

        if avg_chunk_size <= 0:
            raise ValueError("avg_chunk_size must be > 0")
        if avg_chunk_size & (avg_chunk_size - 1) != 0:
            raise ValueError("avg_chunk_size must be a power of two")
        if min_chunk_size <= 0:
            raise ValueError("min_chunk_size must be > 0")
        if avg_chunk_size < min_chunk_size:
            raise ValueError("avg_chunk_size must be >= min_chunk_size")
        if max_chunk_size < avg_chunk_size:
            raise ValueError("max_chunk_size must be >= avg_chunk_size")

        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size

        self.relaxed_mask = avg_chunk_size - 1
        self.strong_mask = (avg_chunk_size * 2) - 1

    def chunk_stream(self, file_stream):
        """Yield (chunk_hash, chunk_data) pairs for an open file stream."""
        yield from self._chunk_fast_c(file_stream)

    def _chunk_fast_c(self, file_stream):
        file_descriptor = file_stream.fileno()
        file_size = os.fstat(file_descriptor).st_size

        if file_size == 0:
            return

        with mmap.mmap(file_descriptor, length=0, access=mmap.ACCESS_READ) as mapped_file:
            boundaries = fast_rabin.get_chunk_boundaries(
                mapped_file,
                self.strong_mask,
                self.relaxed_mask,
                self.min_chunk_size,
                self.avg_chunk_size,
                self.max_chunk_size,
            )

            try:
                start = 0
                for end in boundaries:
                    chunk_data = mapped_file[start:end]
                    chunk_hash = blake3.blake3(chunk_data).hexdigest()
                    yield chunk_hash, chunk_data
                    start = end
            finally:
                del boundaries
