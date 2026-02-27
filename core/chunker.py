import mmap
import os

import blake3

from core import fast_rabin


class FileChunker:
    """
    Divide archivos en bloques de tamaño variable usando Content-Defined Chunking.
    La búsqueda de puntos de corte se delega en la extensión nativa fast_rabin.
    """

    def __init__(self, avg_chunk_size=4096, min_chunk_size=None, max_chunk_size=None):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size or avg_chunk_size // 4
        self.max_chunk_size = max_chunk_size or avg_chunk_size * 4
        self.relaxed_mask = avg_chunk_size - 1
        self.strong_mask = (avg_chunk_size * 2) - 1

    def chunk_file(self, file_path):
        """Lee un archivo y genera pares (hash, datos) para cada bloque."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'rb') as f:
            yield from self.chunk_stream(f)

    def chunk_stream(self, file_stream):
        """Genera bloques desde un stream binario."""
        file_size = os.fstat(file_stream.fileno()).st_size
        if file_size == 0:
            return

        with mmap.mmap(file_stream.fileno(), length=0, access=mmap.ACCESS_READ) as mapped_file:
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
                    yield self._create_chunk(chunk_data)
                    start = end
            finally:
                del boundaries

    def _create_chunk(self, buffer_data):
        """Calcula el BLAKE3 del bloque finalizado y devuelve ambos."""
        data = bytes(buffer_data)
        chunk_hash = blake3.blake3(data).hexdigest()
        return chunk_hash, data
