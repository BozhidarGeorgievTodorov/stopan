import hashlib
import os

try:
    import fast_rabin
except ImportError as exc:
    raise ImportError(
        "fast_rabin is required. Compile it with: python setup.py build_ext --inplace"
    ) from exc


class FileChunker:
    """
    Divide archivos en bloques de tamaño variable usando Content-Defined Chunking.
    La búsqueda de puntos de corte se delega en la extensión nativa fast_rabin.
    """

    def __init__(self, avg_chunk_size=4096, min_chunk_size=None, max_chunk_size=None):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size or avg_chunk_size // 4
        self.max_chunk_size = max_chunk_size or avg_chunk_size * 4
        self.mask = avg_chunk_size - 1

    def chunk_file(self, file_path):
        """Lee un archivo y genera pares (hash, datos) para cada bloque."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'rb') as f:
            yield from self.chunk_stream(f)

    def chunk_stream(self, file_stream):
        """Genera bloques desde un stream binario."""
        data = file_stream.read()
        if not data:
            return

        boundaries = fast_rabin.get_chunk_boundaries(
            data,
            self.mask,
            self.min_chunk_size,
            self.max_chunk_size,
        )

        start = 0
        for end in boundaries:
            chunk_data = data[start:end]
            yield self._create_chunk(chunk_data)
            start = end

    def _create_chunk(self, buffer_data):
        """Calcula el SHA-256 del bloque finalizado y devuelve ambos."""
        data = bytes(buffer_data)
        sha256 = hashlib.sha256(data).hexdigest()
        return sha256, data
