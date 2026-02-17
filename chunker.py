import hashlib
import os

from rabin import RabinKarpRollingHash

READ_SIZE = 4096


class FileChunker:
    """
    Divide archivos en bloques de tamaño variable usando Content-Defined Chunking.
    """

    def __init__(self, avg_chunk_size=1024, min_chunk_size=512, max_chunk_size=2048):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.mask = avg_chunk_size - 1
        self.window_size = 48

    def chunk_file(self, file_path):
        """Lee un archivo y genera pares (hash, datos) para cada bloque."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'rb') as f:
            yield from self.chunk_stream(f)

    def chunk_stream(self, file_stream):
        """Genera bloques desde un stream binario."""
        rolling_hash = RabinKarpRollingHash(window_size=self.window_size)
        buffer = bytearray()

        while True:
            data = file_stream.read(READ_SIZE)
            if not data:
                break

            for byte in data:
                buffer.append(byte)
                current_len = len(buffer)
                hash_value = rolling_hash.update(byte)

                if current_len < self.min_chunk_size:
                    continue

                if (hash_value & self.mask) == 0 or current_len >= self.max_chunk_size:
                    yield self._create_chunk(buffer)
                    buffer = bytearray()

        if buffer:
            yield self._create_chunk(buffer)

    def _create_chunk(self, buffer_data):
        """Calcula el SHA-256 del bloque finalizado y devuelve ambos."""
        data = bytes(buffer_data)
        sha256 = hashlib.sha256(data).hexdigest()
        return sha256, data
