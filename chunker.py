import os
import hashlib

ROLLING_PRIME = 31
ROLLING_MOD = 1_000_000_009
READ_SIZE = 1024 * 1024


class FileChunker:
    """
    Divide archivos en bloques de tamaño variable usando Content-Defined Chunking.
    La lectura se hace por buffers para evitar leer byte a byte desde disco.
    """

    def __init__(self, avg_chunk_size=1024, min_chunk_size=512, max_chunk_size=2048):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.mask = avg_chunk_size - 1
        self.window_size = 48
        self.power_term = pow(ROLLING_PRIME, self.window_size, ROLLING_MOD)

    def chunk_file(self, file_path):
        """Lee un archivo y genera pares (hash, datos) para cada bloque."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'rb') as f:
            buffer = bytearray()

            while True:
                data = f.read(READ_SIZE)
                if not data:
                    break

                buffer.extend(data)

                while len(buffer) >= self.max_chunk_size:
                    cut_index = self._find_cut_point(buffer)

                    if cut_index is None:
                        cut_index = self.max_chunk_size

                    chunk_data = buffer[:cut_index]
                    yield self._create_chunk(chunk_data)
                    del buffer[:cut_index]

            if buffer:
                yield self._create_chunk(buffer)

    def _find_cut_point(self, buffer):
        """Busca un punto de corte entre los tamaños mínimo y máximo configurados."""
        limit = min(len(buffer), self.max_chunk_size)
        if limit < self.min_chunk_size:
            return None

        current_hash = 0
        start = self.min_chunk_size - self.window_size

        for i in range(start, self.min_chunk_size):
            current_hash = (current_hash * ROLLING_PRIME + buffer[i]) % ROLLING_MOD

        for i in range(self.min_chunk_size, limit):
            old_byte = buffer[i - self.window_size]
            new_byte = buffer[i]

            current_hash = (current_hash - old_byte * self.power_term) % ROLLING_MOD
            current_hash = (current_hash * ROLLING_PRIME + new_byte) % ROLLING_MOD

            if (current_hash & self.mask) == 0:
                return i + 1

        return None

    def _create_chunk(self, buffer_data):
        """Calcula el SHA-256 del bloque finalizado y devuelve ambos."""
        data = bytes(buffer_data)
        sha256 = hashlib.sha256(data).hexdigest()
        return sha256, data
