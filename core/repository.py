import os
import uuid

import blake3
import zstandard as zstd

DATA_FOLDER = "_data_chunks"
ZSTD_LEVEL = 3


class CASRepository:
    """
    Almacena bloques direccionados por hash en disco.
    """

    def __init__(self, data_folder=DATA_FOLDER):
        self.data_folder = data_folder
        self.compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        self.decompressor = zstd.ZstdDecompressor()
        os.makedirs(self.data_folder, exist_ok=True)

    def put(self, chunk_hash, chunk_data):
        """Comprime y guarda un bloque si todavía no existe."""
        compressed_data = self.compressor.compress(chunk_data)
        return self.put_compressed(chunk_hash, compressed_data)

    def put_compressed(self, chunk_hash, compressed_data):
        """Guarda un bloque ya comprimido, validando antes su hash."""
        self._validate_compressed_block(chunk_hash, compressed_data)
        path = self._chunk_path(chunk_hash)

        if os.path.exists(path):
            return False

        os.makedirs(os.path.dirname(path), exist_ok=True)
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"

        try:
            with open(temp_path, 'wb') as f:
                f.write(compressed_data)

            os.replace(temp_path, path)
            return True

        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def get(self, chunk_hash):
        """Recupera, descomprime y verifica un bloque guardado por hash."""
        compressed_data = self.get_compressed(chunk_hash)
        return self._decompress_and_validate(chunk_hash, compressed_data)

    def get_compressed(self, chunk_hash):
        """Devuelve el bloque comprimido tal como está almacenado."""
        path = self._chunk_path(chunk_hash)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing block: {chunk_hash}")

        with open(path, 'rb') as f:
            return f.read()

    def _validate_compressed_block(self, chunk_hash, compressed_data):
        self._decompress_and_validate(chunk_hash, compressed_data)

    def _decompress_and_validate(self, chunk_hash, compressed_data):
        try:
            data = self.decompressor.decompress(compressed_data)
        except zstd.ZstdError as exc:
            raise ValueError(f"Corrupt compressed block: {chunk_hash}") from exc

        calculated_hash = blake3.blake3(data).hexdigest()
        if calculated_hash != chunk_hash:
            raise ValueError(f"Corrupt block: {chunk_hash}")

        return data

    def _chunk_path(self, chunk_hash):
        first_dir = chunk_hash[:2]
        second_dir = chunk_hash[2:4]
        return os.path.join(self.data_folder, first_dir, second_dir, chunk_hash)
