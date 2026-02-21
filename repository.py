import hashlib
import os
import uuid
import zlib

DATA_FOLDER = "_data_chunks"


class CASRepository:
    """
    Almacena bloques direccionados por hash en disco.
    """

    def __init__(self, data_folder=DATA_FOLDER):
        self.data_folder = data_folder
        os.makedirs(self.data_folder, exist_ok=True)

    def put(self, chunk_hash, chunk_data):
        """Guarda un bloque si todavía no existe en el repositorio."""
        path = self._chunk_path(chunk_hash)

        if os.path.exists(path):
            return False

        os.makedirs(os.path.dirname(path), exist_ok=True)
        compressed_data = zlib.compress(chunk_data)
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
        path = self._chunk_path(chunk_hash)
        if not os.path.exists(path):
            raise ValueError(f"Missing block: {chunk_hash}")

        with open(path, 'rb') as f:
            compressed_data = f.read()

        try:
            data = zlib.decompress(compressed_data)
        except zlib.error as exc:
            raise ValueError(f"Corrupt compressed block: {chunk_hash}") from exc

        calculated_hash = hashlib.sha256(data).hexdigest()
        if calculated_hash != chunk_hash:
            raise ValueError(f"Corrupt block: {chunk_hash}")

        return data

    def _chunk_path(self, chunk_hash):
        first_dir = chunk_hash[:2]
        second_dir = chunk_hash[2:4]
        return os.path.join(self.data_folder, first_dir, second_dir, chunk_hash)
