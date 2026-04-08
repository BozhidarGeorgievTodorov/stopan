from __future__ import annotations

import os
import threading
import uuid

import blake3
import zstandard as zstd

DATA_FOLDER = "_data_chunks"
ZSTD_LEVEL = 3


class CASRepositoryError(Exception):
    """Error base del repositorio CAS."""


class CASCorruptionError(CASRepositoryError):
    """El bloque existe pero está corrupto o no coincide con su hash."""


class CASRepository:
    """
    Almacena bloques direccionados por hash en disco.

    Semántica:
      - put(chunk_hash, chunk_data) comprime y persiste atómicamente.
      - put_compressed(chunk_hash, compressed_data) valida y persiste un bloque ya comprimido.
      - get(chunk_hash) lee, descomprime y verifica integridad.
      - get_compressed(chunk_hash) devuelve el bloque comprimido tal como está almacenado.
    """

    __slots__ = ("data_folder", "_tls", "_ensured_dirs", "_dir_lock")

    def __init__(self, data_folder=DATA_FOLDER):
        self.data_folder = os.path.abspath(data_folder)
        os.makedirs(self.data_folder, exist_ok=True)

        self._tls = threading.local()
        self._ensured_dirs = {self.data_folder}
        self._dir_lock = threading.Lock()

    def _compressor(self):
        compressor = getattr(self._tls, "compressor", None)
        if compressor is None:
            compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL)
            self._tls.compressor = compressor
        return compressor

    def _decompressor(self):
        decompressor = getattr(self._tls, "decompressor", None)
        if decompressor is None:
            decompressor = zstd.ZstdDecompressor()
            self._tls.decompressor = decompressor
        return decompressor

    def exists_local(self, chunk_hash):
        """Comprueba si el bloque ya existe en el repositorio local."""
        return os.path.exists(self._chunk_path(chunk_hash))

    def put(self, chunk_hash, chunk_data):
        """Comprime y guarda un bloque si todavía no existe."""
        compressed_data = self._compressor().compress(chunk_data)
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def put_compressed(self, chunk_hash, compressed_data):
        """Guarda un bloque ya comprimido, validando antes su hash."""
        self._validate_compressed_block(chunk_hash, compressed_data)
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def get(self, chunk_hash):
        """Recupera, descomprime y verifica un bloque guardado por hash."""
        compressed_data = self.get_compressed(chunk_hash)
        return self._decompress_and_validate(chunk_hash, compressed_data)

    def get_compressed(self, chunk_hash):
        """Devuelve el bloque comprimido tal como está almacenado."""
        path = self._chunk_path(chunk_hash)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing block: {chunk_hash}")

        with open(path, "rb") as f:
            return f.read()

    def _write_once(self, path, data):
        if os.path.exists(path):
            return False

        self._ensure_parent_dir(path)
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        written = False

        try:
            with open(temp_path, "wb") as f:
                f.write(data)

            try:
                os.replace(temp_path, path)
                written = True
            except OSError:
                if os.path.exists(path):
                    written = False
                else:
                    raise

            return written

        finally:
            if not written:
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    def _ensure_parent_dir(self, path):
        parent = os.path.dirname(path)
        if parent in self._ensured_dirs:
            return

        with self._dir_lock:
            if parent in self._ensured_dirs:
                return
            os.makedirs(parent, exist_ok=True)
            self._ensured_dirs.add(parent)

    def _validate_compressed_block(self, chunk_hash, compressed_data):
        self._decompress_and_validate(chunk_hash, compressed_data)

    def _decompress_and_validate(self, chunk_hash, compressed_data):
        try:
            data = self._decompressor().decompress(compressed_data)
        except zstd.ZstdError as exc:
            raise CASCorruptionError(f"Corrupt compressed block: {chunk_hash}") from exc

        calculated_hash = blake3.blake3(data).hexdigest()
        if calculated_hash != chunk_hash:
            raise CASCorruptionError(
                f"Corrupt block: expected {chunk_hash}, got {calculated_hash}"
            )

        return data

    def _chunk_dir(self, chunk_hash):
        return os.path.join(self.data_folder, chunk_hash[:2], chunk_hash[2:4])

    def _chunk_path(self, chunk_hash):
        return os.path.join(self._chunk_dir(chunk_hash), chunk_hash)