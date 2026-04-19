from __future__ import annotations

import os
import threading
import uuid
from typing import Final

import blake3
import zstandard as zstd


DATA_FOLDER = "_data_chunks"


class CASRepositoryError(Exception):
    """Base error for the CAS repository."""


class CASCorruptionError(CASRepositoryError):
    """Stored block is corrupt or does not match its content hash."""


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

    _ZSTD_LEVEL: Final[int] = 3

    def __init__(self, data_folder: str = DATA_FOLDER):
        self.data_folder = os.path.abspath(data_folder)
        os.makedirs(self.data_folder, exist_ok=True)

        self._tls = threading.local()
        self._ensured_dirs = {self.data_folder}
        self._dir_lock = threading.Lock()

    def _compressor(self) -> zstd.ZstdCompressor:
        compressor = getattr(self._tls, "compressor", None)
        if compressor is None:
            compressor = zstd.ZstdCompressor(level=self._ZSTD_LEVEL)
            self._tls.compressor = compressor
        return compressor

    def _decompressor(self) -> zstd.ZstdDecompressor:
        decompressor = getattr(self._tls, "decompressor", None)
        if decompressor is None:
            decompressor = zstd.ZstdDecompressor()
            self._tls.decompressor = decompressor
        return decompressor

    def exists_local(self, chunk_hash: str) -> bool:
        """Comprueba si el bloque ya existe en el repositorio local."""
        return os.path.exists(self._chunk_path(chunk_hash))

    def put(self, chunk_hash: str, raw_data: bytes) -> bool:
        """Comprime y guarda un bloque si todavía no existe."""
        compressed_data = self._compressor().compress(raw_data)
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def put_compressed(self, chunk_hash: str, compressed_data: bytes) -> bool:
        """Guarda un bloque ya comprimido, validando antes su hash."""
        self._validate_compressed_block(chunk_hash, compressed_data)
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def get(self, chunk_hash: str) -> bytes:
        """Recupera, descomprime y verifica un bloque guardado por hash."""
        compressed_data = self.get_compressed(chunk_hash)
        return self._decompress_and_validate(chunk_hash, compressed_data)

    def get_compressed(self, chunk_hash: str) -> bytes:
        """Devuelve el bloque comprimido tal como está almacenado."""
        path = self._chunk_path(chunk_hash)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing block: {chunk_hash}")

        with open(path, "rb") as handle:
            return handle.read()

    def _write_once(self, path: str, data: bytes) -> bool:
        if os.path.exists(path):
            return False

        last_error: Exception | None = None

        for attempt in range(2):
            self._ensure_parent_dir(path)
            temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
            written = False

            try:
                with open(temp_path, "wb") as handle:
                    handle.write(data)

                try:
                    os.replace(temp_path, path)
                    written = True
                    return True

                except FileNotFoundError as exc:
                    last_error = exc
                    if attempt == 0:
                        self._forget_parent_dir(path)
                        continue
                    raise

                except OSError:
                    if os.path.exists(path):
                        return False
                    raise

            except FileNotFoundError as exc:
                last_error = exc
                if attempt == 0:
                    self._forget_parent_dir(path)
                    continue
                raise

            finally:
                if not written:
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

        if last_error is not None:
            raise last_error

        return False

    def _ensure_parent_dir(self, path: str) -> None:
        parent = os.path.dirname(path)
        if parent in self._ensured_dirs:
            return

        with self._dir_lock:
            if parent in self._ensured_dirs:
                return
            os.makedirs(parent, exist_ok=True)
            self._ensured_dirs.add(parent)

    def _forget_parent_dir(self, path: str) -> None:
        parent = os.path.dirname(path)
        with self._dir_lock:
            self._ensured_dirs.discard(parent)

    def _validate_compressed_block(self, chunk_hash: str, compressed_data: bytes) -> None:
        self._decompress_and_validate(chunk_hash, compressed_data)

    def _decompress_and_validate(self, chunk_hash: str, compressed_data: bytes) -> bytes:
        try:
            raw_data = self._decompressor().decompress(compressed_data)
        except zstd.ZstdError as exc:
            raise CASCorruptionError(f"Corrupt compressed block: {chunk_hash}") from exc

        calculated_hash = blake3.blake3(raw_data).hexdigest()
        if calculated_hash != chunk_hash:
            raise CASCorruptionError(
                f"Corrupt block: expected {chunk_hash}, got {calculated_hash}"
            )

        return raw_data

    def _chunk_dir(self, chunk_hash: str) -> str:
        return os.path.join(self.data_folder, chunk_hash[:2], chunk_hash[2:4])

    def _chunk_path(self, chunk_hash: str) -> str:
        return os.path.join(self._chunk_dir(chunk_hash), chunk_hash)
