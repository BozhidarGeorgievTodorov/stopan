"""
Repositorio CAS local para chunks comprimidos.

El CAS almacena blobs comprimidos indexados por el hash BLAKE3 del contenido
sin comprimir. Las rutas internas se derivan directamente del hash, por lo que
este módulo asume que los hashes ya han sido validados antes de llegar aquí.
"""

from __future__ import annotations

import os
import threading
import uuid
from typing import Final

import blake3
import zstandard as zstd

from stopan.config.defaults import DEFAULT_NODE_LOCAL_SHARD_DIR
from stopan.errors import StopanDataError, StopanStorageError


class CASRepositoryError(StopanStorageError):
    """Error base del repositorio CAS."""


class CASCorruptionError(StopanDataError, CASRepositoryError):
    """El chunk almacenado está corrupto o no coincide con su hash de contenido."""


class CASMissingChunkError(CASRepositoryError, FileNotFoundError):
    """Chunk ausente en el CAS local, compatible con FileNotFoundError."""


class CASRepository:
    """
    Gestiona la persistencia y lectura de chunks en disco direccionados por contenido.

    Contrato:
      - chunk_hash siempre representa el BLAKE3 del contenido sin comprimir;
      - las escrituras usan archivo temporal y rename atómico;
      - get() descomprime y verifica el payload contra su hash de contenido;
      - get_compressed() devuelve el blob comprimido sin validarlo;
      - el repositorio asume que chunk_hash ya llega validado como hash canónico de chunk.
    """

    __slots__ = ("data_folder", "_tls", "_ensured_dirs", "_dir_lock")

    _ZSTD_LEVEL: Final[int] = 3

    def __init__(self, data_folder: str = DEFAULT_NODE_LOCAL_SHARD_DIR):
        self.data_folder = os.path.abspath(data_folder)
        try:
            os.makedirs(self.data_folder, exist_ok=True)
        except OSError as exc:
            raise CASRepositoryError(f"No se pudo preparar el directorio CAS {self.data_folder}: {exc}") from exc

        # Los contextos Zstd subyacentes en C no son thread-safe. Se usa un
        # compresor/descompresor por hilo para permitir concurrencia segura.
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
        """Devuelve True si el chunk ya existe persistido en el repositorio local."""
        return os.path.exists(self._chunk_path(chunk_hash))

    def put(self, chunk_hash: str, raw_data: bytes) -> bool:
        """Comprime y guarda un chunk si todavía no existe."""
        compressed_data = self._compressor().compress(raw_data)
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def put_compressed(self, chunk_hash: str, compressed_data: bytes) -> bool:
        """Guarda un chunk comprimido que ya ha sido validado por el caller."""
        return self._write_once(self._chunk_path(chunk_hash), compressed_data)

    def get(self, chunk_hash: str) -> bytes:
        """Recupera, descomprime y verifica un chunk guardado por hash."""
        compressed_data = self.get_compressed(chunk_hash)
        return self._decompress_and_validate(chunk_hash, compressed_data)

    def get_compressed(self, chunk_hash: str) -> bytes:
        """Devuelve el blob comprimido tal como está almacenado."""
        path = self._chunk_path(chunk_hash)
        if not os.path.exists(path):
            raise CASMissingChunkError(f"Chunk no encontrado en CAS: {chunk_hash}")

        try:
            with open(path, "rb") as handle:
                return handle.read()
        except FileNotFoundError as exc:
            raise CASMissingChunkError(f"Chunk no encontrado en CAS: {chunk_hash}") from exc
        except OSError as exc:
            raise CASRepositoryError(f"No se pudo leer el chunk {chunk_hash} en CAS: {exc}") from exc

    def _write_once(self, path: str, data: bytes) -> bool:
        """
        Escribe data de forma atómica si path todavía no existe.

        Devuelve True si este proceso creó el archivo y False si otro proceso o
        hilo ya lo había creado. El archivo temporal evita que una excepción durante
        la escritura deje un chunk parcial visible en la ruta definitiva.
        """
        if os.path.exists(path):
            return False

        last_error: Exception | None = None

        for attempt in range(2):
            self._ensure_parent_dir(path)
            temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
            written = False

            try:
                try:
                    with open(temp_path, "wb") as handle:
                        handle.write(data)
                except OSError as exc:
                    raise CASRepositoryError(f"No se pudo escribir archivo temporal CAS {temp_path}: {exc}") from exc

                try:
                    os.replace(temp_path, path)
                    written = True
                    return True

                except FileNotFoundError as exc:
                    last_error = exc
                    if attempt == 0:
                        self._forget_parent_dir(path)
                        continue
                    raise CASRepositoryError(f"No se pudo publicar chunk CAS {path}: {exc}") from exc

                except OSError as exc:
                    if os.path.exists(path):
                        return False
                    raise CASRepositoryError(f"No se pudo publicar chunk CAS {path}: {exc}") from exc

            except FileNotFoundError as exc:
                last_error = exc
                if attempt == 0:
                    self._forget_parent_dir(path)
                    continue
                raise CASRepositoryError(f"No se pudo escribir chunk CAS {path}: {exc}") from exc

            finally:
                if not written:
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

        if last_error is not None:
            raise CASRepositoryError(f"No se pudo escribir chunk CAS {path}: {last_error}") from last_error

        return False

    def _ensure_parent_dir(self, path: str) -> None:
        """
        Crea el directorio padre bajo demanda y cachea los directorios ya creados.

        La caché evita llamadas repetidas a os.makedirs en rutas calientes del CAS.
        """
        parent = os.path.dirname(path)
        if parent in self._ensured_dirs:
            return

        with self._dir_lock:
            if parent in self._ensured_dirs:
                return
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as exc:
                raise CASRepositoryError(f"No se pudo crear directorio CAS {parent}: {exc}") from exc
            self._ensured_dirs.add(parent)

    def _forget_parent_dir(self, path: str) -> None:
        """
        Elimina el directorio padre de la caché tras detectar que ya no existe.

        Esto permite reintentar la escritura si el directorio fue borrado entre la
        comprobación cacheada y la escritura real.
        """
        parent = os.path.dirname(path)
        with self._dir_lock:
            self._ensured_dirs.discard(parent)

    def _decompress_and_validate(self, chunk_hash: str, compressed_data: bytes) -> bytes:
        """
        Descomprime un chunk y valida que su BLAKE3 coincide con chunk_hash.

        Esta comprobación detecta corrupción local en disco o blobs comprimidos
        asociados al hash equivocado.
        """
        try:
            raw_data = self._decompressor().decompress(compressed_data)
        except zstd.ZstdError as exc:
            raise CASCorruptionError(f"Chunk comprimido corrupto en CAS: {chunk_hash}") from exc

        calculated_hash = blake3.blake3(raw_data).hexdigest()
        if calculated_hash != chunk_hash:
            raise CASCorruptionError(
                f"Chunk corrupto en CAS: se esperaba {chunk_hash}, "
                f"pero se obtuvo {calculated_hash}"
            )

        return raw_data

    def _chunk_dir(self, chunk_hash: str) -> str:
        # Sharding de directorios para evitar demasiados archivos en una sola carpeta.
        return os.path.join(self.data_folder, chunk_hash[:2], chunk_hash[2:4])

    def _chunk_path(self, chunk_hash: str) -> str:
        return os.path.join(self._chunk_dir(chunk_hash), chunk_hash)