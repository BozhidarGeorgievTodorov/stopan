from __future__ import annotations

import errno
import os
import sys
from pathlib import Path

from stopan.errors import StopanStorageOSError


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_private_dir(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise StopanStorageOSError(f"No se pudo crear el directorio {path}: {exc}") from exc

    try:
        os.chmod(path, 0o700)
    except OSError:
        pass


def atomic_rename_noreplace(
    source: str | Path,
    destination: str | Path,
) -> None:
    """
    Renombra ``source`` a ``destination`` sin sustituir un destino existente.

    La comprobación de ausencia y el renombrado forman una única operación del
    sistema de ficheros. Si el destino existe en ese instante se propaga
    ``FileExistsError`` y el origen permanece en su ubicación anterior.

    La operación es una capacidad dependiente de la plataforma. El backend
    actual utiliza ``renameat2(RENAME_NOREPLACE)`` en Linux. Las plataformas
    sin un backend que preserve esta semántica fallan de forma explícita en vez
    de degradar a una secuencia vulnerable a TOCTOU.
    """
    source_path = os.fspath(source)
    destination_path = os.fspath(destination)

    if sys.platform.startswith("linux"):
        _linux_rename_noreplace(source_path, destination_path)
        return

    raise StopanStorageOSError(
        "La plataforma actual no dispone de un backend para publicar rutas "
        "atómicamente sin sustituir un destino existente."
    )


def _linux_rename_noreplace(source: str, destination: str) -> None:
    import ctypes

    at_fdcwd = -100
    rename_noreplace = 1

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:
        raise StopanStorageOSError(
            "La libc de esta instalación no expone renameat2, necesario para "
            "publicar rutas sin sustitución."
        ) from exc

    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int

    ctypes.set_errno(0)
    result = renameat2(
        at_fdcwd,
        os.fsencode(source),
        at_fdcwd,
        os.fsencode(destination),
        rename_noreplace,
    )
    if result == 0:
        return

    error_number = ctypes.get_errno()
    if error_number == 0:
        raise StopanStorageOSError(
            f"renameat2 falló sin proporcionar errno al publicar {destination}"
        )

    if error_number == errno.EEXIST:
        raise FileExistsError(
            error_number,
            os.strerror(error_number),
            destination,
        )

    raise OSError(
        error_number,
        os.strerror(error_number),
        destination,
    )


def atomic_write_bytes(
    path: Path,
    data: bytes,
    *,
    mode: int = 0o600,
    sync_parent_dir: bool = True,
) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise StopanStorageOSError(f"No se pudo crear el directorio padre de {path}: {exc}") from exc

    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{os.urandom(8).hex()}.tmp")
    written = False
    try:
        try:
            fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        except OSError as exc:
            raise StopanStorageOSError(f"No se pudo crear el archivo temporal {temp_path}: {exc}") from exc
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            try:
                os.close(fd)
            except OSError:
                pass
            raise StopanStorageOSError(f"No se pudo escribir {temp_path}: {exc}") from exc
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            raise

        try:
            os.replace(temp_path, path)
        except OSError as exc:
            raise StopanStorageOSError(f"No se pudo reemplazar {path} con {temp_path}: {exc}") from exc

        try:
            os.chmod(path, mode)
        except OSError as exc:
            raise StopanStorageOSError(f"No se pudieron ajustar permisos de {path}: {exc}") from exc
        written = True
        if sync_parent_dir:
            fsync_dir(path.parent)
    finally:
        if not written:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass


def atomic_copy_file(
    source: str | Path,
    destination: str | Path,
    *,
    mode: int = 0o600,
    block_size: int = 1024 * 1024,
    sync_parent_dir: bool = True,
) -> None:
    source_path = Path(source).expanduser().resolve()
    destination_path = Path(destination).expanduser().resolve()
    size = int(block_size)
    if size <= 0:
        raise ValueError("block_size debe ser > 0")

    ensure_private_dir(destination_path.parent)
    temp_path = destination_path.with_name(
        f".{destination_path.name}.{os.getpid()}.{os.urandom(8).hex()}.tmp"
    )
    published = False
    try:
        try:
            source_fh = source_path.open("rb")
        except OSError as exc:
            raise StopanStorageOSError(
                f"No se pudo abrir el archivo de origen {source_path}: {exc}"
            ) from exc

        try:
            fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        except OSError as exc:
            source_fh.close()
            raise StopanStorageOSError(
                f"No se pudo crear el archivo temporal {temp_path}: {exc}"
            ) from exc

        try:
            with source_fh, os.fdopen(fd, "wb") as destination_fh:
                while True:
                    block = source_fh.read(size)
                    if not block:
                        break
                    destination_fh.write(block)
                destination_fh.flush()
                os.fsync(destination_fh.fileno())
        except OSError as exc:
            raise StopanStorageOSError(
                f"No se pudo copiar {source_path} a {temp_path}: {exc}"
            ) from exc

        try:
            os.chmod(temp_path, mode)
            os.replace(temp_path, destination_path)
        except OSError as exc:
            raise StopanStorageOSError(
                f"No se pudo reemplazar {destination_path} con {temp_path}: {exc}"
            ) from exc
        published = True
        if sync_parent_dir:
            fsync_dir(destination_path.parent)
    finally:
        if not published:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

