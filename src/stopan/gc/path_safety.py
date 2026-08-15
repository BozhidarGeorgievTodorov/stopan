from __future__ import annotations

import os
import stat
from pathlib import Path


class GarbageCollectionPathError(RuntimeError):
    """Ruta no apta para una operación destructiva de GC."""


def is_filesystem_redirection(stat_result: os.stat_result) -> bool:
    """Detecta symlinks y junctions/reparse points que redirigen una ruta."""

    if stat.S_ISLNK(stat_result.st_mode):
        return True

    reparse_tag = getattr(stat_result, "st_reparse_tag", 0)
    redirect_tags = {
        tag
        for tag in (
            getattr(stat, "IO_REPARSE_TAG_SYMLINK", 0),
            getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0),
        )
        if tag
    }
    return bool(reparse_tag and reparse_tag in redirect_tags)


def validate_gc_regular_file(
    *,
    root_dir: str | Path,
    path: str | Path,
) -> os.stat_result:
    """Valida que ``path`` sea un fichero regular confinado bajo ``root_dir``.

    La comprobación no sigue redirecciones existentes en los componentes de la
    ruta y contrasta además la resolución canónica. Se devuelve el ``lstat`` del
    fichero para que el llamador pueda comprobar que el candidato no ha sido
    sustituido antes del borrado.
    """

    root = Path(root_dir).expanduser().resolve()
    candidate = Path(os.path.abspath(os.fspath(path)))

    if not _is_within(root, candidate):
        raise GarbageCollectionPathError(
            f"candidato fuera de la raíz de GC: {candidate}"
        )

    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise GarbageCollectionPathError(
            f"candidato fuera de la raíz de GC: {candidate}"
        ) from exc

    current = root
    parts = relative.parts
    for index, component in enumerate(parts):
        current = current / component
        try:
            stat_result = current.lstat()
        except FileNotFoundError as exc:
            raise GarbageCollectionPathError(
                f"candidato desapareció durante el GC: {candidate}"
            ) from exc
        except OSError as exc:
            raise GarbageCollectionPathError(
                f"no se pudo comprobar el candidato de GC {candidate}: {exc}"
            ) from exc

        if is_filesystem_redirection(stat_result):
            raise GarbageCollectionPathError(
                f"candidato de GC atraviesa una redirección del sistema de ficheros: {candidate}"
            )

        if index < len(parts) - 1 and not stat.S_ISDIR(stat_result.st_mode):
            raise GarbageCollectionPathError(
                f"componente intermedio no es directorio en candidato de GC: {candidate}"
            )

    try:
        root_real = Path(os.path.realpath(root))
        candidate_real = Path(os.path.realpath(candidate))
    except OSError as exc:
        raise GarbageCollectionPathError(
            f"no se pudo resolver canónicamente el candidato de GC {candidate}: {exc}"
        ) from exc

    if not _is_within(root_real, candidate_real):
        raise GarbageCollectionPathError(
            f"resolución canónica fuera de la raíz de GC: {candidate}"
        )

    try:
        final_stat = candidate.lstat()
    except OSError as exc:
        raise GarbageCollectionPathError(
            f"no se pudo comprobar el candidato de GC {candidate}: {exc}"
        ) from exc
    if is_filesystem_redirection(final_stat) or not stat.S_ISREG(final_stat.st_mode):
        raise GarbageCollectionPathError(
            f"candidato de GC no es un fichero regular: {candidate}"
        )
    return final_stat


def same_file_identity(first: os.stat_result, second: os.stat_result) -> bool:
    """Comprueba que dos observaciones siguen refiriéndose al mismo objeto."""

    first_inode = int(getattr(first, "st_ino", 0) or 0)
    second_inode = int(getattr(second, "st_ino", 0) or 0)
    if first_inode and second_inode:
        return (first.st_dev, first_inode) == (second.st_dev, second_inode)

    return (
        first.st_mode,
        first.st_size,
        getattr(first, "st_mtime_ns", int(first.st_mtime * 1_000_000_000)),
        getattr(first, "st_ctime_ns", int(first.st_ctime * 1_000_000_000)),
    ) == (
        second.st_mode,
        second.st_size,
        getattr(second, "st_mtime_ns", int(second.st_mtime * 1_000_000_000)),
        getattr(second, "st_ctime_ns", int(second.st_ctime * 1_000_000_000)),
    )


def _is_within(root: Path, candidate: Path) -> bool:
    try:
        return os.path.commonpath([os.fspath(root), os.fspath(candidate)]) == os.fspath(root)
    except ValueError:
        return False
