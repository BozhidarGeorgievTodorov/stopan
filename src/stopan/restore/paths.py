"""
Rutas seguras para restore.

Este módulo evita que rutas guardadas en metadata puedan escapar del directorio
destino durante la restauración y centraliza las rutas de staging y reanudación
usadas para publicar el resultado de forma segura.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass

import blake3

from stopan.restore.errors import RestorePathError


def _is_filesystem_redirection(stat_result: os.stat_result) -> bool:
    """Detecta enlaces simbólicos y junctions que pueden redirigir una ruta."""

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


def _lstat_existing(path: str) -> os.stat_result | None:
    """Devuelve lstat(path) si existe sin seguir redirecciones del último componente."""

    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def validate_restore_root(base_dir: str) -> str:
    """
    Valida que base_dir sea un directorio real apto como árbol de trabajo.

    El propio directorio ``.incomplete`` no puede ser un enlace simbólico ni
    otro reparse point. La función devuelve su ruta absoluta para usarla como
    ancla de las comprobaciones posteriores.
    """

    base_dir_abs = os.path.abspath(base_dir)
    stat_result = _lstat_existing(base_dir_abs)
    if stat_result is None:
        raise RestorePathError(f"El árbol de restauración no existe: {base_dir}")
    if _is_filesystem_redirection(stat_result):
        raise RestorePathError(
            f"El árbol de restauración no puede ser una redirección del sistema de ficheros: {base_dir}"
        )
    if not stat.S_ISDIR(stat_result.st_mode):
        raise RestorePathError(f"El árbol de restauración no es un directorio: {base_dir}")
    return base_dir_abs


def _reject_existing_redirections(base_dir_abs: str, full_path: str, rel_path: str) -> None:
    """Rechaza redirecciones existentes entre base_dir_abs y full_path, ambos incluidos."""

    relative = os.path.relpath(full_path, base_dir_abs)
    if relative in ("", "."):
        return

    current = base_dir_abs
    for component in relative.split(os.sep):
        if component in ("", "."):
            continue
        current = os.path.join(current, component)
        try:
            stat_result = _lstat_existing(current)
        except OSError as exc:
            raise RestorePathError(
                f"No se pudo comprobar la ruta de restauración: {rel_path}"
            ) from exc
        if stat_result is None:
            # Si un componente no existe, ningún descendiente puede existir a
            # través de esa ruta sin que antes se cree el componente ausente.
            break
        if _is_filesystem_redirection(stat_result):
            raise RestorePathError(
                f"Ruta de restauración atraviesa una redirección del sistema de ficheros: {rel_path}"
            )


def _is_within(base_dir: str, candidate: str) -> bool:
    """Comprueba pertenencia de candidate a base_dir manejando unidades distintas."""

    try:
        return os.path.commonpath([base_dir, candidate]) == base_dir
    except ValueError:
        return False


def safe_restore_path(base_dir: str, rel_path: str) -> str:
    """
    Resuelve rel_path dentro de base_dir y rechaza escapes o redirecciones.

    Acepta ``.`` como raíz del restore. Rechaza rutas absolutas, resultados que
    salgan léxica o canónicamente de ``base_dir`` y componentes existentes que
    sean enlaces simbólicos o reparse points.
    """

    base_dir_abs = validate_restore_root(base_dir)
    normalized_rel = os.path.normpath(rel_path)

    if os.path.isabs(normalized_rel):
        raise RestorePathError(f"Ruta fuera del árbol destino: {rel_path}")

    if normalized_rel in ("", "."):
        full_path = base_dir_abs
    else:
        full_path = os.path.abspath(os.path.join(base_dir_abs, normalized_rel))

    if not _is_within(base_dir_abs, full_path):
        raise RestorePathError(f"Ruta fuera del árbol destino: {rel_path}")

    _reject_existing_redirections(base_dir_abs, full_path, rel_path)

    # La comprobación canónica cubre además formas de redirección que la
    # plataforma pueda resolver aunque no se representen como symlink POSIX.
    try:
        base_dir_real = os.path.realpath(base_dir_abs)
        full_path_real = os.path.realpath(full_path)
    except OSError as exc:
        raise RestorePathError(
            f"No se pudo resolver canónicamente la ruta de restauración: {rel_path}"
        ) from exc
    if not _is_within(base_dir_real, full_path_real):
        raise RestorePathError(f"Ruta fuera del árbol destino: {rel_path}")

    return full_path


@dataclass(frozen=True, slots=True)
class RestorePaths:
    """
    Rutas usadas para restaurar un snapshot de forma segura y reanudable.

    ``incomplete_dir`` contiene únicamente elementos completos del árbol de
    trabajo. ``resume_dir`` conserva parciales verificables por fragmentos y
    ``final_dir`` solo aparece al publicar la restauración completa.
    """

    final_dir: str
    incomplete_dir: str
    resume_dir: str

    @staticmethod
    def for_snapshot(base_output_dir: str, snapshot_uuid: str) -> "RestorePaths":
        """Construye las rutas final, de staging y de reanudación de un snapshot."""

        final_dir = os.path.join(base_output_dir, f"snapshot_{snapshot_uuid}")
        incomplete_dir = final_dir + ".incomplete"
        resume_dir = incomplete_dir + ".resume"
        return RestorePaths(
            final_dir=final_dir,
            incomplete_dir=incomplete_dir,
            resume_dir=resume_dir,
        )

    def resume_file(self, rel_path: str) -> str:
        """Deriva una ruta estable y opaca para el parcial de un elemento."""

        digest = blake3.blake3(os.fsencode(rel_path)).hexdigest()
        return os.path.join(self.resume_dir, f"{digest}.part")
