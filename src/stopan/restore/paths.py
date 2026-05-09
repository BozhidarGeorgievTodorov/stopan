"""
Rutas seguras para restore.

Este módulo evita que rutas guardadas en metadata puedan escapar del directorio
destino durante la restauración y centraliza la convención de directorios
.incomplete usados para escrituras seguras.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def safe_restore_path(base_dir: str, rel_path: str) -> str:
    """
    Resuelve rel_path dentro de base_dir y rechaza path traversal.

    Acepta "." como raíz del restore, pero rechaza rutas absolutas, rutas con
    prefijo ".." y cualquier path cuyo resultado final quede fuera de base_dir.
    """

    normalized_rel = os.path.normpath(rel_path)
    if normalized_rel in ("", "."):
        return os.path.abspath(base_dir)
    if os.path.isabs(normalized_rel) or normalized_rel.startswith(".."):
        raise ValueError(f"Ruta fuera del árbol destino: {rel_path}")

    full_path = os.path.abspath(os.path.join(base_dir, normalized_rel))
    base_dir_abs = os.path.abspath(base_dir)

    if os.path.commonpath([base_dir_abs, full_path]) != base_dir_abs:
        raise ValueError(f"Ruta fuera del árbol destino: {rel_path}")

    return full_path


@dataclass(frozen=True, slots=True)
class RestorePaths:
    """
    Par de rutas usado para restaurar un snapshot de forma segura.

    incomplete_dir recibe la escritura inicial. Al completar el restore, se
    puede promover a final_dir evitando exponer árboles restaurados a medias.
    """

    final_dir: str
    incomplete_dir: str

    @staticmethod
    def for_snapshot(base_output_dir: str, snapshot_uuid: str) -> "RestorePaths":
        """Construye las rutas final e incompleta para un snapshot concreto."""
        
        final_dir = os.path.join(base_output_dir, f"snapshot_{snapshot_uuid}")
        incomplete_dir = final_dir + ".incomplete"
        return RestorePaths(final_dir=final_dir, incomplete_dir=incomplete_dir)
