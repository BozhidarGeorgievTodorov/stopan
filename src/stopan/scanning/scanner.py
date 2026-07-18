from __future__ import annotations

import os
import stat as statmod
from collections.abc import Iterator

from stopan.errors import StopanStorageOSError, StopanUsageError


class TreeWalker:
    """
    Recorrido iterativo del árbol de ficheros.

    Contrato:
      - emite tuplas (rel_path, full_path, stat, item_type)
      - incluye la raíz como (".", root_path, stat, "dir")
      - ignora symlinks y archivos especiales
      - detiene el recorrido si una entrada no puede inspeccionarse o recorrerse
      - opcionalmente usa un orden determinista y estable
    """

    __slots__ = ("root_path", "deterministic")

    def __init__(self, root_path: str, deterministic: bool = False):
        root_path = os.path.abspath(root_path)
        if root_path != os.path.abspath(os.sep):
            root_path = root_path.rstrip(os.sep)

        try:
            root_stat = os.stat(root_path, follow_symlinks=False)
        except FileNotFoundError:
            raise StopanUsageError(f"Directorio no encontrado: {root_path}") from None
        except OSError as exc:
            raise StopanStorageOSError(
                f"No se pudo inspeccionar el directorio origen: {root_path}: {exc}"
            ) from exc

        if not statmod.S_ISDIR(root_stat.st_mode):
            raise StopanUsageError(f"No es un directorio: {root_path}")

        self.root_path = root_path
        self.deterministic = bool(deterministic)

    def walk(self) -> Iterator[tuple[str, str, os.stat_result, str]]:
        """
        Genera tuplas (ruta_relativa, ruta_absoluta, stat, item_type).

        item_type puede ser:
          - "file"
          - "dir"
        """
        try:
            root_stat = os.stat(self.root_path, follow_symlinks=False)
        except OSError as exc:
            raise StopanStorageOSError(
                f"No se pudo inspeccionar el directorio origen: {self.root_path}: {exc}"
            ) from exc

        yield ".", self.root_path, root_stat, "dir"

        stack: list[tuple[str, str]] = [(self.root_path, ".")]
        while stack:
            current_path, parent_rel = stack.pop()
            directories: list[tuple[str, str]] = []

            try:
                with os.scandir(current_path) as iterator:
                    entries = (
                        sorted(iterator, key=lambda entry: entry.name)
                        if self.deterministic
                        else iterator
                    )

                    for entry in entries:
                        rel_path = (
                            entry.name
                            if parent_rel == "."
                            else f"{parent_rel}/{entry.name}"
                        )

                        try:
                            stat_info = entry.stat(follow_symlinks=False)
                        except OSError as exc:
                            raise StopanStorageOSError(
                                f"No se pudo inspeccionar la entrada del respaldo: "
                                f"{entry.path}: {exc}"
                            ) from exc

                        mode = stat_info.st_mode
                        if statmod.S_ISREG(mode):
                            yield rel_path, entry.path, stat_info, "file"
                        elif statmod.S_ISDIR(mode):
                            yield rel_path, entry.path, stat_info, "dir"
                            directories.append((entry.path, rel_path))

                        # symlinks y especiales se ignoran explícitamente

            except StopanStorageOSError:
                raise
            except OSError as exc:
                raise StopanStorageOSError(
                    f"No se pudo recorrer el directorio del respaldo: "
                    f"{current_path}: {exc}"
                ) from exc

            if self.deterministic:
                stack.extend(reversed(directories))
            else:
                stack.extend(directories)