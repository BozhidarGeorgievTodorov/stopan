from __future__ import annotations

import os
import stat as statmod


class TreeWalker:
    """
    Recorrido iterativo del árbol de ficheros.

    Semántica:
      - emite tuplas (rel_path, full_path, stat, item_type)
      - incluye la raíz como (".", root_path, stat, "dir")
      - ignora symlinks y archivos especiales
      - ignora directorios inaccesibles por permisos
      - opcionalmente usa un orden determinista y estable
    """

    __slots__ = ("root_path", "deterministic")

    def __init__(self, root_path: str, deterministic: bool = False):
        root_path = os.path.abspath(root_path)
        if root_path != os.path.abspath(os.sep):
            root_path = root_path.rstrip(os.sep)

        if not os.path.exists(root_path):
            raise FileNotFoundError(root_path)
        if not os.path.isdir(root_path):
            raise NotADirectoryError(root_path)

        self.root_path = root_path
        self.deterministic = bool(deterministic)

    def walk(self):
        """
        Genera tuplas (ruta_relativa, ruta_absoluta, stat, item_type).

        item_type puede ser:
          - "file"
          - "dir"
        """
        root_stat = os.stat(self.root_path, follow_symlinks=False)
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
                        except FileNotFoundError:
                            continue

                        mode = stat_info.st_mode
                        if statmod.S_ISREG(mode):
                            yield rel_path, entry.path, stat_info, "file"
                        elif statmod.S_ISDIR(mode):
                            yield rel_path, entry.path, stat_info, "dir"
                            directories.append((entry.path, rel_path))

                        # symlinks y especiales se ignoran explícitamente

            except PermissionError:
                continue

            if self.deterministic:
                stack.extend(reversed(directories))
            else:
                stack.extend(directories)