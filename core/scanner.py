import os
import stat as statmod


class TreeWalker:
    """
    Recorre una carpeta y devuelve archivos y subcarpetas con rutas relativas.
    Usa una pila explícita para evitar depender de la recursión de Python.
    """

    __slots__ = ("root_path", "deterministic")

    def __init__(self, root_path, deterministic=False):
        root_path = os.path.abspath(root_path)
        if root_path != os.path.abspath(os.sep):
            root_path = root_path.rstrip(os.sep)

        self.root_path = root_path
        self.deterministic = deterministic

    def walk(self):
        """Genera tuplas (ruta_relativa, ruta_absoluta, stat, tipo)."""
        if not os.path.isdir(self.root_path):
            raise NotADirectoryError(f"Not a directory: {self.root_path}")

        root_stat = os.stat(self.root_path, follow_symlinks=False)
        yield ".", self.root_path, root_stat, "dir"

        stack = [(self.root_path, ".")]
        while stack:
            current_path, parent_rel = stack.pop()
            directories = []

            try:
                with os.scandir(current_path) as iterator:
                    entries = sorted(iterator, key=lambda entry: entry.name) if self.deterministic else iterator

                    for entry in entries:
                        rel_path = entry.name if parent_rel == "." else f"{parent_rel}/{entry.name}"

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

            except PermissionError:
                print(f"Permission denied: {current_path}")
                continue

            if self.deterministic:
                stack.extend(reversed(directories))
            else:
                stack.extend(directories)
