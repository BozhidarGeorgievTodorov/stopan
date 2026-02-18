import os


class TreeWalker:
    """
    Recorre una carpeta y devuelve archivos y subcarpetas con rutas relativas.
    """

    def __init__(self, root_path):
        self.root_path = os.path.abspath(root_path)

    def walk(self):
        """Genera tuplas (ruta_relativa, ruta_absoluta, stat, tipo)."""
        if not os.path.isdir(self.root_path):
            raise NotADirectoryError(f"Not a directory: {self.root_path}")

        root_stat = os.stat(self.root_path)
        yield ".", self.root_path, root_stat, "dir"
        yield from self._walk_dir(self.root_path)

    def _walk_dir(self, current_path):
        try:
            with os.scandir(current_path) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except PermissionError:
            print(f"Permission denied: {current_path}")
            return

        for entry in entries:
            full_path = entry.path
            rel_path = os.path.relpath(full_path, self.root_path).replace(os.sep, "/")

            try:
                stat_info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue

            if entry.is_symlink():
                print(f"Skipping symlink: {rel_path}")
                continue

            if entry.is_file(follow_symlinks=False):
                yield rel_path, full_path, stat_info, "file"
            elif entry.is_dir(follow_symlinks=False):
                yield rel_path, full_path, stat_info, "dir"
                yield from self._walk_dir(full_path)
            else:
                print(f"Skipping special file: {rel_path}")
