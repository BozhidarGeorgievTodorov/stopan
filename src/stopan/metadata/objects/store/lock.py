"""
Lock de filesystem para el metadata object store.

El lock evita escrituras concurrentes sobre el mismo store local desde procesos
distintos.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

from stopan.common.fs import ensure_private_dir
from stopan.metadata.objects.store.errors import MetadataObjectStoreError


class MetadataObjectStoreLock:

    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir).expanduser().resolve()
        self.lock_path = self.root_dir / ".lock"
        self._fd: int | None = None

    def __enter__(self) -> "MetadataObjectStoreLock":
        ensure_private_dir(self.root_dir)
        try:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError as exc:
            raise MetadataObjectStoreError(f"No se pudo abrir el lock del metadata object store {self.lock_path}: {exc}") from exc

        try:
            os.fchmod(fd, 0o600)
        except OSError:
            pass
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception as exc:
            os.close(fd)
            raise MetadataObjectStoreError(f"No se pudo bloquear metadata object store {self.lock_path}: {exc}") from exc
        self._fd = fd
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def object_store_lock(root_dir: str | Path) -> MetadataObjectStoreLock:
    return MetadataObjectStoreLock(root_dir)
