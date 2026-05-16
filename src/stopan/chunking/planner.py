"""
Planificación de fast-paths durante el backup.

ChunkPlanner decide si un chunk debe procesarse normalmente, saltarse porque ya
existe en el CAS local o saltarse porque existe evidencia suficiente de
protección remota para el placement actual.
"""

from __future__ import annotations

from typing import Literal

from stopan.errors import StopanConfigValueError


ChunkDecision = Literal["process", "skip_local", "skip_synced"]


class ChunkPlanner:
    """
    Decide si un chunk debe materializarse en el CAS local o puede saltarse.

    Política:
      - safe_mode o fast-path desactivado -> siempre "process"
      - si el chunk ya existe localmente -> "skip_local"
      - si está habilitado el salto por protección remota y existe evidencia
        suficiente en chunk_protection -> "skip_synced"
      - en cualquier otro caso -> "process"

    placement_epoch puede ser None para mantener el backup offline-first.
    """

    __slots__ = (
        "repo",
        "db",
        "index",
        "fast_path_enabled",
        "safe_mode",
        "allow_remote_protected_skip",
        "desired_rf",
        "placement_epoch",
    )

    def __init__(
        self,
        repo,
        db,
        *,
        index,
        fast_path_enabled: bool,
        safe_mode: bool,
        allow_remote_protected_skip: bool = False,
        desired_rf: int = 3,
        placement_epoch: str | None = None,
    ):
        if repo is None:
            raise ValueError("ChunkPlanner requiere un repositorio válido.")
        if db is None:
            raise ValueError("ChunkPlanner requiere una base de datos válida.")
        if index is None:
            raise ValueError("ChunkPlanner requiere una instancia válida de ChunkIndex.")

        self.repo = repo
        self.db = db
        self.index = index

        self.fast_path_enabled = bool(fast_path_enabled)
        self.safe_mode = bool(safe_mode)
        self.allow_remote_protected_skip = bool(allow_remote_protected_skip)
        self.desired_rf = int(desired_rf)
        if self.desired_rf < 0:
            raise StopanConfigValueError("desired_rf debe >= 0")
        self.placement_epoch = placement_epoch

    def decide(self, chunk_hash: str) -> ChunkDecision:
        if self.safe_mode or not self.fast_path_enabled:
            return "process"

        if self._exists_local(chunk_hash):
            return "skip_local"

        if self.allow_remote_protected_skip and self._is_remotely_protected(chunk_hash):
            return "skip_synced"

        return "process"

    def _exists_local(self, chunk_hash: str) -> bool:
        """Consulta la caché local y, si falta, comprueba el CAS."""
        cached = self.index.local_exists.get(chunk_hash)
        if cached is not None:
            return bool(cached)

        exists = self.repo.exists_local(chunk_hash)
        self.index.local_exists.set(chunk_hash, exists)
        return exists

    def _is_remotely_protected(self, chunk_hash: str) -> bool:
        """Comprueba si chunk_protection acredita suficientes copias remotas."""
        cached = self.index.remotely_protected.get(chunk_hash)
        if cached is not None:
            return bool(cached)

        protected = self.db.is_chunk_remotely_protected(
            chunk_hash,
            required_rf=self.desired_rf,
            current_epoch=self.placement_epoch,
        )
        self.index.remotely_protected.set(chunk_hash, protected)
        return protected
