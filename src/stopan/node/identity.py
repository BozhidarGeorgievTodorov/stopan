"""
Identidad persistente de nodo P2P.

Cada nodo mantiene un node_id estable y una incarnation que aumenta en cada
arranque. El formato persistido es estricto: dos líneas, node_id e incarnation.
"""

from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

from stopan.common.fs import atomic_write_bytes
from stopan.node.errors import NodeIdentityError, NodeIdentityValueError


_NODE_ID_HEX_ALPHABET = set("0123456789abcdef")


def _require_node_id(value: str, *, source: str) -> str:
    node_id = str(value).strip()
    if len(node_id) != 32 or any(char not in _NODE_ID_HEX_ALPHABET for char in node_id):
        raise NodeIdentityError(
            f"Archivo de identidad inválido: node_id debe tener 32 caracteres "
            f"hexadecimales lowercase en {source}"
        )
    return node_id


def _require_incarnation(value: int, *, source: str) -> int:
    try:
        incarnation = int(value)
    except (TypeError, ValueError) as exc:
        raise NodeIdentityError(
            f"Archivo de identidad inválido: incarnation debe ser un entero en {source}"
        ) from exc

    if incarnation < 1:
        raise NodeIdentityError(
            f"Archivo de identidad inválido: incarnation debe ser >= 1 en {source}"
        )
    return incarnation


@dataclass(frozen=True)
class NodeIdentity:
    node_id: str
    incarnation: int


class NodeIdentityStore:
    """
    Guarda la identidad estable de un nodo P2P.

    El archivo configurado contiene dos datos:
    - node_id: identifica de forma estable este nodo mientras exista su estado persistente;
    - incarnation: número de arranque usado por el protocolo de membership.

    Cada vez que el nodo arranca, incrementa incarnation. Así otros nodos pueden distinguir
    un proceso nuevo de una ejecución anterior que quizá habían marcado como sospechosa o caída.

    Formato del archivo:
        <node_id>
        <incarnation>
    """

    def __init__(self, file_path: str | Path):
        self.file_path = str(Path(file_path).expanduser().resolve())
        self._lock = threading.Lock()

    def load_for_startup(self) -> NodeIdentity:
        """Carga o crea la identidad del nodo e incrementa incarnation para este arranque."""

        with self._lock:
            if os.path.exists(self.file_path):
                node_id, incarnation = self._read_unlocked()
            else:
                node_id, incarnation = uuid.uuid4().hex, 0

            next_incarnation = incarnation + 1
            self._write_unlocked(node_id=node_id, incarnation=next_incarnation)

            return NodeIdentity(node_id=node_id, incarnation=next_incarnation)

    def bump_above(self, *, node_id: str, observed_incarnation: int) -> int:
        """Eleva incarnation por encima de una incarnation observada en membership."""

        node_id = str(node_id).strip()
        if not node_id:
            raise NodeIdentityValueError("NodeIdentityStore.bump_above requiere node_id no vacío.")

        observed_incarnation = int(observed_incarnation)
        if observed_incarnation < 0:
            raise NodeIdentityValueError("observed_incarnation debe ser >= 0.")

        with self._lock:
            stored_node_id, stored_incarnation = self._read_unlocked()
            if stored_node_id != node_id:
                raise NodeIdentityError(
                    "node_id persistido inconsistente: "
                    f"store={stored_node_id} runtime={node_id}"
                )

            next_incarnation = max(stored_incarnation, observed_incarnation) + 1
            self._write_unlocked(node_id=node_id, incarnation=next_incarnation)
            return next_incarnation

    def _read_unlocked(self) -> tuple[str, int]:
        try:
            with open(self.file_path, "r", encoding="utf-8") as handle:
                lines = [line.strip() for line in handle.readlines()]
        except OSError as exc:
            raise NodeIdentityError(f"No se pudo leer el archivo de identidad: {self.file_path}: {exc}") from exc

        if len(lines) != 2:
            raise NodeIdentityError(
                f"Formato inválido de archivo de identidad: {self.file_path}. "
                "Se esperaban exactamente dos líneas: node_id e incarnation."
            )

        node_id = _require_node_id(lines[0], source=self.file_path)
        incarnation = _require_incarnation(lines[1], source=self.file_path)
        return node_id, incarnation

    def _write_unlocked(self, *, node_id: str, incarnation: int) -> None:
        try:
            node_id = _require_node_id(node_id, source=self.file_path)
            incarnation = _require_incarnation(incarnation, source=self.file_path)
        except NodeIdentityError as exc:
            raise NodeIdentityValueError(str(exc)) from exc

        atomic_write_bytes(
            Path(self.file_path),
            f"{node_id}\n{incarnation}\n".encode("utf-8"),
            mode=0o600,
        )
