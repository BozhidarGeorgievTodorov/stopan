from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeIdentity:
    node_id: str
    incarnation: int


class NodeIdentityStore:
    """
    Guarda la identidad estable de un nodo P2P.

    El archivo node_id.txt contiene dos datos:
    - node_id: identifica de forma estable este nodo mientras exista su carpeta de estado;
    - incarnation: número de arranque usado por el protocolo de membership.

    Cada vez que el nodo arranca, incrementa incarnation. Así otros nodos pueden distinguir
    un proceso nuevo de una ejecución anterior que quizá habían marcado como sospechosa o caída.

    Formato de node_id.txt:
        <node_id>
        <incarnation>
    """

    _FILENAME = "node_id.txt"

    def __init__(self, root_path: str):
        self.root_path = os.path.abspath(root_path)
        self.file_path = os.path.join(self.root_path, self._FILENAME)
        self._lock = threading.Lock()

    def load_for_startup(self) -> NodeIdentity:
        with self._lock:
            if os.path.exists(self.file_path):
                node_id, incarnation = self._read_unlocked()
            else:
                node_id, incarnation = uuid.uuid4().hex, 0

            next_incarnation = incarnation + 1
            self._write_unlocked(node_id=node_id, incarnation=next_incarnation)

            return NodeIdentity(node_id=node_id, incarnation=next_incarnation)

    def bump_above(self, *, node_id: str, observed_incarnation: int) -> int:
        node_id = str(node_id).strip()
        if not node_id:
            raise ValueError("NodeIdentityStore.bump_above requires a non-empty node_id.")

        observed_incarnation = int(observed_incarnation)
        if observed_incarnation < 0:
            raise ValueError("observed_incarnation must be >= 0.")

        with self._lock:
            stored_node_id, stored_incarnation = self._read_unlocked()
            if stored_node_id != node_id:
                raise RuntimeError(
                    "Persisted node_id is inconsistent: "
                    f"store={stored_node_id} runtime={node_id}"
                )

            next_incarnation = max(stored_incarnation, observed_incarnation) + 1
            self._write_unlocked(node_id=node_id, incarnation=next_incarnation)
            return next_incarnation

    def _read_unlocked(self) -> tuple[str, int]:
        with open(self.file_path, "r", encoding="utf-8") as handle:
            lines = [line.strip() for line in handle.readlines()]

        if len(lines) != 2:
            raise RuntimeError(
                f"Invalid node identity file format: {self.file_path}. "
                "Expected exactly two lines: node_id and incarnation."
            )

        node_id = lines[0]
        if not node_id:
            raise RuntimeError(f"Invalid node identity file: empty node_id in {self.file_path}")

        try:
            incarnation = int(lines[1])
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid node identity file: incarnation must be an integer in {self.file_path}"
            ) from exc

        if incarnation < 1:
            raise RuntimeError(
                f"Invalid node identity file: incarnation must be >= 1 in {self.file_path}"
            )

        return node_id, incarnation

    def _write_unlocked(self, *, node_id: str, incarnation: int) -> None:
        node_id = str(node_id).strip()
        if not node_id:
            raise ValueError("NodeIdentityStore requires a non-empty node_id.")

        incarnation = int(incarnation)
        if incarnation < 1:
            raise ValueError("NodeIdentityStore requires incarnation >= 1.")

        os.makedirs(self.root_path, exist_ok=True)
        temp_path = f"{self.file_path}.{uuid.uuid4().hex}.tmp"

        with open(temp_path, "w", encoding="utf-8") as handle:
            handle.write(f"{node_id}\n{incarnation}\n")

        os.replace(temp_path, self.file_path)
