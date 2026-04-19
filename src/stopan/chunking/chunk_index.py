from __future__ import annotations

import threading

MAX_ITEMS = 200_000

class FastBoolCache:
    """
    Caché booleana en memoria para una ejecución.

    Estrategia:
      - lecturas lock-free sobre dict
      - escritura ligera
      - clear-on-full con muestreo periódico para evitar comprobar el tamaño
        en cada inserción
    """

    __slots__ = ("max_items", "_data", "_clear_lock", "_write_ops")

    def __init__(self, max_items: int = MAX_ITEMS):
        self.max_items = max(int(max_items), 1)
        self._data: dict[str, bool] = {}
        self._clear_lock = threading.Lock()
        self._write_ops = 0

    def get(self, key: str) -> bool | None:
        return self._data.get(key)

    def set(self, key: str, value: bool) -> None:
        self._data[key] = bool(value)
        self._write_ops += 1

        if (self._write_ops & 1023) == 0 and len(self._data) > self.max_items:
            with self._clear_lock:
                if len(self._data) > self.max_items:
                    self._data.clear()


class ChunkIndex:
    """
    Índice efímero por ejecución.

    local_exists:
      - cachea existencia local en el CAS

    remotely_protected:
      - cachea si el chunk tiene evidencia suficiente de protección remota
        para el contexto actual del planner
    """

    __slots__ = ("local_exists", "remotely_protected")

    def __init__(self, max_items: int = MAX_ITEMS):
        self.local_exists = FastBoolCache(max_items=max_items)
        self.remotely_protected = FastBoolCache(max_items=max_items)
