from __future__ import annotations

import threading


_MAX_INDEX_ITEMS = 200_000


class FastBoolCache:
    """
    Caché booleana simple para fast-paths de chunking.

    Propiedades:
      - lecturas sin lock explícito
      - escritura ligera
      - clear-on-full con muestreo periódico para evitar comprobar el tamaño
        en cada inserción
    """

    __slots__ = ("max_items", "_data", "_clear_lock", "_write_ops")

    def __init__(self, max_items: int = _MAX_INDEX_ITEMS):
        self.max_items = max(int(max_items), 1)
        self._data: dict[str, bool] = {}
        self._clear_lock = threading.Lock()
        self._write_ops = 0

    def get(self, key: str) -> bool | None:
        """Devuelve el valor cacheado o None si la clave no está presente."""
        return self._data.get(key)

    def set(self, key: str, value: bool) -> None:
        """
        Guarda un valor booleano en la caché.

        Si la caché supera el límite configurado, se vacía completa de forma
        best-effort. La pérdida de entradas es aceptable porque no es fuente de verdad.
        """
        self._data[key] = bool(value)
        self._write_ops += 1

        if (self._write_ops & 1023) == 0 and len(self._data) > self.max_items:
            with self._clear_lock:
                if len(self._data) > self.max_items:
                    self._data.clear()
                    self._write_ops = 0

    def clear(self) -> None:
        """Vacía la caché y reinicia el contador de escrituras."""
        with self._clear_lock:
            self._data.clear()
            self._write_ops = 0


class ChunkIndex:
    """
    Índices efímeros compartidos por los workers de backup.

    No son fuente de verdad. Solo evitan repetir consultas al CAS, a la metadata
    local o al estado de protección remota dentro del mismo backup.
    """

    __slots__ = ("local_exists", "remotely_protected")

    def __init__(self, max_items: int = _MAX_INDEX_ITEMS):
        self.local_exists = FastBoolCache(max_items=max_items)
        self.remotely_protected = FastBoolCache(max_items=max_items)
