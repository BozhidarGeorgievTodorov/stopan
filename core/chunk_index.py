import threading


class FastDictCache:
    """Caché sencilla para consultas repetidas durante una ejecución."""

    __slots__ = ("max_items", "_data", "_lock", "_ops")

    def __init__(self, max_items=200_000):
        self.max_items = max_items
        self._data = {}
        self._lock = threading.Lock()
        self._ops = 0

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value
        self._ops += 1

        if (self._ops & 1023) == 0 and len(self._data) > self.max_items:
            with self._lock:
                if len(self._data) > self.max_items:
                    self._data.clear()


class ChunkIndex:
    """Índice en memoria para decisiones de fast-path durante un backup."""

    __slots__ = ("local_exists", "synced")

    def __init__(self, max_items=200_000):
        self.local_exists = FastDictCache(max_items=max_items)
        self.synced = FastDictCache(max_items=max_items)
