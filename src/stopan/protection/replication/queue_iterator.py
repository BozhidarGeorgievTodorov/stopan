from __future__ import annotations

"""
Iterador productor/consumidor para streams gRPC de cliente.

QueueIterator adapta una cola acotada al protocolo de iteración que espera gRPC
cuando el cliente envía un stream de requests. Distingue cierre normal y
cancelación para no contar mensajes que no llegaron a producir ACK.
"""

import queue
import threading


_QUEUE_POLL_TIMEOUT_S = 0.05


class QueueIterator:
    """
    Iterador productor/consumidor para client-streaming gRPC.

    Los dos modos de cierre tienen garantías distintas:
      - finish(): cierre normal; conserva todos los mensajes aceptados en cola y
        después añade el sentinel.
      - cancel(): cierre por error o cancelación; no bloquea indefinidamente y
        puede descartar mensajes pendientes porque no deben contar sin ACK.
    """

    def __init__(self, maxsize: int):
        self._queue = queue.Queue(maxsize=max(1, int(maxsize)))
        self._sentinel = object()
        self._finished = threading.Event()
        self._cancelled = threading.Event()
        self._sentinel_enqueued = threading.Event()
        self._finalize_lock = threading.Lock()

    def put(self, item) -> bool:
        """
        Encola item si el iterador sigue abierto.

        Devuelve False si ya se pidió finish() o cancel().
        """
        while not self._cancelled.is_set() and not self._finished.is_set():
            try:
                self._queue.put(item, timeout=_QUEUE_POLL_TIMEOUT_S)
                return True
            except queue.Full:
                continue
        return False

    def finish(self) -> None:
        """Cierra de manera normal sin descartar mensajes ya aceptados."""
        with self._finalize_lock:
            if self._sentinel_enqueued.is_set():
                return

            self._finished.set()

            while not self._cancelled.is_set():
                try:
                    self._queue.put(self._sentinel, timeout=_QUEUE_POLL_TIMEOUT_S)
                    self._sentinel_enqueued.set()
                    return
                except queue.Full:
                    continue

            self._force_sentinel_after_cancel()

    def cancel(self) -> None:
        """Cierra por error o cancelación de forma idempotente y acotada."""
        self._cancelled.set()
        with self._finalize_lock:
            if self._sentinel_enqueued.is_set():
                return
            self._force_sentinel_after_cancel()

    def _force_sentinel_after_cancel(self) -> None:
        # Al cancelar es válido descartar mensajes pendientes. 
        # El stream ya está fallando, así que esos chunks no deben contar sin ACK explícito.
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

        try:
            self._queue.put_nowait(self._sentinel)
            self._sentinel_enqueued.set()
        except queue.Full:
            # Muy improbable después de vaciar la cola. 
            # Si ocurre, __next__ terminará cuando observe la cancelación y la cola quede vacía.
            pass

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self._cancelled.is_set() and self._queue.empty():
                raise StopIteration

            try:
                item = self._queue.get(timeout=_QUEUE_POLL_TIMEOUT_S)
            except queue.Empty:
                if self._cancelled.is_set():
                    raise StopIteration
                continue

            if item is self._sentinel:
                raise StopIteration

            return item
