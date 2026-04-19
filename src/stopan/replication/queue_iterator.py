from __future__ import annotations

import queue
import threading


class QueueIterator:
    """
    Producer/consumer iterator for gRPC client-streaming.

    finish():
      normal close; keeps all accepted messages and appends a sentinel.

    cancel():
      error/cancellation close; does not block indefinitely and may discard
      queued messages that must not be counted without ACKs.
    """

    def __init__(self, maxsize: int):
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, int(maxsize)))
        self._sentinel = object()
        self._finished = threading.Event()
        self._cancelled = threading.Event()
        self._sentinel_enqueued = threading.Event()
        self._finalize_lock = threading.Lock()

    def put(self, item) -> bool:
        while not self._cancelled.is_set() and not self._finished.is_set():
            try:
                self._queue.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def finish(self) -> None:
        with self._finalize_lock:
            if self._sentinel_enqueued.is_set():
                return
            self._finished.set()

            while not self._cancelled.is_set():
                try:
                    self._queue.put(self._sentinel, timeout=0.05)
                    self._sentinel_enqueued.set()
                    return
                except queue.Full:
                    continue

            self._force_sentinel_after_cancel()

    def cancel(self) -> None:
        self._cancelled.set()
        with self._finalize_lock:
            if self._sentinel_enqueued.is_set():
                return
            self._force_sentinel_after_cancel()

    def _force_sentinel_after_cancel(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

        try:
            self._queue.put_nowait(self._sentinel)
            self._sentinel_enqueued.set()
        except queue.Full:
            pass

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self._cancelled.is_set() and self._queue.empty():
                raise StopIteration

            try:
                item = self._queue.get(timeout=0.05)
            except queue.Empty:
                if self._cancelled.is_set():
                    raise StopIteration
                continue

            if item is self._sentinel:
                raise StopIteration

            if self._cancelled.is_set():
                raise StopIteration

            return item
