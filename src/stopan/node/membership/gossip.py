"""
Buffer temporal de eventos gossip.

Guarda eventos de membership durante una ventana TTL y permite enviar muestras
acotadas en Join, Ping y PingReq.
"""

from __future__ import annotations

import threading
import time

from stopan.protos import membership_pb2

from .validation import is_valid_member_event


class GossipBuffer:
    """Buffer thread-safe de eventos MemberEvent con expiración por TTL."""

    def __init__(self, *, ttl_s: float):
        self._lock = threading.Lock()
        self._ttl_s = float(ttl_s)
        self._events: list[tuple[float, membership_pb2.MemberEvent]] = []

    def add(self, event: membership_pb2.MemberEvent) -> None:
        """Añade un evento válido al buffer y descarta eventos expirados."""
        if not is_valid_member_event(event):
            return

        with self._lock:
            self._events.append((time.time(), event))
            self._gc_locked()

    def sample(self, limit: int) -> list[membership_pb2.MemberEvent]:
        """Devuelve como máximo limit eventos recientes."""
        with self._lock:
            self._gc_locked()
            return [event for _, event in self._events[-max(0, int(limit)) :]]

    def _gc_locked(self) -> None:
        """Elimina eventos que superan el TTL. Requiere _lock adquirido."""
        cutoff = time.time() - self._ttl_s
        self._events = [(ts, event) for ts, event in self._events if ts >= cutoff]
