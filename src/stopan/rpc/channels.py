"""
Ciclo de vida común de canales gRPC.

Este módulo solo conoce mecánica de transporte: crear canales con opciones
coherentes, cerrarlos de forma tolerante y reutilizarlos cuando un dominio lo
necesita. No conoce semántica de chunks, shards, metadata packs ni membership.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from collections.abc import Callable
from typing import Any, TypeVar

from stopan.rpc.options import grpc_channel_options


StubT = TypeVar("StubT")


def create_insecure_channel(
    address: str,
    *,
    max_message_bytes: int,
    keepalive_time_ms: int | None = None,
    keepalive_timeout_ms: int | None = None,
    keepalive_permit_without_calls: bool | None = None,
):
    """Crea un canal gRPC insecure con las opciones comunes de Stopan."""
    import grpc

    kwargs: dict[str, object] = {}
    if keepalive_time_ms is not None:
        kwargs["keepalive_time_ms"] = int(keepalive_time_ms)
    if keepalive_timeout_ms is not None:
        kwargs["keepalive_timeout_ms"] = int(keepalive_timeout_ms)
    if keepalive_permit_without_calls is not None:
        kwargs["keepalive_permit_without_calls"] = bool(keepalive_permit_without_calls)

    return grpc.insecure_channel(
        str(address),
        options=grpc_channel_options(int(max_message_bytes), **kwargs),
    )


def close_channel_safely(channel: object) -> None:
    """Cierra un canal gRPC ignorando errores de limpieza."""
    try:
        close = getattr(channel, "close")
        close()
    except Exception:
        pass


@contextmanager
def temporary_insecure_channel(
    address: str,
    *,
    max_message_bytes: int,
    keepalive_time_ms: int | None = None,
    keepalive_timeout_ms: int | None = None,
    keepalive_permit_without_calls: bool | None = None,
):
    """Abre un canal temporal y lo cierra siempre al salir del contexto."""
    channel = create_insecure_channel(
        address,
        max_message_bytes=max_message_bytes,
        keepalive_time_ms=keepalive_time_ms,
        keepalive_timeout_ms=keepalive_timeout_ms,
        keepalive_permit_without_calls=keepalive_permit_without_calls,
    )
    try:
        yield channel
    finally:
        close_channel_safely(channel)


class RpcChannelCache:
    """Caché thread-safe de canales y stubs por address."""

    def __init__(
        self,
        *,
        max_message_bytes: int,
        stub_factory: Callable[[Any], StubT],
        keepalive_time_ms: int | None = None,
        keepalive_timeout_ms: int | None = None,
        keepalive_permit_without_calls: bool | None = None,
        validate_address: Callable[[str], bool] | None = None,
        invalid_address_error_factory: Callable[[str], Exception] | None = None,
    ):
        self._max_message_bytes = max(int(max_message_bytes), 1)
        self._stub_factory = stub_factory
        self._keepalive_time_ms = keepalive_time_ms
        self._keepalive_timeout_ms = keepalive_timeout_ms
        self._keepalive_permit_without_calls = keepalive_permit_without_calls
        self._validate_address = validate_address
        self._invalid_address_error_factory = invalid_address_error_factory or ValueError
        self._lock = threading.Lock()
        self._map: dict[str, tuple[Any, StubT]] = {}

    def get(self, address: str) -> StubT:
        address = str(address).strip()
        if self._validate_address is not None and not self._validate_address(address):
            raise self._invalid_address_error_factory(address)

        with self._lock:
            item = self._map.get(address)
            if item is not None:
                return item[1]

            channel = create_insecure_channel(
                address,
                max_message_bytes=self._max_message_bytes,
                keepalive_time_ms=self._keepalive_time_ms,
                keepalive_timeout_ms=self._keepalive_timeout_ms,
                keepalive_permit_without_calls=self._keepalive_permit_without_calls,
            )
            stub = self._stub_factory(channel)
            self._map[address] = (channel, stub)
            return stub

    def close_all(self) -> None:
        with self._lock:
            for channel, _stub in self._map.values():
                close_channel_safely(channel)
            self._map.clear()
