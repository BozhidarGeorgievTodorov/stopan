"""
Infraestructura común para clientes gRPC P2PStorage.

Centraliza la creación lazy de canales/stubs y la degradación adaptativa de
batches cuando una respuesta supera el límite de mensaje configurado.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any, TypeVar

from stopan.rpc.channels import close_channel_safely, create_insecure_channel


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class P2PStorageClientRuntime:
    """Pool lazy y thread-safe de canales/stubs P2PStorage."""

    def __init__(
        self,
        *,
        max_message_bytes: int,
        closed_message: str,
        closed_error_factory: Callable[[str], Exception] | None = None,
    ):
        self.max_message_bytes = max(int(max_message_bytes), 1)
        self._closed_message = str(closed_message)
        self._closed_error_factory = closed_error_factory or RuntimeError
        self._channels: dict[str, Any] = {}
        self._stubs: dict[str, Any] = {}
        self._grpc = None
        self._pb = None
        self._pb_grpc = None
        self._lock = threading.Lock()
        self._closed = threading.Event()

    def ensure_open(self) -> None:
        if self._closed.is_set():
            raise self._closed_error_factory(self._closed_message)

    def ensure_runtime(self) -> None:
        if self._grpc is not None:
            return

        with self._lock:
            if self._grpc is not None:
                return

            import grpc
            from stopan.protos import p2p_storage_pb2
            from stopan.protos import p2p_storage_pb2_grpc

            self._grpc = grpc
            self._pb = p2p_storage_pb2
            self._pb_grpc = p2p_storage_pb2_grpc

    @property
    def pb(self):
        self.ensure_runtime()
        return self._pb

    def get_stub(self, address: str):
        self.ensure_runtime()

        with self._lock:
            self.ensure_open()
            stub = self._stubs.get(address)
            if stub is not None:
                return stub

            channel = create_insecure_channel(
                address,
                max_message_bytes=self.max_message_bytes,
            )
            stub = self._pb_grpc.P2PStorageStub(channel)
            self._channels[address] = channel
            self._stubs[address] = stub
            return stub

    def close(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return

            self._closed.set()

            for channel in self._channels.values():
                close_channel_safely(channel)
            self._channels.clear()
            self._stubs.clear()


def run_adaptive_batch_call(
    *,
    items: list[T],
    call_once: Callable[[list[T]], dict[K, V]],
) -> dict[K, V]:
    """
    Ejecuta una llamada por batch y divide recursivamente si la respuesta es grande.

    La función mantiene la semántica de los clientes concretos: solo reconoce el
    error de tamaño máximo de mensaje y deja pasar cualquier otro fallo.
    """

    try:
        return call_once(items)
    except Exception as exc:
        if len(items) > 1 and _is_message_too_large_error(exc):
            mid = max(1, len(items) // 2)
            left = run_adaptive_batch_call(
                items=items[:mid],
                call_once=call_once,
            )
            right = run_adaptive_batch_call(
                items=items[mid:],
                call_once=call_once,
            )
            left.update(right)
            return left

        raise


def _is_message_too_large_error(exc: Exception) -> bool:
    try:
        from stopan.rpc.errors import is_message_too_large_error
    except ModuleNotFoundError:
        return False

    return is_message_too_large_error(exc)
