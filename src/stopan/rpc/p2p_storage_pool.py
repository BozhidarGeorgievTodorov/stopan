"""
Pool thread-safe de stubs P2PStorage.

Reutiliza canales gRPC por address para evitar crear un canal nuevo en cada
probe, stream o restore remoto.
"""

from __future__ import annotations

import threading

import grpc

from stopan.protos import p2p_storage_pb2_grpc
from stopan.rpc.options import grpc_channel_options


class P2PStorageStubPool:
    """Pool thread-safe de stubs P2PStorage indexado por address."""

    def __init__(self, *, max_message_bytes: int):
        self._lock = threading.Lock()
        self._channels: dict[str, grpc.Channel] = {}
        self._stubs: dict[str, p2p_storage_pb2_grpc.P2PStorageStub] = {}
        self._options = grpc_channel_options(max_message_bytes)

    def get_stub(self, address: str) -> p2p_storage_pb2_grpc.P2PStorageStub:
        """Devuelve un stub reutilizable para address, creándolo si no existe."""
        with self._lock:
            stub = self._stubs.get(address)
            if stub is not None:
                return stub

            channel = grpc.insecure_channel(address, options=self._options)
            stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
            self._channels[address] = channel
            self._stubs[address] = stub
            return stub

    def close(self) -> None:
        """Cierra todos los canales abiertos y vacía el pool."""
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()
