"""
Caché de canales gRPC para membership.

El manager reutiliza stubs por address para evitar crear canales en cada ping,
join o ping indirecto.
"""

from __future__ import annotations

import threading

import grpc

from stopan.protos import membership_pb2_grpc
from stopan.node.errors import MembershipConfigError
from stopan.rpc.options import grpc_channel_options

from .validation import is_valid_address


class ChannelCache:
    """Caché thread-safe de canales y stubs Membership por address."""

    def __init__(
        self,
        *,
        max_message_bytes: int,
        keepalive_time_ms: int,
        keepalive_timeout_ms: int,
        keepalive_permit_without_calls: bool,
    ):
        self._lock = threading.Lock()
        self._map: dict[str, tuple[grpc.Channel, membership_pb2_grpc.MembershipStub]] = {}
        self._options = grpc_channel_options(
            max_message_bytes,
            keepalive_time_ms=keepalive_time_ms,
            keepalive_timeout_ms=keepalive_timeout_ms,
            keepalive_permit_without_calls=keepalive_permit_without_calls,
        )

    def get(self, address: str) -> membership_pb2_grpc.MembershipStub:
        """Devuelve un stub Membership reutilizable para address."""
        address = str(address).strip()
        if not is_valid_address(address):
            raise MembershipConfigError(f"Dirección de membership inválida: {address!r}")

        with self._lock:
            if address not in self._map:
                channel = grpc.insecure_channel(address, options=self._options)
                stub = membership_pb2_grpc.MembershipStub(channel)
                self._map[address] = (channel, stub)
            return self._map[address][1]

    def close_all(self) -> None:
        """Cierra todos los canales abiertos y vacía la caché."""
        with self._lock:
            for channel, _ in self._map.values():
                channel.close()
            self._map.clear()
