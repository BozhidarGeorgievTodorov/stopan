"""
Caché de canales gRPC para membership.

El manager reutiliza stubs por address para evitar crear canales en cada ping,
join o ping indirecto.
"""

from __future__ import annotations

from stopan.protos import membership_pb2_grpc
from stopan.node.errors import MembershipConfigError
from stopan.rpc.channels import RpcChannelCache

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
        self._cache = RpcChannelCache(
            max_message_bytes=max_message_bytes,
            stub_factory=membership_pb2_grpc.MembershipStub,
            keepalive_time_ms=keepalive_time_ms,
            keepalive_timeout_ms=keepalive_timeout_ms,
            keepalive_permit_without_calls=keepalive_permit_without_calls,
            validate_address=is_valid_address,
            invalid_address_error_factory=lambda address: MembershipConfigError(
                f"Dirección de membership inválida: {address!r}"
            ),
        )

    def get(self, address: str) -> membership_pb2_grpc.MembershipStub:
        """Devuelve un stub Membership reutilizable para address."""
        return self._cache.get(address)

    def close_all(self) -> None:
        """Cierra todos los canales abiertos y vacía la caché."""
        self._cache.close_all()
