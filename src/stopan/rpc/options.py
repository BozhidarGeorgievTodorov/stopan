"""
Opciones comunes de gRPC para clientes y servidores Stopan.

Centraliza límites de tamaño de mensaje y keepalive para que todos los canales
usen una configuración coherente.
"""

from __future__ import annotations

from stopan.config.defaults import (
    DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
    DEFAULT_GRPC_KEEPALIVE_TIME_MS,
    DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
)


def grpc_channel_options(
    max_message_bytes: int,
    *,
    keepalive_time_ms: int = DEFAULT_GRPC_KEEPALIVE_TIME_MS,
    keepalive_timeout_ms: int = DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
    keepalive_permit_without_calls: bool = DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
) -> list[tuple[str, int]]:
    """Construye opciones de canal gRPC para clientes."""
    options = _message_size_options(max_message_bytes)
    options.extend(
        [
            ("grpc.keepalive_time_ms", int(keepalive_time_ms)),
            ("grpc.keepalive_timeout_ms", int(keepalive_timeout_ms)),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1 if keepalive_permit_without_calls else 0),
        ]
    )
    return options


def grpc_server_options(max_message_bytes: int) -> list[tuple[str, int]]:
    """Construye opciones gRPC para servidores."""
    return _message_size_options(max_message_bytes)


def _message_size_options(max_message_bytes: int) -> list[tuple[str, int]]:
    """Valida y devuelve límites comunes de envío/recepción."""
    value = int(max_message_bytes)
    if value <= 0:
        raise ValueError("grpc.max_message_bytes debe ser > 0")

    return [
        ("grpc.max_send_message_length", value),
        ("grpc.max_receive_message_length", value),
    ]