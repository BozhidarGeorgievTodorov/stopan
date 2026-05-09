"""
Utilidades para clasificar y formatear errores gRPC.

El módulo evita depender directamente de detalles internos de grpc.RpcError en
las capas de replication, restore y metadata.
"""

from __future__ import annotations

import grpc


def format_rpc_error(exc: grpc.RpcError) -> str:
    """Devuelve una representación estable code: details de un error gRPC."""
    code = "UNKNOWN"
    details = str(exc)

    try:
        code = exc.code().name
    except Exception:
        pass

    try:
        details = exc.details() or details
    except Exception:
        pass

    return f"{code}: {details}"


def is_rpc_error(exc: Exception) -> bool:
    return isinstance(exc, grpc.RpcError)


def is_message_too_large_error(exc: Exception) -> bool:
    """
    Detecta errores de tamaño de mensaje gRPC tanto en cliente como en servidor.

    Ejemplos:
      - RESOURCE_EXHAUSTED
      - "Sent message larger than max"
      - "Received message larger than max"
    """
    if not isinstance(exc, grpc.RpcError):
        return False

    try:
        if exc.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
            return True
    except Exception:
        pass

    parts: list[str] = []

    try:
        details = exc.details()
        if details:
            parts.append(str(details))
    except Exception:
        pass

    parts.append(str(exc))

    text = " ".join(parts).lower()
    return (
        "message larger than max" in text
        or "sent message larger than max" in text
        or "received message larger than max" in text
        or "resource exhausted" in text
    )