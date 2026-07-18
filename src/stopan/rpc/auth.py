"""Autorización lógica compartida para las RPC entre nodos.

El token separa de forma lógica las instalaciones que comparten una misma credencial.
"""

from __future__ import annotations

import hmac
from collections.abc import Iterable
from typing import Any

import grpc


CLUSTER_TOKEN_METADATA_KEY = "x-stopan-cluster-token-bin"


def cluster_token_metadata(cluster_token: str) -> tuple[tuple[str, bytes], ...]:
    """Construye la metadata gRPC usada por los clientes de ``P2PStorage``.

    Un token vacío no añade metadata. Esto conserva el modo sin aislamiento
    lógico, pero evita intentar enviar un valor binario vacío innecesario.
    """

    token = str(cluster_token or "")
    if not token:
        return ()
    return ((CLUSTER_TOKEN_METADATA_KEY, token.encode("utf-8")),)


def require_cluster_token_metadata(
    expected_token: str,
    context: grpc.ServicerContext,
) -> None:
    """Exige que la llamada contenga el token configurado en metadata.

    Cuando el token esperado está vacío, la barrera lógica queda desactivada de
    forma explícita, igual que ocurre en los despliegues que conservan el valor
    predeterminado de ``cluster.token``.
    """

    expected = str(expected_token or "")
    if not expected:
        return

    received = _metadata_value(
        context.invocation_metadata(),
        key=CLUSTER_TOKEN_METADATA_KEY,
    )
    if received is None or not hmac.compare_digest(received, expected.encode("utf-8")):
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "cluster_token no coincide")


def _metadata_value(metadata: Iterable[Any], *, key: str) -> bytes | None:
    values: list[bytes] = []
    for item in metadata:
        item_key = str(getattr(item, "key", "") or "").lower()
        if item_key != key:
            continue

        value = getattr(item, "value", b"")
        if isinstance(value, bytes):
            values.append(value)
        else:
            values.append(str(value).encode("utf-8"))

    if len(values) != 1:
        return None
    return values[0]
