"""
Validaciones comunes del protocolo membership.

Centraliza validación de node_id, address, incarnation, estados de eventos y
cluster_token para los endpoints gRPC de membership.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterable

import grpc

from stopan.protos import membership_pb2

from .models import VALID_EVENT_STATES


_NODE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_MAX_ADDRESS_LENGTH = 255


def now_ms() -> int:
    return int(time.time() * 1000)


def is_valid_node_id(node_id: str) -> bool:
    return bool(_NODE_ID_RE.fullmatch(str(node_id).strip()))


def is_valid_address(address: str) -> bool:
    text = str(address).strip()
    if not text or len(text) > _MAX_ADDRESS_LENGTH:
        return False
    if any(char.isspace() or ord(char) < 32 for char in text):
        return False

    if text.startswith("["):
        closing = text.find("]")
        if closing <= 1 or closing + 2 > len(text) or text[closing + 1] != ":":
            return False
        host = text[1:closing]
        port_text = text[closing + 2 :]
    else:
        if ":" not in text:
            return False
        host, port_text = text.rsplit(":", 1)

    if not host or not port_text.isdecimal():
        return False

    port = int(port_text)
    return 0 < port <= 65535


def is_valid_incarnation(value: int) -> bool:
    try:
        return int(value) >= 1
    except (TypeError, ValueError):
        return False


def is_valid_state(state: int) -> bool:
    try:
        return int(state) in VALID_EVENT_STATES
    except (TypeError, ValueError):
        return False


def is_valid_nodeinfo(node: membership_pb2.NodeInfo) -> bool:
    return (
        is_valid_node_id(node.node_id)
        and is_valid_address(node.address)
        and is_valid_incarnation(node.incarnation)
    )


def is_valid_member_event(event: membership_pb2.MemberEvent) -> bool:
    return (
        is_valid_node_id(event.node_id)
        and is_valid_address(event.address)
        and is_valid_incarnation(event.incarnation)
        and is_valid_state(event.state)
    )


def limited_gossip(events: Iterable[membership_pb2.MemberEvent], limit: int):
    for index, event in enumerate(events):
        if index >= max(0, int(limit)):
            break
        yield event


def is_authorized_cluster_token(expected_token: str, provided_token: str) -> bool:
    expected_token = str(expected_token or "")
    if not expected_token.strip():
        return False
    return str(provided_token) == expected_token


def require_authorized_cluster_token(expected_token: str, provided_token: str, context) -> None:
    if not is_authorized_cluster_token(expected_token, provided_token):
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "cluster_token no coincide")
