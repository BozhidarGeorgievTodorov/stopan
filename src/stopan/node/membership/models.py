"""
Modelos y constantes del protocolo de membership.

El membership usa estados tipo SWIM para decidir qué nodos son elegibles para
placement remoto. En esta versión solo ALIVE participa en placement.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.config.defaults import (
    DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    DEFAULT_GRPC_KEEPALIVE_TIME_MS,
    DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
    DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
    DEFAULT_MEMBERSHIP_BOOTSTRAP_RETRY_INTERVAL_S,
    MEMBERSHIP_BOOTSTRAP_RETRY_MAX_INTERVAL_S,
    DEFAULT_MEMBERSHIP_GOSSIP_TTL_S,
    DEFAULT_MEMBERSHIP_INDIRECT_PING_FANOUT,
    DEFAULT_MEMBERSHIP_MAX_GOSSIP_EVENTS,
    DEFAULT_MEMBERSHIP_PING_TIMEOUT_S,
    DEFAULT_MEMBERSHIP_PROTOCOL_PERIOD_S,
    DEFAULT_MEMBERSHIP_RPC_TIMEOUT_S,
    DEFAULT_MEMBERSHIP_SUSPECT_TIMEOUT_S,
)
from stopan.protos import membership_pb2
from stopan.node.errors import MembershipConfigError


STATE_ORDER = {
    membership_pb2.UNKNOWN: 0,
    membership_pb2.ALIVE: 1,
    membership_pb2.SUSPECT: 2,
    membership_pb2.DEAD: 3,
    membership_pb2.LEFT: 4,
}
ELIGIBLE_STATES = {membership_pb2.ALIVE}
VALID_EVENT_STATES = {
    membership_pb2.ALIVE,
    membership_pb2.SUSPECT,
    membership_pb2.DEAD,
    membership_pb2.LEFT,
}


@dataclass(frozen=True)
class MembershipSettings:
    """Parámetros de temporización, gossip y gRPC del protocolo membership."""

    cluster_token: str = ""
    protocol_period_s: float = DEFAULT_MEMBERSHIP_PROTOCOL_PERIOD_S
    bootstrap_retry_interval_s: float = DEFAULT_MEMBERSHIP_BOOTSTRAP_RETRY_INTERVAL_S
    ping_timeout_s: float = DEFAULT_MEMBERSHIP_PING_TIMEOUT_S
    rpc_timeout_s: float = DEFAULT_MEMBERSHIP_RPC_TIMEOUT_S
    suspect_timeout_s: float = DEFAULT_MEMBERSHIP_SUSPECT_TIMEOUT_S
    indirect_ping_fanout: int = DEFAULT_MEMBERSHIP_INDIRECT_PING_FANOUT
    max_gossip_events: int = DEFAULT_MEMBERSHIP_MAX_GOSSIP_EVENTS
    gossip_ttl_s: float = DEFAULT_MEMBERSHIP_GOSSIP_TTL_S
    grpc_max_message_bytes: int = DEFAULT_GRPC_MAX_MESSAGE_BYTES
    grpc_keepalive_time_ms: int = DEFAULT_GRPC_KEEPALIVE_TIME_MS
    grpc_keepalive_timeout_ms: int = DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS
    grpc_keepalive_permit_without_calls: bool = DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS

    def __post_init__(self) -> None:
        """Valida que los parámetros de membership sean utilizables."""
        if float(self.protocol_period_s) <= 0:
            raise MembershipConfigError("membership.protocol_period_s debe ser > 0")
        if float(self.bootstrap_retry_interval_s) <= 0:
            raise MembershipConfigError("membership.bootstrap_retry_interval_s debe ser > 0")
        if float(self.bootstrap_retry_interval_s) > MEMBERSHIP_BOOTSTRAP_RETRY_MAX_INTERVAL_S:
            raise MembershipConfigError(
                "membership.bootstrap_retry_interval_s debe ser <= "
                f"{MEMBERSHIP_BOOTSTRAP_RETRY_MAX_INTERVAL_S}"
            )
        if float(self.ping_timeout_s) <= 0:
            raise MembershipConfigError("membership.ping_timeout_s debe ser > 0")
        if float(self.rpc_timeout_s) <= 0:
            raise MembershipConfigError("membership.rpc_timeout_s debe ser > 0")
        if float(self.suspect_timeout_s) <= 0:
            raise MembershipConfigError("membership.suspect_timeout_s debe ser > 0")
        if int(self.indirect_ping_fanout) < 0:
            raise MembershipConfigError("membership.indirect_ping_fanout debe ser >= 0")
        if int(self.max_gossip_events) < 0:
            raise MembershipConfigError("membership.max_gossip_events debe ser >= 0")
        if float(self.gossip_ttl_s) <= 0:
            raise MembershipConfigError("membership.gossip_ttl_s debe ser > 0")
        if int(self.grpc_max_message_bytes) <= 0:
            raise MembershipConfigError("grpc.max_message_bytes debe ser > 0")
        if int(self.grpc_keepalive_time_ms) <= 0:
            raise MembershipConfigError("grpc.keepalive_time_ms debe ser > 0")
        if int(self.grpc_keepalive_timeout_ms) <= 0:
            raise MembershipConfigError("grpc.keepalive_timeout_ms debe ser > 0")
        if not isinstance(self.grpc_keepalive_permit_without_calls, bool):
            raise MembershipConfigError("grpc.keepalive_permit_without_calls debe ser booleano")


@dataclass
class MemberRecord:
    """Estado local conocido de un miembro del cluster."""

    node_id: str
    address: str
    incarnation: int
    state: int
    last_seen: float
    suspect_since: float | None = None
