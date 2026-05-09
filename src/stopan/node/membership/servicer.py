"""
Servicer gRPC del protocolo de membership.

Expone Join, Ping, PingReq y GetMembers. Cada request valida cluster_token y
normaliza gossip entrante antes de aplicarlo en MembershipManager.
"""

from __future__ import annotations

import grpc

from stopan.protos import membership_pb2
from stopan.protos import membership_pb2_grpc

from .manager import MembershipManager
from .validation import (
    is_valid_nodeinfo,
    limited_gossip,
    require_authorized_cluster_token,
)


class MembershipServicer(membership_pb2_grpc.MembershipServicer):
    """Adaptador gRPC sobre MembershipManager."""

    def __init__(self, manager: MembershipManager):
        self.manager = manager

    def Join(self, request, context):
        """Registra un nodo entrante y devuelve miembros/gossip conocidos."""
        require_authorized_cluster_token(self.manager.cluster_token, request.cluster_token, context)

        if not is_valid_nodeinfo(request.self):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "identidad del nodo join inválida")

        self.manager._apply_nodeinfo(request.self, state=membership_pb2.ALIVE, source="join")
        return membership_pb2.JoinResponse(
            members=self.manager.get_members_snapshot(eligible_only=True),
            gossip=self.manager.gossip.sample(self.manager.max_gossip_events),
        )

    def Ping(self, request, context):
        """Responde a un ping directo y absorbe gossip entrante."""
        require_authorized_cluster_token(self.manager.cluster_token, request.cluster_token, context)

        if not is_valid_nodeinfo(request.from_node):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "identidad de origen de ping inválida")

        self.manager._apply_nodeinfo(request.from_node, state=membership_pb2.ALIVE, source="ping")

        for event in limited_gossip(request.gossip, self.manager.max_gossip_events):
            self.manager.apply_event(event, source="ping-gossip")

        return membership_pb2.PingResponse(
            ok=True,
            gossip=self.manager.gossip.sample(self.manager.max_gossip_events),
        )

    def PingReq(self, request, context):
        """Ejecuta un ping indirecto solicitado por otro nodo."""
        require_authorized_cluster_token(self.manager.cluster_token, request.cluster_token, context)

        if not is_valid_nodeinfo(request.requester):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "identidad del solicitante pingreq inválida")
        if not is_valid_nodeinfo(request.target):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "identidad del target pingreq inválida")

        self.manager._apply_nodeinfo(request.requester, state=membership_pb2.ALIVE, source="pingreq")

        for event in limited_gossip(request.gossip, self.manager.max_gossip_events):
            self.manager.apply_event(event, source="pingreq-gossip")

        ok = False
        try:
            stub = self.manager.channels.get(request.target.address)
            me = membership_pb2.NodeInfo(
                node_id=self.manager.node_id,
                address=self.manager.address,
                incarnation=self.manager.incarnation,
            )
            response = stub.Ping(
                membership_pb2.PingRequest(
                    from_node=me,
                    seq=request.seq,
                    gossip=self.manager.gossip.sample(self.manager.max_gossip_events),
                    cluster_token=self.manager.cluster_token,
                ),
                timeout=float(self.manager.settings.ping_timeout_s),
            )
            ok = response.ok
            for event in limited_gossip(response.gossip, self.manager.max_gossip_events):
                self.manager.apply_event(event, source="pingreq-helper-ack")
        except (grpc.RpcError, ValueError):
            ok = False

        return membership_pb2.PingReqResponse(
            ok=ok,
            gossip=self.manager.gossip.sample(self.manager.max_gossip_events),
        )

    def GetMembers(self, request, context):
        """Devuelve miembros elegibles conocidos por este nodo."""
        require_authorized_cluster_token(self.manager.cluster_token, request.cluster_token, context)

        return membership_pb2.GetMembersResponse(
            members=self.manager.get_members_snapshot(eligible_only=True)
        )
