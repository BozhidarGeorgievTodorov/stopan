"""Cliente read-only de membership para obtener vistas de cluster."""

from __future__ import annotations

from stopan.cluster.view import ClusterMember, ClusterView
from stopan.errors import StopanNetworkError, StopanUsageError
from stopan.protos import membership_pb2, membership_pb2_grpc
from stopan.rpc.channels import temporary_insecure_channel
from stopan.rpc.errors import format_rpc_error


class ClusterMembershipClient:
    """
    Cliente de membership para obtener una vista de cluster apta para placement.

    Este cliente no participa en SWIM ni mantiene estado de membership: solo
    consulta un seed y normaliza los miembros elegibles devueltos por el nodo.
    """

    def __init__(
        self,
        seed_addr: str,
        *,
        self_addr: str,
        cluster_token: str,
        timeout_s: float,
        max_message_bytes: int,
    ):
        seed_addr = seed_addr.strip()
        if not seed_addr:
            raise StopanUsageError("ClusterMembershipClient requiere un seed_addr no vacío")

        self.seed_addr = seed_addr
        self.self_addr = str(self_addr or "").strip()
        self.cluster_token = str(cluster_token or "")
        self.timeout_s = float(timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)

    def get_cluster_view(self) -> ClusterView:
        import grpc

        try:
            with temporary_insecure_channel(
                self.seed_addr,
                max_message_bytes=self.max_message_bytes,
            ) as channel:
                stub = membership_pb2_grpc.MembershipStub(channel)
                response = stub.GetMembers(
                    membership_pb2.GetMembersRequest(cluster_token=self.cluster_token),
                    timeout=self.timeout_s,
                )
        except grpc.RpcError as exc:
            raise StopanNetworkError(
                f"No se pudo obtener la vista del clúster desde seed={self.seed_addr}: "
                f"{format_rpc_error(exc)}"
            ) from exc

        members_by_node_id: dict[str, ClusterMember] = {}
        for member in response.members:
            if not member.node_id or not member.address:
                continue
            members_by_node_id[member.node_id] = ClusterMember(
                node_id=member.node_id,
                address=member.address,
            )

        members = tuple(members_by_node_id.values())
        self_node_id = next(
            (m.node_id for m in members if m.address == self.self_addr),
            None,
        )

        return ClusterView(self_node_id=self_node_id, members=members)
