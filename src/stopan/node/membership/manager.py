"""
Gestor local del protocolo de membership.

Mantiene la vista local de miembros del cluster, ejecuta un bucle tipo SWIM con
ping directo e indirecto, y propaga eventos recientes mediante gossip.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Iterable

import grpc

from stopan.protos import membership_pb2
from stopan.node.errors import MembershipConfigError
from stopan.node.identity import NodeIdentityStore

from .channels import ChannelCache
from .gossip import GossipBuffer
from .models import ELIGIBLE_STATES, STATE_ORDER, MemberRecord, MembershipSettings
from .validation import (
    is_valid_address,
    is_valid_member_event,
    is_valid_node_id,
    is_valid_nodeinfo,
    limited_gossip,
    now_ms,
)


_STOP_JOIN_MIN_TIMEOUT_S = 2.0
_STOP_JOIN_EXTRA_TIMEOUT_S = 0.5


def _member_event(*, node_id: str, address: str, incarnation: int, state: int) -> membership_pb2.MemberEvent:
    return membership_pb2.MemberEvent(
        node_id=node_id,
        address=address,
        incarnation=incarnation,
        state=state,
        ts_ms=now_ms(),
    )


def _new_record_from_event(event: membership_pb2.MemberEvent, *, observed_at: float) -> MemberRecord:
    return MemberRecord(
        node_id=event.node_id,
        address=event.address,
        incarnation=event.incarnation,
        state=event.state,
        last_seen=observed_at,
        suspect_since=(observed_at if event.state == membership_pb2.SUSPECT else None),
    )


def _apply_event_to_record(
    record: MemberRecord,
    event: membership_pb2.MemberEvent,
    *,
    observed_at: float,
    replace_incarnation: bool,
) -> None:
    if replace_incarnation:
        record.incarnation = event.incarnation
        record.address = event.address

    record.state = event.state

    if event.state == membership_pb2.ALIVE:
        record.last_seen = observed_at
        record.suspect_since = None
    elif event.state == membership_pb2.SUSPECT:
        record.suspect_since = observed_at


class MembershipManager:
    """
    Gestiona membership, gossip e incarnation del nodo local.

    Solo los miembros ALIVE se exponen como elegibles para placement. Los estados
    SUSPECT y DEAD se mantienen para convergencia del protocolo, no para elegir
    targets remotos.
    """

    def __init__(
        self,
        *,
        identity_store: NodeIdentityStore,
        node_id: str,
        address: str,
        incarnation: int,
        settings: MembershipSettings | None = None,
    ):
        node_id = str(node_id).strip()
        address = str(address).strip()

        if not is_valid_node_id(node_id):
            raise MembershipConfigError(f"node_id inválido: {node_id!r}")
        if not is_valid_address(address):
            raise MembershipConfigError(f"advertise_addr inválido para membership: {address!r}")

        self.settings = settings or MembershipSettings()
        self.identity_store = identity_store
        self.node_id = node_id
        self.address = address
        self.incarnation = max(int(incarnation), 1)
        self._lifecycle_lock = threading.Lock()

        self._lock = threading.Lock()
        self._members: dict[str, MemberRecord] = {
            self.node_id: MemberRecord(
                node_id=self.node_id,
                address=self.address,
                incarnation=self.incarnation,
                state=membership_pb2.ALIVE,
                last_seen=time.monotonic(),
            )
        }

        self.gossip = GossipBuffer(ttl_s=self.settings.gossip_ttl_s)
        self.channels = ChannelCache(
            max_message_bytes=self.settings.grpc_max_message_bytes,
            keepalive_time_ms=self.settings.grpc_keepalive_time_ms,
            keepalive_timeout_ms=self.settings.grpc_keepalive_timeout_ms,
            keepalive_permit_without_calls=self.settings.grpc_keepalive_permit_without_calls,
        )

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._seq = 0

    @property
    def cluster_token(self) -> str:
        """Token compartido usado para autorizar RPCs de membership."""
        return self.settings.cluster_token

    @property
    def max_gossip_events(self) -> int:
        """Número máximo de eventos gossip por mensaje."""
        return int(self.settings.max_gossip_events)

    def start(self) -> None:
        """Arranca el bucle de membership si aún no está activo."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop.clear()
            self._thread = threading.Thread(
                target=self._swim_loop,
                name="swim-loop",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Solicita parada, espera al bucle y cierra canales remotos."""
        with self._lifecycle_lock:
            self._stop.set()
            thread = self._thread

        if thread is not None and thread is not threading.current_thread():
            thread.join(
                timeout=max(
                    _STOP_JOIN_MIN_TIMEOUT_S,
                    float(self.settings.protocol_period_s) + _STOP_JOIN_EXTRA_TIMEOUT_S,
                )
            )

        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None

        self.channels.close_all()

    def bootstrap_join(self, seeds: Iterable[str]) -> None:
        """Intenta unirse al cluster usando seeds iniciales de membership."""
        normalized_seeds = []
        seen = set()
        for raw_seed in seeds:
            seed = str(raw_seed).strip()
            if not seed or seed == self.address or seed in seen:
                continue
            seen.add(seed)
            normalized_seeds.append(seed)

        if not normalized_seeds:
            return

        me = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )

        for seed in normalized_seeds:
            if not is_valid_address(seed):
                print(f"AVISO: seed de membership inválido ignorado: {seed!r}")
                continue

            try:
                stub = self.channels.get(seed)
                response = stub.Join(
                    membership_pb2.JoinRequest(self=me, cluster_token=self.cluster_token),
                    timeout=float(self.settings.rpc_timeout_s),
                )
                for node in response.members:
                    self.apply_nodeinfo(node, state=membership_pb2.ALIVE, source="join")
                self.apply_gossip(response.gossip, source="join-gossip")
            except grpc.RpcError as exc:
                details = getattr(exc, "details", lambda: str(exc))()
                print(f"AVISO: join contra seed {seed} falló: {details}")
            except ValueError as exc:
                print(f"AVISO: join contra seed {seed} omitido: {exc}")

    def get_alive_peers(self) -> list[MemberRecord]:
        """Devuelve peers ALIVE excluyendo el nodo local."""
        with self._lock:
            return [
                member
                for member in self._members.values()
                if member.node_id != self.node_id and member.state == membership_pb2.ALIVE
            ]

    def get_members_snapshot(self, *, eligible_only: bool = False) -> list[membership_pb2.NodeInfo]:
        """Devuelve una vista NodeInfo de los miembros conocidos."""
        with self._lock:
            members = []
            for member in self._members.values():
                if eligible_only and member.state not in ELIGIBLE_STATES:
                    continue
                members.append(
                    membership_pb2.NodeInfo(
                        node_id=member.node_id,
                        address=member.address,
                        incarnation=member.incarnation,
                    )
                )
            return members

    def apply_event(self, event: membership_pb2.MemberEvent, *, source: str = "") -> None:
        """Aplica un evento de membership si mejora la vista local."""
        if not is_valid_member_event(event):
            return

        if event.node_id == self.node_id:
            self._handle_self_event(event, source=source)
            return

        with self._lock:
            current = self._members.get(event.node_id)
            observed_at = time.monotonic()

            if current is None:
                self._members[event.node_id] = _new_record_from_event(event, observed_at=observed_at)
                self.gossip.add(event)
                return

            if event.incarnation > current.incarnation:
                _apply_event_to_record(
                    current,
                    event,
                    observed_at=observed_at,
                    replace_incarnation=True,
                )
                self.gossip.add(event)
                return

            if event.incarnation < current.incarnation:
                return

            if STATE_ORDER.get(event.state, 0) > STATE_ORDER.get(current.state, 0):
                _apply_event_to_record(
                    current,
                    event,
                    observed_at=observed_at,
                    replace_incarnation=False,
                )
                self.gossip.add(event)

    def apply_gossip(self, events: Iterable[membership_pb2.MemberEvent], *, source: str) -> None:
        """Aplica gossip entrante respetando el límite configurado."""
        for event in limited_gossip(events, self.max_gossip_events):
            self.apply_event(event, source=source)

    def _handle_self_event(self, event: membership_pb2.MemberEvent, *, source: str) -> None:
        """Procesa eventos sobre el propio node_id."""
        if event.address and event.address != self.address:
            print(
                "AVISO: evento de membership ignorado; posible node_id duplicado "
                f"node_id={self.node_id[:8]} self={self.address} "
                f"event_address={event.address} source={source}"
            )
            return

        if event.state in (membership_pb2.SUSPECT, membership_pb2.DEAD):
            if event.incarnation >= self.incarnation:
                self.incarnation = self.identity_store.bump_above(
                    node_id=self.node_id,
                    observed_incarnation=event.incarnation,
                )
                self._announce_alive()

    def apply_nodeinfo(self, node: membership_pb2.NodeInfo, *, state: int, source: str) -> None:
        """Convierte un NodeInfo entrante en MemberEvent local."""
        if not is_valid_nodeinfo(node):
            print(
                f"AVISO: NodeInfo inválido ignorado desde {source}: "
                f"node_id={node.node_id!r} address={node.address!r}"
            )
            return

        event = _member_event(
            node_id=node.node_id,
            address=node.address,
            incarnation=node.incarnation,
            state=state,
        )
        self.apply_event(event, source=source)

    def _announce_alive(self) -> None:
        """Publica ALIVE del nodo local con la incarnation actual."""
        event = _member_event(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
            state=membership_pb2.ALIVE,
        )
        with self._lock:
            me = self._members[self.node_id]
            me.incarnation = self.incarnation
            me.state = membership_pb2.ALIVE
            me.address = self.address
            me.last_seen = time.monotonic()
            me.suspect_since = None
        self.gossip.add(event)

    def _mark_suspect(self, node_id: str, address: str, incarnation: int) -> None:
        """Marca localmente un nodo como SUSPECT."""
        event = _member_event(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.SUSPECT,
        )
        self.apply_event(event, source="local-suspect")

    def _mark_dead(self, node_id: str, address: str, incarnation: int) -> None:
        """Marca localmente un nodo como DEAD."""
        event = _member_event(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.DEAD,
        )
        self.apply_event(event, source="local-dead")

    def _swim_loop(self) -> None:
        """Ejecuta rondas periódicas de ping directo e indirecto."""
        while not self._stop.wait(float(self.settings.protocol_period_s)):
            peers = self.get_alive_peers()
            if not peers:
                continue

            target = random.choice(peers)
            self._seq += 1
            seq = self._seq

            if self._ping(target, seq):
                self._expire_suspects()
                continue

            helpers = [peer for peer in peers if peer.node_id != target.node_id]
            random.shuffle(helpers)
            helpers = helpers[: int(self.settings.indirect_ping_fanout)]

            indirect_ok = False
            for helper in helpers:
                if self._ping_req(helper=helper, target=target, seq=seq):
                    indirect_ok = True
                    break

            if indirect_ok:
                self._expire_suspects()
                continue

            self._mark_suspect(target.node_id, target.address, target.incarnation)
            self._expire_suspects()

    def _expire_suspects(self) -> None:
        """Promueve SUSPECT a DEAD al superar suspect_timeout_s."""
        now = time.monotonic()
        expired = []

        with self._lock:
            for member in self._members.values():
                if member.state == membership_pb2.SUSPECT and member.suspect_since is not None:
                    if (now - member.suspect_since) >= float(self.settings.suspect_timeout_s):
                        expired.append((member.node_id, member.address, member.incarnation))

        for node_id, address, incarnation in expired:
            self._mark_dead(node_id, address, incarnation)

    def _ping(self, target: MemberRecord, seq: int) -> bool:
        """Ejecuta un Ping directo contra target."""
        me = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )
        gossip = self.gossip.sample(self.max_gossip_events)

        try:
            stub = self.channels.get(target.address)
            response = stub.Ping(
                membership_pb2.PingRequest(
                    from_node=me,
                    seq=seq,
                    gossip=gossip,
                    cluster_token=self.cluster_token,
                ),
                timeout=float(self.settings.ping_timeout_s),
            )
            self.apply_gossip(response.gossip, source="ping-ack")

            with self._lock:
                current = self._members.get(target.node_id)
                if current and current.state == membership_pb2.ALIVE:
                    current.last_seen = time.monotonic()

            return response.ok
        except (grpc.RpcError, ValueError):
            return False

    def _ping_req(self, *, helper: MemberRecord, target: MemberRecord, seq: int) -> bool:
        """Solicita a helper que haga Ping indirecto contra target."""
        requester = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )
        gossip = self.gossip.sample(self.max_gossip_events)

        try:
            stub = self.channels.get(helper.address)
            response = stub.PingReq(
                membership_pb2.PingReqRequest(
                    requester=requester,
                    target=membership_pb2.NodeInfo(
                        node_id=target.node_id,
                        address=target.address,
                        incarnation=target.incarnation,
                    ),
                    seq=seq,
                    gossip=gossip,
                    cluster_token=self.cluster_token,
                ),
                timeout=float(self.settings.ping_timeout_s),
            )
            self.apply_gossip(response.gossip, source="pingreq-ack")
            return response.ok
        except (grpc.RpcError, ValueError):
            return False
