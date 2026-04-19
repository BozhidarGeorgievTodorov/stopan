from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass

import grpc

from stopan.node.identity import NodeIdentityStore
from stopan.protos import membership_pb2
from stopan.protos import membership_pb2_grpc


PROTOCOL_PERIOD_S = float(os.getenv("SWIM_PERIOD_S", "1.0"))
PING_TIMEOUT_S = float(os.getenv("SWIM_PING_TIMEOUT_S", "0.25"))
SUSPECT_TIMEOUT_S = float(os.getenv("SWIM_SUSPECT_TIMEOUT_S", "6.0"))
INDIRECT_PING_FANOUT = int(os.getenv("SWIM_INDIRECT_FANOUT", "3"))

MAX_GOSSIP_EVENTS = int(os.getenv("SWIM_MAX_GOSSIP", "20"))
GOSSIP_TTL_S = float(os.getenv("SWIM_GOSSIP_TTL_S", "60.0"))

CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")

STATE_ORDER = {
    membership_pb2.UNKNOWN: 0,
    membership_pb2.ALIVE: 1,
    membership_pb2.SUSPECT: 2,
    membership_pb2.DEAD: 3,
    membership_pb2.LEFT: 4,
}
ELIGIBLE_STATES = {membership_pb2.ALIVE}


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class MemberRecord:
    node_id: str
    address: str
    incarnation: int
    state: int
    last_seen: float
    suspect_since: float | None = None


class ChannelCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._map: dict[str, tuple[grpc.Channel, membership_pb2_grpc.MembershipStub]] = {}

    def get(self, address: str) -> membership_pb2_grpc.MembershipStub:
        with self._lock:
            if address not in self._map:
                channel = grpc.insecure_channel(address)
                stub = membership_pb2_grpc.MembershipStub(channel)
                self._map[address] = (channel, stub)
            return self._map[address][1]

    def close_all(self) -> None:
        with self._lock:
            for channel, _ in self._map.values():
                channel.close()
            self._map.clear()


class GossipBuffer:
    def __init__(self):
        self._lock = threading.Lock()
        self._events: list[tuple[float, membership_pb2.MemberEvent]] = []

    def add(self, event: membership_pb2.MemberEvent) -> None:
        with self._lock:
            self._events.append((time.time(), event))
            self._gc_locked()

    def sample(self, limit: int) -> list[membership_pb2.MemberEvent]:
        with self._lock:
            self._gc_locked()
            return [event for _, event in self._events[-limit:]]

    def _gc_locked(self) -> None:
        cutoff = time.time() - GOSSIP_TTL_S
        self._events = [(ts, event) for ts, event in self._events if ts >= cutoff]


class MembershipManager:
    def __init__(
        self,
        *,
        identity_store: NodeIdentityStore,
        node_id: str,
        address: str,
        incarnation: int,
    ):
        self.identity_store = identity_store
        self.node_id = node_id
        self.address = address
        self.incarnation = max(int(incarnation), 1)

        self._lock = threading.Lock()
        self._members: dict[str, MemberRecord] = {
            self.node_id: MemberRecord(
                node_id=self.node_id,
                address=self.address,
                incarnation=self.incarnation,
                state=membership_pb2.ALIVE,
                last_seen=time.time(),
            )
        }

        self.gossip = GossipBuffer()
        self.channels = ChannelCache()

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._seq = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._swim_loop, name="swim-loop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self.channels.close_all()

    def bootstrap_join(self, seeds: list[str]) -> None:
        if not seeds:
            return

        me = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )

        for seed in seeds:
            if seed == self.address:
                continue
            try:
                stub = self.channels.get(seed)
                response = stub.Join(
                    membership_pb2.JoinRequest(self=me, cluster_token=CLUSTER_TOKEN),
                    timeout=2.0,
                )
                for node in response.members:
                    self._apply_nodeinfo(node, state=membership_pb2.ALIVE, source="join")
                for event in response.gossip:
                    self.apply_event(event, source="join-gossip")
            except grpc.RpcError:
                continue

    def get_alive_peers(self) -> list[MemberRecord]:
        with self._lock:
            return [
                member
                for member in self._members.values()
                if member.node_id != self.node_id and member.state == membership_pb2.ALIVE
            ]

    def get_members_snapshot(self, *, eligible_only: bool = False) -> list[membership_pb2.NodeInfo]:
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
        if event.node_id == self.node_id and event.state in (membership_pb2.SUSPECT, membership_pb2.DEAD):
            if event.incarnation >= self.incarnation:
                self.incarnation = self.identity_store.bump_above(
                    node_id=self.node_id,
                    observed_incarnation=event.incarnation,
                )
                self._announce_alive()
            return

        with self._lock:
            current = self._members.get(event.node_id)

            if current is None:
                self._members[event.node_id] = MemberRecord(
                    node_id=event.node_id,
                    address=event.address,
                    incarnation=event.incarnation,
                    state=event.state,
                    last_seen=time.time(),
                    suspect_since=(time.time() if event.state == membership_pb2.SUSPECT else None),
                )
                self.gossip.add(event)
                return

            if event.incarnation > current.incarnation:
                current.incarnation = event.incarnation
                current.state = event.state
                if event.address:
                    current.address = event.address
                if event.state == membership_pb2.ALIVE:
                    current.last_seen = time.time()
                    current.suspect_since = None
                elif event.state == membership_pb2.SUSPECT:
                    current.suspect_since = time.time()
                self.gossip.add(event)
                return

            if event.incarnation < current.incarnation:
                return

            if STATE_ORDER.get(event.state, 0) > STATE_ORDER.get(current.state, 0):
                current.state = event.state
                if event.state == membership_pb2.ALIVE:
                    current.last_seen = time.time()
                    current.suspect_since = None
                elif event.state == membership_pb2.SUSPECT:
                    current.suspect_since = time.time()
                self.gossip.add(event)

    def _apply_nodeinfo(self, node: membership_pb2.NodeInfo, *, state: int, source: str) -> None:
        event = membership_pb2.MemberEvent(
            node_id=node.node_id,
            address=node.address,
            incarnation=node.incarnation,
            state=state,
            ts_ms=now_ms(),
        )
        self.apply_event(event, source=source)

    def _announce_alive(self) -> None:
        event = membership_pb2.MemberEvent(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
            state=membership_pb2.ALIVE,
            ts_ms=now_ms(),
        )
        with self._lock:
            me = self._members[self.node_id]
            me.incarnation = self.incarnation
            me.state = membership_pb2.ALIVE
            me.last_seen = time.time()
            me.suspect_since = None
        self.gossip.add(event)

    def _mark_suspect(self, node_id: str, address: str, incarnation: int) -> None:
        event = membership_pb2.MemberEvent(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.SUSPECT,
            ts_ms=now_ms(),
        )
        self.apply_event(event, source="local-suspect")

    def _mark_dead(self, node_id: str, address: str, incarnation: int) -> None:
        event = membership_pb2.MemberEvent(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.DEAD,
            ts_ms=now_ms(),
        )
        self.apply_event(event, source="local-dead")

    def _swim_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(PROTOCOL_PERIOD_S)
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
            helpers = helpers[:INDIRECT_PING_FANOUT]

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
        now = time.time()
        expired = []

        with self._lock:
            for member in self._members.values():
                if member.state == membership_pb2.SUSPECT and member.suspect_since is not None:
                    if (now - member.suspect_since) >= SUSPECT_TIMEOUT_S:
                        expired.append((member.node_id, member.address, member.incarnation))

        for node_id, address, incarnation in expired:
            self._mark_dead(node_id, address, incarnation)

    def _ping(self, target: MemberRecord, seq: int) -> bool:
        me = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )
        gossip = self.gossip.sample(MAX_GOSSIP_EVENTS)

        try:
            stub = self.channels.get(target.address)
            response = stub.Ping(
                membership_pb2.PingRequest(from_node=me, seq=seq, gossip=gossip),
                timeout=PING_TIMEOUT_S,
            )
            for event in response.gossip:
                self.apply_event(event, source="ping-ack")

            with self._lock:
                current = self._members.get(target.node_id)
                if current and current.state == membership_pb2.ALIVE:
                    current.last_seen = time.time()

            return response.ok
        except grpc.RpcError:
            return False

    def _ping_req(self, *, helper: MemberRecord, target: MemberRecord, seq: int) -> bool:
        requester = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )
        gossip = self.gossip.sample(MAX_GOSSIP_EVENTS)

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
                ),
                timeout=PING_TIMEOUT_S,
            )
            for event in response.gossip:
                self.apply_event(event, source="pingreq-ack")
            return response.ok
        except grpc.RpcError:
            return False


class MembershipServicer(membership_pb2_grpc.MembershipServicer):
    def __init__(self, manager: MembershipManager):
        self.manager = manager

    def Join(self, request, context):
        if CLUSTER_TOKEN and request.cluster_token != CLUSTER_TOKEN:
            return membership_pb2.JoinResponse(members=[], gossip=[])

        self.manager._apply_nodeinfo(request.self, state=membership_pb2.ALIVE, source="join")
        return membership_pb2.JoinResponse(
            members=self.manager.get_members_snapshot(eligible_only=True),
            gossip=self.manager.gossip.sample(MAX_GOSSIP_EVENTS),
        )

    def Ping(self, request, context):
        if request.from_node.node_id:
            self.manager._apply_nodeinfo(request.from_node, state=membership_pb2.ALIVE, source="ping")

        for event in request.gossip:
            self.manager.apply_event(event, source="ping-gossip")

        return membership_pb2.PingResponse(ok=True, gossip=self.manager.gossip.sample(MAX_GOSSIP_EVENTS))

    def PingReq(self, request, context):
        if request.requester.node_id:
            self.manager._apply_nodeinfo(request.requester, state=membership_pb2.ALIVE, source="pingreq")

        for event in request.gossip:
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
                    gossip=self.manager.gossip.sample(MAX_GOSSIP_EVENTS),
                ),
                timeout=PING_TIMEOUT_S,
            )
            ok = response.ok
            for event in response.gossip:
                self.manager.apply_event(event, source="pingreq-helper-ack")
        except grpc.RpcError:
            ok = False

        return membership_pb2.PingReqResponse(
            ok=ok,
            gossip=self.manager.gossip.sample(MAX_GOSSIP_EVENTS),
        )

    def GetMembers(self, request, context):
        return membership_pb2.GetMembersResponse(
            members=self.manager.get_members_snapshot(eligible_only=True)
        )
