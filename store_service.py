import os
import time
import uuid
import random
import threading
import grpc
from concurrent import futures
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import blake3
import zstandard as zstd

from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc
from protos import membership_pb2
from protos import membership_pb2_grpc

from core.repository import CASRepository

PROTOCOL_PERIOD_S = float(os.getenv("SWIM_PERIOD_S", "1.0"))
PING_TIMEOUT_S = float(os.getenv("SWIM_PING_TIMEOUT_S", "0.25"))
SUSPECT_TIMEOUT_S = float(os.getenv("SWIM_SUSPECT_TIMEOUT_S", "6.0"))
INDIRECT_PING_FANOUT = int(os.getenv("SWIM_INDIRECT_FANOUT", "3"))

MAX_GOSSIP_EVENTS = int(os.getenv("SWIM_MAX_GOSSIP", "20"))
GOSSIP_TTL_S = float(os.getenv("SWIM_GOSSIP_TTL_S", "60.0"))
MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", str(8 * 1024 * 1024)))

CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
ADVERTISE_ADDR = os.getenv("ADVERTISE_ADDR", "")
BIND_ADDR = os.getenv("BIND_ADDR", "[::]:50051")
REPO_STORE_DIR = os.getenv("REPO_STORE_DIR", "node_store")
SEEDS = [s.strip() for s in os.getenv("SEEDS", "").split(",") if s.strip()]
STORAGE_RPC_WORKERS = int(os.getenv("STORAGE_RPC_WORKERS", "64"))
GRPC_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))

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
    suspect_since: Optional[float] = None


class ChannelCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._map: Dict[str, Tuple[grpc.Channel, membership_pb2_grpc.MembershipStub]] = {}

    def get(self, address: str) -> membership_pb2_grpc.MembershipStub:
        with self._lock:
            if address not in self._map:
                ch = grpc.insecure_channel(address)
                stub = membership_pb2_grpc.MembershipStub(ch)
                self._map[address] = (ch, stub)
            return self._map[address][1]

    def close_all(self):
        with self._lock:
            for ch, _ in self._map.values():
                ch.close()
            self._map.clear()


class GossipBuffer:
    def __init__(self):
        self._lock = threading.Lock()
        self._events: List[Tuple[float, membership_pb2.MemberEvent]] = []

    def add(self, ev: membership_pb2.MemberEvent):
        with self._lock:
            self._events.append((time.time(), ev))
            self._gc_locked()

    def sample(self, limit: int) -> List[membership_pb2.MemberEvent]:
        with self._lock:
            self._gc_locked()
            return [ev for _, ev in self._events[-limit:]]

    def _gc_locked(self):
        cutoff = time.time() - GOSSIP_TTL_S
        self._events = [(t, ev) for (t, ev) in self._events if t >= cutoff]


class MembershipManager:
    def __init__(self, node_id: str, address: str):
        self.node_id = node_id
        self.address = address
        self._lock = threading.Lock()
        self._members: Dict[str, MemberRecord] = {}
        self.gossip = GossipBuffer()
        self.channels = ChannelCache()
        self.incarnation = 1

        self._members[self.node_id] = MemberRecord(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
            state=membership_pb2.ALIVE,
            last_seen=time.time(),
        )

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._seq = 0

    def start(self):
        self._thread = threading.Thread(target=self._swim_loop, name="swim-loop", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self.channels.close_all()

    def bootstrap_join(self, seeds: List[str]):
        if not seeds:
            return
        me = membership_pb2.NodeInfo(node_id=self.node_id, address=self.address, incarnation=self.incarnation)
        for seed in seeds:
            if seed == self.address:
                continue
            try:
                stub = self.channels.get(seed)
                resp = stub.Join(membership_pb2.JoinRequest(self=me, cluster_token=CLUSTER_TOKEN), timeout=2.0)
                for n in resp.members:
                    self._apply_nodeinfo(n, state=membership_pb2.ALIVE, ts_ms=now_ms(), source="join")
                for ev in resp.gossip:
                    self.apply_event(ev, source="join-gossip")
            except grpc.RpcError:
                continue

    def get_alive_peers(self) -> List[MemberRecord]:
        with self._lock:
            return [m for m in self._members.values() if m.node_id != self.node_id and m.state == membership_pb2.ALIVE]

    def get_members_snapshot(self, *, eligible_only: bool = False) -> List[membership_pb2.NodeInfo]:
        with self._lock:
            out = []
            for m in self._members.values():
                if eligible_only and m.state not in ELIGIBLE_STATES:
                    continue
                out.append(membership_pb2.NodeInfo(node_id=m.node_id, address=m.address, incarnation=m.incarnation))
            return out

    def apply_event(self, ev: membership_pb2.MemberEvent, source: str = ""):
        if ev.node_id == self.node_id and ev.state in (membership_pb2.SUSPECT, membership_pb2.DEAD):
            if ev.incarnation >= self.incarnation:
                self.incarnation = ev.incarnation + 1
                self._announce_alive()
            return

        with self._lock:
            cur = self._members.get(ev.node_id)
            if cur is None:
                self._members[ev.node_id] = MemberRecord(
                    node_id=ev.node_id,
                    address=ev.address,
                    incarnation=ev.incarnation,
                    state=ev.state,
                    last_seen=time.time(),
                    suspect_since=(time.time() if ev.state == membership_pb2.SUSPECT else None),
                )
                self.gossip.add(ev)
                return

            if ev.incarnation > cur.incarnation:
                cur.incarnation = ev.incarnation
                cur.state = ev.state
                if ev.address:
                    cur.address = ev.address
                if ev.state == membership_pb2.ALIVE:
                    cur.last_seen = time.time()
                    cur.suspect_since = None
                elif ev.state == membership_pb2.SUSPECT:
                    cur.suspect_since = time.time()
                self.gossip.add(ev)
                return

            if ev.incarnation < cur.incarnation:
                return

            if STATE_ORDER.get(ev.state, 0) > STATE_ORDER.get(cur.state, 0):
                cur.state = ev.state
                if ev.state == membership_pb2.ALIVE:
                    cur.last_seen = time.time()
                    cur.suspect_since = None
                elif ev.state == membership_pb2.SUSPECT:
                    cur.suspect_since = time.time()
                self.gossip.add(ev)

    def _apply_nodeinfo(self, n: membership_pb2.NodeInfo, state: int, ts_ms: int, source: str):
        ev = membership_pb2.MemberEvent(
            node_id=n.node_id,
            address=n.address,
            incarnation=n.incarnation,
            state=state,
            ts_ms=ts_ms,
        )
        self.apply_event(ev, source=source)

    def _announce_alive(self):
        ev = membership_pb2.MemberEvent(
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
        self.gossip.add(ev)

    def _mark_suspect(self, node_id: str, address: str, incarnation: int):
        ev = membership_pb2.MemberEvent(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.SUSPECT,
            ts_ms=now_ms(),
        )
        self.apply_event(ev, source="local-suspect")

    def _mark_dead(self, node_id: str, address: str, incarnation: int):
        ev = membership_pb2.MemberEvent(
            node_id=node_id,
            address=address,
            incarnation=incarnation,
            state=membership_pb2.DEAD,
            ts_ms=now_ms(),
        )
        self.apply_event(ev, source="local-dead")

    def _swim_loop(self):
        while not self._stop.is_set():
            time.sleep(PROTOCOL_PERIOD_S)
            peers = self.get_alive_peers()
            if not peers:
                continue

            target = random.choice(peers)
            self._seq += 1
            seq = self._seq

            ok = self._ping(target, seq)
            if ok:
                self._expire_suspects()
                continue

            helpers = [p for p in peers if p.node_id != target.node_id]
            random.shuffle(helpers)
            helpers = helpers[:INDIRECT_PING_FANOUT]

            indirect_ok = False
            for h in helpers:
                if self._ping_req(helper=h, target=target, seq=seq):
                    indirect_ok = True
                    break

            if indirect_ok:
                self._expire_suspects()
                continue

            self._mark_suspect(target.node_id, target.address, target.incarnation)
            self._expire_suspects()

    def _expire_suspects(self):
        now = time.time()
        expired = []
        with self._lock:
            for m in self._members.values():
                if m.state == membership_pb2.SUSPECT and m.suspect_since is not None:
                    if (now - m.suspect_since) >= SUSPECT_TIMEOUT_S:
                        expired.append((m.node_id, m.address, m.incarnation))

        for node_id, address, incarnation in expired:
            self._mark_dead(node_id, address, incarnation)

    def _ping(self, target: MemberRecord, seq: int) -> bool:
        me = membership_pb2.NodeInfo(node_id=self.node_id, address=self.address, incarnation=self.incarnation)
        gossip = self.gossip.sample(MAX_GOSSIP_EVENTS)
        try:
            stub = self.channels.get(target.address)
            resp = stub.Ping(membership_pb2.PingRequest(from_node=me, seq=seq, gossip=gossip), timeout=PING_TIMEOUT_S)
            for ev in resp.gossip:
                self.apply_event(ev, source="ping-ack")
            with self._lock:
                cur = self._members.get(target.node_id)
                if cur and cur.state == membership_pb2.ALIVE:
                    cur.last_seen = time.time()
            return resp.ok
        except grpc.RpcError:
            return False

    def _ping_req(self, helper: MemberRecord, target: MemberRecord, seq: int) -> bool:
        me = membership_pb2.NodeInfo(node_id=self.node_id, address=self.address, incarnation=self.incarnation)
        gossip = self.gossip.sample(MAX_GOSSIP_EVENTS)
        try:
            stub = self.channels.get(helper.address)
            resp = stub.PingReq(
                membership_pb2.PingReqRequest(
                    requester=me,
                    target=membership_pb2.NodeInfo(node_id=target.node_id, address=target.address, incarnation=target.incarnation),
                    seq=seq,
                    gossip=gossip,
                ),
                timeout=PING_TIMEOUT_S,
            )
            for ev in resp.gossip:
                self.apply_event(ev, source="pingreq-ack")
            return resp.ok
        except grpc.RpcError:
            return False


class MembershipServicer(membership_pb2_grpc.MembershipServicer):
    def __init__(self, mgr: MembershipManager):
        self.mgr = mgr

    def Join(self, request, context):
        if CLUSTER_TOKEN and request.cluster_token != CLUSTER_TOKEN:
            return membership_pb2.JoinResponse(members=[], gossip=[])

        self.mgr._apply_nodeinfo(request.self, state=membership_pb2.ALIVE, ts_ms=now_ms(), source="join")
        members = self.mgr.get_members_snapshot(eligible_only=True)
        gossip = self.mgr.gossip.sample(MAX_GOSSIP_EVENTS)
        return membership_pb2.JoinResponse(members=members, gossip=gossip)

    def Ping(self, request, context):
        if request.from_node.node_id:
            self.mgr._apply_nodeinfo(request.from_node, state=membership_pb2.ALIVE, ts_ms=now_ms(), source="ping")
        for ev in request.gossip:
            self.mgr.apply_event(ev, source="ping-gossip")
        gossip = self.mgr.gossip.sample(MAX_GOSSIP_EVENTS)
        return membership_pb2.PingResponse(ok=True, gossip=gossip)

    def PingReq(self, request, context):
        if request.requester.node_id:
            self.mgr._apply_nodeinfo(request.requester, state=membership_pb2.ALIVE, ts_ms=now_ms(), source="pingreq")
        for ev in request.gossip:
            self.mgr.apply_event(ev, source="pingreq-gossip")

        ok = False
        try:
            stub = self.mgr.channels.get(request.target.address)
            me = membership_pb2.NodeInfo(node_id=self.mgr.node_id, address=self.mgr.address, incarnation=self.mgr.incarnation)
            resp = stub.Ping(
                membership_pb2.PingRequest(from_node=me, seq=request.seq, gossip=self.mgr.gossip.sample(MAX_GOSSIP_EVENTS)),
                timeout=PING_TIMEOUT_S,
            )
            ok = resp.ok
            for ev in resp.gossip:
                self.mgr.apply_event(ev, source="pingreq-helper-ack")
        except grpc.RpcError:
            ok = False

        gossip = self.mgr.gossip.sample(MAX_GOSSIP_EVENTS)
        return membership_pb2.PingReqResponse(ok=ok, gossip=gossip)

    def GetMembers(self, request, context):
        return membership_pb2.GetMembersResponse(members=self.mgr.get_members_snapshot(eligible_only=True))


class StorageNodeServicer(p2p_storage_pb2_grpc.P2PStorageServicer):
    """Servicio gRPC que recibe y sirve chunks comprimidos."""

    def __init__(self, repo_store_dir: str):
        self.repo = CASRepository(repo_store_dir)
        self._thread_local = threading.local()
        print(f"Storage node using repository: {os.path.abspath(repo_store_dir)}")

    def _get_thread_local_decompressor(self):
        """Devuelve un descompresor Zstandard propio del hilo actual."""
        decompressor = getattr(self._thread_local, "decompressor", None)
        if decompressor is None:
            decompressor = zstd.ZstdDecompressor()
            self._thread_local.decompressor = decompressor
        return decompressor

    def _validate_and_store_one(self, chunk_hash: str, chunk_data: bytes):
        """Valida un chunk comprimido y lo guarda si todavía no existe."""
        if self.repo.exists_local(chunk_hash):
            return True, True, "already present"

        try:
            raw = self._get_thread_local_decompressor().decompress(
                chunk_data,
                max_output_size=MAX_CHUNK_SIZE,
            )
        except zstd.ZstdError:
            return False, False, "rejected: corrupt zstd data"
        except Exception:
            return False, False, "rejected: decompressed output too large"

        h = blake3.blake3(raw).hexdigest()
        if h != chunk_hash:
            return False, False, "rejected: hash mismatch"

        is_new = self.repo.put_compressed(chunk_hash, chunk_data)
        if is_new:
            return True, False, "stored"
        return True, True, "already present"

    def StoreChunk(self, request, context):
        try:
            success, already_present, message = self._validate_and_store_one(request.chunk_hash, request.chunk_data)
            return p2p_storage_pb2.StoreResponse(success=success, message=message)
        except Exception as e:
            return p2p_storage_pb2.StoreResponse(success=False, message=str(e))

    def ProbeMissingChunks(self, request, context):
        """Devuelve solo los hashes que este nodo no tiene en local."""
        try:
            missing = [h for h in request.chunk_hashes if h and (not self.repo.exists_local(h))]
            return p2p_storage_pb2.MissingChunksResponse(missing_hashes=missing)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return p2p_storage_pb2.MissingChunksResponse()

    def StoreChunkBatch(self, request, context):
        """Guarda varios chunks en una sola llamada gRPC."""
        try:
            results = []
            for item in request.items:
                success, already_present, message = self._validate_and_store_one(item.chunk_hash, item.chunk_data)
                results.append(
                    p2p_storage_pb2.BatchStoreResult(
                        chunk_hash=item.chunk_hash,
                        success=success,
                        already_present=already_present,
                        message=message,
                    )
                )
            return p2p_storage_pb2.StoreChunkBatchResponse(results=results)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return p2p_storage_pb2.StoreChunkBatchResponse()

    def RetrieveChunk(self, request, context):
        """Devuelve el chunk comprimido tal como está almacenado."""
        try:
            data = self.repo.get_compressed(request.chunk_hash)
            return p2p_storage_pb2.RetrieveResponse(success=True, chunk_data=data, message="ok")
        except Exception as e:
            return p2p_storage_pb2.RetrieveResponse(success=False, message=str(e))


def load_or_create_node_id(path: str) -> str:
    """Carga un identificador persistente del nodo o crea uno nuevo."""
    os.makedirs(path, exist_ok=True)
    f = os.path.join(path, "node_id.txt")
    if os.path.exists(f):
        with open(f, "r", encoding="utf-8") as r:
            v = r.read().strip()
            if v:
                return v
    v = uuid.uuid4().hex
    with open(f, "w", encoding="utf-8") as w:
        w.write(v)
    return v


def serve():
    repo_store_dir = REPO_STORE_DIR
    if not ADVERTISE_ADDR:
        raise RuntimeError("Missing ADVERTISE_ADDR (for example node1:50051).")

    node_id = load_or_create_node_id(repo_store_dir)
    mgr = MembershipManager(node_id=node_id, address=ADVERTISE_ADDR)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=STORAGE_RPC_WORKERS),
        options=[
            ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_BYTES),
            ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    p2p_storage_pb2_grpc.add_P2PStorageServicer_to_server(StorageNodeServicer(repo_store_dir), server)
    membership_pb2_grpc.add_MembershipServicer_to_server(MembershipServicer(mgr), server)

    server.add_insecure_port(BIND_ADDR)
    server.start()
    print(f"Node {node_id[:8]} listening on {BIND_ADDR} (advertise={ADVERTISE_ADDR})")

    mgr.bootstrap_join(SEEDS)
    mgr.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        mgr.stop()
        server.stop(0)


if __name__ == "__main__":
    serve()
