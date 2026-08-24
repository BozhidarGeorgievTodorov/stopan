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
from concurrent import futures

import grpc

from stopan.config.defaults import MEMBERSHIP_BOOTSTRAP_RETRY_MAX_INTERVAL_S
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


_LEAVE_MAX_WORKERS = 16

# Los reintentos de bootstrap deben recuperarse rápido de una indisponibilidad
# breve sin convertir un nodo aislado o mal configurado en una fuente de tráfico
# constante. El intervalo configurado actúa como base y crece exponencialmente
# hasta este techo. El jitter evita que varios nodos arrancados a la vez queden
# sincronizados contra los mismos seeds.
_BOOTSTRAP_RETRY_BACKOFF_FACTOR = 2.0
_BOOTSTRAP_RETRY_JITTER_RATIO = 0.20


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

    Solo los miembros ALIVE se exponen como elegibles para placement. SUSPECT y
    DEAD modelan fallos detectados y LEFT una salida voluntaria; ninguno de ellos
    participa en la selección de targets remotos.
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
        self._leaving = threading.Event()
        self._leave_complete = threading.Event()
        self._thread: threading.Thread | None = None
        self._seq = 0

        # Bootstrap es una fase de descubrimiento inicial, no un mecanismo de
        # reparación permanente. Si existen seeds externos, se reintenta hasta
        # descubrir al menos un miembro ALIVE y después SWIM toma el relevo.
        self._bootstrap_seeds: tuple[str, ...] = ()
        self._bootstrap_retry_index = 0
        self._bootstrap_complete = threading.Event()
        self._next_bootstrap_retry_at: float | None = None
        self._bootstrap_retry_nominal_s = float(self.settings.bootstrap_retry_interval_s)
        self._bootstrap_seed_failures: dict[str, str] = {}

    @property
    def cluster_token(self) -> str:
        """Token compartido usado para autorizar RPCs de membership."""
        return self.settings.cluster_token

    @property
    def max_gossip_events(self) -> int:
        """Número máximo de eventos gossip por mensaje."""
        return int(self.settings.max_gossip_events)

    @property
    def leaving(self) -> bool:
        """Indica si el nodo ya ha iniciado una salida voluntaria."""
        return self._leaving.is_set()

    def start(self) -> None:
        """Arranca el bucle de membership si aún no está activo."""
        with self._lifecycle_lock:
            if self._leaving.is_set():
                raise RuntimeError("un MembershipManager que ha publicado LEFT no puede reiniciarse")
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop.clear()
            self._thread = threading.Thread(
                target=self._swim_loop,
                name="swim-loop",
                daemon=True,
            )
            self._thread.start()

    def _stop_protocol_loop(self) -> None:
        """Detiene únicamente el bucle SWIM, conservando los canales abiertos."""
        with self._lifecycle_lock:
            self._stop.set()
            thread = self._thread

        if thread is not None and thread is not threading.current_thread():
            # Todas las E/S iniciadas por el bucle SWIM tienen deadline propio.
            # Esperar realmente al hilo conserva una frontera fuerte: cuando
            # este método retorna ya no queda actividad periódica capaz de usar
            # los canales de membership durante el teardown.
            thread.join()

        with self._lifecycle_lock:
            if self._thread is thread and (thread is None or not thread.is_alive()):
                self._thread = None

    def stop(self) -> None:
        """Detiene el manager sin iniciar una salida voluntaria nueva."""
        if self._leaving.is_set() and not self._leave_complete.is_set():
            self._leave_complete.wait()
        self._stop_protocol_loop()
        self.channels.close_all()

    def leave(self) -> None:
        """Publica LEFT de forma best-effort y detiene la actividad periódica."""
        with self._lifecycle_lock:
            if self._leaving.is_set():
                first_leave = False
            else:
                self._leaving.set()
                self._stop.set()
                first_leave = True

        if not first_leave:
            self._leave_complete.wait()
            return

        try:
            # El estado local cambia antes de cualquier E/S remota. Desde este
            # punto GetMembers deja de exponer al propio nodo como elegible y el
            # evento LEFT queda disponible para respuestas/gossip concurrentes.
            self._announce_left()
            peer_addresses = tuple(
                dict.fromkeys(
                    [peer.address for peer in self.get_alive_peers()]
                    + list(self._bootstrap_seeds)
                )
            )
            peer_addresses = tuple(
                address for address in peer_addresses if address != self.address
            )

            if peer_addresses:
                max_workers = min(_LEAVE_MAX_WORKERS, len(peer_addresses))
                deadline = time.monotonic() + float(self.settings.rpc_timeout_s)
                executor = futures.ThreadPoolExecutor(
                    max_workers=max_workers,
                    thread_name_prefix="membership-leave",
                )
                pending = [
                    executor.submit(self._send_leave, address, deadline=deadline)
                    for address in peer_addresses
                ]
                try:
                    done, unfinished = futures.wait(
                        pending,
                        timeout=max(0.0, deadline - time.monotonic()),
                    )
                    for completed in done:
                        try:
                            completed.result()
                        except Exception:
                            # _send_leave ya encapsula fallos esperables. Esta
                            # defensa conserva la naturaleza best-effort del
                            # anuncio directo.
                            pass
                    for future in unfinished:
                        future.cancel()
                finally:
                    # Todas las tareas comparten un deadline absoluto. Las que
                    # comienzan tarde reducen su propio timeout y las que aún no
                    # han empezado se cancelan, evitando encadenar oleadas de
                    # rpc_timeout_s al crecer el número de peers.
                    executor.shutdown(wait=True, cancel_futures=True)
        finally:
            try:
                self._stop_protocol_loop()
            finally:
                self._leave_complete.set()

    def _send_leave(self, address: str, *, deadline: float) -> None:
        """Anuncia LEFT a una dirección dentro del presupuesto global de salida."""
        timeout_s = deadline - time.monotonic()
        if timeout_s <= 0:
            return

        try:
            stub = self.channels.get(address)
            stub.Leave(
                membership_pb2.LeaveRequest(
                    self=membership_pb2.NodeInfo(
                        node_id=self.node_id,
                        address=self.address,
                        incarnation=self.incarnation,
                    ),
                    cluster_token=self.cluster_token,
                ),
                timeout=timeout_s,
            )
        except (grpc.RpcError, ValueError):
            return

    def bootstrap_join(
        self,
        seeds: Iterable[str],
        *,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Intenta el descubrimiento inicial y programa reintentos si sigue aislado.

        ``cancel_event`` permite que la ruta de apagado interrumpa la sucesión de
        seeds durante el arranque. Una RPC Join ya iniciada conserva su deadline,
        pero no se inicia un nuevo intento después de recibir la cancelación.
        """
        normalized_seeds = self._normalize_bootstrap_seeds(seeds)
        self._bootstrap_seeds = normalized_seeds
        self._bootstrap_retry_index = 0
        self._bootstrap_retry_nominal_s = float(self.settings.bootstrap_retry_interval_s)

        if not normalized_seeds:
            self._bootstrap_complete.set()
            self._next_bootstrap_retry_at = None
            return

        self._bootstrap_complete.clear()

        # Se conserva el intento inmediato del arranque. Los seeds actúan como
        # alternativas de entrada: en cuanto uno permite descubrir un par, no
        # hace falta bloquear el arranque probando el resto. Una solicitud de
        # parada impide encadenar nuevos Join durante esta fase síncrona.
        for seed in normalized_seeds:
            if self._stop.is_set() or (cancel_event is not None and cancel_event.is_set()):
                return
            if self._refresh_bootstrap_completion():
                break
            self._join_seed(seed)

        if self._stop.is_set() or (cancel_event is not None and cancel_event.is_set()):
            return
        if not self._refresh_bootstrap_completion():
            self._schedule_next_bootstrap_retry()

    def _normalize_bootstrap_seeds(self, seeds: Iterable[str]) -> tuple[str, ...]:
        """Normaliza seeds externos, elimina duplicados y descarta entradas inválidas."""
        normalized: list[str] = []
        seen: set[str] = set()

        for raw_seed in seeds:
            seed = str(raw_seed).strip()
            if not seed or seed == self.address or seed in seen:
                continue
            seen.add(seed)

            if not is_valid_address(seed):
                print(f"AVISO: seed de membership inválido ignorado: {seed!r}")
                continue

            normalized.append(seed)

        return tuple(normalized)

    def _join_seed(self, seed: str) -> None:
        """Ejecuta un Join contra un seed y absorbe la vista devuelta."""
        me = membership_pb2.NodeInfo(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
        )

        try:
            stub = self.channels.get(seed)
            response = stub.Join(
                membership_pb2.JoinRequest(self=me, cluster_token=self.cluster_token),
                timeout=float(self.settings.rpc_timeout_s),
            )
            for node in response.members:
                self.apply_nodeinfo(node, state=membership_pb2.ALIVE, source="join")
            self.apply_gossip(response.gossip, source="join-gossip")

            if seed in self._bootstrap_seed_failures:
                self._bootstrap_seed_failures.pop(seed, None)
                print(f"Membership: join contra seed {seed} restablecido")
        except grpc.RpcError as exc:
            details = getattr(exc, "details", lambda: str(exc))()
            self._report_bootstrap_failure(seed, f"falló: {details}")
        except ValueError as exc:
            self._report_bootstrap_failure(seed, f"omitido: {exc}")

    def _report_bootstrap_failure(self, seed: str, detail: str) -> None:
        """Informa solo cuando cambia el fallo observado para evitar spam de reintentos."""
        if self._bootstrap_seed_failures.get(seed) == detail:
            return

        self._bootstrap_seed_failures[seed] = detail
        print(f"AVISO: join contra seed {seed} {detail}")

    def _complete_bootstrap(self) -> None:
        """Cierra de forma irreversible la fase de descubrimiento inicial."""
        self._bootstrap_complete.set()
        self._next_bootstrap_retry_at = None

    def _refresh_bootstrap_completion(self) -> bool:
        """Fija el bootstrap como completo al conocer un par ALIVE por cualquier vía."""
        if self._bootstrap_complete.is_set():
            return True

        if not self.get_alive_peers():
            return False

        self._complete_bootstrap()
        return True

    def _schedule_next_bootstrap_retry(self) -> None:
        """Programa el siguiente reintento con backoff acotado y jitter."""
        max_interval_s = MEMBERSHIP_BOOTSTRAP_RETRY_MAX_INTERVAL_S
        nominal_s = min(self._bootstrap_retry_nominal_s, max_interval_s)

        jitter_s = nominal_s * _BOOTSTRAP_RETRY_JITTER_RATIO
        min_delay_s = max(0.0, nominal_s - jitter_s)
        max_delay_s = min(max_interval_s, nominal_s + jitter_s)
        delay_s = random.uniform(min_delay_s, max_delay_s)

        self._next_bootstrap_retry_at = time.monotonic() + delay_s
        self._bootstrap_retry_nominal_s = min(
            max_interval_s,
            nominal_s * _BOOTSTRAP_RETRY_BACKOFF_FACTOR,
        )

    def _retry_bootstrap_if_due(self) -> None:
        """Reintenta un único seed cuando el descubrimiento inicial sigue pendiente."""
        if self._bootstrap_complete.is_set() or not self._bootstrap_seeds:
            return

        if self._refresh_bootstrap_completion():
            return

        now = time.monotonic()
        next_retry_at = self._next_bootstrap_retry_at
        if next_retry_at is not None and now < next_retry_at:
            return

        seed = self._bootstrap_seeds[self._bootstrap_retry_index]
        self._bootstrap_retry_index = (
            self._bootstrap_retry_index + 1
        ) % len(self._bootstrap_seeds)

        self._join_seed(seed)

        if not self._refresh_bootstrap_completion():
            self._schedule_next_bootstrap_retry()

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
                if eligible_only and member.node_id == self.node_id and self._leaving.is_set():
                    # La barrera lógica de salida precede a la publicación del
                    # MemberEvent LEFT. No exponer el propio nodo en esa ventana
                    # evita que una consulta concurrente lo seleccione de nuevo.
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
                if event.state == membership_pb2.ALIVE:
                    self._complete_bootstrap()
                return

            if event.incarnation > current.incarnation:
                _apply_event_to_record(
                    current,
                    event,
                    observed_at=observed_at,
                    replace_incarnation=True,
                )
                self.gossip.add(event)
                if event.state == membership_pb2.ALIVE:
                    self._complete_bootstrap()
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
                if event.state == membership_pb2.ALIVE:
                    self._complete_bootstrap()

    def apply_gossip(self, events: Iterable[membership_pb2.MemberEvent], *, source: str) -> None:
        """Aplica gossip entrante respetando el límite configurado."""
        for event in limited_gossip(events, self.max_gossip_events):
            self.apply_event(event, source=source)

    def _handle_self_event(self, event: membership_pb2.MemberEvent, *, source: str) -> None:
        """Procesa eventos sobre el propio node_id."""
        if self._leaving.is_set():
            return

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
        if self._leaving.is_set():
            return

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

    def _announce_left(self) -> None:
        """Marca al nodo local como LEFT y conserva el evento para gossip residual."""
        event = _member_event(
            node_id=self.node_id,
            address=self.address,
            incarnation=self.incarnation,
            state=membership_pb2.LEFT,
        )
        with self._lock:
            me = self._members[self.node_id]
            me.incarnation = self.incarnation
            me.state = membership_pb2.LEFT
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
        """Coordina reintentos de bootstrap y rondas periódicas de SWIM."""
        protocol_period_s = float(self.settings.protocol_period_s)
        next_protocol_at = time.monotonic() + protocol_period_s

        while True:
            now = time.monotonic()
            wake_at = next_protocol_at

            if not self._bootstrap_complete.is_set() and self._bootstrap_seeds:
                retry_at = self._next_bootstrap_retry_at
                if retry_at is not None:
                    wake_at = min(wake_at, retry_at)

            if self._stop.wait(max(0.0, wake_at - now)):
                return

            self._retry_bootstrap_if_due()

            now = time.monotonic()
            if now < next_protocol_at:
                continue

            next_protocol_at = now + protocol_period_s
            self._run_swim_round()

    def _run_swim_round(self) -> None:
        """Ejecuta una ronda de detección de fallos y expiración de sospechas."""
        # La expiración de sospechas no depende de que quede algún miembro
        # ALIVE disponible para sondear en esta ronda.
        self._expire_suspects()

        peers = self.get_alive_peers()
        if not peers:
            return

        target = random.choice(peers)
        self._seq += 1
        seq = self._seq

        if self._ping(target, seq):
            self._expire_suspects()
            return

        helpers = [peer for peer in peers if peer.node_id != target.node_id]
        random.shuffle(helpers)
        helpers = helpers[: int(self.settings.indirect_ping_fanout)]

        for helper in helpers:
            if self._ping_req(helper=helper, target=target, seq=seq):
                self._expire_suspects()
                return

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
