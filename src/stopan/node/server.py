"""
Arranque del nodo Stopan.

Inicializa identidad persistente, membership, almacenamiento P2P, servicio de
metadata distribuida y servidor gRPC del nodo.
"""

from __future__ import annotations

import signal
import threading
from concurrent import futures

import grpc

from stopan.config.model import StopanConfig
from stopan.errors import StopanConfigError, StopanNetworkError
from stopan.protos import membership_pb2_grpc
from stopan.protos import p2p_storage_pb2_grpc
from stopan.rpc.options import grpc_server_options

from .identity import NodeIdentityStore
from .lifecycle import NodeControlServer, NodeDrainController
from .membership import MembershipManager, MembershipSettings
from .services import MembershipServicer, MetadataPackServiceServicer, StorageNodeServicer


_SHUTDOWN_GRACE_S = 5.0
_SHUTDOWN_POLL_S = 0.1


def _install_shutdown_handlers(stop_event: threading.Event, request_draining=None):
    """Instala handlers SIGINT/SIGTERM para solicitar parada ordenada."""

    previous_handlers = {}
    signal_seen = False

    def request_shutdown(_signum, _frame):
        nonlocal signal_seen
        # Los handlers de Python pueden interrumpir al propio handler. Marcar
        # primero la solicitud evita reentrar en primitivas de sincronización si
        # llegan varias señales de parada casi simultáneas.
        if signal_seen:
            return
        signal_seen = True
        if request_draining is not None:
            request_draining()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, request_shutdown)
        except (AttributeError, OSError, ValueError):
            continue

    return previous_handlers


def _restore_shutdown_handlers(previous_handlers) -> None:
    """Restaura los handlers de señal anteriores."""

    for sig, handler in previous_handlers.items():
        try:
            signal.signal(sig, handler)
        except (AttributeError, OSError, ValueError):
            continue


def _warn_shutdown_failure(step: str, exc: BaseException) -> None:
    """Informa de un fallo de teardown sin impedir el resto de liberaciones."""
    print(f"AVISO: fallo durante {step}: {exc}")


def serve(config: StopanConfig) -> None:
    """Arranca el nodo y aplica una parada ordenada con drenaje al recibir SIGINT/SIGTERM."""

    advertise_addr = config.node.advertise_addr.strip()
    if not advertise_addr:
        raise StopanConfigError("Falta node.advertise_addr, por ejemplo node1:50051.")

    cluster_token = str(config.cluster.token or "")
    if not cluster_token.strip():
        raise StopanConfigError(
            "Falta cluster.token. El nodo no puede publicar servicios gRPC sin una "
            "credencial de clúster no vacía."
        )

    identity_store = NodeIdentityStore(config.node.identity_file)
    identity = identity_store.load_for_startup()

    shutdown_requested = threading.Event()
    drain_controller = NodeDrainController()
    previous_handlers = {}

    membership_manager: MembershipManager | None = None
    storage_servicer: StorageNodeServicer | None = None
    rpc_executor: futures.ThreadPoolExecutor | None = None
    server = None
    control_server: NodeControlServer | None = None
    control_started = False
    grpc_started = False

    try:
        membership_settings = MembershipSettings(
            cluster_token=cluster_token,
            protocol_period_s=config.membership.protocol_period_s,
            bootstrap_retry_interval_s=config.membership.bootstrap_retry_interval_s,
            ping_timeout_s=config.membership.ping_timeout_s,
            rpc_timeout_s=config.membership.rpc_timeout_s,
            suspect_timeout_s=config.membership.suspect_timeout_s,
            indirect_ping_fanout=config.membership.indirect_ping_fanout,
            max_gossip_events=config.membership.max_gossip_events,
            gossip_ttl_s=config.membership.gossip_ttl_s,
            grpc_max_message_bytes=config.grpc.max_message_bytes,
            grpc_keepalive_time_ms=config.grpc.keepalive_time_ms,
            grpc_keepalive_timeout_ms=config.grpc.keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=config.grpc.keepalive_permit_without_calls,
        )
        membership_manager = MembershipManager(
            identity_store=identity_store,
            node_id=identity.node_id,
            address=advertise_addr,
            incarnation=identity.incarnation,
            settings=membership_settings,
        )
        storage_servicer = StorageNodeServicer(
            config.storage.custody_dir,
            cluster_token=cluster_token,
            commit_workers=config.storage.commit_workers,
            commit_queue_items=config.storage.commit_queue_items,
            max_chunk_size=config.storage.max_chunk_size,
            drain_controller=drain_controller,
        )
        metadata_servicer = MetadataPackServiceServicer(
            config.metadata.custody_pack_store_dir,
            cluster_token=cluster_token,
            max_pack_bytes=config.metadata.max_distributed_pack_bytes,
            max_packs_per_owner=config.metadata.max_distributed_packs_per_owner,
            max_total_bytes_per_owner=config.metadata.max_distributed_pack_bytes_per_owner,
            max_total_store_bytes=config.metadata.max_distributed_pack_store_bytes,
            max_age_days=config.gc.received_metadata_pack_max_age_days,
            max_message_bytes=config.grpc.max_message_bytes,
            drain_controller=drain_controller,
        )

        rpc_executor = futures.ThreadPoolExecutor(
            max_workers=config.storage.rpc_workers,
            thread_name_prefix="node-rpc",
        )
        server = grpc.server(
            rpc_executor,
            options=grpc_server_options(config.grpc.max_message_bytes),
        )

        p2p_storage_pb2_grpc.add_P2PStorageServicer_to_server(storage_servicer, server)
        p2p_storage_pb2_grpc.add_MetadataPackServiceServicer_to_server(metadata_servicer, server)
        membership_pb2_grpc.add_MembershipServicer_to_server(
            MembershipServicer(membership_manager), server
        )

        bound_port = server.add_insecure_port(config.node.bind_addr)
        if bound_port == 0:
            raise StopanNetworkError(
                f"No se pudo abrir el listener gRPC en bind_addr={config.node.bind_addr!r}"
            )

        # Los handlers se instalan solo después de validar y construir todos los
        # recursos necesarios para el arranque. Cualquier fallo posterior entra
        # en el mismo teardown ordenado.
        previous_handlers = _install_shutdown_handlers(
            shutdown_requested,
            drain_controller.request_draining,
        )

        control_server = NodeControlServer(
            controller=drain_controller,
            request_shutdown=shutdown_requested.set,
            node_id=identity.node_id,
            advertise_addr=advertise_addr,
        )
        control_server.start()
        control_started = True
        if shutdown_requested.is_set():
            return

        server.start()
        grpc_started = True
        print(
            f"Nodo {identity.node_id[:8]} inc={identity.incarnation} "
            f"escuchando en {config.node.bind_addr} (advertise={advertise_addr})"
        )
        if shutdown_requested.is_set():
            return

        membership_manager.bootstrap_join(
            config.cluster.seeds,
            cancel_event=shutdown_requested,
        )
        if shutdown_requested.is_set():
            return
        membership_manager.start()

        try:
            while not shutdown_requested.wait(timeout=_SHUTDOWN_POLL_S):
                pass
        except KeyboardInterrupt:
            # Defensa para entornos donde no haya sido posible instalar el
            # handler de SIGINT (por ejemplo, ejecución fuera del hilo principal).
            drain_controller.request_draining()
            shutdown_requested.set()

    finally:
        # La transición y los contadores comparten el mismo cerrojo en
        # NodeDrainController. Tras esta llamada no puede cruzar la barrera una
        # operación local o RPC de datos nueva.
        drain_controller.request_draining()

        if grpc_started:
            print("Drenando nodo Stopan...")

        if membership_manager is not None and grpc_started:
            try:
                # LEFT retira al nodo del conjunto elegible antes de esperar el
                # trabajo existente. El anuncio directo es best-effort y el
                # gossip/failure detection conserva la convergencia si algún par
                # no recibe el aviso.
                membership_manager.leave()
            except Exception as exc:
                _warn_shutdown_failure("el anuncio LEFT", exc)

        if control_started or grpc_started:
            local_ops, active_rpcs = drain_controller.active_work()
            if local_ops or active_rpcs:
                print(
                    "Esperando trabajo en curso: "
                    f"operaciones_locales={local_ops} rpc_datos={active_rpcs}"
                )
            drain_controller.wait_for_idle()

        if server is not None:
            try:
                # add_insecure_port puede haber reservado el listener antes de
                # que server.start() llegue a ejecutarse. stop() también es
                # válido en ese estado y garantiza la liberación determinista
                # de recursos ante fallos parciales de arranque.
                grace_s = _SHUTDOWN_GRACE_S if grpc_started else 0.0
                stop_event = server.stop(grace_s)
                stop_event.wait(timeout=grace_s if grpc_started else None)
            except Exception as exc:
                _warn_shutdown_failure("el cierre del servidor gRPC", exc)

        if rpc_executor is not None:
            try:
                # gRPC puede notificar una cancelación antes de que el handler
                # haya terminado su bloque finally. Se espera primero a todos
                # los handlers del executor para no cerrar repositorios bajo
                # código de servicio que todavía esté desenrollándose.
                rpc_executor.shutdown(wait=True, cancel_futures=False)
            except Exception as exc:
                _warn_shutdown_failure("el cierre del executor RPC", exc)

        if membership_manager is not None:
            try:
                # Los canales de membership se conservan hasta que gRPC ha
                # dejado de aceptar llamadas y sus handlers han terminado. Así
                # un PingReq ya admitido no pierde su canal auxiliar durante el
                # teardown. El bucle SWIM ya quedó detenido por leave().
                membership_manager.stop()
            except Exception as exc:
                _warn_shutdown_failure("el cierre de membership", exc)

        if storage_servicer is not None:
            try:
                # El commit engine tiene su propio executor. Su cierre espera
                # también commits ya aceptados que una RPC cancelada hubiera
                # dejado pendientes.
                storage_servicer.close()
            except Exception as exc:
                _warn_shutdown_failure("el cierre del motor de persistencia", exc)

        if grpc_started:
            print("Nodo Stopan detenido.")

        # Los clientes de `stopan node stop` esperan esta marca. Debe publicarse
        # incluso si alguno de los pasos best-effort del teardown ha fallado.
        drain_controller.mark_stopped()
        if control_server is not None and control_started:
            try:
                control_server.close()
            except Exception as exc:
                _warn_shutdown_failure("el cierre del control local", exc)
        _restore_shutdown_handlers(previous_handlers)
