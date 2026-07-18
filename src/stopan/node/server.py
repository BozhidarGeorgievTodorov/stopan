"""
Arranque del nodo Stopan.

Inicializa identidad persistente, membership, almacenamiento P2P, servicio de
metadata distribuida y servidor gRPC del nodo.
"""

from __future__ import annotations

import signal
import threading
from concurrent import futures
from pathlib import Path

import grpc

from stopan.config.model import StopanConfig
from stopan.errors import StopanConfigError, StopanNetworkError
from stopan.protos import membership_pb2_grpc
from stopan.protos import p2p_storage_pb2_grpc
from stopan.rpc.options import grpc_server_options

from .identity import NodeIdentityStore
from .membership import MembershipManager, MembershipSettings
from .services import MembershipServicer, MetadataPackServiceServicer, StorageNodeServicer


_SHUTDOWN_GRACE_S = 5.0
_SHUTDOWN_POLL_S = 0.5


def _install_shutdown_handlers(stop_event: threading.Event):
    """Instala handlers SIGINT/SIGTERM para solicitar parada ordenada."""

    previous_handlers = {}

    def request_shutdown(_signum, _frame):
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


def _resolve_metadata_pack_store_dir(config: StopanConfig) -> str:
    """Resuelve la ruta del almacén distribuido de metadata packs."""

    path = Path(config.metadata.distributed_pack_store_dir).expanduser()
    if path.is_absolute():
        return str(path)
    return str(Path(config.node.repo_store_dir).expanduser().resolve() / path)


def serve(config: StopanConfig) -> None:
    """Arranca el nodo Stopan y bloquea hasta recibir señal de parada."""
    
    advertise_addr = config.node.advertise_addr.strip()
    if not advertise_addr:
        raise StopanConfigError("Falta node.advertise_addr, por ejemplo node1:50051.")

    identity_store = NodeIdentityStore(config.node.repo_store_dir)
    identity = identity_store.load_for_startup()

    membership_settings = MembershipSettings(
        cluster_token=config.cluster.token,
        protocol_period_s=config.membership.protocol_period_s,
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
        config.node.repo_store_dir,
        cluster_token=config.cluster.token,
        commit_workers=config.storage.commit_workers,
        commit_queue_items=config.storage.commit_queue_items,
        max_chunk_size=config.storage.max_chunk_size,
    )
    metadata_servicer = MetadataPackServiceServicer(
        _resolve_metadata_pack_store_dir(config),
        cluster_token=config.cluster.token,
        max_pack_bytes=config.metadata.max_distributed_pack_bytes,
        max_packs_per_owner=config.metadata.max_distributed_packs_per_owner,
        max_total_bytes_per_owner=config.metadata.max_distributed_pack_bytes_per_owner,
        max_total_store_bytes=config.metadata.max_distributed_pack_store_bytes,
        max_age_days=config.gc.received_metadata_pack_max_age_days,
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
    membership_pb2_grpc.add_MembershipServicer_to_server(MembershipServicer(membership_manager), server)

    bound_port = server.add_insecure_port(config.node.bind_addr)
    if bound_port == 0:
        raise StopanNetworkError(f"No se pudo abrir el listener gRPC en bind_addr={config.node.bind_addr!r}")

    shutdown_requested = threading.Event()
    previous_handlers = _install_shutdown_handlers(shutdown_requested)

    server.start()
    print(
        f"Nodo {identity.node_id[:8]} inc={identity.incarnation} "
        f"escuchando en {config.node.bind_addr} (advertise={advertise_addr})"
    )

    membership_manager.bootstrap_join(config.cluster.seeds)
    membership_manager.start()

    try:
        while not shutdown_requested.wait(timeout=_SHUTDOWN_POLL_S):
            pass
    except KeyboardInterrupt:
        shutdown_requested.set()
    finally:
        print("Deteniendo nodo Stopan...")

        try:
            membership_manager.stop()
            stop_event = server.stop(_SHUTDOWN_GRACE_S)
            stop_event.wait(timeout=_SHUTDOWN_GRACE_S)
            storage_servicer.close()
            rpc_executor.shutdown(wait=True, cancel_futures=False)
        finally:
            _restore_shutdown_handlers(previous_handlers)
