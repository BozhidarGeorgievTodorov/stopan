from __future__ import annotations

import os
from concurrent import futures

import grpc

from stopan.node.identity import NodeIdentityStore
from stopan.node.membership import MembershipManager, MembershipServicer
from stopan.node.storage_rpc import StorageNodeServicer
from stopan.protos import membership_pb2_grpc
from stopan.protos import p2p_storage_pb2_grpc


GRPC_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))
STORAGE_RPC_WORKERS = int(os.getenv("STORAGE_RPC_WORKERS", "64"))

ADVERTISE_ADDR = os.getenv("ADVERTISE_ADDR", "")
BIND_ADDR = os.getenv("BIND_ADDR", "[::]:50051")
REPO_STORE_DIR = os.getenv("REPO_STORE_DIR", "node_store")
SEEDS = [seed.strip() for seed in os.getenv("SEEDS", "").split(",") if seed.strip()]


def serve() -> None:
    if not ADVERTISE_ADDR:
        raise RuntimeError("ADVERTISE_ADDR is required, for example node1:50051.")

    identity_store = NodeIdentityStore(REPO_STORE_DIR)
    identity = identity_store.load_for_startup()

    membership_manager = MembershipManager(
        identity_store=identity_store,
        node_id=identity.node_id,
        address=ADVERTISE_ADDR,
        incarnation=identity.incarnation,
    )
    storage_servicer = StorageNodeServicer(REPO_STORE_DIR)

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

    p2p_storage_pb2_grpc.add_P2PStorageServicer_to_server(storage_servicer, server)
    membership_pb2_grpc.add_MembershipServicer_to_server(
        MembershipServicer(membership_manager),
        server,
    )

    server.add_insecure_port(BIND_ADDR)
    server.start()
    print(
        f"Node {identity.node_id[:8]} inc={identity.incarnation} "
        f"listening on {BIND_ADDR} (advertise={ADVERTISE_ADDR})"
    )

    membership_manager.bootstrap_join(SEEDS)
    membership_manager.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        membership_manager.stop()
        storage_servicer.close()
        server.stop(0)


if __name__ == "__main__":
    serve()
