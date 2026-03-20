import argparse
import os
import time

from core.cluster_view import ClusterMembershipClient
from core.database import MetadataDB
from core.replication import ReplicationCoordinator
from core.repository import CASRepository


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")

DEFAULT_RPC_TIMEOUT_S = float(os.getenv("REPLICATION_RPC_TIMEOUT_S", "10.0"))
DEFAULT_CHUNK_WORKERS = int(os.getenv("REPLICATION_CHUNK_WORKERS", "8"))
DEFAULT_TARGET_WORKERS = int(os.getenv("REPLICATION_TARGET_WORKERS", "4"))
DEFAULT_TARGET_INFLIGHT = int(os.getenv("REPLICATION_TARGET_INFLIGHT", "32"))
DEFAULT_MAX_PENDING_CHUNKS = int(os.getenv("REPLICATION_MAX_PENDING_CHUNKS", "128"))
DEFAULT_COMMIT_EVERY = int(os.getenv("REPLICATION_COMMIT_EVERY", "100"))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))


def push_to_network(
    seed: str,
    *,
    rf: int,
    limit: int | None = None,
    chunk_workers: int = DEFAULT_CHUNK_WORKERS,
    target_workers: int = DEFAULT_TARGET_WORKERS,
    target_inflight: int = DEFAULT_TARGET_INFLIGHT,
    max_pending_chunks: int = DEFAULT_MAX_PENDING_CHUNKS,
    rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
    commit_every: int = DEFAULT_COMMIT_EVERY,
):
    repo = CASRepository(LOCAL_SHARD_DIR)
    db = MetadataDB(DB_FILE)

    protected = 0
    attempted = 0
    failed = 0
    pending_chunks = []
    coordinator = None
    start_time = time.perf_counter()
    current_epoch = "unknown"

    try:
        self_addr = os.getenv("ADVERTISE_ADDR", "")
        cluster = ClusterMembershipClient(seed, self_addr=self_addr).get_cluster_view()

        if not cluster.members:
            print(f"No eligible members returned by seed {seed}.")
            return

        current_epoch = cluster.placement_epoch(
            desired_rf=rf,
            cluster_token=CLUSTER_TOKEN,
        )
        db.mark_stale_protection(desired_rf=rf, current_epoch=current_epoch)
        pending_chunks = db.get_pending_protection_chunks(
            desired_rf=rf,
            current_epoch=current_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print("No chunks pending for the current protection policy.")
            return

        coordinator = ReplicationCoordinator(
            repo=repo,
            cluster=cluster,
            rf=rf,
            cluster_token=CLUSTER_TOKEN,
            rpc_timeout_s=rpc_timeout_s,
            chunk_workers=chunk_workers,
            target_workers=target_workers,
            target_inflight=target_inflight,
            max_pending_chunks=max_pending_chunks,
            max_message_bytes=DEFAULT_MAX_MESSAGE_BYTES,
        )

        print(f"Push: {len(pending_chunks)} chunks pending for protection")
        print(f"Eligible members: {[f'{m.node_id[:8]}@{m.address}' for m in cluster.members]}")
        if cluster.self_node_id:
            print(f"Self: {cluster.self_node_id[:8]}@{self_addr}")
        print(f"RF targets: {min(max(rf, 1), len(cluster.members))}")
        print(f"placement_epoch={current_epoch[:12]}")
        print(
            f"Pipeline: chunk_workers={chunk_workers} target_workers={target_workers} "
            f"target_inflight={target_inflight} max_pending_chunks={max_pending_chunks}"
        )

        dirty = 0
        for outcome in coordinator.replicate_chunks(pending_chunks):
            attempted += 1
            if outcome.success:
                db.mark_chunk_placed(
                    outcome.chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=outcome.protected_remote_copies,
                    placement_epoch=current_epoch,
                )
                protected += 1
            else:
                db.mark_chunk_failed(
                    outcome.chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=outcome.protected_remote_copies,
                    placement_epoch=current_epoch,
                    error=outcome.error or "replication failed",
                )
                failed += 1
                print(f"   {outcome.chunk_hash[:8]} failed: {outcome.error}")

            dirty += 1
            if dirty >= max(1, commit_every):
                db.commit()
                dirty = 0

    except KeyboardInterrupt:
        print("\nPush interrupted by user.")

    finally:
        if coordinator is not None:
            coordinator.close()

        db.commit()
        db.close()

        elapsed = time.perf_counter() - start_time
        speed = protected / elapsed if elapsed > 0 else 0

        print("-" * 40)
        print(f"Push finished in {elapsed:.2f} seconds.")
        print(f"Protection policy: RF={rf} (epoch={current_epoch[:12]})")
        print(f"Chunks protected: {protected}/{len(pending_chunks)} pending")
        print(f"Attempted/failed: {attempted}/{failed}")
        if protected > 0:
            print(f"Average speed: {speed:.2f} chunks/second")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    push_parser = sub.add_parser("push")
    push_parser.add_argument("--seed", default="node1:50051", help="Seed de membership, por ejemplo node1:50051")
    push_parser.add_argument("--rf", type=int, default=int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3"))), help="Factor de réplica usado para calcular targets HRW")
    push_parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución")
    push_parser.add_argument("--chunk-workers", type=int, default=DEFAULT_CHUNK_WORKERS, help="Paralelismo a nivel de chunks")
    push_parser.add_argument("--target-workers", type=int, default=DEFAULT_TARGET_WORKERS, help="Paralelismo por nodo destino")
    push_parser.add_argument("--target-inflight", type=int, default=DEFAULT_TARGET_INFLIGHT, help="Máximo de chunks en vuelo por nodo destino")
    push_parser.add_argument("--max-pending-chunks", type=int, default=DEFAULT_MAX_PENDING_CHUNKS, help="Máximo de chunks concurrentes dentro del coordinador")
    push_parser.add_argument("--rpc-timeout", type=float, default=DEFAULT_RPC_TIMEOUT_S, help="Timeout de cada llamada StoreChunk")
    push_parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY, help="Guardar progreso cada N resultados")

    args = parser.parse_args()

    if args.cmd == "push":
        push_to_network(
            seed=args.seed,
            rf=args.rf,
            limit=args.limit,
            chunk_workers=args.chunk_workers,
            target_workers=args.target_workers,
            target_inflight=args.target_inflight,
            max_pending_chunks=args.max_pending_chunks,
            rpc_timeout_s=args.rpc_timeout,
            commit_every=args.commit_every,
        )
