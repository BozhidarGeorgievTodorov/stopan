import argparse
import os
import time

from core.cluster_view import ClusterMembershipClient
from core.database import MetadataDB
from core.replication import (
    BatchReplicationCoordinator,
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_PROBE_BATCH_HASHES,
    DEFAULT_RPC_TIMEOUT_S,
    DEFAULT_STORE_BATCH_BYTES,
    DEFAULT_STORE_BATCH_ITEMS,
    DEFAULT_TARGET_PARALLELISM,
)
from core.repository import CASRepository


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_COMMIT_EVERY = int(os.getenv("REPLICATION_COMMIT_EVERY", "100"))


def push_to_network(
    seed: str,
    *,
    rf: int,
    limit: int | None = None,
    target_parallelism: int = DEFAULT_TARGET_PARALLELISM,
    probe_batch_hashes: int = DEFAULT_PROBE_BATCH_HASHES,
    store_batch_items: int = DEFAULT_STORE_BATCH_ITEMS,
    store_batch_bytes: int = DEFAULT_STORE_BATCH_BYTES,
    rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    commit_every: int = DEFAULT_COMMIT_EVERY,
):
    repo = CASRepository(LOCAL_SHARD_DIR)
    db = MetadataDB(DB_FILE)
    coordinator = None

    protected = 0
    failed = 0
    attempted = 0
    stored_remote = 0
    already_present_remote = 0
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

        current_epoch = cluster.placement_epoch(desired_rf=rf, cluster_token=CLUSTER_TOKEN)
        db.mark_stale_protection(desired_rf=rf, current_epoch=current_epoch)
        pending_chunks = db.get_pending_protection_chunks(
            desired_rf=rf,
            current_epoch=current_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print("No chunks pending for the current protection policy.")
            return

        coordinator = BatchReplicationCoordinator(
            repo=repo,
            cluster=cluster,
            rf=rf,
            cluster_token=CLUSTER_TOKEN,
            rpc_timeout_s=rpc_timeout_s,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            store_batch_items=store_batch_items,
            store_batch_bytes=store_batch_bytes,
            max_message_bytes=max_message_bytes,
        )

        print(f"Push batch: {len(pending_chunks)} chunks pending for protection")
        print(f"Eligible members: {[f'{m.node_id[:8]}@{m.address}' for m in cluster.members]}")
        if cluster.self_node_id:
            print(f"Self: {cluster.self_node_id[:8]}@{self_addr}")
        print(f"RF targets: {min(max(rf, 1), len(cluster.members))}")
        print(f"placement_epoch={current_epoch[:12]}")
        print(
            f"Pipeline: target_parallelism={target_parallelism} "
            f"probe_batch_hashes={probe_batch_hashes} "
            f"store_batch_items={store_batch_items} "
            f"store_batch_bytes={store_batch_bytes}"
        )

        for outcome in coordinator.replicate_chunks(pending_chunks):
            attempted += 1
            stored_remote += outcome.stored_remote_copies
            already_present_remote += outcome.already_present_remote_copies

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
                print(
                    f"   {outcome.chunk_hash[:8]} failed: "
                    f"protected={outcome.protected_remote_copies}/{outcome.required_remote_copies} "
                    f"error={outcome.error}"
                )

            if attempted % max(1, int(commit_every)) == 0:
                db.commit()
                print(
                    f"   progress {attempted}/{len(pending_chunks)} | "
                    f"protected={protected} | failed={failed} | "
                    f"stored_remote={stored_remote} | "
                    f"already_present_remote={already_present_remote}"
                )

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
        print(
            f"Push finished in {elapsed:.2f} seconds. "
            f"protected={protected}/{len(pending_chunks)} | failed={failed} | "
            f"attempted={attempted} | stored_remote={stored_remote} | "
            f"already_present_remote={already_present_remote}"
        )
        if protected > 0:
            print(f"Average speed: {speed:.2f} chunks/second")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    push_parser = sub.add_parser("push")
    push_parser.add_argument("--seed", default="node1:50051", help="Seed de membership, por ejemplo node1:50051")
    push_parser.add_argument("--rf", type=int, default=int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3"))), help="Factor de réplica usado para calcular targets HRW")
    push_parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución")
    push_parser.add_argument("--target-parallelism", type=int, default=DEFAULT_TARGET_PARALLELISM, help="Número de nodos destino procesados en paralelo")
    push_parser.add_argument("--probe-batch-hashes", type=int, default=DEFAULT_PROBE_BATCH_HASHES, help="Hashes por lote de inventario remoto")
    push_parser.add_argument("--store-batch-items", type=int, default=DEFAULT_STORE_BATCH_ITEMS, help="Máximo de chunks por lote de escritura")
    push_parser.add_argument("--store-batch-bytes", type=int, default=DEFAULT_STORE_BATCH_BYTES, help="Máximo de bytes por lote de escritura")
    push_parser.add_argument("--rpc-timeout", type=float, default=DEFAULT_RPC_TIMEOUT_S, help="Timeout RPC en segundos")
    push_parser.add_argument("--max-message-bytes", type=int, default=DEFAULT_MAX_MESSAGE_BYTES, help="Límite de mensaje gRPC")
    push_parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY, help="Guardar progreso cada N resultados")

    args = parser.parse_args()

    if args.cmd == "push":
        push_to_network(
            seed=args.seed,
            rf=args.rf,
            limit=args.limit,
            target_parallelism=args.target_parallelism,
            probe_batch_hashes=args.probe_batch_hashes,
            store_batch_items=args.store_batch_items,
            store_batch_bytes=args.store_batch_bytes,
            rpc_timeout_s=args.rpc_timeout,
            max_message_bytes=args.max_message_bytes,
            commit_every=args.commit_every,
        )
