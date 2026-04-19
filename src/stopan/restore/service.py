from __future__ import annotations

from stopan.cas.repository import CASRepository
from stopan.metadata.database import MetadataDB
from stopan.restore.cluster import LazyClusterResolver
from stopan.restore.config import DB_FILE, DEFAULT_RPC_TIMEOUT_S, LOCAL_SHARD_DIR, REPO_STORE_DIR
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.remote_client import RemoteStorageClientPool
from stopan.restore.restorer import SnapshotRestorer


def restore_snapshot(
    snapshot_id: int,
    *,
    seed: str | None,
    base_output_dir: str,
    rf: int,
    batch_target_parallelism: int,
    prefetch_window: int,
) -> None:
    db = MetadataDB(DB_FILE)
    remote_pool: RemoteStorageClientPool | None = None

    try:
        origin_node_id = db.get_snapshot_origin_node_id(snapshot_id)
        if not origin_node_id:
            raise RuntimeError(
                f"Snapshot {snapshot_id} has no origin_node_id; remote placement cannot be reconstructed."
            )

        repo = CASRepository(LOCAL_SHARD_DIR)
        p2p_local_repo = CASRepository(REPO_STORE_DIR)
        remote_pool = RemoteStorageClientPool(timeout_s=DEFAULT_RPC_TIMEOUT_S)
        cluster_resolver = LazyClusterResolver(
            seed=seed,
            rf=rf,
            origin_node_id=origin_node_id,
        )
        fetch_service = ChunkFetchService(
            repo=repo,
            p2p_local_repo=p2p_local_repo,
            cluster_resolver=cluster_resolver,
            remote_pool=remote_pool,
            rf=rf,
        )
        restorer = SnapshotRestorer(
            db=db,
            fetch_service=fetch_service,
            base_output_dir=base_output_dir,
            batch_target_parallelism=batch_target_parallelism,
            prefetch_window=prefetch_window,
        )

        restorer.restore(snapshot_id)

    finally:
        db.close()
        if remote_pool is not None:
            remote_pool.close()
