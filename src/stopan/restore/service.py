"""
Servicio de alto nivel para ejecutar restore.

Construye las dependencias necesarias para restaurar un snapshot: metadata,
CAS local, CAS P2P local, resolución lazy de cluster, cliente remoto y restorer.
"""

from __future__ import annotations

from stopan.cas.repository import CASRepository
from stopan.metadata.database import MetadataDB
from stopan.restore.cluster import LazyClusterResolver
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.remote_client import RemoteStorageClientPool
from stopan.restore.restorer import RestoreResult, SnapshotRestorer
from stopan.protection.policy import normalize_remote_rf


def restore_snapshot(
    snapshot_id: int,
    *,
    membership_seed: str | None,
    base_output_dir: str,
    rf: int,
    batch_target_parallelism: int,
    prefetch_window: int,
    db_file: str,
    local_shard_dir: str,
    repo_store_dir: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    rpc_timeout_s: float,
    max_message_bytes: int,
    max_chunk_size: int,
) -> RestoreResult:
    """
    Restaura un snapshot preparando todos los servicios necesarios.

    El restore intenta leer primero desde el CAS local principal, después desde
    el CAS P2P local y, si rf > 0, desde targets remotos calculados por HRW.
    """

    db = MetadataDB(db_file)
    remote_pool: RemoteStorageClientPool | None = None

    try:
        origin_node_id = db.get_snapshot_origin_node_id(snapshot_id)
        if not origin_node_id:
            raise RuntimeError(
                f"Snapshot {snapshot_id} no tiene origin_node_id; "
                "el snapshot no cumple el formato distribuido esperado."
            )

        repo = CASRepository(local_shard_dir)
        p2p_local_repo = CASRepository(repo_store_dir)
        remote_rf = normalize_remote_rf(rf)
        remote_pool = RemoteStorageClientPool(
            timeout_s=rpc_timeout_s,
            max_message_bytes=max_message_bytes,
        )
        cluster_resolver = LazyClusterResolver(
            membership_seed=membership_seed,
            rf=remote_rf,
            origin_node_id=origin_node_id,
            self_addr=self_addr,
            cluster_token=cluster_token,
            membership_timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
        )
        fetch_service = ChunkFetchService(
            repo=repo,
            p2p_local_repo=p2p_local_repo,
            cluster_resolver=cluster_resolver,
            remote_pool=remote_pool,
            rf=remote_rf,
            cluster_token=cluster_token,
            max_chunk_size=max_chunk_size,
        )
        restorer = SnapshotRestorer(
            db=db,
            fetch_service=fetch_service,
            base_output_dir=base_output_dir,
            batch_target_parallelism=batch_target_parallelism,
            prefetch_window=prefetch_window,
        )

        return restorer.restore(snapshot_id)

    finally:
        if remote_pool is not None:
            remote_pool.close()
        db.close()
