"""
Servicio de alto nivel para ejecutar restore.

Construye las dependencias necesarias para restaurar un snapshot: metadata,
CAS local, CAS P2P local, resolución lazy de cluster, cliente remoto y restorer.
"""

from __future__ import annotations

from stopan.cas.repository import CASRepository
from stopan.metadata.database import MetadataDB, MetadataDBAccessMode
from stopan.restore.cluster import LazyClusterResolver
from stopan.protection.ec.remote_client import RemoteDataPackShardClientPool
from stopan.restore.ec_fetch import ErasureChunkRecoveryService
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.remote_client import RemoteStorageClientPool
from stopan.restore.restorer import RestoreResult, SnapshotRestorer
from stopan.restore.errors import RestoreDataError
from stopan.protection.policy import normalize_remote_rf


_REMOTE_RECOVERY_MODES = {"none", "replication", "ec", "auto"}


def restore_snapshot(
    snapshot_id: int,
    *,
    membership_seed: str | None,
    base_output_dir: str,
    rf: int,
    batch_target_parallelism: int,
    prefetch_window: int,
    db_file: str,
    local_chunk_dir: str,
    custody_chunk_dir: str,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    rpc_timeout_s: float,
    max_message_bytes: int,
    max_chunk_size: int,
    remote_recovery: str = "replication",
) -> RestoreResult:
    """
    Restaura un snapshot preparando todos los servicios necesarios.

    El restore intenta leer primero desde el CAS local principal, después desde
    el CAS P2P local y, según el modo de remote_recovery, desde réplicas remota HRW
    o mediante reconstrucción por borrado (Erasure Coding).
    """
    remote_recovery = _normalize_remote_recovery(remote_recovery)
    use_remote_chunks = remote_recovery in {"replication", "auto"}
    use_ec_recovery = remote_recovery in {"ec", "auto"}

    db = MetadataDB(
        db_file,
        init_schema=False,
        access_mode=MetadataDBAccessMode.READ_ONLY,
    )
    remote_pool: RemoteStorageClientPool | None = None
    ec_remote_pool: RemoteDataPackShardClientPool | None = None
    cluster_resolver: LazyClusterResolver | None = None
    ec_recovery_service: ErasureChunkRecoveryService | None = None

    try:
        origin_node_id = db.get_snapshot_origin_node_id(snapshot_id)
        if not origin_node_id:
            raise RestoreDataError(
                f"Snapshot {snapshot_id} no tiene origin_node_id; "
                "el snapshot no cumple el formato distribuido esperado."
            )

        repo = CASRepository(local_chunk_dir)
        p2p_local_repo = CASRepository(custody_chunk_dir)
        cluster_rf = normalize_remote_rf(rf)

        if use_remote_chunks or use_ec_recovery:
            cluster_resolver = LazyClusterResolver(
                membership_seed=membership_seed,
                rf=cluster_rf,
                origin_node_id=origin_node_id,
                self_addr=self_addr,
                cluster_token=cluster_token,
                membership_timeout_s=membership_timeout_s,
                max_message_bytes=max_message_bytes,
            )

        if use_remote_chunks:
            remote_pool = RemoteStorageClientPool(
                cluster_token=cluster_token,
                timeout_s=rpc_timeout_s,
                max_message_bytes=max_message_bytes,
            )

        if use_ec_recovery:
            ec_remote_pool = RemoteDataPackShardClientPool(
                cluster_token=cluster_token,
                timeout_s=rpc_timeout_s,
                max_message_bytes=max_message_bytes,
            )
            ec_recovery_service = ErasureChunkRecoveryService(
                db=db,
                repo=repo,
                remote_pool=ec_remote_pool,
                cluster_resolver=cluster_resolver,
            )
        fetch_service = ChunkFetchService(
            repo=repo,
            p2p_local_repo=p2p_local_repo,
            cluster_resolver=cluster_resolver,
            remote_pool=remote_pool,
            rf=cluster_rf,
            cluster_token=cluster_token,
            max_chunk_size=max_chunk_size,
            ec_recovery_service=ec_recovery_service,
            remote_chunk_recovery=use_remote_chunks,
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
        if ec_remote_pool is not None:
            ec_remote_pool.close()
        db.close()


def _normalize_remote_recovery(value: str) -> str:
    mode = value.strip().lower()
    if mode not in _REMOTE_RECOVERY_MODES:
        raise RestoreDataError(
            "remote_recovery debe ser one of: " + ", ".join(sorted(_REMOTE_RECOVERY_MODES))
        )
    return mode
