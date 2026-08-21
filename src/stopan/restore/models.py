"""Modelos de resultado y métricas del flujo de restore."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RestoreRunStats:
    """Contadores acumulados de una ejecución de restore."""

    processed_items: int = 0
    successful_items: int = 0
    directories_created: int = 0
    files_restored: int = 0
    files_failed: int = 0
    chunks_requested: int = 0
    chunks_from_local_cas: int = 0
    chunks_from_local_p2p_cas: int = 0
    chunks_from_remote_replication: int = 0
    chunks_from_ec: int = 0
    chunks_failed: int = 0
    remote_chunks_corrupt: int = 0
    ec_recovery_failures: int = 0
    files_reused: int = 0
    files_resumed: int = 0
    chunks_reused: int = 0
    bytes_reused: int = 0
    bytes_written: int = 0
