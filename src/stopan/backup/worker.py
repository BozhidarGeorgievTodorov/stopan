"""
Worker de procesamiento de archivos durante backup.

Cada worker mantiene herramientas thread-local para evitar recrear chunker,
repositorio CAS y conexión SQLite de solo lectura en cada archivo procesado.
"""

from __future__ import annotations

import os
import threading

from .models import WorkerStats
from stopan.cas.repository import CASRepository
from stopan.chunking.chunker import FileChunker
from stopan.chunking.planner import ChunkPlanner
from stopan.chunking.recipes import compute_recipe_hash
from stopan.metadata.database import MetadataDB, MetadataDBAccessMode


_thread_local = threading.local()


def get_thread_local_tools(*, local_shard_dir: str, db_file: str):
    """
    Devuelve herramientas reutilizables por hilo.

    Si cambia el CAS o la base de metadata, se cierra la conexión anterior y se
    inicializa un nuevo conjunto de herramientas para ese hilo.
    """
    key = (os.path.abspath(local_shard_dir), os.path.abspath(db_file))
    if getattr(_thread_local, "tools_key", None) != key:
        old_db = getattr(_thread_local, "db_ro", None)
        if old_db is not None:
            try:
                old_db.close()
            except Exception:
                pass

        _thread_local.tools_key = key
        _thread_local.chunker = FileChunker()
        _thread_local.repo = CASRepository(local_shard_dir)
        _thread_local.db_ro = MetadataDB(
            db_file,
            init_schema=False,
            access_mode=MetadataDBAccessMode.READ_ONLY,
        )

    return _thread_local.chunker, _thread_local.repo, _thread_local.db_ro


def process_file_worker(
    full_path: str,
    *,
    local_shard_dir: str,
    db_file: str,
    fast_local_enabled: bool,
    fast_remote_enabled: bool,
    safe_mode: bool,
    shared_index,
    desired_rf: int,
    placement_epoch: str | None,
):
    """
    Procesa un archivo y devuelve su recipe junto con estadísticas de chunks.

    El worker no escribe metadata del snapshot. Solo materializa chunks cuando
    la política lo requiere y devuelve la información necesaria para que el hilo
    coordinador actualice SQLite.
    """
    chunker, repo, db_ro = get_thread_local_tools(
        local_shard_dir=local_shard_dir,
        db_file=db_file,
    )

    planner = ChunkPlanner(
        repo,
        db_ro,
        index=shared_index,
        fast_path_enabled=fast_local_enabled,
        safe_mode=safe_mode,
        allow_remote_protected_skip=fast_remote_enabled,
        desired_rf=desired_rf,
        placement_epoch=placement_epoch,
    )

    chunks: list[tuple[int, str, int]] = []
    file_size = 0
    chunks_total = 0
    processed = 0
    skipped = 0
    skipped_local = 0
    skipped_remote = 0
    written = 0

    try:
        with open(full_path, "rb") as handle:
            for order, (chunk_hash, chunk_data) in enumerate(chunker.chunk_stream(handle)):
                chunk_size = len(chunk_data)
                file_size += chunk_size
                chunks_total += 1

                decision = planner.decide(chunk_hash)

                if decision == "process":
                    processed += 1
                    if repo.put(chunk_hash, chunk_data):
                        written += 1
                        shared_index.local_exists.set(chunk_hash, True)

                elif decision == "skip_local":
                    skipped += 1
                    skipped_local += 1

                elif decision == "skip_synced":
                    skipped += 1
                    skipped_remote += 1

                else:
                    raise RuntimeError(f"Decisión desconocida del planner: {decision}")

                chunks.append((order, chunk_hash, chunk_size))

        return (
            True,
            chunks,
            file_size,
            compute_recipe_hash(chunks),
            WorkerStats(
                chunks_total=chunks_total,
                processed=processed,
                skipped=skipped,
                skipped_local=skipped_local,
                skipped_remote=skipped_remote,
                written=written,
            ),
        )

    except Exception as exc:
        return False, str(exc), 0, None, WorkerStats()
