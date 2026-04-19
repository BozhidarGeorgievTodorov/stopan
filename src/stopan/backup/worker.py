from __future__ import annotations

import hashlib
import threading

from stopan.backup.config import DB_FILE, LOCAL_SHARD_DIR
from stopan.backup.models import WorkerStats
from stopan.cas.repository import CASRepository
from stopan.chunking.chunker import FileChunker
from stopan.chunking.planner import ChunkPlanner
from stopan.metadata.database import MetadataDB


_thread_local = threading.local()


def compute_recipe_hash(chunks: list[tuple[int, str, int]]) -> str:
    """Calcula un hash estable para una receta ordenada de chunks."""
    digest = hashlib.sha256()
    for order, chunk_hash, size in chunks:
        digest.update(order.to_bytes(4, "big", signed=False))
        digest.update(bytes.fromhex(chunk_hash))
        digest.update(size.to_bytes(8, "big", signed=False))
    return digest.hexdigest()


def get_thread_local_tools():
    if not hasattr(_thread_local, "chunker"):
        _thread_local.chunker = FileChunker()
        _thread_local.repo = CASRepository(LOCAL_SHARD_DIR)
        _thread_local.db_ro = MetadataDB(DB_FILE, init_schema=False)

    return _thread_local.chunker, _thread_local.repo, _thread_local.db_ro


def process_file_worker(
    full_path: str,
    *,
    fast_local_enabled: bool,
    fast_remote_enabled: bool,
    safe_mode: bool,
    shared_index,
    desired_rf: int,
    current_placement_epoch: str | None,
):
    chunker, repo, db_ro = get_thread_local_tools()

    planner = ChunkPlanner(
        repo,
        db_ro,
        index=shared_index,
        fast_path_enabled=fast_local_enabled,
        safe_mode=safe_mode,
        allow_remote_protected_skip=fast_remote_enabled,
        desired_rf=desired_rf,
        current_placement_epoch=current_placement_epoch,
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
                    raise RuntimeError(f"Unknown planner decision: {decision}")

                chunks.append((order, chunk_hash, chunk_size))

        return (
            True,
            chunks,
            file_size,  
            compute_recipe_hash(chunks),
            WorkerStats(
                chunks_total=chunks_total,
                chunks_processed=processed,
                chunks_skipped=skipped,
                chunks_skipped_local=skipped_local,
                chunks_skipped_remote=skipped_remote,
                chunks_written=written,
            ),
        )

    except Exception as exc:
        return False, str(exc), 0, None, WorkerStats()
