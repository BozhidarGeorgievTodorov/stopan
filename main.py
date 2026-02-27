import concurrent.futures
import os
import sys
import threading
import time

from core.chunker import FileChunker
from core.database import MetadataDB
from core.repository import CASRepository
from core.scanner import TreeWalker

AVG_CHUNK_SIZE = 1024 * 1024
MIN_CHUNK_SIZE = 512 * 1024
MAX_CHUNK_SIZE = 8 * 1024 * 1024
DEFAULT_WORKERS = 4

_thread_state = threading.local()


def _get_worker_tools():
    if not hasattr(_thread_state, "chunker"):
        _thread_state.chunker = FileChunker(
            avg_chunk_size=AVG_CHUNK_SIZE,
            min_chunk_size=MIN_CHUNK_SIZE,
            max_chunk_size=MAX_CHUNK_SIZE,
        )
        _thread_state.repo = CASRepository()

    return _thread_state.chunker, _thread_state.repo


def _process_file(full_path):
    chunker, repo = _get_worker_tools()
    chunk_rows = []
    chunks_new = 0
    chunks_existing = 0
    total_size = 0

    with open(full_path, 'rb') as f:
        for order, (chunk_hash, chunk_data) in enumerate(chunker.chunk_stream(f)):
            if repo.put(chunk_hash, chunk_data):
                chunks_new += 1
            else:
                chunks_existing += 1

            chunk_rows.append((order, chunk_hash, len(chunk_data)))
            total_size += len(chunk_data)

    return chunk_rows, total_size, chunks_new, chunks_existing


def backup(source_path, workers=DEFAULT_WORKERS):
    """Crea un snapshot de una carpeta."""
    if not os.path.isdir(source_path):
        print(f"Directory not found: {source_path}")
        return

    max_workers = _normalize_worker_count(workers)
    db = MetadataDB()

    print(f"Starting backup for: {source_path}")
    print(f"Workers: {max_workers}")
    start_time = time.perf_counter()

    try:
        root_path = os.path.abspath(source_path)
        snapshot_id = db.create_snapshot(root_path)
        walker = TreeWalker(root_path)

        total_files = 0
        total_size = 0
        chunks_new = 0
        chunks_existing = 0
        pending = {}
        errors = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for rel_path, full_path, stat_info, item_type in walker.walk():
                item_id = db.add_item(snapshot_id, rel_path, stat_info, item_type)

                if item_type != "file":
                    continue

                future = executor.submit(_process_file, full_path)
                pending[future] = (item_id, rel_path, stat_info.st_size)

            for future in concurrent.futures.as_completed(pending):
                item_id, rel_path, file_size = pending[future]

                try:
                    chunk_rows, stored_size, file_new, file_existing = future.result()
                except Exception as exc:
                    errors.append(f"{rel_path}: {exc}")
                    continue

                db_rows = [
                    (item_id, order, chunk_hash, chunk_size)
                    for order, chunk_hash, chunk_size in chunk_rows
                ]
                db.add_chunks_batch(db_rows)

                total_files += 1
                total_size += file_size or stored_size
                chunks_new += file_new
                chunks_existing += file_existing

        if errors:
            raise RuntimeError("Could not read all files: " + "; ".join(errors))

        db.finish_snapshot(snapshot_id, total_size, total_files)
        db.commit()

        elapsed = time.perf_counter() - start_time
        speed = _format_speed(total_size, elapsed)

        print(f"Backup completed: Snapshot {snapshot_id}")
        print(f"Files: {total_files}")
        print(f"Size: {total_size} bytes")
        print(f"Time: {elapsed:.2f} seconds")
        print(f"Speed: {speed}")
        print(f"Stats: {chunks_new} blocks stored, {chunks_existing} reused.")

    except Exception as e:
        db.rollback()
        print(f"Error creating backup: {e}")

    finally:
        db.close()


def _normalize_worker_count(workers):
    cpu_count = os.cpu_count() or DEFAULT_WORKERS
    try:
        requested = int(workers)
    except (TypeError, ValueError):
        requested = DEFAULT_WORKERS

    return max(1, min(requested, cpu_count))


def _format_speed(total_size, elapsed):
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"


def print_usage():
    print("Usage:")
    print("  python main.py backup <source_dir> [workers]")


if __name__ == "__main__":
    if len(sys.argv) in (3, 4) and sys.argv[1] == "backup":
        worker_count = int(sys.argv[3]) if len(sys.argv) == 4 else DEFAULT_WORKERS
        backup(sys.argv[2], worker_count)
    else:
        print_usage()
        sys.exit(1)
