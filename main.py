import os
import sys
import time

from core.chunker import FileChunker
from core.database import MetadataDB
from core.repository import CASRepository
from core.scanner import TreeWalker

AVG_CHUNK_SIZE = 1024 * 1024
MIN_CHUNK_SIZE = 512 * 1024
MAX_CHUNK_SIZE = 8 * 1024 * 1024


def backup(source_path):
    """Crea un snapshot de una carpeta."""
    if not os.path.isdir(source_path):
        print(f"Directory not found: {source_path}")
        return

    repo = CASRepository()
    db = MetadataDB()
    chunker = FileChunker(
        avg_chunk_size=AVG_CHUNK_SIZE,
        min_chunk_size=MIN_CHUNK_SIZE,
        max_chunk_size=MAX_CHUNK_SIZE,
    )

    print(f"Starting backup for: {source_path}")
    start_time = time.perf_counter()

    try:
        root_path = os.path.abspath(source_path)
        snapshot_id = db.create_snapshot(root_path)
        walker = TreeWalker(root_path)

        total_files = 0
        total_size = 0
        chunks_new = 0
        chunks_existing = 0

        for rel_path, full_path, stat_info, item_type in walker.walk():
            item_id = db.add_item(snapshot_id, rel_path, stat_info, item_type)

            if item_type != "file":
                continue

            chunk_rows = []
            with open(full_path, 'rb') as f:
                for order, (chunk_hash, chunk_data) in enumerate(chunker.chunk_stream(f)):
                    is_new = repo.put(chunk_hash, chunk_data)
                    if is_new:
                        chunks_new += 1
                    else:
                        chunks_existing += 1

                    chunk_rows.append((item_id, order, chunk_hash, len(chunk_data)))

            db.add_chunks_batch(chunk_rows)
            total_files += 1
            total_size += stat_info.st_size

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


def _format_speed(total_size, elapsed):
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"


def print_usage():
    print("Usage:")
    print("  python main.py backup <source_dir>")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "backup":
        backup(sys.argv[2])
    else:
        print_usage()
        sys.exit(1)
