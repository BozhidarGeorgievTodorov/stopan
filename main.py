import os
import sys
import time

from chunker import FileChunker
from database import MetadataDB
from repository import CASRepository
from scanner import TreeWalker

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


def restore(snapshot_id, output_dir):
    """Reconstruye un snapshot dentro de una carpeta de destino."""
    repo = CASRepository()
    db = MetadataDB()

    print(f"Restoring snapshot {snapshot_id} to {output_dir}")
    start_time = time.perf_counter()

    try:
        items = db.get_snapshot_items(snapshot_id)
        if not items:
            print(f"Snapshot {snapshot_id} not found.")
            return

        output_root = os.path.abspath(output_dir)
        os.makedirs(output_root, exist_ok=True)
        directories = []

        for item in items:
            target_path = _safe_target_path(output_root, item['path'])
            if target_path is None:
                print(f"Skipping unsafe path: {item['path']}")
                continue

            if item['item_type'] == 'dir':
                os.makedirs(target_path, exist_ok=True)
                directories.append((target_path, item))
                continue

            if item['item_type'] == 'file':
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                chunks = db.get_item_chunks(item['id'])

                with open(target_path, 'wb') as f:
                    for chunk_hash in chunks:
                        f.write(repo.get(chunk_hash))

                _restore_file_metadata(target_path, item)
                print(f"Restored: {item['path']}")

        directories.sort(key=lambda pair: len(pair[0]), reverse=True)
        for dir_path, item in directories:
            _restore_file_metadata(dir_path, item)

        elapsed = time.perf_counter() - start_time
        print("Restore completed successfully")
        print(f"Time: {elapsed:.2f} seconds")

    except Exception as e:
        print(f"Error restoring snapshot: {e}")

    finally:
        db.close()


def _safe_target_path(output_root, rel_path):
    if rel_path == ".":
        return output_root

    normalized_path = os.path.normpath(rel_path)
    target_path = os.path.abspath(os.path.join(output_root, normalized_path))

    if os.path.commonpath([output_root, target_path]) != output_root:
        return None

    return target_path


def _restore_file_metadata(path, item):
    if item.get('mtime') is not None:
        os.utime(path, (item['mtime'], item['mtime']))

    if item.get('mode') is not None:
        os.chmod(path, item['mode'])


def _format_speed(total_size, elapsed):
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"


def print_usage():
    print("Usage:")
    print("  python main.py backup <source_dir>")
    print("  python main.py restore <snapshot_id> <output_dir>")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    if command == "backup" and len(sys.argv) == 3:
        backup(sys.argv[2])
    elif command == "restore" and len(sys.argv) == 4:
        restore(int(sys.argv[2]), sys.argv[3])
    else:
        print_usage()
        sys.exit(1)
