import os
import sys

from chunker import FileChunker
from database import MetadataDB
from repository import CASRepository


def backup(file_path):
    """Crea un nuevo backup para un archivo."""
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    repo = CASRepository()
    db = MetadataDB()
    chunker = FileChunker()

    print(f"Starting backup for: {file_path}")

    try:
        stat_info = os.stat(file_path)
        version_id, version_number = db.create_version(file_path, stat_info)

        chunks_new = 0
        chunks_existing = 0
        total_size = 0

        with open(file_path, 'rb') as f:
            for order, (chunk_hash, chunk_data) in enumerate(chunker.chunk_stream(f)):
                is_new = repo.put(chunk_hash, chunk_data)
                if is_new:
                    chunks_new += 1
                else:
                    chunks_existing += 1

                chunk_size = len(chunk_data)
                total_size += chunk_size
                db.add_chunk_to_version(version_id, order, chunk_hash, chunk_size)

        db.commit()

        print(f"Backup completed: Version {version_number}")
        print(f"Stats: {chunks_new} blocks stored, {chunks_existing} reused.")
        print(f"Size: {total_size} bytes")

    except Exception as e:
        db.rollback()
        print(f"Error creating backup: {e}")

    finally:
        db.close()


def restore(filename, version, output_path):
    """Reconstruye una versión de un archivo."""
    repo = CASRepository()
    db = MetadataDB()

    print(f"Restoring {filename} ({version}) to {output_path}")

    try:
        metadata, recipe = db.get_version(filename, version)
        if metadata is None:
            print(f"Version {version} of {filename} not found.")
            return

        with open(output_path, 'wb') as f:
            for chunk_hash in recipe:
                data = repo.get(chunk_hash)
                f.write(data)

        if metadata['mtime'] is not None:
            os.utime(output_path, (metadata['mtime'], metadata['mtime']))

        if metadata['mode'] is not None:
            os.chmod(output_path, metadata['mode'])

        print("Restore completed successfully")

    except Exception as e:
        print(f"Error restoring file: {e}")

    finally:
        db.close()


def print_usage():
    print("Usage:")
    print("  python main.py backup <file_path>")
    print("  python main.py restore <original_file> <version> <output_file>")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    if command == "backup" and len(sys.argv) == 3:
        backup(sys.argv[2])
    elif command == "restore" and len(sys.argv) == 5:
        restore(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print_usage()
        sys.exit(1)
