from chunker import FileChunker
from database import MetadataDB
import sys


def backup(file_path):
    """
    Cliente de respaldo.
    Divide el archivo en bloques, guarda los bloques únicos y registra una versión.
    """
    chunker = FileChunker()
    storage = MetadataDB()

    print(f"Starting backup for: {file_path}")

    recipe = []
    chunks_new = 0
    chunks_existing = 0

    try:
        for chunk_hash, chunk_data in chunker.chunk_file(file_path):
            recipe.append((chunk_hash, len(chunk_data)))

            is_new = storage.save_chunk(chunk_hash, chunk_data)
            if is_new:
                chunks_new += 1
            else:
                chunks_existing += 1

        version_id = storage.register_version(file_path, recipe)

        print(f"Backup completed: Version {version_id}")
        print(f"Stats: {chunks_new} blocks uploaded, {chunks_existing} reused.")

    except Exception as e:
        print(f"Error creating backup: {e}")

    finally:
        storage.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        backup(sys.argv[1])
    else:
        print("Usage: python backup_client.py <file_path>")
