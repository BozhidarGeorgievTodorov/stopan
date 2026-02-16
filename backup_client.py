from chunker import FileChunker
from storage import StorageSystem
import sys

def backup(file_path):
    """
    Cliente de respaldo.
    Coordina la división del archivo mediante CDC y el envío de bloques
    únicos al sistema de almacenamiento, registrando la versión final.
    """
    chunker = FileChunker()
    storage = StorageSystem()
    
    print(f"Starting backup for: {file_path}")
    
    recipe = []
    chunks_new = 0
    chunks_existing = 0
    
    for c_hash, c_data in chunker.chunk_file(file_path):
        recipe.append(c_hash) 
        
        is_new = storage.save_chunk(c_hash, c_data)
        
        if is_new:
            chunks_new += 1
        else:
            chunks_existing += 1
            
    version_id = storage.register_version(file_path, recipe)
    
    print(f"Backup completed: Version {version_id}")
    print(f"Stats: {chunks_new} blocks uploaded, {chunks_existing} reused.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        backup(sys.argv[1])
    else:
        print("Usage: python backup_client.py <file_path>")