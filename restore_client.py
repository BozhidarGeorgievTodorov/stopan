from storage import StorageSystem
import sys

def restore(filename, version, output_path):
    """
    Cliente de recuperación.
    Reconstruye un archivo original solicitando los bloques al almacén local
    en el orden especificado por la receta de la versión.
    """
    storage = StorageSystem()
    print(f"Restoring {filename} ({version}) to {output_path}")
    
    try:
        recipe = storage.get_recipe(filename, version)
        
        with open(output_path, 'wb') as f:
            for c_hash in recipe:
                data = storage.get_chunk(c_hash)
                f.write(data)
                
        print("Restore completed successfully")
        
    except Exception as e:
        print(f"Error restoring file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 3:
        restore(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print("Usage: python restore_client.py <original_file> <version> <output_file>")