import os
import json

DATA_FOLDER = "_data_chunks"
DB_FILE = "_metadata.json"

class StorageSystem:
    """
    Gestor de almacenamiento local que simula el comportamiento de un nodo.
    Implementa un esquema de Content-Addressable Storage (CAS), guardando
    la información indexada por su hash para garantizar la deduplicación.
    """
    def __init__(self):
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)
        
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'r') as f:
                self.db = json.load(f)
        else:
            self.db = {} 

    def save_chunk(self, chunk_hash, chunk_data):
        """Guarda un chunk en disco solo si su hash no existe."""
        path = os.path.join(DATA_FOLDER, chunk_hash)
        
        if os.path.exists(path):
            return False 
        
        with open(path, 'wb') as f:
            f.write(chunk_data)
        return True 

    def register_version(self, filename, recipe):
        """Guarda la lista ordenada de hashes (receta) que componen una versión del archivo."""
        if filename not in self.db:
            self.db[filename] = {}
        
        next_version = str(len(self.db[filename]) + 1)
        self.db[filename][next_version] = recipe
        
        with open(DB_FILE, 'w') as f:
            json.dump(self.db, f, indent=4)
        
        return next_version

    def get_chunk(self, chunk_hash):
        """Recupera los bytes crudos de un chunk específico desde el disco."""
        path = os.path.join(DATA_FOLDER, chunk_hash)
        if not os.path.exists(path):
            raise ValueError(f"Missing block: {chunk_hash}")
        
        with open(path, 'rb') as f:
            return f.read()

    def get_recipe(self, filename, version):
        """Devuelve la lista de hashes necesaria para reconstruir un archivo."""
        if filename not in self.db or version not in self.db[filename]:
            raise ValueError(f"Version {version} of {filename} not found.")
        return self.db[filename][version]