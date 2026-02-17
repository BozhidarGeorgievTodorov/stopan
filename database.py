import os
import sqlite3

DATA_FOLDER = "_data_chunks"
DB_FILE = "_metadata.db"


class MetadataDB:
    """
    Gestor de almacenamiento local.
    Guarda los bloques por hash y mantiene las recetas de versiones en SQLite.
    """

    def __init__(self, data_folder=DATA_FOLDER, db_file=DB_FILE):
        self.data_folder = data_folder
        self.db_file = db_file

        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)

        self.conn = sqlite3.connect(self.db_file)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                version_number INTEGER NOT NULL,
                total_size INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(file_id) REFERENCES files(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_chunks (
                version_id INTEGER NOT NULL,
                chunk_order INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                FOREIGN KEY(version_id) REFERENCES versions(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                hash TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                ref_count INTEGER NOT NULL DEFAULT 0
            )
        ''')

        self.conn.commit()

    def save_chunk(self, chunk_hash, chunk_data):
        """Guarda un chunk en disco solo si su hash no existe."""
        path = os.path.join(self.data_folder, chunk_hash)
        is_new = not os.path.exists(path)

        if is_new:
            with open(path, 'wb') as f:
                f.write(chunk_data)

        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, 0)
            ON CONFLICT(hash) DO NOTHING
        ''', (chunk_hash, len(chunk_data)))
        self.conn.commit()

        return is_new

    def register_version(self, filename, recipe):
        """Registra una nueva versión del archivo a partir de su receta de bloques."""
        file_id = self._get_or_create_file(filename)
        next_version = self._next_version_number(file_id)
        total_size = sum(chunk_size for _, chunk_size in recipe)

        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO versions (file_id, version_number, total_size)
            VALUES (?, ?, ?)
        ''', (file_id, next_version, total_size))
        version_id = cursor.lastrowid

        for order, (chunk_hash, chunk_size) in enumerate(recipe):
            cursor.execute('''
                INSERT INTO file_chunks (version_id, chunk_order, chunk_hash, chunk_size)
                VALUES (?, ?, ?, ?)
            ''', (version_id, order, chunk_hash, chunk_size))

            cursor.execute('''
                UPDATE chunks
                SET ref_count = ref_count + 1
                WHERE hash = ?
            ''', (chunk_hash,))

        self.conn.commit()
        return str(next_version)

    def get_chunk(self, chunk_hash):
        """Recupera los bytes de un bloque guardado."""
        path = os.path.join(self.data_folder, chunk_hash)
        if not os.path.exists(path):
            raise ValueError(f"Missing block: {chunk_hash}")

        with open(path, 'rb') as f:
            return f.read()

    def get_recipe(self, filename, version):
        """Devuelve la receta necesaria para reconstruir una versión del archivo."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT fc.chunk_hash
            FROM file_chunks fc
            JOIN versions v ON fc.version_id = v.id
            JOIN files f ON v.file_id = f.id
            WHERE f.path = ? AND v.version_number = ?
            ORDER BY fc.chunk_order ASC
        ''', (filename, int(version)))

        recipe = [row['chunk_hash'] for row in cursor.fetchall()]
        if not recipe:
            raise ValueError(f"Version {version} of {filename} not found.")

        return recipe

    def close(self):
        self.conn.close()

    def _get_or_create_file(self, filename):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM files WHERE path = ?", (filename,))
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor.execute("INSERT INTO files (path) VALUES (?)", (filename,))
        self.conn.commit()
        return cursor.lastrowid

    def _next_version_number(self, file_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT MAX(version_number) AS last_version
            FROM versions
            WHERE file_id = ?
        ''', (file_id,))
        row = cursor.fetchone()
        return (row['last_version'] or 0) + 1
