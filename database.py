import sqlite3

DB_FILE = "_metadata.db"


class MetadataDB:
    """
    Guarda la información lógica de archivos, versiones y recetas de bloques.
    """

    def __init__(self, db_file=DB_FILE):
        self.db_file = db_file
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
                mode INTEGER,
                mtime REAL,
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

    def create_version(self, filename, stat_info):
        """Crea una versión nueva y guarda metadatos básicos del archivo."""
        file_id = self._get_or_create_file(filename)
        next_version = self._next_version_number(file_id)

        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO versions (file_id, version_number, total_size, mode, mtime)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            file_id,
            next_version,
            stat_info.st_size,
            stat_info.st_mode,
            stat_info.st_mtime,
        ))

        return cursor.lastrowid, str(next_version)

    def add_chunk_to_version(self, version_id, order, chunk_hash, chunk_size):
        """Añade un bloque a la receta de una versión."""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO file_chunks (version_id, chunk_order, chunk_hash, chunk_size)
            VALUES (?, ?, ?, ?)
        ''', (version_id, order, chunk_hash, chunk_size))

        cursor.execute('''
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, 1)
            ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + 1
        ''', (chunk_hash, chunk_size))

    def get_version(self, filename, version):
        """Devuelve los metadatos y la receta de una versión concreta."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT v.*
            FROM versions v
            JOIN files f ON v.file_id = f.id
            WHERE f.path = ? AND v.version_number = ?
        ''', (filename, int(version)))

        metadata = cursor.fetchone()
        if metadata is None:
            return None, []

        cursor.execute('''
            SELECT chunk_hash
            FROM file_chunks
            WHERE version_id = ?
            ORDER BY chunk_order ASC
        ''', (metadata['id'],))

        recipe = [row['chunk_hash'] for row in cursor.fetchall()]
        return metadata, recipe

    def get_recipe(self, filename, version):
        """Devuelve solo la receta de hashes de una versión."""
        metadata, recipe = self.get_version(filename, version)
        if metadata is None:
            raise ValueError(f"Version {version} of {filename} not found.")
        return recipe

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()

    def _get_or_create_file(self, filename):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM files WHERE path = ?", (filename,))
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor.execute("INSERT INTO files (path) VALUES (?)", (filename,))
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
