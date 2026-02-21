import sqlite3
from collections import defaultdict

DB_FILE = "_metadata.db"


class MetadataDB:
    """
    Guarda snapshots de directorios, sus elementos y las recetas de bloques.
    """

    def __init__(self, db_file=DB_FILE):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_path TEXT NOT NULL,
                total_size INTEGER NOT NULL DEFAULT 0,
                total_files INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshot_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                item_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                mode INTEGER,
                mtime REAL,
                uid INTEGER,
                gid INTEGER,
                FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS item_chunks (
                item_id INTEGER NOT NULL,
                chunk_order INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                FOREIGN KEY(item_id) REFERENCES snapshot_items(id)
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

    def create_snapshot(self, root_path):
        """Crea un snapshot nuevo para una carpeta raíz."""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO snapshots (root_path)
            VALUES (?)
        ''', (root_path,))
        return cursor.lastrowid

    def finish_snapshot(self, snapshot_id, total_size, total_files):
        """Actualiza el resumen de un snapshot al terminar el backup."""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE snapshots
            SET total_size = ?, total_files = ?
            WHERE id = ?
        ''', (total_size, total_files, snapshot_id))

    def add_item(self, snapshot_id, rel_path, stat_info, item_type):
        """Registra un archivo o directorio dentro de un snapshot."""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO snapshot_items (
                snapshot_id, path, item_type, size, mode, mtime, uid, gid
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            snapshot_id,
            rel_path,
            item_type,
            stat_info.st_size,
            stat_info.st_mode,
            stat_info.st_mtime,
            getattr(stat_info, 'st_uid', None),
            getattr(stat_info, 'st_gid', None),
        ))
        return cursor.lastrowid

    def add_chunk_to_item(self, item_id, order, chunk_hash, chunk_size):
        """Añade un bloque a la receta de un archivo del snapshot."""
        self.add_chunks_batch([(item_id, order, chunk_hash, chunk_size)])

    def add_chunks_batch(self, chunks):
        """Añade en bloque la receta de chunks de un archivo."""
        if not chunks:
            return

        self.conn.executemany('''
            INSERT INTO item_chunks (item_id, chunk_order, chunk_hash, chunk_size)
            VALUES (?, ?, ?, ?)
        ''', chunks)

        ref_counts = defaultdict(lambda: [0, 0])
        for _, _, chunk_hash, chunk_size in chunks:
            ref_counts[chunk_hash][0] = chunk_size
            ref_counts[chunk_hash][1] += 1

        self.conn.executemany('''
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, ?)
            ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + excluded.ref_count
        ''', (
            (chunk_hash, chunk_size, ref_count)
            for chunk_hash, (chunk_size, ref_count) in ref_counts.items()
        ))

    def get_snapshot_items(self, snapshot_id):
        """Devuelve los elementos de un snapshot en orden de ruta."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT *
            FROM snapshot_items
            WHERE snapshot_id = ?
            ORDER BY path ASC
        ''', (snapshot_id,))
        return [dict(row) for row in cursor.fetchall()]

    def get_item_chunks(self, item_id):
        """Devuelve la receta de hashes de un archivo del snapshot."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT chunk_hash
            FROM item_chunks
            WHERE item_id = ?
            ORDER BY chunk_order ASC
        ''', (item_id,))
        return [row['chunk_hash'] for row in cursor.fetchall()]

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
