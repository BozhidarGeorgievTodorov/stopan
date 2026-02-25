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
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA cache_size = -64000")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute("PRAGMA mmap_size = 2147483648")
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
                ref_count INTEGER NOT NULL DEFAULT 0,
                is_synced INTEGER NOT NULL DEFAULT 0
            )
        ''')

        self._ensure_chunks_sync_column(cursor)

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshot_items_snapshot_path
            ON snapshot_items(snapshot_id, path)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_item_chunks_item_order
            ON item_chunks(item_id, chunk_order)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_chunks_unsynced
            ON chunks(is_synced)
            WHERE is_synced = 0
        ''')

        self.conn.commit()

    def _ensure_chunks_sync_column(self, cursor):
        cursor.execute("PRAGMA table_info(chunks)")
        columns = {row['name'] for row in cursor.fetchall()}
        if 'is_synced' not in columns:
            cursor.execute("ALTER TABLE chunks ADD COLUMN is_synced INTEGER NOT NULL DEFAULT 0")

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
            INSERT INTO chunks (hash, size, ref_count, is_synced)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + excluded.ref_count
        ''', (
            (chunk_hash, chunk_size, ref_count)
            for chunk_hash, (chunk_size, ref_count) in ref_counts.items()
        ))

    def get_snapshot_items(self, snapshot_id):
        """Genera los elementos de un snapshot en orden de ruta."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT *
            FROM snapshot_items
            WHERE snapshot_id = ?
            ORDER BY path ASC
        ''', (snapshot_id,))

        for row in cursor:
            yield dict(row)

    def get_item_chunks(self, item_id):
        """Genera la receta de hashes de un archivo del snapshot."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT chunk_hash
            FROM item_chunks
            WHERE item_id = ?
            ORDER BY chunk_order ASC
        ''', (item_id,))

        for row in cursor:
            yield row['chunk_hash']

    def get_pending_sync_chunks(self, limit=None):
        """Devuelve los bloques que todavía no se han marcado como sincronizados."""
        query = '''
            SELECT hash
            FROM chunks
            WHERE is_synced = 0
            ORDER BY hash ASC
        '''
        params = ()

        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return [row['hash'] for row in cursor.fetchall()]

    def mark_chunk_as_synced(self, chunk_hash):
        """Marca un bloque como enviado correctamente a la red."""
        self.mark_chunks_as_synced([chunk_hash])

    def mark_chunks_as_synced(self, chunk_hashes):
        """Marca varios bloques como enviados correctamente a la red."""
        if not chunk_hashes:
            return

        self.conn.executemany('''
            UPDATE chunks
            SET is_synced = 1
            WHERE hash = ?
        ''', ((chunk_hash,) for chunk_hash in chunk_hashes))

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
