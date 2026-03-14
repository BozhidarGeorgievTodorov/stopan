import sqlite3
import time
import uuid
from collections import defaultdict

from core.protection import ProtectionRecord, is_record_sufficient

DB_FILE = "_metadata.db"


class MetadataDB:
    """
    Guarda snapshots, recetas de chunks y estado de protección distribuida.
    """

    def __init__(self, db_file=DB_FILE, *, init_schema=True):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA cache_size = -64000")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute("PRAGMA mmap_size = 2147483648")

        if init_schema:
            self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                root_path TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'CREATING',
                error TEXT,
                total_size INTEGER NOT NULL DEFAULT 0,
                total_files INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_root_status_id
            ON snapshots(root_path, status, id DESC)
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snapshot_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                item_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                mode INTEGER,
                mtime REAL,
                mtime_ns INTEGER,
                uid INTEGER,
                gid INTEGER,
                recipe_id INTEGER,
                FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,
                FOREIGN KEY(recipe_id) REFERENCES recipes(id),
                UNIQUE(snapshot_id, path)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshot_items_snapshot_path
            ON snapshot_items(snapshot_id, path)
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_hash TEXT UNIQUE NOT NULL,
                chunk_count INTEGER NOT NULL,
                total_size INTEGER NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipe_chunks (
                recipe_id INTEGER NOT NULL,
                chunk_order INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                PRIMARY KEY(recipe_id, chunk_order),
                FOREIGN KEY(recipe_id) REFERENCES recipes(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_chunks_hash
            ON recipe_chunks(chunk_hash)
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                hash TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                ref_count INTEGER NOT NULL DEFAULT 0,
                is_synced INTEGER NOT NULL DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_unsynced
            ON chunks(is_synced)
            WHERE is_synced = 0
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunk_protection (
                chunk_hash TEXT PRIMARY KEY,
                desired_rf INTEGER NOT NULL DEFAULT 1,
                protection_state TEXT NOT NULL DEFAULT 'PENDING',
                protected_remote_copies INTEGER NOT NULL DEFAULT 0,
                placement_epoch TEXT,
                last_push_at REAL,
                last_verify_at REAL,
                last_error TEXT,
                FOREIGN KEY(chunk_hash) REFERENCES chunks(hash) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunk_protection_state
            ON chunk_protection(protection_state)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunk_protection_epoch
            ON chunk_protection(placement_epoch)
        """)

        cursor.execute("""
            INSERT OR IGNORE INTO chunk_protection (
                chunk_hash, desired_rf, protection_state, protected_remote_copies,
                placement_epoch, last_push_at, last_verify_at, last_error
            )
            SELECT hash, 1, 'PENDING', 0, NULL, NULL, NULL, NULL
            FROM chunks
        """)

        self.conn.commit()

    def create_snapshot(self, root_path):
        """Crea un snapshot nuevo en estado CREATING."""
        snapshot_uuid = str(uuid.uuid4())
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO snapshots (uuid, root_path, status)
            VALUES (?, ?, 'CREATING')
        """, (snapshot_uuid, root_path))
        return cursor.lastrowid

    def finish_snapshot(self, snapshot_id, total_size, total_files):
        """Marca un snapshot como completado."""
        self.conn.execute("""
            UPDATE snapshots
            SET total_size = ?, total_files = ?, status = 'COMPLETE', error = NULL
            WHERE id = ?
        """, (total_size, total_files, snapshot_id))

    def fail_snapshot(self, snapshot_id, error):
        """Marca un snapshot como fallido y guarda la causa."""
        self.conn.execute("""
            UPDATE snapshots
            SET status = 'FAILED', error = ?
            WHERE id = ?
        """, (str(error), snapshot_id))

    def get_snapshot_status(self, snapshot_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status, error
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,))
        row = cursor.fetchone()
        if row is None:
            return None, None
        return row["status"], row["error"]

    def get_snapshot_uuid(self, snapshot_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT uuid
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,))
        row = cursor.fetchone()
        return row["uuid"] if row else None

    def add_item(self, snapshot_id, rel_path, stat_info, item_type):
        """Registra un archivo o directorio dentro de un snapshot."""
        mtime_ns = getattr(stat_info, "st_mtime_ns", int(stat_info.st_mtime * 1_000_000_000))
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO snapshot_items (
                snapshot_id, path, item_type, size, mode, mtime, mtime_ns, uid, gid, recipe_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (
            snapshot_id,
            rel_path,
            item_type,
            stat_info.st_size,
            stat_info.st_mode,
            stat_info.st_mtime,
            mtime_ns,
            getattr(stat_info, "st_uid", None),
            getattr(stat_info, "st_gid", None),
        ))
        return cursor.lastrowid

    def get_prev_snapshot_id(self, root_path, current_snapshot_id):
        """Devuelve el snapshot completo anterior de la misma raíz."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id
            FROM snapshots
            WHERE root_path = ? AND id < ? AND status = 'COMPLETE'
            ORDER BY id DESC
            LIMIT 1
        """, (root_path, current_snapshot_id))
        row = cursor.fetchone()
        return row["id"] if row else None

    def get_item_by_path(self, snapshot_id, rel_path):
        """Devuelve un archivo de un snapshot anterior por ruta relativa."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, size, mode, mtime_ns, uid, gid, recipe_id
            FROM snapshot_items
            WHERE snapshot_id = ? AND path = ? AND item_type = 'file'
            LIMIT 1
        """, (snapshot_id, rel_path))
        row = cursor.fetchone()
        return dict(row) if row else None

    def set_item_recipe(self, item_id, recipe_id):
        """Asocia un item del snapshot con una receta."""
        self.conn.execute("""
            UPDATE snapshot_items
            SET recipe_id = ?
            WHERE id = ?
        """, (recipe_id, item_id))

    def get_or_create_recipe(self, recipe_hash, chunks, *, desired_rf=3):
        """Crea o reutiliza una receta deduplicada de chunks."""
        chunks = list(chunks)
        chunk_count = len(chunks)
        total_size = sum(chunk_size for _, _, chunk_size in chunks)

        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO recipes (recipe_hash, chunk_count, total_size)
                VALUES (?, ?, ?)
            """, (recipe_hash, chunk_count, total_size))

            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id
                FROM recipes
                WHERE recipe_hash = ?
                LIMIT 1
            """, (recipe_hash,))
            recipe_id = cursor.fetchone()["id"]

            cursor.execute("""
                SELECT 1
                FROM recipe_chunks
                WHERE recipe_id = ?
                LIMIT 1
            """, (recipe_id,))
            if cursor.fetchone() is None and chunk_count > 0:
                self.conn.executemany("""
                    INSERT INTO recipe_chunks (recipe_id, chunk_order, chunk_hash, chunk_size)
                    VALUES (?, ?, ?, ?)
                """, (
                    (recipe_id, order, chunk_hash, chunk_size)
                    for order, chunk_hash, chunk_size in chunks
                ))

            self._register_chunks(chunks)
            self._ensure_chunk_protection_rows(chunks, desired_rf=desired_rf)

        return recipe_id

    def ensure_recipe_protection(self, recipe_id, *, desired_rf=3):
        """Asegura filas de protección para los chunks de una receta reutilizada."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT chunk_order, chunk_hash, chunk_size
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
        """, (recipe_id,))
        chunks = [(row["chunk_order"], row["chunk_hash"], row["chunk_size"]) for row in cursor.fetchall()]
        self._ensure_chunk_protection_rows(chunks, desired_rf=desired_rf)

    def _register_chunks(self, chunks):
        ref_counts = defaultdict(lambda: [0, 0])
        for _, chunk_hash, chunk_size in chunks:
            ref_counts[chunk_hash][0] = chunk_size
            ref_counts[chunk_hash][1] += 1

        self.conn.executemany("""
            INSERT INTO chunks (hash, size, ref_count, is_synced)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + excluded.ref_count
        """, (
            (chunk_hash, chunk_size, ref_count)
            for chunk_hash, (chunk_size, ref_count) in ref_counts.items()
        ))

    def _ensure_chunk_protection_rows(self, chunks, *, desired_rf):
        desired_rf = max(int(desired_rf), 1)
        rows = [(chunk_hash, desired_rf) for _, chunk_hash, _ in chunks]
        if not rows:
            return

        self.conn.executemany("""
            INSERT OR IGNORE INTO chunk_protection (
                chunk_hash, desired_rf, protection_state, protected_remote_copies,
                placement_epoch, last_push_at, last_verify_at, last_error
            )
            VALUES (?, ?, 'PENDING', 0, NULL, NULL, NULL, NULL)
        """, rows)

        self.conn.executemany("""
            UPDATE chunk_protection
            SET desired_rf = CASE WHEN desired_rf < ? THEN ? ELSE desired_rf END
            WHERE chunk_hash = ?
        """, ((desired_rf, desired_rf, chunk_hash) for _, chunk_hash, _ in chunks))

    def get_snapshot_items(self, snapshot_id):
        """Genera los elementos de un snapshot en orden de ruta."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT *
            FROM snapshot_items
            WHERE snapshot_id = ?
            ORDER BY path ASC
        """, (snapshot_id,))

        for row in cursor:
            yield dict(row)

    def get_item_chunks(self, item_id):
        """Genera la receta de hashes de un archivo."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT recipe_id
            FROM snapshot_items
            WHERE id = ?
        """, (item_id,))
        row = cursor.fetchone()
        if row is None or row["recipe_id"] is None:
            raise ValueError(f"Item {item_id} does not have an associated recipe.")

        cursor.execute("""
            SELECT chunk_hash
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
        """, (row["recipe_id"],))

        for row in cursor:
            yield row["chunk_hash"]

    def get_chunk_protection(self, chunk_hash):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT chunk_hash, desired_rf, protection_state, protected_remote_copies,
                   placement_epoch, last_push_at, last_verify_at, last_error
            FROM chunk_protection
            WHERE chunk_hash = ?
            LIMIT 1
        """, (chunk_hash,))
        row = cursor.fetchone()
        if row is None:
            return None

        return ProtectionRecord(
            chunk_hash=row["chunk_hash"],
            desired_rf=row["desired_rf"],
            protection_state=row["protection_state"],
            protected_remote_copies=row["protected_remote_copies"],
            placement_epoch=row["placement_epoch"],
            last_push_at=row["last_push_at"],
            last_verify_at=row["last_verify_at"],
            last_error=row["last_error"],
        )

    def is_chunk_remotely_protected(self, chunk_hash, *, required_rf, current_epoch=None):
        record = self.get_chunk_protection(chunk_hash)
        return is_record_sufficient(record, required_rf=required_rf, current_epoch=current_epoch)

    def get_pending_protection_chunks(self, *, desired_rf, current_epoch=None, limit=None):
        desired_rf = max(int(desired_rf), 1)
        params = [desired_rf]
        query = """
            SELECT chunk_hash
            FROM chunk_protection
            WHERE protection_state IN ('PENDING', 'DEGRADED', 'FAILED')
               OR desired_rf < ?
        """

        if current_epoch is not None:
            query += """
               OR (
                    protection_state IN ('PLACED', 'VERIFIED')
                    AND (placement_epoch IS NULL OR placement_epoch <> ?)
               )
            """
            params.append(current_epoch)

        query += " ORDER BY chunk_hash ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        cursor = self.conn.cursor()
        cursor.execute(query, tuple(params))
        return [row["chunk_hash"] for row in cursor.fetchall()]

    def mark_stale_protection(self, *, desired_rf, current_epoch=None):
        desired_rf = max(int(desired_rf), 1)
        with self.conn:
            self.conn.execute("""
                UPDATE chunk_protection
                SET protection_state = 'DEGRADED',
                    last_error = 'desired_rf increased'
                WHERE desired_rf < ?
                  AND protection_state IN ('PLACED', 'VERIFIED')
            """, (desired_rf,))

            if current_epoch is not None:
                self.conn.execute("""
                    UPDATE chunk_protection
                    SET protection_state = 'DEGRADED',
                        last_error = 'placement_epoch changed'
                    WHERE protection_state IN ('PLACED', 'VERIFIED')
                      AND (placement_epoch IS NULL OR placement_epoch <> ?)
                """, (current_epoch,))

            self.conn.execute("""
                UPDATE chunks
                SET is_synced = 0
                WHERE hash IN (
                    SELECT chunk_hash
                    FROM chunk_protection
                    WHERE protection_state IN ('PENDING', 'DEGRADED', 'FAILED')
                )
            """)

    def mark_chunk_placed(self, chunk_hash, *, desired_rf, protected_remote_copies, placement_epoch):
        self._mark_chunk_protection(
            chunk_hash,
            state="PLACED",
            desired_rf=desired_rf,
            protected_remote_copies=protected_remote_copies,
            placement_epoch=placement_epoch,
            error=None,
            verified=False,
        )

    def mark_chunk_failed(self, chunk_hash, *, desired_rf, protected_remote_copies, placement_epoch, error):
        self._mark_chunk_protection(
            chunk_hash,
            state="FAILED",
            desired_rf=desired_rf,
            protected_remote_copies=protected_remote_copies,
            placement_epoch=placement_epoch,
            error=error,
            verified=False,
        )

    def mark_chunk_verified(self, chunk_hash, *, desired_rf, protected_remote_copies, placement_epoch):
        self._mark_chunk_protection(
            chunk_hash,
            state="VERIFIED",
            desired_rf=desired_rf,
            protected_remote_copies=protected_remote_copies,
            placement_epoch=placement_epoch,
            error=None,
            verified=True,
        )

    def _mark_chunk_protection(
        self,
        chunk_hash,
        *,
        state,
        desired_rf,
        protected_remote_copies,
        placement_epoch,
        error,
        verified,
    ):
        desired_rf = max(int(desired_rf), 1)
        protected_remote_copies = max(int(protected_remote_copies), 0)
        now = time.time()

        with self.conn:
            self.conn.execute("""
                INSERT INTO chunk_protection (
                    chunk_hash, desired_rf, protection_state, protected_remote_copies,
                    placement_epoch, last_push_at, last_verify_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chunk_hash) DO UPDATE SET
                    desired_rf = excluded.desired_rf,
                    protection_state = excluded.protection_state,
                    protected_remote_copies = excluded.protected_remote_copies,
                    placement_epoch = excluded.placement_epoch,
                    last_push_at = excluded.last_push_at,
                    last_verify_at = excluded.last_verify_at,
                    last_error = excluded.last_error
            """, (
                chunk_hash,
                desired_rf,
                state,
                protected_remote_copies,
                placement_epoch,
                None if verified else now,
                now if verified else None,
                error,
            ))

            self.conn.execute("""
                UPDATE chunks
                SET is_synced = ?
                WHERE hash = ?
            """, (1 if state in ("PLACED", "VERIFIED") else 0, chunk_hash))

    def has_chunk(self, chunk_hash):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 1
            FROM chunks
            WHERE hash = ?
            LIMIT 1
        """, (chunk_hash,))
        return cursor.fetchone() is not None

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
