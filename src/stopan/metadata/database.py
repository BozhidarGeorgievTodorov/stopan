from __future__ import annotations

import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from collections.abc import Iterable

from stopan.config.defaults import DEFAULT_NODE_DB_FILE
from stopan.protection.policy import ProtectionRecord, ProtectionState, is_record_sufficient



@dataclass(frozen=True)
class VerificationCandidate:
    chunk_hash: str
    desired_rf: int
    protection_state: ProtectionState


class MetadataDB:
    """
    Guarda snapshots, recetas de chunks y estado de protección distribuida.

    Modelo canónico:
      - snapshots
      - snapshot_items
      - recipes / recipe_chunks
      - chunks
      - chunk_protection

    La protección remota se decide exclusivamente desde chunk_protection.
    """

    def __init__(self, db_file: str = DEFAULT_NODE_DB_FILE, *, init_schema: bool = True):
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                root_path TEXT NOT NULL,
                origin_node_id TEXT NOT NULL,
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
            CREATE INDEX IF NOT EXISTS idx_snapshots_origin_node_id
            ON snapshots(origin_node_id)
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
                ref_count INTEGER NOT NULL DEFAULT 0
            )
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

        self.conn.commit()

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    def create_snapshot(self, root_path: str, *, origin_node_id: str) -> int:
        """Crea un snapshot nuevo en estado CREATING."""
        origin_node_id = str(origin_node_id).strip()
        if not origin_node_id:
            raise ValueError("create_snapshot requires a non-empty origin_node_id.")

        snapshot_uuid = str(uuid.uuid4())
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO snapshots (uuid, root_path, origin_node_id, status)
            VALUES (?, ?, ?, 'CREATING')
        """, (snapshot_uuid, root_path, origin_node_id))
        return cursor.lastrowid

    def finish_snapshot(self, snapshot_id: int, total_size: int, total_files: int) -> None:
        """Marca un snapshot como completado."""
        self.conn.execute("""
            UPDATE snapshots
            SET total_size = ?, total_files = ?, status = 'COMPLETE', error = NULL
            WHERE id = ?
        """, (total_size, total_files, snapshot_id))

    def fail_snapshot(self, snapshot_id: int, error: str) -> None:
        """Marca un snapshot como fallido y guarda la causa."""
        self.conn.execute("""
            UPDATE snapshots
            SET status = 'FAILED', error = ?
            WHERE id = ?
        """, (str(error), snapshot_id))

    def get_snapshot_status(self, snapshot_id: int) -> tuple[str | None, str | None]:
        row = self.conn.execute("""
            SELECT status, error
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,)).fetchone()

        if row is None:
            return None, None

        return row["status"], row["error"]


    def get_snapshot_uuid(self, snapshot_id: int) -> str | None:
        row = self.conn.execute("""
            SELECT uuid
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,)).fetchone()

        return row["uuid"] if row else None


    def get_snapshot_origin_node_id(self, snapshot_id: int) -> str | None:
        row = self.conn.execute("""
            SELECT origin_node_id
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,)).fetchone()

        return row["origin_node_id"] if row else None

    def get_prev_snapshot_id(self, root_path: str, current_snapshot_id: int) -> int | None:
        """Devuelve el snapshot completo anterior de la misma raíz."""
        row = self.conn.execute("""
            SELECT id
            FROM snapshots
            WHERE root_path = ? AND id < ? AND status = 'COMPLETE'
            ORDER BY id DESC
            LIMIT 1
        """, (root_path, current_snapshot_id)).fetchone()

        return row["id"] if row else None

    # ------------------------------------------------------------------
    # Snapshot items
    # ------------------------------------------------------------------

    def add_item(self, snapshot_id: int, rel_path: str, stat_info, item_type: str) -> int:
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

    def get_snapshot_items(self, snapshot_id: int):
        """Genera los elementos de un snapshot en orden de ruta."""
        cursor = self.conn.execute("""
            SELECT *
            FROM snapshot_items
            WHERE snapshot_id = ?
            ORDER BY path ASC
        """, (snapshot_id,))

        for row in cursor:
            yield dict(row)

    def get_item_by_path(self, snapshot_id: int, rel_path: str) -> dict | None:
        """Devuelve un archivo de un snapshot anterior por ruta relativa."""
        row = self.conn.execute("""
            SELECT id, size, mode, mtime_ns, uid, gid, recipe_id
            FROM snapshot_items
            WHERE snapshot_id = ? AND path = ? AND item_type = 'file'
            LIMIT 1
        """, (snapshot_id, rel_path)).fetchone()

        return dict(row) if row else None

    def set_item_recipe(self, item_id: int, recipe_id: int) -> None:
        """Asocia un item del snapshot con una receta."""
        self.conn.execute("""
            UPDATE snapshot_items
            SET recipe_id = ?
            WHERE id = ?
        """, (recipe_id, item_id))

    def get_item_chunks(self, item_id: int):
        """Genera la receta de hashes de un archivo."""
        row = self.conn.execute("""
            SELECT recipe_id
            FROM snapshot_items
            WHERE id = ?
        """, (item_id,)).fetchone()

        if row is None:
            raise ValueError(f"Snapshot item does not exist: {item_id}")

        if row["recipe_id"] is None:
            raise ValueError(f"Snapshot item has no associated recipe: {item_id}")

        cursor = self.conn.execute("""
            SELECT chunk_hash
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
        """, (row["recipe_id"],))

        for row in cursor:
            yield row["chunk_hash"]

    # ------------------------------------------------------------------
    # Recipes / chunks
    # ------------------------------------------------------------------

    def get_or_create_recipe(
        self,
        recipe_hash: str,
        chunks: Iterable[tuple[int, str, int]],
        *,
        desired_rf: int = 3,
    ) -> int:
        """Crea o reutiliza una receta deduplicada de chunks."""
        chunks = list(chunks)
        desired_rf = int(desired_rf)
        chunk_count = len(chunks)
        total_size = sum(chunk_size for _, _, chunk_size in chunks)

        for expected_order, (chunk_order, chunk_hash, chunk_size) in enumerate(chunks):
            if chunk_order != expected_order:
                raise RuntimeError(
                    "recipe chunks must be contiguous from 0: "
                    f"expected={expected_order} got={chunk_order}"
                )
            if not isinstance(chunk_hash, str) or len(chunk_hash) != 64 or any(c not in "0123456789abcdef" for c in chunk_hash):
                raise RuntimeError(f"invalid chunk hash in recipe {recipe_hash}: {chunk_hash!r}")
            if chunk_size < 1:
                raise RuntimeError(f"invalid chunk size in recipe {recipe_hash}: {chunk_size}")

        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO recipes (recipe_hash, chunk_count, total_size)
                VALUES (?, ?, ?)
            """, (recipe_hash, chunk_count, total_size))

            row = self.conn.execute("""
                SELECT id, chunk_count, total_size
                FROM recipes
                WHERE recipe_hash = ?
                LIMIT 1
            """, (recipe_hash,)).fetchone()
            if row is None:
                raise RuntimeError(f"Could not resolve recipe_id for {recipe_hash}")

            if row["chunk_count"] != chunk_count or row["total_size"] != total_size:
                raise RuntimeError(
                    "Inconsistent recipe_hash: "
                    f"{recipe_hash} already maps to chunk_count={row['chunk_count']} "
                    f"total_size={row['total_size']}, attempted chunk_count={chunk_count} "
                    f"total_size={total_size}"
                )

            recipe_id = row["id"]

            existing_rows = self.conn.execute("""
                SELECT chunk_order, chunk_hash, chunk_size
                FROM recipe_chunks
                WHERE recipe_id = ?
                ORDER BY chunk_order ASC
            """, (recipe_id,)).fetchall()

            existing_chunks = [
                (row["chunk_order"], row["chunk_hash"], row["chunk_size"])
                for row in existing_rows
            ]
            if existing_chunks:
                if existing_chunks != chunks:
                    raise RuntimeError(f"Inconsistent recipe_chunks for recipe_hash: {recipe_hash}")
            elif chunk_count > 0:
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

    def ensure_recipe_protection(self, recipe_id: int, *, desired_rf: int = 3) -> None:
        """Asegura filas de protección para los chunks de una receta reutilizada."""
        rows = self.conn.execute("""
            SELECT chunk_order, chunk_hash, chunk_size
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
        """, (recipe_id,)).fetchall()

        chunks = [(row["chunk_order"], row["chunk_hash"], row["chunk_size"]) for row in rows]
        self._ensure_chunk_protection_rows(chunks, desired_rf=desired_rf)

    def _register_chunks(self, chunks: Iterable[tuple[int, str, int]]) -> None:
        ref_counts = defaultdict(lambda: [0, 0])
        for _, chunk_hash, chunk_size in chunks:
            ref_counts[chunk_hash][0] = chunk_size
            ref_counts[chunk_hash][1] += 1

        self.conn.executemany("""
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, ?)
            ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + excluded.ref_count
        """, (
            (chunk_hash, chunk_size, ref_count)
            for chunk_hash, (chunk_size, ref_count) in ref_counts.items()
        ))

    def _ensure_chunk_protection_rows(
        self,
        chunks: Iterable[tuple[int, str, int]],
        *,
        desired_rf: int,
    ) -> None:
        desired_rf = int(desired_rf)
        rows = [(chunk_hash, desired_rf) for _, chunk_hash, _ in chunks]
        if not rows:
            return

        self.conn.executemany("""
            INSERT OR IGNORE INTO chunk_protection (
                chunk_hash, desired_rf, protection_state, protected_remote_copies,
                placement_epoch, last_push_at, last_verify_at, last_error
            )
            VALUES (?, ?, ?, 0, NULL, NULL, NULL, NULL)
        """, (
            (chunk_hash, desired_rf, ProtectionState.PENDING.value)
            for chunk_hash, desired_rf in rows
        ))

        self.conn.executemany("""
            UPDATE chunk_protection
            SET desired_rf = CASE WHEN desired_rf < ? THEN ? ELSE desired_rf END,
                protection_state = CASE
                    WHEN desired_rf < ? AND protection_state IN (?, ?)
                    THEN ?
                    ELSE protection_state
                END,
                last_error = CASE
                    WHEN desired_rf < ? AND protection_state IN (?, ?)
                    THEN 'desired_rf increased'
                    ELSE last_error
                END
            WHERE chunk_hash = ?
        """, (
            (
                desired_rf,
                desired_rf,
                desired_rf,
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
                ProtectionState.DEGRADED.value,
                desired_rf,
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
                chunk_hash,
            )
            for chunk_hash, _ in rows
        ))





    def has_chunk(self, chunk_hash: str) -> bool:
        row = self.conn.execute("""
            SELECT 1
            FROM chunks
            WHERE hash = ?
        """, (chunk_hash,)).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Distributed protection
    # ------------------------------------------------------------------

    def get_chunk_protection(self, chunk_hash: str) -> ProtectionRecord | None:
        row = self.conn.execute("""
            SELECT chunk_hash, desired_rf, protection_state, protected_remote_copies,
                   placement_epoch, last_push_at, last_verify_at, last_error
            FROM chunk_protection
            WHERE chunk_hash = ?
        """, (chunk_hash,)).fetchone()

        if row is None:
            return None

        return ProtectionRecord(
            chunk_hash=row["chunk_hash"],
            desired_rf=int(row["desired_rf"]),
            protection_state=ProtectionState(row["protection_state"]),
            protected_remote_copies=int(row["protected_remote_copies"]),
            placement_epoch=row["placement_epoch"],
            last_push_at=row["last_push_at"],
            last_verify_at=row["last_verify_at"],
            last_error=row["last_error"],
        )

    def is_chunk_remotely_protected(
        self,
        chunk_hash: str,
        *,
        required_rf: int,
        current_epoch: str | None = None,
    ) -> bool:
        return is_record_sufficient(
            self.get_chunk_protection(chunk_hash),
            required_rf=required_rf,
            current_epoch=current_epoch,
        )

    def get_pending_protection_chunks(
        self,
        *,
        desired_rf: int,
        current_epoch: str | None = None,
        limit: int | None = None,
    ) -> list[str]:
        desired_rf = int(desired_rf)
        params: list[object] = [
            ProtectionState.PENDING.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
            desired_rf,
        ]

        query = """
            SELECT chunk_hash
            FROM chunk_protection
            WHERE protection_state IN (?, ?, ?)
               OR desired_rf < ?
        """

        if current_epoch is not None:
            query += """
               OR (
                    protection_state IN (?, ?)
                    AND (placement_epoch IS NULL OR placement_epoch <> ?)
               )
            """
            params.extend([
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
                current_epoch,
            ])

        query += " ORDER BY chunk_hash ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [row["chunk_hash"] for row in rows]

    def get_verification_candidates(
        self,
        *,
        include_verified: bool = False,
        limit: int | None = None,
    ) -> list[VerificationCandidate]:
        states = [
            ProtectionState.PLACED.value,
            ProtectionState.DEGRADED.value,
        ]
        if include_verified:
            states.append(ProtectionState.VERIFIED.value)

        placeholders = ", ".join("?" for _ in states)
        query = f"""
            SELECT chunk_hash, desired_rf, protection_state
            FROM chunk_protection
            WHERE protection_state IN ({placeholders})
            ORDER BY chunk_hash ASC
        """
        params: list[object] = list(states)

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [
            VerificationCandidate(
                chunk_hash=row["chunk_hash"],
                desired_rf=int(row["desired_rf"]),
                protection_state=ProtectionState(row["protection_state"]),
            )
            for row in rows
        ]

    def mark_stale_protection(
        self,
        *,
        desired_rf: int,
        current_epoch: str | None = None,
    ) -> None:
        desired_rf = int(desired_rf)

        with self.conn:
            self.conn.execute("""
                UPDATE chunk_protection
                SET protection_state = ?,
                    last_error = CASE
                        WHEN desired_rf < ? THEN 'desired_rf increased'
                        ELSE last_error
                    END
                WHERE desired_rf < ?
                  AND protection_state IN (?, ?)
            """, (
                ProtectionState.DEGRADED.value,
                desired_rf,
                desired_rf,
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
            ))

            if current_epoch is not None:
                self.conn.execute("""
                    UPDATE chunk_protection
                    SET protection_state = ?,
                        last_error = 'placement_epoch changed'
                    WHERE protection_state IN (?, ?)
                      AND (placement_epoch IS NULL OR placement_epoch <> ?)
                """, (
                    ProtectionState.DEGRADED.value,
                    ProtectionState.PLACED.value,
                    ProtectionState.VERIFIED.value,
                    current_epoch,
                ))

    def mark_chunk_placed(
        self,
        chunk_hash: str,
        *,
        desired_rf: int,
        protected_remote_copies: int,
        placement_epoch: str | None,
    ) -> None:
        with self.conn:
            self._upsert_chunk_protection_state(
                chunk_hash=chunk_hash,
                desired_rf=desired_rf,
                protection_state=ProtectionState.PLACED,
                protected_remote_copies=protected_remote_copies,
                placement_epoch=placement_epoch,
                last_push_at=time.time(),
                last_verify_at=None,
                last_error=None,
            )

    def mark_chunk_failed(
        self,
        chunk_hash: str,
        *,
        desired_rf: int,
        protected_remote_copies: int,
        placement_epoch: str | None,
        error: str,
    ) -> None:
        with self.conn:
            self._upsert_chunk_protection_state(
                chunk_hash=chunk_hash,
                desired_rf=desired_rf,
                protection_state=ProtectionState.FAILED,
                protected_remote_copies=protected_remote_copies,
                placement_epoch=placement_epoch,
                last_push_at=time.time(),
                last_verify_at=None,
                last_error=error,
            )

    def mark_chunk_push_degraded(
        self,
        chunk_hash: str,
        *,
        desired_rf: int,
        protected_remote_copies: int,
        placement_epoch: str | None,
        error: str,
    ) -> None:
        """Registra degradación observada durante push/replicación."""
        with self.conn:
            self._upsert_chunk_protection_state(
                chunk_hash=chunk_hash,
                desired_rf=desired_rf,
                protection_state=ProtectionState.DEGRADED,
                protected_remote_copies=protected_remote_copies,
                placement_epoch=placement_epoch,
                last_push_at=time.time(),
                last_verify_at=None,
                last_error=error,
            )

    def mark_chunk_verified(
        self,
        chunk_hash: str,
        *,
        desired_rf: int,
        protected_remote_copies: int,
        placement_epoch: str | None,
    ) -> None:
        with self.conn:
            self._upsert_chunk_verification_state(
                chunk_hash=chunk_hash,
                desired_rf=desired_rf,
                protection_state=ProtectionState.VERIFIED,
                protected_remote_copies=protected_remote_copies,
                placement_epoch=placement_epoch,
                last_verify_at=time.time(),
                last_error=None,
            )

    def mark_chunk_degraded(
        self,
        chunk_hash: str,
        *,
        desired_rf: int,
        protected_remote_copies: int,
        placement_epoch: str | None,
        error: str,
    ) -> None:
        with self.conn:
            self._upsert_chunk_verification_state(
                chunk_hash=chunk_hash,
                desired_rf=desired_rf,
                protection_state=ProtectionState.DEGRADED,
                protected_remote_copies=protected_remote_copies,
                placement_epoch=placement_epoch,
                last_verify_at=time.time(),
                last_error=error,
            )

    def _upsert_chunk_protection_state(
        self,
        *,
        chunk_hash: str,
        desired_rf: int,
        protection_state: ProtectionState,
        protected_remote_copies: int,
        placement_epoch: str | None,
        last_push_at: float | None,
        last_verify_at: float | None,
        last_error: str | None,
    ) -> None:
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
            int(desired_rf),
            protection_state.value,
            max(int(protected_remote_copies), 0),
            placement_epoch,
            last_push_at,
            last_verify_at,
            last_error,
        ))

    def _upsert_chunk_verification_state(
        self,
        *,
        chunk_hash: str,
        desired_rf: int,
        protection_state: ProtectionState,
        protected_remote_copies: int,
        placement_epoch: str | None,
        last_verify_at: float,
        last_error: str | None,
    ) -> None:
        self.conn.execute("""
            INSERT INTO chunk_protection (
                chunk_hash, desired_rf, protection_state, protected_remote_copies,
                placement_epoch, last_push_at, last_verify_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(chunk_hash) DO UPDATE SET
                desired_rf = excluded.desired_rf,
                protection_state = excluded.protection_state,
                protected_remote_copies = excluded.protected_remote_copies,
                placement_epoch = excluded.placement_epoch,
                last_verify_at = excluded.last_verify_at,
                last_error = excluded.last_error
        """, (
            chunk_hash,
            int(desired_rf),
            protection_state.value,
            max(int(protected_remote_copies), 0),
            placement_epoch,
            last_verify_at,
            last_error,
        ))

