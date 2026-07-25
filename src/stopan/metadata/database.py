from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from stopan.config.defaults import DEFAULT_NODE_DB_FILE
from stopan.errors import StopanDataError
from stopan.protection.policy import ProtectionRecord, ProtectionState, is_record_sufficient


_SQLITE_CACHE_SIZE_KIB = 64 * 1024
_SQLITE_MMAP_SIZE_BYTES = 2 * 1024**3


class MetadataDatabaseError(StopanDataError, RuntimeError):
    """Inconsistencia en metadata persistida o filas derivadas de la base SQLite."""


class MetadataDatabaseValueError(StopanDataError, ValueError):
    """Valor persistido inválido."""


@dataclass(frozen=True)
class VerificationCandidate:
    chunk_hash: str
    desired_rf: int
    protection_state: ProtectionState


@dataclass(frozen=True)
class ErasureDataPackRecord:
    pack_hash: str
    codec: str
    data_shards: int
    parity_shards: int
    payload_size: int
    padded_size: int
    shard_size: int
    protection_state: ProtectionState
    placement_epoch: str | None
    created_at: float
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None


@dataclass(frozen=True)
class ErasureDataPackChunkRecord:
    chunk_hash: str
    pack_hash: str
    offset: int
    length: int
    ordinal: int


@dataclass(frozen=True)
class ErasureDataPackShardRecord:
    pack_hash: str
    shard_index: int
    shard_hash: str
    node_id: str
    size: int
    protection_state: ProtectionState
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None


@dataclass(frozen=True)
class MetadataPackPublicationRecord:
    owner_id: str
    pack_hash: str
    desired_copies: int
    pushed_at: float
    pack_size_bytes: int
    attempted_targets: int
    successful_targets: int
    stored_targets: int
    already_present_targets: int
    failed_targets: int


class MetadataDB:
    """Catálogo operativo local del nodo.

    Conserva la identidad lógica del catálogo, las instantáneas y sus recetas,
    los fragmentos conocidos, los estados de protección por replicación y
    codificación de borrado, y el último resultado de publicación de cada
    paquete de metadatos.

    La estructura estable del contenido permanece separada de la evidencia
    mutable generada por los envíos, reintentos y verificaciones.
    """

    def __init__(
        self,
        db_file: str = DEFAULT_NODE_DB_FILE,
        *,
        init_schema: bool = True,
    ) -> None:
        self.db_file = db_file
        self.conn: sqlite3.Connection = sqlite3.connect(self.db_file)
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()

        if init_schema:
            self._init_db()

    # ------------------------------------------------------------------
    # Connection and lifecycle
    # ------------------------------------------------------------------

    def _configure_connection(self) -> None:
        """Aplica a cada conexión los ajustes de integridad, concurrencia y memoria."""
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute(f"PRAGMA cache_size = -{_SQLITE_CACHE_SIZE_KIB}")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute(f"PRAGMA mmap_size = {_SQLITE_MMAP_SIZE_BYTES}")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self.conn:
            yield

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        """Crea las tablas y los índices requeridos por los recorridos actuales."""
        with self.conn:
            cursor = self.conn.cursor()

            # Identidad lógica del catálogo.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata_vault (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            # Estructura estable de instantáneas, recetas y fragmentos.
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
                CREATE TABLE IF NOT EXISTS recipes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipe_hash TEXT UNIQUE NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    total_size INTEGER NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    hash TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    ref_count INTEGER NOT NULL DEFAULT 0
                )
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
                    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
                        ON DELETE CASCADE,
                    FOREIGN KEY(recipe_id) REFERENCES recipes(id),
                    UNIQUE(snapshot_id, path)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recipe_chunks (
                    recipe_id INTEGER NOT NULL,
                    chunk_order INTEGER NOT NULL,
                    chunk_hash TEXT NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    PRIMARY KEY(recipe_id, chunk_order),
                    FOREIGN KEY(recipe_id) REFERENCES recipes(id)
                        ON DELETE CASCADE,
                    FOREIGN KEY(chunk_hash) REFERENCES chunks(hash)
                )
            """)

            # Evidencia mutable de protección por replicación.
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
                    FOREIGN KEY(chunk_hash) REFERENCES chunks(hash)
                        ON DELETE CASCADE
                )
            """)

            # Paquetes y fragmentos de codificación de borrado.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS erasure_data_packs (
                    pack_hash TEXT PRIMARY KEY,
                    codec TEXT NOT NULL,
                    data_shards INTEGER NOT NULL,
                    parity_shards INTEGER NOT NULL,
                    payload_size INTEGER NOT NULL,
                    padded_size INTEGER NOT NULL,
                    shard_size INTEGER NOT NULL,
                    protection_state TEXT NOT NULL DEFAULT 'PENDING',
                    placement_epoch TEXT,
                    created_at REAL NOT NULL,
                    last_push_at REAL,
                    last_verify_at REAL,
                    last_error TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS erasure_data_pack_chunks (
                    chunk_hash TEXT PRIMARY KEY,
                    pack_hash TEXT NOT NULL,
                    chunk_offset INTEGER NOT NULL,
                    chunk_length INTEGER NOT NULL,
                    chunk_ordinal INTEGER NOT NULL,
                    FOREIGN KEY(chunk_hash) REFERENCES chunks(hash)
                        ON DELETE CASCADE,
                    FOREIGN KEY(pack_hash) REFERENCES erasure_data_packs(pack_hash)
                        ON DELETE CASCADE,
                    UNIQUE(pack_hash, chunk_ordinal)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS erasure_data_pack_shards (
                    pack_hash TEXT NOT NULL,
                    shard_index INTEGER NOT NULL,
                    shard_hash TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    protection_state TEXT NOT NULL DEFAULT 'PENDING',
                    last_push_at REAL,
                    last_verify_at REAL,
                    last_error TEXT,
                    PRIMARY KEY(pack_hash, shard_index),
                    FOREIGN KEY(pack_hash) REFERENCES erasure_data_packs(pack_hash)
                        ON DELETE CASCADE
                )
            """)

            # Último resultado agregado de publicación de metadatos.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata_pack_publications (
                    owner_id TEXT NOT NULL,
                    pack_hash TEXT NOT NULL,
                    desired_copies INTEGER NOT NULL,
                    pushed_at REAL NOT NULL,
                    pack_size_bytes INTEGER NOT NULL DEFAULT 0,
                    attempted_targets INTEGER NOT NULL DEFAULT 0,
                    successful_targets INTEGER NOT NULL DEFAULT 0,
                    stored_targets INTEGER NOT NULL DEFAULT 0,
                    already_present_targets INTEGER NOT NULL DEFAULT 0,
                    failed_targets INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(owner_id, pack_hash)
                )
            """)

            # Índices explícitos vinculados a consultas actuales. Las claves
            # primarias y restricciones UNIQUE ya aportan el resto de recorridos.
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_root_status_id
                ON snapshots(root_path, status, id DESC)
            """)

            # SQLite no indexa automáticamente la columna hija de esta FK.
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recipe_chunks_hash
                ON recipe_chunks(chunk_hash)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunk_protection_state
                ON chunk_protection(protection_state)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_erasure_data_packs_state
                ON erasure_data_packs(protection_state)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metadata_pack_publications_owner_pushed
                ON metadata_pack_publications(owner_id, pushed_at DESC)
            """)

    # ------------------------------------------------------------------
    # Vault identity
    # ------------------------------------------------------------------

    def get_or_create_vault_id(self) -> str:
        row = self.conn.execute(
            "SELECT value FROM metadata_vault WHERE key = 'vault_id' LIMIT 1"
        ).fetchone()
        if row is not None:
            return _require_vault_id("vault_id", row["value"])

        vault_id = uuid.uuid4().hex
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO metadata_vault (key, value) VALUES ('vault_id', ?)",
                (vault_id,),
            )
        row = self.conn.execute(
            "SELECT value FROM metadata_vault WHERE key = 'vault_id' LIMIT 1"
        ).fetchone()
        if row is None:
            raise MetadataDatabaseError("no se pudo crear vault_id en metadata DB")
        return _require_vault_id("vault_id", row["value"])

    def set_vault_id(self, vault_id: str) -> None:
        value = _require_vault_id("vault_id", vault_id)
        row = self.conn.execute(
            "SELECT value FROM metadata_vault WHERE key = 'vault_id' LIMIT 1"
        ).fetchone()
        if row is not None:
            current = _require_vault_id("vault_id", row["value"])
            if current != value:
                raise MetadataDatabaseError(
                    f"metadata DB pertenece a vault_id={current}; no se puede importar vault_id={value}"
                )
            return

        with self.conn:
            self.conn.execute(
                "INSERT INTO metadata_vault (key, value) VALUES ('vault_id', ?)",
                (value,),
            )

    # ------------------------------------------------------------------
    # Metadata pack publications
    # ------------------------------------------------------------------

    def record_metadata_pack_publication(
        self,
        *,
        owner_id: str,
        pack_hash: str,
        desired_copies: int,
        pack_size_bytes: int,
        attempted_targets: int,
        successful_targets: int,
        stored_targets: int,
        already_present_targets: int,
        failed_targets: int,
        pushed_at: float | None = None,
    ) -> None:
        owner = _require_hash64("owner_id", owner_id)
        pack = _require_hash64("pack_hash", pack_hash)
        desired = _require_positive_int("desired_copies", desired_copies)
        pack_size = _require_non_negative_int("pack_size_bytes", pack_size_bytes)
        attempted = _require_non_negative_int("attempted_targets", attempted_targets)
        successful = _require_non_negative_int("successful_targets", successful_targets)
        stored = _require_non_negative_int("stored_targets", stored_targets)
        already_present = _require_non_negative_int("already_present_targets", already_present_targets)
        failed = _require_non_negative_int("failed_targets", failed_targets)
        pushed = time.time() if pushed_at is None else float(pushed_at)

        with self.conn:
            self.conn.execute("""
                INSERT INTO metadata_pack_publications (
                    owner_id, pack_hash, desired_copies, pushed_at, pack_size_bytes,
                    attempted_targets, successful_targets, stored_targets,
                    already_present_targets, failed_targets
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_id, pack_hash) DO UPDATE SET
                    desired_copies = excluded.desired_copies,
                    pushed_at = excluded.pushed_at,
                    pack_size_bytes = excluded.pack_size_bytes,
                    attempted_targets = excluded.attempted_targets,
                    successful_targets = excluded.successful_targets,
                    stored_targets = excluded.stored_targets,
                    already_present_targets = excluded.already_present_targets,
                    failed_targets = excluded.failed_targets
            """, (
                owner,
                pack,
                desired,
                pushed,
                pack_size,
                attempted,
                successful,
                stored,
                already_present,
                failed,
            ))

    def get_metadata_pack_publication(
        self,
        *,
        owner_id: str,
        pack_hash: str,
    ) -> MetadataPackPublicationRecord | None:
        owner = _require_hash64("owner_id", owner_id)
        pack = _require_hash64("pack_hash", pack_hash)
        row = self.conn.execute("""
            SELECT owner_id, pack_hash, desired_copies, pushed_at, pack_size_bytes,
                   attempted_targets, successful_targets, stored_targets,
                   already_present_targets, failed_targets
            FROM metadata_pack_publications
            WHERE owner_id = ? AND pack_hash = ?
            LIMIT 1
        """, (owner, pack)).fetchone()
        return _metadata_pack_publication_record(row) if row is not None else None

    def get_metadata_pack_publications(
        self,
        *,
        owner_id: str,
        pack_hashes: Iterable[str] | None = None,
    ) -> list[MetadataPackPublicationRecord]:
        owner = _require_hash64("owner_id", owner_id)
        if pack_hashes is None:
            rows = self.conn.execute("""
                SELECT owner_id, pack_hash, desired_copies, pushed_at, pack_size_bytes,
                       attempted_targets, successful_targets, stored_targets,
                       already_present_targets, failed_targets
                FROM metadata_pack_publications
                WHERE owner_id = ?
                ORDER BY pushed_at DESC, pack_hash ASC
            """, (owner,)).fetchall()
            return [_metadata_pack_publication_record(row) for row in rows]

        hashes = [_require_hash64("pack_hash", pack_hash) for pack_hash in pack_hashes]
        if not hashes:
            return []
        placeholders = ", ".join("?" for _ in hashes)
        rows = self.conn.execute(f"""
            SELECT owner_id, pack_hash, desired_copies, pushed_at, pack_size_bytes,
                   attempted_targets, successful_targets, stored_targets,
                   already_present_targets, failed_targets
            FROM metadata_pack_publications
            WHERE owner_id = ? AND pack_hash IN ({placeholders})
            ORDER BY pushed_at DESC, pack_hash ASC
        """, (owner, *hashes)).fetchall()
        return [_metadata_pack_publication_record(row) for row in rows]


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

    def get_snapshot_items(self, snapshot_id: int) -> Iterator[dict[str, object]]:
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

    def get_item_chunks(self, item_id: int) -> Iterator[str]:
        """Genera la receta de hashes de un archivo."""
        row = self.conn.execute("""
            SELECT recipe_id
            FROM snapshot_items
            WHERE id = ?
        """, (item_id,)).fetchone()

        if row is None:
            raise MetadataDatabaseValueError(f"Snapshot item does not exist: {item_id}")

        if row["recipe_id"] is None:
            raise MetadataDatabaseValueError(f"Snapshot item has no associated recipe: {item_id}")

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
        recipe_hash = _require_hash64("recipe_hash", recipe_hash)
        chunks = list(chunks)
        desired_rf = int(desired_rf)
        chunk_count = len(chunks)
        total_size = sum(chunk_size for _, _, chunk_size in chunks)

        for expected_order, (chunk_order, chunk_hash, chunk_size) in enumerate(chunks):
            if chunk_order != expected_order:
                raise MetadataDatabaseError(
                    "recipe chunks must be contiguous from 0: "
                    f"expected={expected_order} got={chunk_order}"
                )
            if not isinstance(chunk_hash, str) or len(chunk_hash) != 64 or any(c not in "0123456789abcdef" for c in chunk_hash):
                raise MetadataDatabaseError(f"invalid chunk hash in recipe {recipe_hash}: {chunk_hash!r}")
            if chunk_size < 1:
                raise MetadataDatabaseError(f"invalid chunk size in recipe {recipe_hash}: {chunk_size}")

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
                raise MetadataDatabaseError(f"Could not resolve recipe_id for {recipe_hash}")

            if row["chunk_count"] != chunk_count or row["total_size"] != total_size:
                raise MetadataDatabaseError(
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
                    raise MetadataDatabaseError(f"Inconsistent recipe_chunks for recipe_hash: {recipe_hash}")

            self._register_chunks(chunks)

            if not existing_chunks and chunk_count > 0:
                self.conn.executemany("""
                    INSERT INTO recipe_chunks (recipe_id, chunk_order, chunk_hash, chunk_size)
                    VALUES (?, ?, ?, ?)
                """, (
                    (recipe_id, order, chunk_hash, chunk_size)
                    for order, chunk_hash, chunk_size in chunks
                ))

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
        ref_counts: dict[str, list[int]] = {}
        for _, chunk_hash, chunk_size in chunks:
            current = ref_counts.get(chunk_hash)
            if current is None:
                ref_counts[chunk_hash] = [chunk_size, 1]
                continue
            if current[0] != chunk_size:
                raise MetadataDatabaseError(
                    "un mismo chunk no puede tener tamaños distintos dentro de una receta: "
                    f"hash={chunk_hash} tamaños={current[0]},{chunk_size}"
                )
            current[1] += 1

        self.conn.executemany("""
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, ?)
            ON CONFLICT(hash) DO UPDATE SET
                ref_count = ref_count + excluded.ref_count
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
        chunk_hashes = _unique_hashes(chunk_hash for _, chunk_hash, _ in chunks)
        rows = [(chunk_hash, desired_rf) for chunk_hash in chunk_hashes]
        if not rows:
            return

        self.conn.executemany("""
            INSERT INTO chunk_protection (
                chunk_hash, desired_rf, protection_state, protected_remote_copies,
                placement_epoch, last_push_at, last_verify_at, last_error
            )
            VALUES (?, ?, ?, 0, NULL, NULL, NULL, NULL)
            ON CONFLICT(chunk_hash) DO UPDATE SET
                desired_rf = MAX(
                    chunk_protection.desired_rf,
                    excluded.desired_rf
                ),
                protection_state = CASE
                    WHEN chunk_protection.desired_rf < excluded.desired_rf
                         AND chunk_protection.protection_state IN (?, ?)
                    THEN ?
                    ELSE chunk_protection.protection_state
                END,
                last_error = CASE
                    WHEN chunk_protection.desired_rf < excluded.desired_rf
                         AND chunk_protection.protection_state IN (?, ?)
                    THEN 'desired_rf increased'
                    ELSE chunk_protection.last_error
                END
        """, (
            (
                chunk_hash,
                desired_rf,
                ProtectionState.PENDING.value,
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
                ProtectionState.DEGRADED.value,
                ProtectionState.PLACED.value,
                ProtectionState.VERIFIED.value,
            )
            for chunk_hash, desired_rf in rows
        ))

    def has_chunk(self, chunk_hash: str) -> bool:
        row = self.conn.execute("""
            SELECT 1
            FROM chunks
            WHERE hash = ?
        """, (chunk_hash,)).fetchone()
        return row is not None

    def sum_chunk_sizes(self, chunk_hashes: Iterable[str]) -> int:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return 0

        total = 0
        batch_size = 500
        for offset in range(0, len(hashes), batch_size):
            batch = hashes[offset:offset + batch_size]
            placeholders = ", ".join("?" for _ in batch)
            row = self.conn.execute(
                f"""
                SELECT COALESCE(SUM(size), 0) AS total_size
                FROM chunks
                WHERE hash IN ({placeholders})
                """,
                tuple(batch),
            ).fetchone()
            total += int(row["total_size"] or 0)
        return total


    # ------------------------------------------------------------------
    # Erasure coding data packs
    # ------------------------------------------------------------------

    def register_erasure_data_pack(
        self,
        *,
        pack_hash: str,
        codec: str,
        data_shards: int,
        parity_shards: int,
        payload_size: int,
        padded_size: int,
        shard_size: int,
        chunks: Iterable[tuple[str, int, int, int]],
        shards: Iterable[tuple[int, str, str, int]],
        protection_state: ProtectionState = ProtectionState.PENDING,
        placement_epoch: str | None = None,
    ) -> None:
        """
        Registra un data pack EC sellado.

        chunks usa tuplas (chunk_hash, offset, length, ordinal).
        shards usa tuplas (shard_index, shard_hash, node_id, size).
        """
        pack_hash = _require_hash64("pack_hash", pack_hash)
        codec = _require_non_empty_text("codec", codec)
        data_shards = _require_positive_int("data_shards", data_shards)
        parity_shards = _require_non_negative_int("parity_shards", parity_shards)
        payload_size = _require_non_negative_int("payload_size", payload_size)
        padded_size = _require_non_negative_int("padded_size", padded_size)
        shard_size = _require_positive_int("shard_size", shard_size)
        state_value = _protection_state_value(protection_state)
        total_shards = data_shards + parity_shards

        if padded_size != shard_size * data_shards:
            raise MetadataDatabaseError("padded_size debe coincidir con shard_size * data_shards")
        if payload_size > padded_size:
            raise MetadataDatabaseError("payload_size no puede ser mayor que padded_size")

        chunk_rows = _normalize_erasure_chunk_rows(chunks, payload_size)
        shard_rows = _normalize_erasure_shard_rows(shards, total_shards)
        now = time.time()
        last_push_at = now if state_value != ProtectionState.PENDING.value else None

        with self.conn:
            self._insert_erasure_data_pack_once(
                pack_hash=pack_hash,
                codec=codec,
                data_shards=data_shards,
                parity_shards=parity_shards,
                payload_size=payload_size,
                padded_size=padded_size,
                shard_size=shard_size,
                protection_state=state_value,
                placement_epoch=placement_epoch,
                created_at=now,
                last_push_at=last_push_at,
            )
            self._insert_erasure_pack_chunks_once(pack_hash, chunk_rows)
            self._insert_erasure_pack_shards_once(pack_hash, shard_rows, state_value, last_push_at)


    def refresh_erasure_data_pack_push(
        self,
        *,
        pack_hash: str,
        codec: str,
        data_shards: int,
        parity_shards: int,
        payload_size: int,
        padded_size: int,
        shard_size: int,
        chunks: Iterable[tuple[str, int, int, int]],
        shards: Iterable[tuple[int, str, str, int]],
        protection_state: ProtectionState,
        placement_epoch: str | None,
    ) -> None:
        """
        Actualiza el resultado de push de un data pack EC ya registrado.

        No reasigna chunks a otro pack ni cambia la identidad del pack. Solo
        refresca estado, epoch y filas de shards tras un reintento.
        """
        pack_hash = _require_hash64("pack_hash", pack_hash)
        codec = _require_non_empty_text("codec", codec)
        data_shards = _require_positive_int("data_shards", data_shards)
        parity_shards = _require_non_negative_int("parity_shards", parity_shards)
        payload_size = _require_non_negative_int("payload_size", payload_size)
        padded_size = _require_non_negative_int("padded_size", padded_size)
        shard_size = _require_positive_int("shard_size", shard_size)
        state_value = _protection_state_value(protection_state)
        total_shards = data_shards + parity_shards

        if padded_size != shard_size * data_shards:
            raise MetadataDatabaseError("padded_size debe coincidir con shard_size * data_shards")
        if payload_size > padded_size:
            raise MetadataDatabaseError("payload_size no puede ser mayor que padded_size")

        chunk_rows = _normalize_erasure_chunk_rows(chunks, payload_size)
        shard_rows = _normalize_erasure_shard_rows(shards, total_shards)
        now = time.time()

        with self.conn:
            existing = self.conn.execute("""
                SELECT codec, data_shards, parity_shards, payload_size, padded_size,
                       shard_size
                FROM erasure_data_packs
                WHERE pack_hash = ?
            """, (pack_hash,)).fetchone()
            if existing is None:
                raise MetadataDatabaseError(f"erasure data pack no registrado: {pack_hash}")

            current = (
                existing["codec"],
                int(existing["data_shards"]),
                int(existing["parity_shards"]),
                int(existing["payload_size"]),
                int(existing["padded_size"]),
                int(existing["shard_size"]),
            )
            expected = (
                codec,
                data_shards,
                parity_shards,
                payload_size,
                padded_size,
                shard_size,
            )
            if current != expected:
                raise MetadataDatabaseError(f"erasure data pack inconsistente: {pack_hash}")

            self._assert_erasure_pack_chunks_match(pack_hash, chunk_rows)
            self.conn.execute("""
                UPDATE erasure_data_packs
                SET protection_state = ?,
                    placement_epoch = ?,
                    last_push_at = ?,
                    last_verify_at = NULL,
                    last_error = NULL
                WHERE pack_hash = ?
            """, (
                state_value,
                placement_epoch,
                now,
                pack_hash,
            ))
            self._upsert_erasure_pack_shards_for_push(
                pack_hash=pack_hash,
                rows=shard_rows,
                protection_state=state_value,
                last_push_at=now,
            )

    def get_erasure_data_pack(self, pack_hash: str) -> ErasureDataPackRecord | None:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        row = self.conn.execute("""
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            WHERE pack_hash = ?
        """, (pack_hash,)).fetchone()

        return _erasure_pack_record(row) if row is not None else None

    def get_erasure_chunk_location(
        self,
        chunk_hash: str,
    ) -> ErasureDataPackChunkRecord | None:
        chunk_hash = _require_hash64("chunk_hash", chunk_hash)
        row = self.conn.execute("""
            SELECT chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
            FROM erasure_data_pack_chunks
            WHERE chunk_hash = ?
        """, (chunk_hash,)).fetchone()

        return _erasure_chunk_record(row) if row is not None else None

    def get_erasure_chunk_locations(
        self,
        chunk_hashes: Iterable[str],
    ) -> dict[str, ErasureDataPackChunkRecord]:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return {}

        placeholders = ", ".join("?" for _ in hashes)
        rows = self.conn.execute(f"""
            SELECT chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
            FROM erasure_data_pack_chunks
            WHERE chunk_hash IN ({placeholders})
            ORDER BY pack_hash ASC, chunk_ordinal ASC
        """, tuple(hashes)).fetchall()

        return {
            row["chunk_hash"]: _erasure_chunk_record(row)
            for row in rows
        }

    def get_erasure_pack_chunks(
        self,
        pack_hash: str,
    ) -> list[ErasureDataPackChunkRecord]:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        rows = self.conn.execute("""
            SELECT chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
            FROM erasure_data_pack_chunks
            WHERE pack_hash = ?
            ORDER BY chunk_ordinal ASC
        """, (pack_hash,)).fetchall()

        return [_erasure_chunk_record(row) for row in rows]

    def get_erasure_pack_shards(
        self,
        pack_hash: str,
    ) -> list[ErasureDataPackShardRecord]:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        rows = self.conn.execute("""
            SELECT pack_hash, shard_index, shard_hash, node_id, size,
                   protection_state, last_push_at, last_verify_at, last_error
            FROM erasure_data_pack_shards
            WHERE pack_hash = ?
            ORDER BY shard_index ASC
        """, (pack_hash,)).fetchall()

        return [_erasure_shard_record(row) for row in rows]

    def get_erasure_unprotected_chunks(self, *, limit: int | None = None) -> list[str]:
        query = """
            SELECT c.hash
            FROM chunks c
            LEFT JOIN erasure_data_pack_chunks epc ON epc.chunk_hash = c.hash
            WHERE epc.chunk_hash IS NULL
            ORDER BY c.hash ASC
        """
        params: list[object] = []

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [row["hash"] for row in rows]

    def get_pending_erasure_data_packs(
        self,
        *,
        current_epoch: str | None = None,
        limit: int | None = None,
    ) -> list[ErasureDataPackRecord]:
        params: list[object] = [
            ProtectionState.PENDING.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
        ]
        query = """
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            WHERE protection_state IN (?, ?, ?)
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

        query += " ORDER BY pack_hash ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [_erasure_pack_record(row) for row in rows]

    def get_erasure_verification_candidates(
        self,
        *,
        include_verified: bool = False,
        limit: int | None = None,
    ) -> list[ErasureDataPackRecord]:
        states = [
            ProtectionState.PLACED.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
        ]
        if include_verified:
            states.append(ProtectionState.VERIFIED.value)

        placeholders = ", ".join("?" for _ in states)
        query = f"""
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            WHERE protection_state IN ({placeholders})
            ORDER BY pack_hash ASC
        """
        params: list[object] = list(states)

        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [_erasure_pack_record(row) for row in rows]

    def mark_erasure_data_pack_verification(
        self,
        pack_hash: str,
        *,
        protection_state: ProtectionState,
        verified_shard_indexes: Iterable[int],
        error: str | None = None,
    ) -> None:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        state_value = _protection_state_value(protection_state)
        verified_indexes = {
            _require_non_negative_int("verified_shard_index", index)
            for index in verified_shard_indexes
        }
        now = time.time()

        with self.conn:
            self.conn.execute("""
                UPDATE erasure_data_packs
                SET protection_state = ?,
                    last_verify_at = ?,
                    last_error = ?
                WHERE pack_hash = ?
            """, (
                state_value,
                now,
                error,
                pack_hash,
            ))

            if verified_indexes:
                placeholders = ", ".join("?" for _ in verified_indexes)
                self.conn.execute(f"""
                    UPDATE erasure_data_pack_shards
                    SET protection_state = ?,
                        last_verify_at = ?,
                        last_error = NULL
                    WHERE pack_hash = ?
                      AND shard_index IN ({placeholders})
                """, (
                    ProtectionState.VERIFIED.value,
                    now,
                    pack_hash,
                    *sorted(verified_indexes),
                ))

            missing_state = (
                ProtectionState.FAILED
                if protection_state == ProtectionState.FAILED
                else ProtectionState.DEGRADED
            )
            self.conn.execute("""
                UPDATE erasure_data_pack_shards
                SET protection_state = ?,
                    last_verify_at = ?,
                    last_error = ?
                WHERE pack_hash = ?
            """ + (
                ""
                if not verified_indexes
                else f" AND shard_index NOT IN ({', '.join('?' for _ in verified_indexes)})"
            ), (
                missing_state.value,
                now,
                error,
                pack_hash,
                *sorted(verified_indexes),
            ))

    def mark_erasure_data_pack_state(
        self,
        pack_hash: str,
        *,
        protection_state: ProtectionState,
        placement_epoch: str | None = None,
        error: str | None = None,
    ) -> None:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        state_value = _protection_state_value(protection_state)
        now = time.time()

        last_push_at = now if protection_state in (
            ProtectionState.PLACED,
            ProtectionState.DEGRADED,
            ProtectionState.FAILED,
        ) else None
        last_verify_at = now if protection_state == ProtectionState.VERIFIED else None

        with self.conn:
            self.conn.execute("""
                UPDATE erasure_data_packs
                SET protection_state = ?,
                    placement_epoch = COALESCE(?, placement_epoch),
                    last_push_at = COALESCE(?, last_push_at),
                    last_verify_at = COALESCE(?, last_verify_at),
                    last_error = ?
                WHERE pack_hash = ?
            """, (
                state_value,
                placement_epoch,
                last_push_at,
                last_verify_at,
                error,
                pack_hash,
            ))
            self.conn.execute("""
                UPDATE erasure_data_pack_shards
                SET protection_state = ?,
                    last_push_at = COALESCE(?, last_push_at),
                    last_verify_at = COALESCE(?, last_verify_at),
                    last_error = ?
                WHERE pack_hash = ?
            """, (
                state_value,
                last_push_at,
                last_verify_at,
                error,
                pack_hash,
            ))

    def _insert_erasure_data_pack_once(
        self,
        *,
        pack_hash: str,
        codec: str,
        data_shards: int,
        parity_shards: int,
        payload_size: int,
        padded_size: int,
        shard_size: int,
        protection_state: str,
        placement_epoch: str | None,
        created_at: float,
        last_push_at: float | None,
    ) -> None:
        existing = self.conn.execute("""
            SELECT codec, data_shards, parity_shards, payload_size, padded_size,
                   shard_size
            FROM erasure_data_packs
            WHERE pack_hash = ?
        """, (pack_hash,)).fetchone()

        expected = (
            codec,
            data_shards,
            parity_shards,
            payload_size,
            padded_size,
            shard_size,
        )
        if existing is not None:
            current = (
                existing["codec"],
                int(existing["data_shards"]),
                int(existing["parity_shards"]),
                int(existing["payload_size"]),
                int(existing["padded_size"]),
                int(existing["shard_size"]),
            )
            if current != expected:
                raise MetadataDatabaseError(f"erasure data pack inconsistente: {pack_hash}")
            return

        self.conn.execute("""
            INSERT INTO erasure_data_packs (
                pack_hash, codec, data_shards, parity_shards, payload_size,
                padded_size, shard_size, protection_state, placement_epoch,
                created_at, last_push_at, last_verify_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """, (
            pack_hash,
            codec,
            data_shards,
            parity_shards,
            payload_size,
            padded_size,
            shard_size,
            protection_state,
            placement_epoch,
            created_at,
            last_push_at,
        ))

    def _insert_erasure_pack_chunks_once(
        self,
        pack_hash: str,
        rows: list[tuple[str, int, int, int]],
    ) -> None:
        for chunk_hash, offset, length, ordinal in rows:
            existing = self.conn.execute("""
                SELECT pack_hash, chunk_offset, chunk_length, chunk_ordinal
                FROM erasure_data_pack_chunks
                WHERE chunk_hash = ?
            """, (chunk_hash,)).fetchone()
            if existing is not None:
                current = (
                    existing["pack_hash"],
                    int(existing["chunk_offset"]),
                    int(existing["chunk_length"]),
                    int(existing["chunk_ordinal"]),
                )
                if current != (pack_hash, offset, length, ordinal):
                    raise MetadataDatabaseError(f"chunk EC ya asignado a otro pack: {chunk_hash}")
                continue

            self.conn.execute("""
                INSERT INTO erasure_data_pack_chunks (
                    chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
                )
                VALUES (?, ?, ?, ?, ?)
            """, (chunk_hash, pack_hash, offset, length, ordinal))

    def _insert_erasure_pack_shards_once(
        self,
        pack_hash: str,
        rows: list[tuple[int, str, str, int]],
        protection_state: str,
        last_push_at: float | None,
    ) -> None:
        for shard_index, shard_hash, node_id, size in rows:
            existing = self.conn.execute("""
                SELECT shard_hash, node_id, size
                FROM erasure_data_pack_shards
                WHERE pack_hash = ? AND shard_index = ?
            """, (pack_hash, shard_index)).fetchone()
            if existing is not None:
                current = (
                    existing["shard_hash"],
                    existing["node_id"],
                    int(existing["size"]),
                )
                if current != (shard_hash, node_id, size):
                    raise MetadataDatabaseError(
                        f"shard EC inconsistente: pack={pack_hash} index={shard_index}"
                    )
                continue

            self.conn.execute("""
                INSERT INTO erasure_data_pack_shards (
                    pack_hash, shard_index, shard_hash, node_id, size,
                    protection_state, last_push_at, last_verify_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """, (
                pack_hash,
                shard_index,
                shard_hash,
                node_id,
                size,
                protection_state,
                last_push_at,
            ))

    def _assert_erasure_pack_chunks_match(
        self,
        pack_hash: str,
        rows: list[tuple[str, int, int, int]],
    ) -> None:
        existing_rows = self.conn.execute("""
            SELECT chunk_hash, chunk_offset, chunk_length, chunk_ordinal
            FROM erasure_data_pack_chunks
            WHERE pack_hash = ?
            ORDER BY chunk_ordinal ASC
        """, (pack_hash,)).fetchall()
        current = [
            (
                row["chunk_hash"],
                int(row["chunk_offset"]),
                int(row["chunk_length"]),
                int(row["chunk_ordinal"]),
            )
            for row in existing_rows
        ]
        if current != rows:
            raise MetadataDatabaseError(f"chunks EC inconsistentes para pack: {pack_hash}")

    def _upsert_erasure_pack_shards_for_push(
        self,
        *,
        pack_hash: str,
        rows: list[tuple[int, str, str, int]],
        protection_state: str,
        last_push_at: float,
    ) -> None:
        seen_indexes = {shard_index for shard_index, _shard_hash, _node_id, _size in rows}
        for shard_index, shard_hash, node_id, size in rows:
            existing = self.conn.execute("""
                SELECT 1
                FROM erasure_data_pack_shards
                WHERE pack_hash = ? AND shard_index = ?
            """, (pack_hash, shard_index)).fetchone()
            if existing is None:
                self.conn.execute("""
                    INSERT INTO erasure_data_pack_shards (
                        pack_hash, shard_index, shard_hash, node_id, size,
                        protection_state, last_push_at, last_verify_at, last_error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """, (
                    pack_hash,
                    shard_index,
                    shard_hash,
                    node_id,
                    size,
                    protection_state,
                    last_push_at,
                ))
                continue

            self.conn.execute("""
                UPDATE erasure_data_pack_shards
                SET shard_hash = ?,
                    node_id = ?,
                    size = ?,
                    protection_state = ?,
                    last_push_at = ?,
                    last_verify_at = NULL,
                    last_error = NULL
                WHERE pack_hash = ?
                  AND shard_index = ?
            """, (
                shard_hash,
                node_id,
                size,
                protection_state,
                last_push_at,
                pack_hash,
                shard_index,
            ))

        placeholders = ", ".join("?" for _ in seen_indexes)
        if placeholders:
            self.conn.execute(f"""
                DELETE FROM erasure_data_pack_shards
                WHERE pack_hash = ?
                  AND shard_index NOT IN ({placeholders})
            """, (pack_hash, *sorted(seen_indexes)))


    # ------------------------------------------------------------------
    # Metadata object graph import/export helpers
    # ------------------------------------------------------------------

    def count_operational_rows(self, tables: Iterable[str]) -> dict[str, int]:
        allowed = {
            "snapshots",
            "snapshot_items",
            "recipes",
            "recipe_chunks",
            "chunks",
            "chunk_protection",
            "erasure_data_packs",
            "erasure_data_pack_chunks",
            "erasure_data_pack_shards",
        }
        counts: dict[str, int] = {}
        for table in tables:
            if table not in allowed:
                raise MetadataDatabaseError(f"tabla operacional no permitida: {table!r}")
            row = self.conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            counts[table] = int(row["n"]) if row is not None else 0
        return counts

    def object_export_snapshot_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT id, root_path, origin_node_id, created_at, total_size,
                   total_files, status, error, uuid
            FROM snapshots
            ORDER BY created_at ASC, uuid ASC
            """
        ).fetchall()

    def object_export_snapshot_item_rows(self, snapshot_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT id, path, item_type, size, mode, mtime, mtime_ns, uid, gid, recipe_id
            FROM snapshot_items
            WHERE snapshot_id = ?
            ORDER BY path ASC
            """,
            (int(snapshot_id),),
        ).fetchall()

    def object_export_recipe_row(self, recipe_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT id, recipe_hash, chunk_count, total_size
            FROM recipes
            WHERE id = ?
            """,
            (int(recipe_id),),
        ).fetchone()

    def object_export_recipe_hash(self, recipe_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT recipe_hash FROM recipes WHERE id = ?",
            (int(recipe_id),),
        ).fetchone()
        return None if row is None else row["recipe_hash"]

    def object_export_recipe_chunk_rows(self, recipe_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT chunk_order, chunk_hash, chunk_size
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
            """,
            (int(recipe_id),),
        ).fetchall()

    def object_export_known_chunk_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT hash, size
            FROM chunks
            ORDER BY hash ASC
            """
        ).fetchall()

    def object_export_protection_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT chunk_hash, desired_rf, protection_state, protected_remote_copies,
                   placement_epoch, last_push_at, last_verify_at, last_error
            FROM chunk_protection
            ORDER BY chunk_hash ASC
            """
        ).fetchall()

    def object_export_erasure_pack_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            ORDER BY pack_hash ASC
            """
        ).fetchall()

    def object_export_erasure_pack_chunk_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
            FROM erasure_data_pack_chunks
            ORDER BY pack_hash ASC, chunk_ordinal ASC
            """
        ).fetchall()

    def object_export_erasure_pack_shard_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT pack_hash, shard_index, shard_hash, node_id, size,
                   protection_state, last_push_at, last_verify_at, last_error
            FROM erasure_data_pack_shards
            ORDER BY pack_hash ASC, shard_index ASC
            """
        ).fetchall()

    def object_import_insert_chunks(self, chunks: dict[str, int]) -> int:
        if not chunks:
            return 0
        self.conn.executemany(
            """
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, 0)
            """,
            [(chunk_hash, int(size)) for chunk_hash, size in sorted(chunks.items())],
        )
        return len(chunks)

    def object_import_insert_snapshot(
        self,
        *,
        snapshot_uuid: str,
        root_path: str,
        origin_node_id: str,
        status: str,
        error: str | None,
        total_size: int,
        total_files: int,
        created_at: str,
    ) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO snapshots
                (uuid, root_path, origin_node_id, status, error, total_size, total_files, created_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_uuid,
                root_path,
                origin_node_id,
                status,
                error,
                int(total_size),
                int(total_files),
                created_at,
            ),
        )
        return int(cursor.lastrowid)

    def object_import_insert_protection_records(
        self,
        rows: Iterable[tuple[str, int, str, int, str | None, object, object, object]],
    ) -> int:
        normalized = [tuple(row) for row in rows]
        if not normalized:
            return 0
        self.conn.executemany(
            """
            INSERT INTO chunk_protection
                (chunk_hash, desired_rf, protection_state, protected_remote_copies,
                 placement_epoch, last_push_at, last_verify_at, last_error)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized,
        )
        return len(normalized)

    def object_import_insert_erasure_data_pack_records(self, records: Iterable[object]) -> int:
        count = 0
        for record in records:
            self.conn.execute(
                """
                INSERT INTO erasure_data_packs (
                    pack_hash, codec, data_shards, parity_shards, payload_size,
                    padded_size, shard_size, protection_state, placement_epoch,
                    created_at, last_push_at, last_verify_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                record.pack,
            )
            self.conn.executemany(
                """
                INSERT INTO erasure_data_pack_chunks (
                    chunk_hash, pack_hash, chunk_offset, chunk_length, chunk_ordinal
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                record.chunks,
            )
            self.conn.executemany(
                """
                INSERT INTO erasure_data_pack_shards (
                    pack_hash, shard_index, shard_hash, node_id, size,
                    protection_state, last_push_at, last_verify_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                record.shards,
            )
            count += 1
        return count

    def object_import_insert_chunk_zero_ref(self, chunk_hash: str, chunk_size: int) -> None:
        self.conn.execute(
            "INSERT INTO chunks (hash, size, ref_count) VALUES (?, ?, 0)",
            (chunk_hash, int(chunk_size)),
        )

    def object_import_insert_recipe_if_missing(
        self,
        *,
        recipe_hash: str,
        chunk_count: int,
        total_size: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO recipes (recipe_hash, chunk_count, total_size)
            VALUES (?, ?, ?)
            """,
            (recipe_hash, int(chunk_count), int(total_size)),
        )

    def object_import_recipe_summary_by_hash(self, recipe_hash: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT id, chunk_count, total_size
            FROM recipes
            WHERE recipe_hash = ?
            """,
            (recipe_hash,),
        ).fetchone()

    def object_import_recipe_chunk_rows(self, recipe_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT chunk_order, chunk_hash, chunk_size
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
            """,
            (int(recipe_id),),
        ).fetchall()

    def object_import_insert_recipe_chunks(
        self,
        recipe_id: int,
        chunks: Iterable[tuple[int, str, int]],
    ) -> None:
        self.conn.executemany(
            """
            INSERT INTO recipe_chunks (recipe_id, chunk_order, chunk_hash, chunk_size)
            VALUES (?, ?, ?, ?)
            """,
            [
                (int(recipe_id), int(order), chunk_hash, int(chunk_size))
                for order, chunk_hash, chunk_size in chunks
            ],
        )

    def object_import_increment_chunk_ref_counts(self, increments: dict[str, int]) -> None:
        if not increments:
            return
        self.conn.executemany(
            """
            UPDATE chunks
            SET ref_count = ref_count + ?
            WHERE hash = ?
            """,
            [(int(count), chunk_hash) for chunk_hash, count in sorted(increments.items())],
        )

    def object_import_insert_snapshot_item(
        self,
        *,
        snapshot_id: int,
        path: str,
        item_type: str,
        size: int,
        mode: int,
        mtime: float,
        mtime_ns: int,
        uid: int,
        gid: int,
        recipe_id: int | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO snapshot_items
                (snapshot_id, path, item_type, size, mode, mtime, mtime_ns, uid, gid, recipe_id)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(snapshot_id),
                path,
                item_type,
                int(size),
                int(mode),
                float(mtime),
                int(mtime_ns),
                int(uid),
                int(gid),
                recipe_id,
            ),
        )

    # ------------------------------------------------------------------
    # Protection scope queries
    # ------------------------------------------------------------------

    def snapshot_chunk_hashes(self, snapshot_id: int) -> tuple[str, ...]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT rc.chunk_hash
            FROM snapshot_items AS si
            JOIN recipe_chunks AS rc ON rc.recipe_id = si.recipe_id
            WHERE si.snapshot_id = ?
              AND si.item_type = 'file'
              AND si.recipe_id IS NOT NULL
            ORDER BY rc.chunk_hash ASC
            """,
            (int(snapshot_id),),
        ).fetchall()
        return tuple(row["chunk_hash"] for row in rows)

    def all_reachable_chunk_hashes(self) -> tuple[str, ...]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT rc.chunk_hash
            FROM snapshots AS s
            JOIN snapshot_items AS si ON si.snapshot_id = s.id
            JOIN recipe_chunks AS rc ON rc.recipe_id = si.recipe_id
            WHERE s.status = 'COMPLETE'
              AND si.item_type = 'file'
              AND si.recipe_id IS NOT NULL
            ORDER BY rc.chunk_hash ASC
            """
        ).fetchall()
        return tuple(row["chunk_hash"] for row in rows)

    def all_known_chunk_hashes(self) -> tuple[str, ...]:
        rows = self.conn.execute("SELECT hash FROM chunks ORDER BY hash ASC").fetchall()
        return tuple(row["hash"] for row in rows)

    def chunk_protection_verification_candidates_for_hashes(
        self,
        chunk_hashes: Iterable[str],
        *,
        include_verified: bool,
        limit: int | None,
    ) -> list[VerificationCandidate]:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return []
        states = [
            ProtectionState.PENDING.value,
            ProtectionState.PLACED.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
        ]
        if include_verified:
            states.append(ProtectionState.VERIFIED.value)
        hash_placeholders = ", ".join("?" for _ in hashes)
        state_placeholders = ", ".join("?" for _ in states)
        query = f"""
            SELECT chunk_hash, desired_rf, protection_state
            FROM chunk_protection
            WHERE chunk_hash IN ({hash_placeholders})
              AND protection_state IN ({state_placeholders})
            ORDER BY chunk_hash ASC
        """
        params: list[object] = list(hashes) + list(states)
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

    def pending_protection_chunks_for_hashes(
        self,
        chunk_hashes: Iterable[str],
        *,
        desired_rf: int,
        current_epoch: str | None,
        limit: int | None,
    ) -> list[str]:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return []
        hash_placeholders = ", ".join("?" for _ in hashes)
        params: list[object] = [
            *hashes,
            ProtectionState.PENDING.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
            int(desired_rf),
        ]
        query = f"""
            SELECT chunk_hash
            FROM chunk_protection
            WHERE chunk_hash IN ({hash_placeholders})
              AND (
                    protection_state IN (?, ?, ?)
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
        query += """
              )
            ORDER BY chunk_hash ASC
        """
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [row["chunk_hash"] for row in rows]

    def erasure_pack_hashes_for_chunks(self, chunk_hashes: Iterable[str]) -> tuple[str, ...]:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return ()
        placeholders = ", ".join("?" for _ in hashes)
        rows = self.conn.execute(
            f"""
            SELECT DISTINCT pack_hash
            FROM erasure_data_pack_chunks
            WHERE chunk_hash IN ({placeholders})
            ORDER BY pack_hash ASC
            """,
            hashes,
        ).fetchall()
        return tuple(row["pack_hash"] for row in rows)

    def all_erasure_pack_hashes(self) -> tuple[str, ...]:
        rows = self.conn.execute(
            "SELECT pack_hash FROM erasure_data_packs ORDER BY pack_hash ASC"
        ).fetchall()
        return tuple(row["pack_hash"] for row in rows)

    def erasure_data_packs_by_hashes(
        self,
        pack_hashes: Iterable[str],
        *,
        include_verified: bool,
        limit: int | None,
    ) -> list[ErasureDataPackRecord]:
        rows = self._select_erasure_data_pack_rows_by_hashes(
            pack_hashes,
            include_verified=include_verified,
            limit=limit,
        )
        return [_erasure_pack_record(row) for row in rows]

    def pending_erasure_data_packs_by_hashes(
        self,
        pack_hashes: Iterable[str],
        *,
        current_epoch: str | None,
        limit: int | None,
    ) -> list[ErasureDataPackRecord]:
        hashes = _unique_hashes(pack_hashes)
        if not hashes:
            return []
        hash_placeholders = ", ".join("?" for _ in hashes)
        params: list[object] = [
            *hashes,
            ProtectionState.PENDING.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
        ]
        query = f"""
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            WHERE pack_hash IN ({hash_placeholders})
              AND (
                    protection_state IN (?, ?, ?)
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
        query += """
              )
            ORDER BY pack_hash ASC
        """
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [_erasure_pack_record(row) for row in rows]

    def erasure_unprotected_chunks_for_hashes(
        self,
        chunk_hashes: Iterable[str],
        *,
        limit: int | None,
    ) -> list[str]:
        hashes = _unique_hashes(chunk_hashes)
        if not hashes:
            return []
        placeholders = ", ".join("?" for _ in hashes)
        query = f"""
            SELECT c.hash
            FROM chunks c
            LEFT JOIN erasure_data_pack_chunks epc ON epc.chunk_hash = c.hash
            WHERE c.hash IN ({placeholders})
              AND epc.chunk_hash IS NULL
            ORDER BY c.hash ASC
        """
        params: list[object] = list(hashes)
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [row["hash"] for row in rows]

    def _select_erasure_data_pack_rows_by_hashes(
        self,
        pack_hashes: Iterable[str],
        *,
        include_verified: bool,
        limit: int | None,
    ) -> list[sqlite3.Row]:
        hashes = _unique_hashes(pack_hashes)
        if not hashes:
            return []
        states = [
            ProtectionState.PENDING.value,
            ProtectionState.PLACED.value,
            ProtectionState.DEGRADED.value,
            ProtectionState.FAILED.value,
        ]
        if include_verified:
            states.append(ProtectionState.VERIFIED.value)
        hash_placeholders = ", ".join("?" for _ in hashes)
        state_placeholders = ", ".join("?" for _ in states)
        query = f"""
            SELECT pack_hash, codec, data_shards, parity_shards, payload_size,
                   padded_size, shard_size, protection_state, placement_epoch,
                   created_at, last_push_at, last_verify_at, last_error
            FROM erasure_data_packs
            WHERE pack_hash IN ({hash_placeholders})
              AND protection_state IN ({state_placeholders})
            ORDER BY pack_hash ASC
        """
        params: list[object] = list(hashes) + list(states)
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        return self.conn.execute(query, tuple(params)).fetchall()

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


_HASH64_ALPHABET = set("0123456789abcdef")


def _unique_hashes(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_require_hash64("hash", value) for value in values))


def _require_vault_id(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataDatabaseValueError(f"{name} debe ser str")
    text = value.strip()
    if len(text) != 32 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataDatabaseValueError(f"{name} debe tener 32 caracteres hexadecimales lowercase")
    return text


def _require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataDatabaseError(f"{name} debe ser str; recibido {type(value).__name__}")
    text = value.strip()
    if len(text) != 64 or any(char not in _HASH64_ALPHABET for char in text):
        raise MetadataDatabaseError(f"{name} debe tener 64 caracteres hexadecimales lowercase")
    return text


def _require_non_empty_text(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataDatabaseError(f"{name} debe ser str; recibido {type(value).__name__}")
    text = value.strip()
    if not text:
        raise MetadataDatabaseError(f"{name} no puede estar vacío")
    return text


def _require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataDatabaseError(f"{name} debe ser int; recibido {type(value).__name__}")
    if value < 0:
        raise MetadataDatabaseError(f"{name} debe ser >= 0; recibido {value}")
    return value


def _require_positive_int(name: str, value: object) -> int:
    number = _require_non_negative_int(name, value)
    if number <= 0:
        raise MetadataDatabaseError(f"{name} debe ser > 0; recibido {number}")
    return number


def _protection_state_value(value: ProtectionState) -> str:
    if not isinstance(value, ProtectionState):
        raise MetadataDatabaseError("protection_state debe ser ProtectionState")
    return value.value


def _normalize_erasure_chunk_rows(
    chunks: Iterable[tuple[str, int, int, int]],
    payload_size: int,
) -> list[tuple[str, int, int, int]]:
    rows = [
        (
            _require_hash64("chunk_hash", chunk_hash),
            _require_non_negative_int("chunk_offset", offset),
            _require_positive_int("chunk_length", length),
            _require_non_negative_int("chunk_ordinal", ordinal),
        )
        for chunk_hash, offset, length, ordinal in chunks
    ]
    if not rows:
        raise MetadataDatabaseError("un data pack EC debe contener al menos un chunk")

    rows.sort(key=lambda item: item[3])
    previous_end = 0
    for expected_ordinal, (_chunk_hash, offset, length, ordinal) in enumerate(rows):
        if ordinal != expected_ordinal:
            raise MetadataDatabaseError("los chunks EC deben tener ordinales consecutivos desde 0")
        if offset != previous_end:
            raise MetadataDatabaseError("los chunks EC deben cubrir el payload de forma contigua")
        previous_end = offset + length

    if previous_end != payload_size:
        raise MetadataDatabaseError("los chunks EC no cubren exactamente payload_size")

    return rows


def _normalize_erasure_shard_rows(
    shards: Iterable[tuple[int, str, str, int]],
    total_shards: int,
) -> list[tuple[int, str, str, int]]:
    rows = [
        (
            _require_non_negative_int("shard_index", shard_index),
            _require_hash64("shard_hash", shard_hash),
            _require_non_empty_text("node_id", node_id),
            _require_positive_int("shard_size", size),
        )
        for shard_index, shard_hash, node_id, size in shards
    ]
    rows.sort(key=lambda item: item[0])

    indexes = [row[0] for row in rows]
    if indexes != list(range(total_shards)):
        raise MetadataDatabaseError("los shards EC deben cubrir todos los índices esperados")

    node_ids = [row[2] for row in rows]
    if len(node_ids) != len(set(node_ids)):
        raise MetadataDatabaseError("los shards EC de un pack deben ir a nodos distintos")

    return rows


def _metadata_pack_publication_record(row: sqlite3.Row) -> MetadataPackPublicationRecord:
    return MetadataPackPublicationRecord(
        owner_id=row["owner_id"],
        pack_hash=row["pack_hash"],
        desired_copies=int(row["desired_copies"]),
        pushed_at=float(row["pushed_at"]),
        pack_size_bytes=int(row["pack_size_bytes"]),
        attempted_targets=int(row["attempted_targets"]),
        successful_targets=int(row["successful_targets"]),
        stored_targets=int(row["stored_targets"]),
        already_present_targets=int(row["already_present_targets"]),
        failed_targets=int(row["failed_targets"]),
    )


def _erasure_pack_record(row: sqlite3.Row) -> ErasureDataPackRecord:
    return ErasureDataPackRecord(
        pack_hash=row["pack_hash"],
        codec=row["codec"],
        data_shards=int(row["data_shards"]),
        parity_shards=int(row["parity_shards"]),
        payload_size=int(row["payload_size"]),
        padded_size=int(row["padded_size"]),
        shard_size=int(row["shard_size"]),
        protection_state=ProtectionState(row["protection_state"]),
        placement_epoch=row["placement_epoch"],
        created_at=float(row["created_at"]),
        last_push_at=row["last_push_at"],
        last_verify_at=row["last_verify_at"],
        last_error=row["last_error"],
    )


def _erasure_chunk_record(row: sqlite3.Row) -> ErasureDataPackChunkRecord:
    return ErasureDataPackChunkRecord(
        chunk_hash=row["chunk_hash"],
        pack_hash=row["pack_hash"],
        offset=int(row["chunk_offset"]),
        length=int(row["chunk_length"]),
        ordinal=int(row["chunk_ordinal"]),
    )


def _erasure_shard_record(row: sqlite3.Row) -> ErasureDataPackShardRecord:
    return ErasureDataPackShardRecord(
        pack_hash=row["pack_hash"],
        shard_index=int(row["shard_index"]),
        shard_hash=row["shard_hash"],
        node_id=row["node_id"],
        size=int(row["size"]),
        protection_state=ProtectionState(row["protection_state"]),
        last_push_at=row["last_push_at"],
        last_verify_at=row["last_verify_at"],
        last_error=row["last_error"],
    )
