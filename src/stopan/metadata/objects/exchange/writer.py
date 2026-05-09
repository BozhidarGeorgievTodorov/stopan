"""
Materialización de metadata object graphs en MetadataDB.

El writer concentra el estado mutable de importación en DB: cachés de recipes,
mapa de tamaños de chunks y creación de filas operacionales.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from stopan.chunking.recipes import compute_recipe_hash
from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.models import MetadataObjectType
from stopan.protection.policy import ProtectionState

from .errors import MetadataObjectImportError
from .reader import MetadataObjectReader
from .validation import (
    optional_ref,
    require_hash64,
    require_int,
    require_number,
    require_protection_state,
    require_ref,
    require_str,
)


@dataclass(frozen=True, slots=True)
class RecipeImportResult:
    recipe_id: int
    recipe_hash: str
    chunk_count: int
    total_size: int
    created: bool


@dataclass(frozen=True, slots=True)
class SnapshotImportStats:
    snapshots_imported: int
    complete_snapshots_imported: int
    items_imported: int
    recipes_created: int
    recipes_reused: int


class MetadataDBImportWriter:

    def __init__(
        self,
        *,
        reader: MetadataObjectReader,
        default_desired_rf: int,
        chunk_sizes: dict[str, int],
    ):
        self.reader = reader
        self.default_desired_rf = int(default_desired_rf)
        self.chunk_sizes = chunk_sizes
        self._recipe_id_by_hash: dict[str, int] = {}
        self._recipe_chunks_by_hash: dict[str, tuple[tuple[int, str, int], ...]] = {}

    def require_empty_operational_db(self, db: MetadataDB) -> None:
        tables = (
            "snapshots",
            "snapshot_items",
            "recipes",
            "recipe_chunks",
            "chunks",
            "chunk_protection",
        )
        non_empty: list[str] = []
        for table in tables:
            row = db.conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            if row is not None and int(row["n"]) > 0:
                non_empty.append(f"{table}={int(row['n'])}")
        if non_empty:
            raise MetadataObjectImportError(
                "import-object-graph requiere una DB nueva o vacía; se encontró metadata existente: "
                + ", ".join(non_empty)
            )

    def insert_chunks(self, db: MetadataDB, chunks: dict[str, int]) -> int:
        if not chunks:
            return 0
        db.conn.executemany(
            """
            INSERT INTO chunks (hash, size, ref_count)
            VALUES (?, ?, 0)
            """,
            [(chunk_hash, size) for chunk_hash, size in sorted(chunks.items())],
        )
        return len(chunks)

    def insert_snapshots(
        self,
        db: MetadataDB,
        snapshot_entries: list[dict[str, Any]],
    ) -> SnapshotImportStats:
        snapshots_imported = 0
        complete_snapshots = 0
        items_imported = 0
        recipes_created = 0
        recipes_reused = 0

        for entry in snapshot_entries:
            if not isinstance(entry, dict):
                raise MetadataObjectImportError("snapshot_index entry inválida")

            snapshot_uuid = require_str("snapshot.uuid", entry.get("snapshot_uuid"))
            status = require_str("snapshot.status", entry.get("status"))
            root_path = require_str(
                "snapshot.root_path",
                entry.get("root_path"),
                allow_empty=True,
            )
            origin_node_id = require_str("snapshot.origin_node_id", entry.get("origin_node_id"))
            created_at = require_str("snapshot.created_at", entry.get("created_at"))
            total_size = require_int(
                "snapshot.total_size", entry.get("total_size"), min_value=0
            )
            total_files = require_int(
                "snapshot.total_files", entry.get("total_files"), min_value=0
            )
            error = entry.get("error")
            if error is not None:
                error = require_str("snapshot.error", error, allow_empty=True)

            cursor = db.conn.execute(
                """
                INSERT INTO snapshots
                    (uuid, root_path, origin_node_id, status, error, total_size, total_files, created_at)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (snapshot_uuid, root_path, origin_node_id, status, error, total_size, total_files, created_at),
            )
            snapshot_id = int(cursor.lastrowid)
            snapshots_imported += 1

            snapshot_root_hash = optional_ref(
                "snapshot.snapshot_root",
                entry.get("snapshot_root"),
                MetadataObjectType.SNAPSHOT_ROOT,
            )
            if status == "COMPLETE":
                if snapshot_root_hash is None:
                    raise MetadataObjectImportError(f"snapshot COMPLETE sin root: {snapshot_uuid}")
                root = self.reader.payload(snapshot_root_hash, MetadataObjectType.SNAPSHOT_ROOT)
                self._verify_snapshot_root_matches_entry(root, entry)
                root_tree_hash = require_ref(
                    "snapshot_root.root_tree",
                    root.get("root_tree"),
                    MetadataObjectType.TREE,
                )
                sub_items, sub_created, sub_reused = self._insert_tree(
                    db,
                    snapshot_id=snapshot_id,
                    tree_hash=root_tree_hash,
                    path_prefix="",
                )
                items_imported += sub_items
                recipes_created += sub_created
                recipes_reused += sub_reused
                complete_snapshots += 1

        return SnapshotImportStats(
            snapshots_imported=snapshots_imported,
            complete_snapshots_imported=complete_snapshots,
            items_imported=items_imported,
            recipes_created=recipes_created,
            recipes_reused=recipes_reused,
        )

    def insert_protection_records(
        self,
        db: MetadataDB,
        records: list[dict[str, Any]],
    ) -> int:
        if not records:
            return 0

        rows = []
        for record in records:
            chunk_hash = require_hash64(
                "protection.chunk_hash", record.get("chunk_hash")
            )
            if chunk_hash not in self.chunk_sizes:
                raise MetadataObjectImportError(
                    f"chunk_protection referencia chunk inexistente: {chunk_hash}"
                )
            rows.append(
                (
                    chunk_hash,
                    require_int(
                        "protection.desired_rf",
                        record.get("desired_rf"),
                        min_value=0,
                    ),
                    require_protection_state(
                        "protection.protection_state",
                        record.get("protection_state"),
                    ),
                    require_int(
                        "protection.protected_remote_copies",
                        record.get("protected_remote_copies"),
                        min_value=0,
                    ),
                    record.get("placement_epoch"),
                    record.get("last_push_at"),
                    record.get("last_verify_at"),
                    record.get("last_error"),
                )
            )

        db.conn.executemany(
            """
            INSERT INTO chunk_protection
                (chunk_hash, desired_rf, protection_state, protected_remote_copies,
                 placement_epoch, last_push_at, last_verify_at, last_error)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    def insert_pending_protection_for_chunks(self, db: MetadataDB) -> int:
        rows = [
            (
                chunk_hash,
                self.default_desired_rf,
                ProtectionState.PENDING.value,
                0,
                None,
                None,
                None,
                None,
            )
            for chunk_hash in sorted(self.chunk_sizes.keys())
        ]
        if not rows:
            return 0
        db.conn.executemany(
            """
            INSERT INTO chunk_protection
                (chunk_hash, desired_rf, protection_state, protected_remote_copies,
                 placement_epoch, last_push_at, last_verify_at, last_error)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    def _verify_snapshot_root_matches_entry(
        self,
        root: dict[str, Any],
        entry: dict[str, Any],
    ) -> None:
        checks = (
            "snapshot_uuid",
            "root_path",
            "origin_node_id",
            "created_at",
            "total_size",
            "total_files",
        )
        for key in checks:
            if root.get(key) != entry.get(key):
                raise MetadataObjectImportError(
                    "snapshot_root no coincide con snapshot_index para "
                    f"{entry.get('snapshot_uuid')}: {key}"
                )

    def _insert_tree(
        self,
        db: MetadataDB,
        *,
        snapshot_id: int,
        tree_hash: str,
        path_prefix: str,
    ) -> tuple[int, int, int]:
        tree = self.reader.payload(tree_hash, MetadataObjectType.TREE)
        entries = tree.get("entries")
        if not isinstance(entries, list):
            raise MetadataObjectImportError("tree.entries debe ser lista")

        items_inserted = 0
        recipes_created = 0
        recipes_reused = 0

        for entry in entries:
            if not isinstance(entry, dict):
                raise MetadataObjectImportError("tree entry inválida")
            name = require_str("tree_entry.name", entry.get("name"))
            if "/" in name or name in {".", ".."}:
                raise MetadataObjectImportError(f"tree entry name inválido: {name!r}")
            item_type = require_str("tree_entry.item_type", entry.get("item_type"))
            rel_path = f"{path_prefix}/{name}" if path_prefix else name
            size = require_int("tree_entry.size", entry.get("size"), min_value=0)
            mode = require_int("tree_entry.mode", entry.get("mode"), min_value=0)
            mtime = require_number("tree_entry.mtime", entry.get("mtime"))
            mtime_ns = require_int(
                "tree_entry.mtime_ns", entry.get("mtime_ns"), min_value=0
            )
            uid = require_int("tree_entry.uid", entry.get("uid"), min_value=0)
            gid = require_int("tree_entry.gid", entry.get("gid"), min_value=0)

            if item_type == "dir":
                child_tree_hash = require_ref(
                    "tree_entry.ref",
                    entry.get("ref"),
                    MetadataObjectType.TREE,
                )
                self._insert_snapshot_item(
                    db,
                    snapshot_id=snapshot_id,
                    path=rel_path,
                    item_type="dir",
                    size=size,
                    mode=mode,
                    mtime=mtime,
                    mtime_ns=mtime_ns,
                    uid=uid,
                    gid=gid,
                    recipe_id=None,
                )
                items_inserted += 1
                sub_items, sub_created, sub_reused = self._insert_tree(
                    db,
                    snapshot_id=snapshot_id,
                    tree_hash=child_tree_hash,
                    path_prefix=rel_path,
                )
                items_inserted += sub_items
                recipes_created += sub_created
                recipes_reused += sub_reused
                continue

            if item_type != "file":
                raise MetadataObjectImportError(f"tree entry type no soportado: {item_type!r}")

            file_hash = require_ref("tree_entry.ref", entry.get("ref"), MetadataObjectType.FILE)
            file_payload = self.reader.payload(file_hash, MetadataObjectType.FILE)
            file_size = require_int("file.size", file_payload.get("size"), min_value=0)
            if file_size != size:
                raise MetadataObjectImportError(
                    f"file.size no coincide con tree_entry.size para {rel_path!r}"
                )
            recipe_hash = require_hash64("file.recipe_hash", file_payload.get("recipe_hash"))
            recipe_object_hash = require_ref(
                "file.recipe",
                file_payload.get("recipe"),
                MetadataObjectType.RECIPE,
            )
            recipe = self._ensure_recipe(
                db,
                recipe_object_hash=recipe_object_hash,
                expected_recipe_hash=recipe_hash,
            )

            self._insert_snapshot_item(
                db,
                snapshot_id=snapshot_id,
                path=rel_path,
                item_type="file",
                size=size,
                mode=mode,
                mtime=mtime,
                mtime_ns=mtime_ns,
                uid=uid,
                gid=gid,
                recipe_id=recipe.recipe_id,
            )
            items_inserted += 1
            if recipe.created:
                recipes_created += 1
            else:
                recipes_reused += 1

        return items_inserted, recipes_created, recipes_reused

    def _ensure_recipe(
        self,
        db: MetadataDB,
        *,
        recipe_object_hash: str,
        expected_recipe_hash: str,
    ) -> RecipeImportResult:
        recipe_payload = self.reader.payload(recipe_object_hash, MetadataObjectType.RECIPE)
        recipe_hash = require_hash64("recipe.recipe_hash", recipe_payload.get("recipe_hash"))
        if recipe_hash != expected_recipe_hash:
            raise MetadataObjectImportError("file.recipe_hash no coincide con recipe.recipe_hash")

        chunk_count = require_int("recipe.chunk_count", recipe_payload.get("chunk_count"), min_value=0)
        total_size = require_int("recipe.total_size", recipe_payload.get("total_size"), min_value=0)
        chunk_list_hash = require_ref(
            "recipe.chunk_list",
            recipe_payload.get("chunk_list"),
            MetadataObjectType.CHUNK_LIST,
        )
        chunk_list = self.reader.payload(chunk_list_hash, MetadataObjectType.CHUNK_LIST)
        raw_chunks = chunk_list.get("chunks")
        if not isinstance(raw_chunks, list):
            raise MetadataObjectImportError("chunk_list.chunks debe ser lista")

        chunks: list[tuple[int, str, int]] = []
        for index, raw_chunk in enumerate(raw_chunks):
            if not isinstance(raw_chunk, dict):
                raise MetadataObjectImportError("chunk_list entry inválida")
            order = require_int("chunk.order", raw_chunk.get("order"), min_value=0)
            if order != index:
                raise MetadataObjectImportError("chunk_list order no es contiguo desde 0")
            chunk_hash = require_hash64("chunk.chunk_hash", raw_chunk.get("chunk_hash"))
            chunk_size = require_int("chunk.size", raw_chunk.get("size"), min_value=1)
            known_size = self.chunk_sizes.get(chunk_hash)
            if known_size is not None and known_size != chunk_size:
                raise MetadataObjectImportError(f"tamaño de chunk no coincide para {chunk_hash}")
            if known_size is None:
                db.conn.execute(
                    "INSERT INTO chunks (hash, size, ref_count) VALUES (?, ?, 0)",
                    (chunk_hash, chunk_size),
                )
                self.chunk_sizes[chunk_hash] = chunk_size
            chunks.append((order, chunk_hash, chunk_size))

        if len(chunks) != chunk_count:
            raise MetadataObjectImportError("recipe.chunk_count no coincide con chunk_list")
        if sum(size for _, _, size in chunks) != total_size:
            raise MetadataObjectImportError("recipe.total_size no coincide con chunk_list")
        calculated_recipe_hash = compute_recipe_hash(chunks)
        if calculated_recipe_hash != recipe_hash:
            raise MetadataObjectImportError(
                "recipe_hash no coincide: "
                f"esperado={recipe_hash} calculado={calculated_recipe_hash}"
            )

        tuple_chunks = tuple(chunks)
        cached_id = self._recipe_id_by_hash.get(recipe_hash)
        if cached_id is not None:
            existing = self._recipe_chunks_by_hash[recipe_hash]
            if existing != tuple_chunks:
                raise MetadataObjectImportError(f"recipe_hash duplicado con chunks distintos: {recipe_hash}")
            self._increment_chunk_ref_counts(db, tuple_chunks)
            return RecipeImportResult(
                cached_id,
                recipe_hash,
                chunk_count,
                total_size,
                created=False,
            )

        db.conn.execute(
            """
            INSERT OR IGNORE INTO recipes (recipe_hash, chunk_count, total_size)
            VALUES (?, ?, ?)
            """,
            (recipe_hash, chunk_count, total_size),
        )
        row = db.conn.execute(
            """
            SELECT id, chunk_count, total_size
            FROM recipes
            WHERE recipe_hash = ?
            """,
            (recipe_hash,),
        ).fetchone()
        if row is None:
            raise MetadataObjectImportError(f"no se pudo resolver recipe_id para {recipe_hash}")
        if row["chunk_count"] != chunk_count or row["total_size"] != total_size:
            raise MetadataObjectImportError(f"recipe_hash inconsistente: {recipe_hash}")

        recipe_id = row["id"]
        existing_rows = db.conn.execute(
            """
            SELECT chunk_order, chunk_hash, chunk_size
            FROM recipe_chunks
            WHERE recipe_id = ?
            ORDER BY chunk_order ASC
            """,
            (recipe_id,),
        ).fetchall()
        existing_chunks = tuple(
            (row["chunk_order"], row["chunk_hash"], row["chunk_size"])
            for row in existing_rows
        )
        created = False
        if existing_chunks:
            if existing_chunks != tuple_chunks:
                raise MetadataObjectImportError(f"recipe_hash duplicado con chunks distintos: {recipe_hash}")
        elif chunks:
            db.conn.executemany(
                """
                INSERT INTO recipe_chunks (recipe_id, chunk_order, chunk_hash, chunk_size)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (recipe_id, order, chunk_hash, chunk_size)
                    for order, chunk_hash, chunk_size in chunks
                ],
            )
            created = True
        else:
            created = True

        self._recipe_id_by_hash[recipe_hash] = recipe_id
        self._recipe_chunks_by_hash[recipe_hash] = tuple_chunks
        self._increment_chunk_ref_counts(db, tuple_chunks)
        return RecipeImportResult(recipe_id, recipe_hash, chunk_count, total_size, created=created)

    def _increment_chunk_ref_counts(
        self,
        db: MetadataDB,
        chunks: tuple[tuple[int, str, int], ...],
    ) -> None:
        increments: dict[str, int] = defaultdict(int)
        for _order, chunk_hash, _size in chunks:
            increments[chunk_hash] += 1
        if not increments:
            return
        db.conn.executemany(
            """
            UPDATE chunks
            SET ref_count = ref_count + ?
            WHERE hash = ?
            """,
            [(count, chunk_hash) for chunk_hash, count in sorted(increments.items())],
        )

    def _insert_snapshot_item(
        self,
        db: MetadataDB,
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
        db.conn.execute(
            """
            INSERT INTO snapshot_items
                (snapshot_id, path, item_type, size, mode, mtime, mtime_ns, uid, gid, recipe_id)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, path, item_type, size, mode, mtime, mtime_ns, uid, gid, recipe_id),
        )
