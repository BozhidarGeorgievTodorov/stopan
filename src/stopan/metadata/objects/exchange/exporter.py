"""
Exportación del estado operacional de MetadataDB a metadata objects.

El exporter construye un grafo canónico y deduplicado sin modificar la DB. Los
hashes de objeto se calculan sobre bytes plaintext canónicos antes de cifrar o
empaquetar el grafo.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.codec import (
    EncodedMetadataObject,
    canonical_state_digest,
    encode_metadata_object,
)
from stopan.metadata.objects.graph import MetadataObjectGraph
from stopan.metadata.objects.models import (
    CatalogObject,
    ChunkListObject,
    ChunkRef,
    ErasureDataPackChunkObject,
    ErasureDataPackIndexObject,
    ErasureDataPackRecordObject,
    ErasureDataPackShardObject,
    ErasureDataPackShardPlacementObject,
    FileObject,
    IndexShardRef,
    KnownChunkIndexObject,
    KnownChunkRecord,
    KnownChunkShardObject,
    MetadataObjectError,
    MetadataObjectType,
    MetadataPlainObject,
    ObjectRef,
    ProtectionIndexObject,
    ProtectionRecordObject,
    ProtectionShardObject,
    RecipeObject,
    SnapshotIndexEntry,
    SnapshotIndexObject,
    SnapshotRootObject,
    TreeEntry,
    TreeObject,
)


@dataclass
class _TreeNode:
    metadata: dict[str, Any] | None = None
    dirs: dict[str, "_TreeNode"] | None = None
    files: dict[str, tuple[dict[str, Any], ObjectRef]] | None = None

    def __post_init__(self) -> None:
        if self.dirs is None:
            self.dirs = {}
        if self.files is None:
            self.files = {}


class MetadataObjectGraphExporter:

    def __init__(self, db: MetadataDB):
        self.db = db
        self._objects: dict[str, EncodedMetadataObject] = {}
        self._recipe_ref_cache: dict[int, ObjectRef] = {}

    def export_current_state(self, *, include_protection: bool = True) -> MetadataObjectGraph:
        self._objects = {}
        self._recipe_ref_cache = {}

        vault_id = self.db.get_or_create_vault_id()
        snapshot_index_ref, snapshot_count = self._export_snapshot_index()
        known_chunk_index_ref, known_chunk_count = self._export_known_chunk_index()
        protection_index_ref = None
        protection_record_count = 0
        erasure_data_pack_index_ref = None
        erasure_pack_count = 0
        erasure_chunk_count = 0
        erasure_shard_count = 0

        if include_protection:
            protection_index_ref, protection_record_count = self._export_protection_index()
            (
                erasure_data_pack_index_ref,
                erasure_pack_count,
                erasure_chunk_count,
                erasure_shard_count,
            ) = self._export_erasure_data_pack_index()

        catalog = CatalogObject(
            vault_id=vault_id,
            snapshot_index=snapshot_index_ref,
            known_chunk_index=known_chunk_index_ref,
            protection_index=protection_index_ref,
            snapshot_count=snapshot_count,
            known_chunk_count=known_chunk_count,
            protection_record_count=protection_record_count,
            erasure_data_pack_index=erasure_data_pack_index_ref,
            erasure_data_pack_count=erasure_pack_count,
            erasure_data_pack_chunk_count=erasure_chunk_count,
            erasure_data_pack_shard_count=erasure_shard_count,
        )
        catalog_ref = self._put(catalog)
        state_digest = canonical_state_digest(
            catalog_ref.object_hash,
            tuple(self._objects.keys()),
        )

        return MetadataObjectGraph(
            vault_id=vault_id,
            catalog_hash=catalog_ref.object_hash,
            catalog_ref=catalog_ref,
            state_digest=state_digest,
            objects=dict(self._objects),
            snapshot_count=snapshot_count,
            known_chunk_count=known_chunk_count,
            protection_record_count=protection_record_count,
        )

    def _put(self, obj: MetadataPlainObject) -> ObjectRef:
        encoded = encode_metadata_object(obj)
        existing = self._objects.get(encoded.object_hash)
        if existing is not None:
            if existing.canonical_bytes != encoded.canonical_bytes:
                raise MetadataObjectError(f"colisión de hash en metadata object: {encoded.object_hash}")
            return ObjectRef(object_type=encoded.object_type, object_hash=encoded.object_hash)

        self._objects[encoded.object_hash] = encoded
        return ObjectRef(object_type=encoded.object_type, object_hash=encoded.object_hash)

    def _export_snapshot_index(self) -> tuple[ObjectRef, int]:
        rows = self.db.object_export_snapshot_rows()

        entries: list[SnapshotIndexEntry] = []
        for row in rows:
            snapshot_root_ref = None
            status = row["status"]
            if status == "COMPLETE":
                snapshot_root_ref = self._export_snapshot_root(row)

            entries.append(
                SnapshotIndexEntry(
                    snapshot_uuid=row["uuid"],
                    status=status,
                    root_path=row["root_path"],
                    origin_node_id=row["origin_node_id"],
                    created_at=row["created_at"],
                    total_size=row["total_size"],
                    total_files=row["total_files"],
                    snapshot_root=snapshot_root_ref,
                    error=row["error"],
                )
            )

        snapshot_index = SnapshotIndexObject(snapshots=tuple(entries))
        return self._put(snapshot_index), len(entries)

    def _export_snapshot_root(self, snapshot_row) -> ObjectRef:
        root_tree_ref = self._export_snapshot_tree(snapshot_row["id"])
        snapshot_root = SnapshotRootObject(
            snapshot_uuid=snapshot_row["uuid"],
            root_path=snapshot_row["root_path"],
            origin_node_id=snapshot_row["origin_node_id"],
            created_at=snapshot_row["created_at"],
            total_size=snapshot_row["total_size"],
            total_files=snapshot_row["total_files"],
            root_tree=root_tree_ref,
        )
        return self._put(snapshot_root)

    def _export_snapshot_tree(self, snapshot_id: int) -> ObjectRef:
        root = _TreeNode()
        rows = self.db.object_export_snapshot_item_rows(snapshot_id)

        for row in rows:
            path = row["path"]
            item_type = row["item_type"]
            if path in {"", "."}:
                if item_type == "dir":
                    root.metadata = dict(row)
                    continue
                raise MetadataObjectError(f"invalid root snapshot item type for snapshot {snapshot_id}: {item_type!r}")

            parts = [part for part in path.split("/") if part]
            if not parts:
                raise MetadataObjectError(f"invalid empty snapshot item path in snapshot {snapshot_id}")

            if item_type == "dir":
                node = self._ensure_dir_node(root, parts)
                node.metadata = dict(row)
                continue

            if item_type != "file":
                raise MetadataObjectError(f"unsupported snapshot item type {item_type!r} in snapshot {snapshot_id}")

            parent = self._ensure_dir_node(root, parts[:-1])
            name = parts[-1]
            recipe_id = row["recipe_id"]
            if recipe_id is None:
                raise MetadataObjectError(f"COMPLETE snapshot {snapshot_id} has file without recipe: {path!r}")
            recipe_ref, recipe_hash = self._export_recipe(recipe_id)
            file_ref = self._put(
                FileObject(
                    size=row["size"],
                    recipe_hash=recipe_hash,
                    recipe=recipe_ref,
                )
            )
            if parent.files is None:
                parent.files = {}
            if name in parent.files:
                raise MetadataObjectError(f"duplicate file path in snapshot {snapshot_id}: {path!r}")
            parent.files[name] = (dict(row), file_ref)

        return self._emit_tree(root, path="")

    def _ensure_dir_node(self, root: _TreeNode, parts: list[str]) -> _TreeNode:
        node = root
        for part in parts:
            if not part or part in {".", ".."} or "/" in part:
                raise MetadataObjectError(f"invalid path component: {part!r}")
            assert node.dirs is not None
            node = node.dirs.setdefault(part, _TreeNode())
        return node

    def _emit_tree(self, node: _TreeNode, *, path: str) -> ObjectRef:
        entries: list[TreeEntry] = []

        for name in sorted((node.dirs or {}).keys()):
            child = node.dirs[name]
            child_path = f"{path}/{name}" if path else name
            if child.metadata is None:
                raise MetadataObjectError(
                    f"missing directory metadata for {child_path!r}; refusing to invent filesystem details"
                )
            child_ref = self._emit_tree(child, path=child_path)
            entries.append(self._tree_entry_from_row(name, child.metadata, child_ref))

        for name in sorted((node.files or {}).keys()):
            row, file_ref = node.files[name]
            entries.append(self._tree_entry_from_row(name, row, file_ref))

        entries.sort(key=lambda item: item.name)
        return self._put(TreeObject(entries=tuple(entries)))

    def _tree_entry_from_row(self, name: str, row: dict[str, Any], ref: ObjectRef) -> TreeEntry:
        return TreeEntry(
            name=name,
            item_type=row["item_type"],
            size=row["size"],
            mode=row["mode"],
            mtime=row["mtime"],
            mtime_ns=row["mtime_ns"],
            uid=row["uid"],
            gid=row["gid"],
            ref=ref,
        )

    def _export_recipe(self, recipe_id: int) -> tuple[ObjectRef, str]:
        cached = self._recipe_ref_cache.get(recipe_id)
        if cached is not None:
            recipe_hash = self._recipe_hash_for_id(recipe_id)
            return cached, recipe_hash

        recipe_row = self.db.object_export_recipe_row(recipe_id)
        if recipe_row is None:
            raise MetadataObjectError(f"snapshot item references missing recipe_id={recipe_id}")

        chunk_rows = self.db.object_export_recipe_chunk_rows(recipe_id)

        chunk_refs = tuple(
            ChunkRef(
                order=row["chunk_order"],
                chunk_hash=row["chunk_hash"],
                size=row["chunk_size"],
            )
            for row in chunk_rows
        )
        chunk_list_ref = self._put(ChunkListObject(chunks=chunk_refs))
        recipe_hash = recipe_row["recipe_hash"]
        recipe_ref = self._put(
            RecipeObject(
                recipe_hash=recipe_hash,
                chunk_count=recipe_row["chunk_count"],
                total_size=recipe_row["total_size"],
                chunk_list=chunk_list_ref,
            )
        )
        self._recipe_ref_cache[recipe_id] = recipe_ref
        return recipe_ref, recipe_hash

    def _recipe_hash_for_id(self, recipe_id: int) -> str:
        recipe_hash = self.db.object_export_recipe_hash(recipe_id)
        if recipe_hash is None:
            raise MetadataObjectError(f"recipe_id cacheado no encontrado: {recipe_id}")
        return recipe_hash

    def _export_known_chunk_index(self) -> tuple[ObjectRef, int]:
        rows = self.db.object_export_known_chunk_rows()

        grouped: dict[str, list[KnownChunkRecord]] = defaultdict(list)
        for row in rows:
            chunk_hash = row["hash"]
            grouped[chunk_hash[:2]].append(
                KnownChunkRecord(chunk_hash=chunk_hash, size=row["size"])
            )

        shards: list[IndexShardRef] = []
        for prefix in sorted(grouped.keys()):
            records = tuple(grouped[prefix])
            shard_ref = self._put(KnownChunkShardObject(prefix=prefix, chunks=records))
            shards.append(IndexShardRef(prefix=prefix, count=len(records), ref=shard_ref))

        index_ref = self._put(KnownChunkIndexObject(shards=tuple(shards), total_chunks=len(rows)))
        return index_ref, len(rows)

    def _export_protection_index(self) -> tuple[ObjectRef, int]:
        rows = self.db.object_export_protection_rows()

        grouped: dict[str, list[ProtectionRecordObject]] = defaultdict(list)
        for row in rows:
            chunk_hash = row["chunk_hash"]
            grouped[chunk_hash[:2]].append(
                ProtectionRecordObject(
                    chunk_hash=chunk_hash,
                    desired_rf=row["desired_rf"],
                    protection_state=row["protection_state"],
                    protected_remote_copies=row["protected_remote_copies"],
                    placement_epoch=row["placement_epoch"],
                    last_push_at=row["last_push_at"],
                    last_verify_at=row["last_verify_at"],
                    last_error=row["last_error"],
                )
            )

        shards: list[IndexShardRef] = []
        for prefix in sorted(grouped.keys()):
            records = tuple(grouped[prefix])
            shard_ref = self._put(ProtectionShardObject(prefix=prefix, records=records))
            shards.append(IndexShardRef(prefix=prefix, count=len(records), ref=shard_ref))

        index_ref = self._put(ProtectionIndexObject(shards=tuple(shards), total_records=len(rows)))
        return index_ref, len(rows)


    def _export_erasure_data_pack_index(self) -> tuple[ObjectRef, int, int, int]:
        pack_rows = self.db.object_export_erasure_pack_rows()

        chunk_rows = self.db.object_export_erasure_pack_chunk_rows()
        shard_rows = self.db.object_export_erasure_pack_shard_rows()

        chunks_by_pack: dict[str, list[ErasureDataPackChunkObject]] = defaultdict(list)
        for row in chunk_rows:
            chunks_by_pack[row["pack_hash"]].append(
                ErasureDataPackChunkObject(
                    chunk_hash=row["chunk_hash"],
                    offset=row["chunk_offset"],
                    length=row["chunk_length"],
                    ordinal=row["chunk_ordinal"],
                )
            )

        shards_by_pack: dict[str, list[ErasureDataPackShardPlacementObject]] = defaultdict(list)
        for row in shard_rows:
            shards_by_pack[row["pack_hash"]].append(
                ErasureDataPackShardPlacementObject(
                    shard_index=row["shard_index"],
                    shard_hash=row["shard_hash"],
                    node_id=row["node_id"],
                    size=row["size"],
                    protection_state=row["protection_state"],
                    last_push_at=row["last_push_at"],
                    last_verify_at=row["last_verify_at"],
                    last_error=row["last_error"],
                )
            )

        grouped: dict[str, list[ErasureDataPackRecordObject]] = defaultdict(list)
        for row in pack_rows:
            pack_hash = row["pack_hash"]
            grouped[pack_hash[:2]].append(
                ErasureDataPackRecordObject(
                    pack_hash=pack_hash,
                    codec=row["codec"],
                    data_shards=row["data_shards"],
                    parity_shards=row["parity_shards"],
                    payload_size=row["payload_size"],
                    padded_size=row["padded_size"],
                    shard_size=row["shard_size"],
                    protection_state=row["protection_state"],
                    placement_epoch=row["placement_epoch"],
                    created_at=row["created_at"],
                    last_push_at=row["last_push_at"],
                    last_verify_at=row["last_verify_at"],
                    last_error=row["last_error"],
                    chunks=tuple(chunks_by_pack[pack_hash]),
                    shards=tuple(shards_by_pack[pack_hash]),
                )
            )

        shards: list[IndexShardRef] = []
        for prefix in sorted(grouped.keys()):
            records = tuple(grouped[prefix])
            shard_ref = self._put(ErasureDataPackShardObject(prefix=prefix, records=records))
            shards.append(IndexShardRef(prefix=prefix, count=len(records), ref=shard_ref))

        index_ref = self._put(
            ErasureDataPackIndexObject(
                shards=tuple(shards),
                total_packs=len(pack_rows),
                total_chunks=len(chunk_rows),
                total_shards=len(shard_rows),
            )
        )
        return index_ref, len(pack_rows), len(chunk_rows), len(shard_rows)
