"""Validación semántica transversal de metadata object graphs.

La validación local de cada payload pertenece al modelo canónico. Este módulo
comprueba además relaciones que atraviesan varios objetos y que deben cumplirse
antes de considerar recuperable un grafo completo.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from stopan.chunking.recipes import compute_recipe_hash
from stopan.metadata.objects.codec import decode_metadata_object_payload
from stopan.metadata.objects.models import (
    CatalogObject,
    ChunkListObject,
    ErasureDataPackIndexObject,
    ErasureDataPackShardObject,
    FileObject,
    KnownChunkIndexObject,
    KnownChunkShardObject,
    MetadataObjectError,
    MetadataObjectType,
    MetadataPlainObject,
    ProtectionIndexObject,
    ProtectionShardObject,
    RecipeObject,
    SnapshotIndexObject,
    SnapshotRootObject,
    TreeObject,
)


@dataclass(frozen=True, slots=True)
class GraphObjectPayload:
    object_type: MetadataObjectType
    payload: dict[str, Any]


def _typed_objects(
    objects_by_hash: Mapping[str, GraphObjectPayload],
) -> dict[str, MetadataPlainObject]:
    out: dict[str, MetadataPlainObject] = {}
    for object_hash, item in objects_by_hash.items():
        try:
            out[object_hash] = decode_metadata_object_payload(item.object_type, item.payload)
        except Exception as exc:
            raise MetadataObjectError(
                f"metadata object semánticamente inválido {object_hash} ({item.object_type.value}): {exc}"
            ) from exc
    return out


def _expect(
    typed: Mapping[str, MetadataPlainObject],
    object_hash: str,
    expected_type: MetadataObjectType,
) -> MetadataPlainObject:
    obj = typed.get(object_hash)
    if obj is None:
        raise MetadataObjectError(f"metadata object referenciado no existe: {object_hash}")
    actual = getattr(obj, "object_type", None)
    if actual != expected_type:
        actual_text = actual.value if isinstance(actual, MetadataObjectType) else repr(actual)
        raise MetadataObjectError(
            f"tipo de metadata object no coincide para {object_hash}: "
            f"esperado={expected_type.value} recibido={actual_text}"
        )
    return obj


def validate_metadata_object_graph_semantics(
    *,
    catalog_hash: str,
    objects_by_hash: Mapping[str, GraphObjectPayload],
    expected_snapshot_count: int | None = None,
    expected_known_chunk_count: int | None = None,
    expected_protection_record_count: int | None = None,
) -> None:
    """Comprueba que el grafo alcanzable puede materializarse coherentemente.

    Esta función no escribe SQLite ni el object store. Valida los modelos
    canónicos y las relaciones entre índices, shards, snapshots, árboles,
    ficheros, recetas, protección y paquetes EC.
    """

    typed = _typed_objects(objects_by_hash)
    catalog_obj = _expect(typed, catalog_hash, MetadataObjectType.CATALOG)
    if not isinstance(catalog_obj, CatalogObject):
        raise MetadataObjectError("latest.catalog_hash no resuelve a CatalogObject")
    catalog = catalog_obj

    snapshot_index_obj = _expect(
        typed,
        catalog.snapshot_index.object_hash,
        MetadataObjectType.SNAPSHOT_INDEX,
    )
    known_index_obj = _expect(
        typed,
        catalog.known_chunk_index.object_hash,
        MetadataObjectType.KNOWN_CHUNK_INDEX,
    )
    if not isinstance(snapshot_index_obj, SnapshotIndexObject):
        raise MetadataObjectError("catalog.snapshot_index inválido")
    if not isinstance(known_index_obj, KnownChunkIndexObject):
        raise MetadataObjectError("catalog.known_chunk_index inválido")

    snapshot_index = snapshot_index_obj
    known_index = known_index_obj

    if catalog.snapshot_count != len(snapshot_index.snapshots):
        raise MetadataObjectError(
            "catalog.snapshot_count no coincide con snapshot_index: "
            f"catalog={catalog.snapshot_count} index={len(snapshot_index.snapshots)}"
        )
    if catalog.known_chunk_count != known_index.total_chunks:
        raise MetadataObjectError(
            "catalog.known_chunk_count no coincide con known_chunk_index: "
            f"catalog={catalog.known_chunk_count} index={known_index.total_chunks}"
        )
    if expected_snapshot_count is not None and int(expected_snapshot_count) != catalog.snapshot_count:
        raise MetadataObjectError(
            "latest.snapshot_count no coincide con catalog.snapshot_count: "
            f"latest={expected_snapshot_count} catalog={catalog.snapshot_count}"
        )
    if expected_known_chunk_count is not None and int(expected_known_chunk_count) != catalog.known_chunk_count:
        raise MetadataObjectError(
            "latest.known_chunk_count no coincide con catalog.known_chunk_count: "
            f"latest={expected_known_chunk_count} catalog={catalog.known_chunk_count}"
        )

    known_chunks: dict[str, int] = {}
    for shard_ref in known_index.shards:
        shard_obj = _expect(
            typed,
            shard_ref.ref.object_hash,
            MetadataObjectType.KNOWN_CHUNK_SHARD,
        )
        if not isinstance(shard_obj, KnownChunkShardObject):
            raise MetadataObjectError("known_chunk_index contiene un shard de tipo inválido")
        if shard_obj.prefix != shard_ref.prefix:
            raise MetadataObjectError("prefix de known_chunk shard no coincide con su índice")
        if len(shard_obj.chunks) != shard_ref.count:
            raise MetadataObjectError("count de known_chunk shard no coincide con su contenido")
        for record in shard_obj.chunks:
            previous = known_chunks.get(record.chunk_hash)
            if previous is not None and previous != record.size:
                raise MetadataObjectError(
                    f"known chunk duplicado con tamaño distinto: {record.chunk_hash}"
                )
            known_chunks[record.chunk_hash] = record.size
    if len(known_chunks) != known_index.total_chunks:
        raise MetadataObjectError(
            "known_chunk_index.total_chunks no coincide con los chunks materializados"
        )

    protection_records = 0
    if catalog.protection_index is None:
        if catalog.protection_record_count != 0:
            raise MetadataObjectError(
                "catalog.protection_record_count debe ser 0 cuando no existe protection_index"
            )
    else:
        protection_index_obj = _expect(
            typed,
            catalog.protection_index.object_hash,
            MetadataObjectType.PROTECTION_INDEX,
        )
        if not isinstance(protection_index_obj, ProtectionIndexObject):
            raise MetadataObjectError("catalog.protection_index inválido")
        if catalog.protection_record_count != protection_index_obj.total_records:
            raise MetadataObjectError(
                "catalog.protection_record_count no coincide con protection_index.total_records"
            )
        seen_protection: set[str] = set()
        for shard_ref in protection_index_obj.shards:
            shard_obj = _expect(
                typed,
                shard_ref.ref.object_hash,
                MetadataObjectType.PROTECTION_SHARD,
            )
            if not isinstance(shard_obj, ProtectionShardObject):
                raise MetadataObjectError("protection_index contiene un shard de tipo inválido")
            if shard_obj.prefix != shard_ref.prefix:
                raise MetadataObjectError("prefix de protection shard no coincide con su índice")
            if len(shard_obj.records) != shard_ref.count:
                raise MetadataObjectError("count de protection shard no coincide con su contenido")
            for record in shard_obj.records:
                if record.chunk_hash in seen_protection:
                    raise MetadataObjectError(f"protection record duplicado: {record.chunk_hash}")
                if record.chunk_hash not in known_chunks:
                    raise MetadataObjectError(
                        f"protection record referencia chunk inexistente: {record.chunk_hash}"
                    )
                seen_protection.add(record.chunk_hash)
        protection_records = len(seen_protection)
        if protection_records != protection_index_obj.total_records:
            raise MetadataObjectError(
                "protection_index.total_records no coincide con los records materializados"
            )

    if expected_protection_record_count is not None and int(expected_protection_record_count) != protection_records:
        raise MetadataObjectError(
            "latest.protection_record_count no coincide con el grafo: "
            f"latest={expected_protection_record_count} grafo={protection_records}"
        )

    _validate_erasure_data_packs(typed=typed, catalog=catalog, known_chunks=known_chunks)
    _validate_snapshots(typed=typed, snapshot_index=snapshot_index, known_chunks=known_chunks)


def _validate_erasure_data_packs(
    *,
    typed: Mapping[str, MetadataPlainObject],
    catalog: CatalogObject,
    known_chunks: Mapping[str, int],
) -> None:
    if catalog.erasure_data_pack_index is None:
        if any(
            (
                catalog.erasure_data_pack_count,
                catalog.erasure_data_pack_chunk_count,
                catalog.erasure_data_pack_shard_count,
            )
        ):
            raise MetadataObjectError(
                "contadores EC del catalog deben ser 0 cuando no existe erasure_data_pack_index"
            )
        return

    index_obj = _expect(
        typed,
        catalog.erasure_data_pack_index.object_hash,
        MetadataObjectType.ERASURE_DATA_PACK_INDEX,
    )
    if not isinstance(index_obj, ErasureDataPackIndexObject):
        raise MetadataObjectError("catalog.erasure_data_pack_index inválido")

    if (
        catalog.erasure_data_pack_count != index_obj.total_packs
        or catalog.erasure_data_pack_chunk_count != index_obj.total_chunks
        or catalog.erasure_data_pack_shard_count != index_obj.total_shards
    ):
        raise MetadataObjectError("contadores EC del catalog no coinciden con erasure_data_pack_index")

    seen_packs: set[str] = set()
    seen_chunks: set[str] = set()
    chunk_count = 0
    shard_count = 0
    for shard_ref in index_obj.shards:
        shard_obj = _expect(
            typed,
            shard_ref.ref.object_hash,
            MetadataObjectType.ERASURE_DATA_PACK_SHARD,
        )
        if not isinstance(shard_obj, ErasureDataPackShardObject):
            raise MetadataObjectError("erasure_data_pack_index contiene un shard inválido")
        if shard_obj.prefix != shard_ref.prefix:
            raise MetadataObjectError("prefix de erasure_data_pack shard no coincide con su índice")
        if len(shard_obj.records) != shard_ref.count:
            raise MetadataObjectError("count de erasure_data_pack shard no coincide con su contenido")

        for record in shard_obj.records:
            if record.pack_hash in seen_packs:
                raise MetadataObjectError(f"erasure data pack duplicado: {record.pack_hash}")
            seen_packs.add(record.pack_hash)
            chunk_count += len(record.chunks)
            shard_count += len(record.shards)
            for chunk in record.chunks:
                known_size = known_chunks.get(chunk.chunk_hash)
                if known_size is None:
                    raise MetadataObjectError(
                        f"erasure data pack referencia chunk inexistente: {chunk.chunk_hash}"
                    )
                if chunk.length != known_size:
                    raise MetadataObjectError(
                        "longitud EC no coincide con el tamaño conocido del chunk: "
                        f"{chunk.chunk_hash}: ec={chunk.length} known={known_size}"
                    )
                if chunk.chunk_hash in seen_chunks:
                    raise MetadataObjectError(
                        f"chunk EC duplicado entre data packs: {chunk.chunk_hash}"
                    )
                seen_chunks.add(chunk.chunk_hash)

    if len(seen_packs) != index_obj.total_packs:
        raise MetadataObjectError("erasure_data_pack_index.total_packs no coincide con sus records")
    if chunk_count != index_obj.total_chunks:
        raise MetadataObjectError("erasure_data_pack_index.total_chunks no coincide con sus records")
    if shard_count != index_obj.total_shards:
        raise MetadataObjectError("erasure_data_pack_index.total_shards no coincide con sus records")


def _validate_snapshots(
    *,
    typed: Mapping[str, MetadataPlainObject],
    snapshot_index: SnapshotIndexObject,
    known_chunks: Mapping[str, int],
) -> None:
    recipe_chunks_by_hash: dict[str, tuple[tuple[int, str, int], ...]] = {}

    for entry in snapshot_index.snapshots:
        if entry.status != "COMPLETE" and entry.snapshot_root is not None:
            raise MetadataObjectError(
                f"snapshot {entry.snapshot_uuid} no COMPLETE no debe referenciar snapshot_root"
            )
        if entry.snapshot_root is None:
            continue
        root_obj = _expect(
            typed,
            entry.snapshot_root.object_hash,
            MetadataObjectType.SNAPSHOT_ROOT,
        )
        if not isinstance(root_obj, SnapshotRootObject):
            raise MetadataObjectError("snapshot_index referencia un snapshot_root inválido")
        for field in (
            "snapshot_uuid",
            "root_path",
            "origin_node_id",
            "created_at",
            "total_size",
            "total_files",
        ):
            if getattr(root_obj, field) != getattr(entry, field):
                raise MetadataObjectError(
                    f"snapshot_root no coincide con snapshot_index para {entry.snapshot_uuid}: {field}"
                )
        _validate_tree(
            typed=typed,
            tree_hash=root_obj.root_tree.object_hash,
            known_chunks=known_chunks,
            recipe_chunks_by_hash=recipe_chunks_by_hash,
            active_trees=set(),
        )


def _validate_tree(
    *,
    typed: Mapping[str, MetadataPlainObject],
    tree_hash: str,
    known_chunks: Mapping[str, int],
    recipe_chunks_by_hash: dict[str, tuple[tuple[int, str, int], ...]],
    active_trees: set[str],
) -> None:
    if tree_hash in active_trees:
        raise MetadataObjectError(f"ciclo detectado en el árbol de metadata: {tree_hash}")
    tree_obj = _expect(typed, tree_hash, MetadataObjectType.TREE)
    if not isinstance(tree_obj, TreeObject):
        raise MetadataObjectError("tree ref no resuelve a TreeObject")

    active_trees.add(tree_hash)
    try:
        for entry in tree_obj.entries:
            if entry.item_type == "dir":
                _validate_tree(
                    typed=typed,
                    tree_hash=entry.ref.object_hash,
                    known_chunks=known_chunks,
                    recipe_chunks_by_hash=recipe_chunks_by_hash,
                    active_trees=active_trees,
                )
                continue

            file_obj = _expect(typed, entry.ref.object_hash, MetadataObjectType.FILE)
            if not isinstance(file_obj, FileObject):
                raise MetadataObjectError("file ref no resuelve a FileObject")
            if file_obj.size != entry.size:
                raise MetadataObjectError("file.size no coincide con tree_entry.size")

            recipe_obj = _expect(
                typed,
                file_obj.recipe.object_hash,
                MetadataObjectType.RECIPE,
            )
            if not isinstance(recipe_obj, RecipeObject):
                raise MetadataObjectError("file.recipe no resuelve a RecipeObject")
            if file_obj.recipe_hash != recipe_obj.recipe_hash:
                raise MetadataObjectError("file.recipe_hash no coincide con recipe.recipe_hash")

            chunk_list_obj = _expect(
                typed,
                recipe_obj.chunk_list.object_hash,
                MetadataObjectType.CHUNK_LIST,
            )
            if not isinstance(chunk_list_obj, ChunkListObject):
                raise MetadataObjectError("recipe.chunk_list no resuelve a ChunkListObject")
            chunks = tuple(
                (chunk.order, chunk.chunk_hash, chunk.size)
                for chunk in chunk_list_obj.chunks
            )
            if len(chunks) != recipe_obj.chunk_count:
                raise MetadataObjectError("recipe.chunk_count no coincide con chunk_list")
            if sum(size for _, _, size in chunks) != recipe_obj.total_size:
                raise MetadataObjectError("recipe.total_size no coincide con chunk_list")
            calculated = compute_recipe_hash(list(chunks))
            if calculated != recipe_obj.recipe_hash:
                raise MetadataObjectError(
                    "recipe_hash no coincide con la secuencia canónica de chunks: "
                    f"esperado={recipe_obj.recipe_hash} calculado={calculated}"
                )
            previous = recipe_chunks_by_hash.get(recipe_obj.recipe_hash)
            if previous is not None and previous != chunks:
                raise MetadataObjectError(
                    f"recipe_hash duplicado con chunks distintos: {recipe_obj.recipe_hash}"
                )
            recipe_chunks_by_hash.setdefault(recipe_obj.recipe_hash, chunks)

            for _order, chunk_hash, chunk_size in chunks:
                known_size = known_chunks.get(chunk_hash)
                if known_size is None:
                    raise MetadataObjectError(
                        f"recipe referencia chunk ausente de known_chunk_index: {chunk_hash}"
                    )
                if known_size != chunk_size:
                    raise MetadataObjectError(
                        f"tamaño de chunk no coincide con known_chunk_index: {chunk_hash}"
                    )
    finally:
        active_trees.remove(tree_hash)
