"""
Modelo canónico de metadata objects.

Las dataclasses validan invariantes estructurales del grafo: hashes, tipos de
referencia, orden estable, estados permitidos y contadores agregados.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from stopan.errors import StopanDataError


METADATA_OBJECT_FORMAT = "stopan.metadata_object"
METADATA_OBJECT_VERSION = 1

_HASH64_RE = re.compile(r"^[0-9a-f]{64}$")
_NODE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_VALID_SNAPSHOT_STATES = frozenset({"CREATING", "COMPLETE", "FAILED"})
_VALID_ITEM_TYPES = frozenset({"file", "dir"})
_VALID_PROTECTION_STATES = frozenset(
    {"PENDING", "PLACED", "DEGRADED", "VERIFIED", "FAILED"}
)


class MetadataObjectError(StopanDataError, ValueError):
    pass


class MetadataObjectType(str, Enum):
    CATALOG = "catalog"
    SNAPSHOT_INDEX = "snapshot_index"
    SNAPSHOT_ROOT = "snapshot_root"
    TREE = "tree"
    FILE = "file"
    RECIPE = "recipe"
    CHUNK_LIST = "chunk_list"
    KNOWN_CHUNK_INDEX = "known_chunk_index"
    KNOWN_CHUNK_SHARD = "known_chunk_shard"
    PROTECTION_INDEX = "protection_index"
    PROTECTION_SHARD = "protection_shard"
    ERASURE_DATA_PACK_INDEX = "erasure_data_pack_index"
    ERASURE_DATA_PACK_SHARD = "erasure_data_pack_shard"


def _require_str(name: str, value: object, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise MetadataObjectError(f"{name} debe ser str. Recibido {type(value).__name__}")
    if not allow_empty and not value:
        raise MetadataObjectError(f"{name} no puede estar vacío")
    return value


def _require_int(name: str, value: object, *, min_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataObjectError(f"{name} debe ser int. Recibido {type(value).__name__}")
    if value < min_value:
        raise MetadataObjectError(f"{name} debe ser >= {min_value}. Recibido {value}")
    return value


def _require_number(name: str, value: object, *, min_value: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetadataObjectError(f"{name} debe ser numérico. Recibido {type(value).__name__}")
    number = float(value)
    if min_value is not None and number < min_value:
        raise MetadataObjectError(f"{name} debe ser >= {min_value}. Recibido {value}")
    return number


def _require_hash64(name: str, value: object) -> str:
    text = _require_str(name, value)
    if not _HASH64_RE.fullmatch(text):
        raise MetadataObjectError(f"{name} debe tener 64 caracteres hexadecimales lowercase")
    return text


def _require_node_id(name: str, value: object) -> str:
    text = _require_str(name, value)
    if not _NODE_ID_RE.fullmatch(text):
        raise MetadataObjectError(f"{name} debe tener 32 caracteres hexadecimales lowercase")
    return text


def _require_vault_id(name: str, value: object) -> str:
    text = _require_str(name, value)
    if not _NODE_ID_RE.fullmatch(text):
        raise MetadataObjectError(f"{name} debe tener 32 caracteres hexadecimales lowercase")
    return text


def _require_uuid(name: str, value: object) -> str:
    text = _require_str(name, value)
    if not _UUID_RE.fullmatch(text):
        raise MetadataObjectError(f"{name} debe ser UUID canónico lowercase")
    return text


def _as_tuple(name: str, values: tuple[Any, ...] | list[Any]) -> tuple[Any, ...]:
    if isinstance(values, tuple):
        return values
    if isinstance(values, list):
        return tuple(values)
    raise MetadataObjectError(f"{name} debe ser tuple/list. Recibido {type(values).__name__}")


@dataclass(frozen=True, slots=True)
class ObjectRef:
    object_type: MetadataObjectType
    object_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.object_type, MetadataObjectType):
            raise MetadataObjectError("object_ref.object_type debe ser MetadataObjectType")
        _require_hash64("object_ref.object_hash", self.object_hash)


@dataclass(frozen=True, slots=True)
class ChunkRef:
    order: int
    chunk_hash: str
    size: int

    def __post_init__(self) -> None:
        _require_int("chunk.order", self.order, min_value=0)
        _require_hash64("chunk.chunk_hash", self.chunk_hash)
        _require_int("chunk.size", self.size, min_value=1)


@dataclass(frozen=True, slots=True)
class ChunkListObject:
    chunks: tuple[ChunkRef, ...]

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.CHUNK_LIST,
    )

    def __post_init__(self) -> None:
        chunks = _as_tuple("chunk_list.chunks", self.chunks)
        object.__setattr__(self, "chunks", chunks)
        expected_order = 0
        for chunk in chunks:
            if not isinstance(chunk, ChunkRef):
                raise MetadataObjectError("chunk_list.chunks debe contener ChunkRef")
            if chunk.order != expected_order:
                raise MetadataObjectError(
                    "órdenes de chunk_list deben ser contiguos desde 0. "
                    f"Esperado {expected_order}, recibido {chunk.order}"
                )
            expected_order += 1


@dataclass(frozen=True, slots=True)
class RecipeObject:
    recipe_hash: str
    chunk_count: int
    total_size: int
    chunk_list: ObjectRef

    object_type: MetadataObjectType = field(init=False, default=MetadataObjectType.RECIPE)

    def __post_init__(self) -> None:
        _require_hash64("recipe.recipe_hash", self.recipe_hash)
        _require_int("recipe.chunk_count", self.chunk_count, min_value=0)
        _require_int("recipe.total_size", self.total_size, min_value=0)
        if not isinstance(self.chunk_list, ObjectRef):
            raise MetadataObjectError("recipe.chunk_list debe ser ObjectRef")
        if self.chunk_list.object_type != MetadataObjectType.CHUNK_LIST:
            raise MetadataObjectError("recipe.chunk_list debe referenciar un objeto chunk_list")


@dataclass(frozen=True, slots=True)
class FileObject:
    size: int
    recipe_hash: str
    recipe: ObjectRef

    object_type: MetadataObjectType = field(init=False, default=MetadataObjectType.FILE)

    def __post_init__(self) -> None:
        _require_int("file.size", self.size, min_value=0)
        _require_hash64("file.recipe_hash", self.recipe_hash)
        if not isinstance(self.recipe, ObjectRef):
            raise MetadataObjectError("file.recipe debe ser ObjectRef")
        if self.recipe.object_type != MetadataObjectType.RECIPE:
            raise MetadataObjectError("file.recipe debe referenciar un objeto recipe")


@dataclass(frozen=True, slots=True)
class TreeEntry:
    name: str
    item_type: str
    size: int
    mode: int
    mtime: float
    mtime_ns: int
    uid: int
    gid: int
    ref: ObjectRef

    def __post_init__(self) -> None:
        _require_str("tree_entry.name", self.name)
        if "/" in self.name or self.name in {".", ".."}:
            raise MetadataObjectError(f"nombre de tree entry inválido: {self.name!r}")
        _require_str("tree_entry.item_type", self.item_type)
        if self.item_type not in _VALID_ITEM_TYPES:
            raise MetadataObjectError(f"item_type de tree entry inválido: {self.item_type!r}")
        _require_int("tree_entry.size", self.size, min_value=0)
        _require_int("tree_entry.mode", self.mode, min_value=0)
        _require_number("tree_entry.mtime", self.mtime)
        _require_int("tree_entry.mtime_ns", self.mtime_ns, min_value=0)
        _require_int("tree_entry.uid", self.uid, min_value=0)
        _require_int("tree_entry.gid", self.gid, min_value=0)
        if not isinstance(self.ref, ObjectRef):
            raise MetadataObjectError("tree_entry.ref debe ser ObjectRef")
        expected_type = (
            MetadataObjectType.FILE
            if self.item_type == "file"
            else MetadataObjectType.TREE
        )
        if self.ref.object_type != expected_type:
            raise MetadataObjectError(
                f"tree_entry.ref debe referenciar {expected_type.value}. "
                f"Recibido {self.ref.object_type.value}"
            )


@dataclass(frozen=True, slots=True)
class TreeObject:
    entries: tuple[TreeEntry, ...]

    object_type: MetadataObjectType = field(init=False, default=MetadataObjectType.TREE)

    def __post_init__(self) -> None:
        entries = _as_tuple("tree.entries", self.entries)
        object.__setattr__(self, "entries", entries)
        names = set()
        previous_name: str | None = None
        for entry in entries:
            if not isinstance(entry, TreeEntry):
                raise MetadataObjectError("tree.entries debe contener TreeEntry")
            if entry.name in names:
                raise MetadataObjectError(f"nombre de tree entry duplicado: {entry.name!r}")
            if previous_name is not None and entry.name < previous_name:
                raise MetadataObjectError("tree.entries debe estar ordenado por name")
            names.add(entry.name)
            previous_name = entry.name


@dataclass(frozen=True, slots=True)
class SnapshotRootObject:
    snapshot_uuid: str
    root_path: str
    origin_node_id: str
    created_at: str
    total_size: int
    total_files: int
    root_tree: ObjectRef

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.SNAPSHOT_ROOT,
    )

    def __post_init__(self) -> None:
        _require_uuid("snapshot_root.snapshot_uuid", self.snapshot_uuid)
        _require_str("snapshot_root.root_path", self.root_path, allow_empty=True)
        _require_node_id("snapshot_root.origin_node_id", self.origin_node_id)
        _require_str("snapshot_root.created_at", self.created_at)
        _require_int("snapshot_root.total_size", self.total_size, min_value=0)
        _require_int("snapshot_root.total_files", self.total_files, min_value=0)
        if not isinstance(self.root_tree, ObjectRef):
            raise MetadataObjectError("snapshot_root.root_tree debe ser ObjectRef")
        if self.root_tree.object_type != MetadataObjectType.TREE:
            raise MetadataObjectError("snapshot_root.root_tree debe referenciar un objeto tree")


@dataclass(frozen=True, slots=True)
class SnapshotIndexEntry:
    snapshot_uuid: str
    status: str
    root_path: str
    origin_node_id: str
    created_at: str
    total_size: int
    total_files: int
    snapshot_root: ObjectRef | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_uuid("snapshot_index_entry.snapshot_uuid", self.snapshot_uuid)
        _require_str("snapshot_index_entry.status", self.status)
        if self.status not in _VALID_SNAPSHOT_STATES:
            raise MetadataObjectError(f"estado de snapshot inválido: {self.status!r}")
        _require_str("snapshot_index_entry.root_path", self.root_path, allow_empty=True)
        _require_node_id("snapshot_index_entry.origin_node_id", self.origin_node_id)
        _require_str("snapshot_index_entry.created_at", self.created_at)
        _require_int("snapshot_index_entry.total_size", self.total_size, min_value=0)
        _require_int("snapshot_index_entry.total_files", self.total_files, min_value=0)
        if self.snapshot_root is not None:
            if not isinstance(self.snapshot_root, ObjectRef):
                raise MetadataObjectError("snapshot_index_entry.snapshot_root debe ser ObjectRef")
            if self.snapshot_root.object_type != MetadataObjectType.SNAPSHOT_ROOT:
                raise MetadataObjectError("snapshot_root debe referenciar un objeto snapshot_root")
        if self.status == "COMPLETE" and self.snapshot_root is None:
            raise MetadataObjectError("snapshots COMPLETE deben referenciar un objeto snapshot_root")
        if self.error is not None:
            _require_str("snapshot_index_entry.error", self.error, allow_empty=True)


@dataclass(frozen=True, slots=True)
class SnapshotIndexObject:
    snapshots: tuple[SnapshotIndexEntry, ...]

    object_type: MetadataObjectType = field(init=False, default=MetadataObjectType.SNAPSHOT_INDEX)

    def __post_init__(self) -> None:
        snapshots = _as_tuple("snapshot_index.snapshots", self.snapshots)
        object.__setattr__(self, "snapshots", snapshots)
        seen = set()
        previous_created_uuid: tuple[str, str] | None = None
        for entry in snapshots:
            if not isinstance(entry, SnapshotIndexEntry):
                raise MetadataObjectError("snapshot_index.snapshots debe contener SnapshotIndexEntry")
            if entry.snapshot_uuid in seen:
                raise MetadataObjectError(f"snapshot uuid duplicado: {entry.snapshot_uuid}")
            current_key = (entry.created_at, entry.snapshot_uuid)
            if previous_created_uuid is not None and current_key < previous_created_uuid:
                raise MetadataObjectError(
                    "snapshot_index.snapshots debe estar ordenado por "
                    "created_at, snapshot_uuid"
                )
            seen.add(entry.snapshot_uuid)
            previous_created_uuid = current_key


@dataclass(frozen=True, slots=True)
class KnownChunkRecord:
    chunk_hash: str
    size: int

    def __post_init__(self) -> None:
        _require_hash64("known_chunk.chunk_hash", self.chunk_hash)
        _require_int("known_chunk.size", self.size, min_value=1)


@dataclass(frozen=True, slots=True)
class KnownChunkShardObject:
    prefix: str
    chunks: tuple[KnownChunkRecord, ...]

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.KNOWN_CHUNK_SHARD,
    )

    def __post_init__(self) -> None:
        _require_str("known_chunk_shard.prefix", self.prefix)
        if not re.fullmatch(r"[0-9a-f]{2}", self.prefix):
            raise MetadataObjectError("known_chunk_shard.prefix debe tener dos caracteres hexadecimales lowercase")
        chunks = _as_tuple("known_chunk_shard.chunks", self.chunks)
        object.__setattr__(self, "chunks", chunks)
        seen = set()
        previous_hash: str | None = None
        for record in chunks:
            if not isinstance(record, KnownChunkRecord):
                raise MetadataObjectError("known_chunk_shard.chunks debe contener KnownChunkRecord")
            if not record.chunk_hash.startswith(self.prefix):
                raise MetadataObjectError("known_chunk_shard contiene un record fuera de su prefix")
            if record.chunk_hash in seen:
                raise MetadataObjectError(f"known chunk duplicado: {record.chunk_hash}")
            if previous_hash is not None and record.chunk_hash < previous_hash:
                raise MetadataObjectError("known_chunk_shard.chunks debe estar ordenado por chunk_hash")
            seen.add(record.chunk_hash)
            previous_hash = record.chunk_hash


@dataclass(frozen=True, slots=True)
class IndexShardRef:
    prefix: str
    count: int
    ref: ObjectRef

    def __post_init__(self) -> None:
        _require_str("index_shard_ref.prefix", self.prefix)
        if not re.fullmatch(r"[0-9a-f]{2}", self.prefix):
            raise MetadataObjectError("index_shard_ref.prefix debe tener dos caracteres hexadecimales lowercase")
        _require_int("index_shard_ref.count", self.count, min_value=0)
        if not isinstance(self.ref, ObjectRef):
            raise MetadataObjectError("index_shard_ref.ref debe ser ObjectRef")


@dataclass(frozen=True, slots=True)
class KnownChunkIndexObject:
    shards: tuple[IndexShardRef, ...]
    total_chunks: int

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.KNOWN_CHUNK_INDEX,
    )

    def __post_init__(self) -> None:
        shards = _as_tuple("known_chunk_index.shards", self.shards)
        object.__setattr__(self, "shards", shards)
        _require_int("known_chunk_index.total_chunks", self.total_chunks, min_value=0)
        seen = set()
        previous_prefix: str | None = None
        total = 0
        for shard in shards:
            if not isinstance(shard, IndexShardRef):
                raise MetadataObjectError("known_chunk_index.shards debe contener IndexShardRef")
            if shard.ref.object_type != MetadataObjectType.KNOWN_CHUNK_SHARD:
                raise MetadataObjectError("known_chunk_index shard ref debe referenciar known_chunk_shard")
            if shard.prefix in seen:
                raise MetadataObjectError(f"prefix de known_chunk_index duplicado: {shard.prefix}")
            if previous_prefix is not None and shard.prefix < previous_prefix:
                raise MetadataObjectError("known_chunk_index.shards debe estar ordenado por prefix")
            seen.add(shard.prefix)
            previous_prefix = shard.prefix
            total += shard.count
        if total != self.total_chunks:
            raise MetadataObjectError("known_chunk_index.total_chunks no coincide con los contadores de shards")


@dataclass(frozen=True, slots=True)
class ProtectionRecordObject:
    chunk_hash: str
    desired_rf: int
    protection_state: str
    protected_remote_copies: int
    placement_epoch: str | None
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None

    def __post_init__(self) -> None:
        _require_hash64("protection.chunk_hash", self.chunk_hash)
        _require_int("protection.desired_rf", self.desired_rf, min_value=0)
        _require_str("protection.protection_state", self.protection_state)
        if self.protection_state not in _VALID_PROTECTION_STATES:
            raise MetadataObjectError(f"estado de protection inválido: {self.protection_state!r}")
        _require_int("protection.protected_remote_copies", self.protected_remote_copies, min_value=0)
        if self.placement_epoch is not None:
            _require_str("protection.placement_epoch", self.placement_epoch, allow_empty=True)
        if self.last_push_at is not None:
            _require_number("protection.last_push_at", self.last_push_at, min_value=0.0)
        if self.last_verify_at is not None:
            _require_number("protection.last_verify_at", self.last_verify_at, min_value=0.0)
        if self.last_error is not None:
            _require_str("protection.last_error", self.last_error, allow_empty=True)


@dataclass(frozen=True, slots=True)
class ProtectionShardObject:
    prefix: str
    records: tuple[ProtectionRecordObject, ...]

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.PROTECTION_SHARD,
    )

    def __post_init__(self) -> None:
        _require_str("protection_shard.prefix", self.prefix)
        if not re.fullmatch(r"[0-9a-f]{2}", self.prefix):
            raise MetadataObjectError("protection_shard.prefix debe tener dos caracteres hexadecimales lowercase")
        records = _as_tuple("protection_shard.records", self.records)
        object.__setattr__(self, "records", records)
        seen = set()
        previous_hash: str | None = None
        for record in records:
            if not isinstance(record, ProtectionRecordObject):
                raise MetadataObjectError("protection_shard.records debe contener ProtectionRecordObject")
            if not record.chunk_hash.startswith(self.prefix):
                raise MetadataObjectError("protection_shard contiene un record fuera de su prefix")
            if record.chunk_hash in seen:
                raise MetadataObjectError(f"protection record duplicado: {record.chunk_hash}")
            if previous_hash is not None and record.chunk_hash < previous_hash:
                raise MetadataObjectError("protection_shard.records debe estar ordenado por chunk_hash")
            seen.add(record.chunk_hash)
            previous_hash = record.chunk_hash


@dataclass(frozen=True, slots=True)
class ProtectionIndexObject:
    shards: tuple[IndexShardRef, ...]
    total_records: int

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.PROTECTION_INDEX,
    )

    def __post_init__(self) -> None:
        shards = _as_tuple("protection_index.shards", self.shards)
        object.__setattr__(self, "shards", shards)
        _require_int("protection_index.total_records", self.total_records, min_value=0)
        seen = set()
        previous_prefix: str | None = None
        total = 0
        for shard in shards:
            if not isinstance(shard, IndexShardRef):
                raise MetadataObjectError("protection_index.shards debe contener IndexShardRef")
            if shard.ref.object_type != MetadataObjectType.PROTECTION_SHARD:
                raise MetadataObjectError("protection_index shard ref debe referenciar protection_shard")
            if shard.prefix in seen:
                raise MetadataObjectError(f"prefix de protection_index duplicado: {shard.prefix}")
            if previous_prefix is not None and shard.prefix < previous_prefix:
                raise MetadataObjectError("protection_index.shards debe estar ordenado por prefix")
            seen.add(shard.prefix)
            previous_prefix = shard.prefix
            total += shard.count
        if total != self.total_records:
            raise MetadataObjectError("protection_index.total_records no coincide con los contadores de shards")


@dataclass(frozen=True, slots=True)
class ErasureDataPackChunkObject:
    chunk_hash: str
    offset: int
    length: int
    ordinal: int

    def __post_init__(self) -> None:
        _require_hash64("erasure_pack_chunk.chunk_hash", self.chunk_hash)
        _require_int("erasure_pack_chunk.offset", self.offset, min_value=0)
        _require_int("erasure_pack_chunk.length", self.length, min_value=1)
        _require_int("erasure_pack_chunk.ordinal", self.ordinal, min_value=0)


@dataclass(frozen=True, slots=True)
class ErasureDataPackShardPlacementObject:
    shard_index: int
    shard_hash: str
    node_id: str
    size: int
    protection_state: str
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None

    def __post_init__(self) -> None:
        _require_int("erasure_pack_shard.shard_index", self.shard_index, min_value=0)
        _require_hash64("erasure_pack_shard.shard_hash", self.shard_hash)
        _require_node_id("erasure_pack_shard.node_id", self.node_id)
        _require_int("erasure_pack_shard.size", self.size, min_value=1)
        _require_str("erasure_pack_shard.protection_state", self.protection_state)
        if self.protection_state not in _VALID_PROTECTION_STATES:
            raise MetadataObjectError(f"estado de erasure pack shard inválido: {self.protection_state!r}")
        if self.last_push_at is not None:
            _require_number("erasure_pack_shard.last_push_at", self.last_push_at, min_value=0.0)
        if self.last_verify_at is not None:
            _require_number("erasure_pack_shard.last_verify_at", self.last_verify_at, min_value=0.0)
        if self.last_error is not None:
            _require_str("erasure_pack_shard.last_error", self.last_error, allow_empty=True)


@dataclass(frozen=True, slots=True)
class ErasureDataPackRecordObject:
    pack_hash: str
    codec: str
    data_shards: int
    parity_shards: int
    payload_size: int
    padded_size: int
    shard_size: int
    protection_state: str
    placement_epoch: str | None
    created_at: float
    last_push_at: float | None
    last_verify_at: float | None
    last_error: str | None
    chunks: tuple[ErasureDataPackChunkObject, ...]
    shards: tuple[ErasureDataPackShardPlacementObject, ...]

    def __post_init__(self) -> None:
        _require_hash64("erasure_data_pack.pack_hash", self.pack_hash)
        _require_str("erasure_data_pack.codec", self.codec)
        _require_int("erasure_data_pack.data_shards", self.data_shards, min_value=1)
        _require_int("erasure_data_pack.parity_shards", self.parity_shards, min_value=0)
        _require_int("erasure_data_pack.payload_size", self.payload_size, min_value=0)
        _require_int("erasure_data_pack.padded_size", self.padded_size, min_value=0)
        _require_int("erasure_data_pack.shard_size", self.shard_size, min_value=1)
        if self.padded_size != self.shard_size * self.data_shards:
            raise MetadataObjectError("erasure_data_pack.padded_size debe coincidir con shard_size * data_shards")
        if self.payload_size > self.padded_size:
            raise MetadataObjectError("erasure_data_pack.payload_size no puede ser mayor que padded_size")
        _require_str("erasure_data_pack.protection_state", self.protection_state)
        if self.protection_state not in _VALID_PROTECTION_STATES:
            raise MetadataObjectError(f"estado de erasure data pack inválido: {self.protection_state!r}")
        if self.placement_epoch is not None:
            _require_str("erasure_data_pack.placement_epoch", self.placement_epoch, allow_empty=True)
        _require_number("erasure_data_pack.created_at", self.created_at, min_value=0.0)
        if self.last_push_at is not None:
            _require_number("erasure_data_pack.last_push_at", self.last_push_at, min_value=0.0)
        if self.last_verify_at is not None:
            _require_number("erasure_data_pack.last_verify_at", self.last_verify_at, min_value=0.0)
        if self.last_error is not None:
            _require_str("erasure_data_pack.last_error", self.last_error, allow_empty=True)

        chunks = _as_tuple("erasure_data_pack.chunks", self.chunks)
        shards = _as_tuple("erasure_data_pack.shards", self.shards)
        object.__setattr__(self, "chunks", chunks)
        object.__setattr__(self, "shards", shards)
        if not chunks:
            raise MetadataObjectError("erasure_data_pack debe contener al menos un chunk")

        previous_end = 0
        seen_chunks: set[str] = set()
        for expected_ordinal, chunk in enumerate(chunks):
            if not isinstance(chunk, ErasureDataPackChunkObject):
                raise MetadataObjectError("erasure_data_pack.chunks debe contener ErasureDataPackChunkObject")
            if chunk.chunk_hash in seen_chunks:
                raise MetadataObjectError(f"chunk EC duplicado: {chunk.chunk_hash}")
            if chunk.ordinal != expected_ordinal:
                raise MetadataObjectError("chunks de erasure_data_pack deben tener ordinales consecutivos desde 0")
            if chunk.offset != previous_end:
                raise MetadataObjectError("chunks de erasure_data_pack deben cubrir el payload de forma contigua")
            previous_end = chunk.offset + chunk.length
            seen_chunks.add(chunk.chunk_hash)
        if previous_end != self.payload_size:
            raise MetadataObjectError("chunks de erasure_data_pack no cubren exactamente payload_size")

        expected_indexes = list(range(self.data_shards + self.parity_shards))
        indexes: list[int] = []
        node_ids: set[str] = set()
        for shard in shards:
            if not isinstance(shard, ErasureDataPackShardPlacementObject):
                raise MetadataObjectError("erasure_data_pack.shards debe contener ErasureDataPackShardPlacementObject")
            indexes.append(shard.shard_index)
            if shard.node_id in node_ids:
                raise MetadataObjectError("shards de erasure_data_pack deben estar en nodos distintos")
            node_ids.add(shard.node_id)
        if indexes != expected_indexes:
            raise MetadataObjectError("shards de erasure_data_pack deben cubrir todos los índices esperados")


@dataclass(frozen=True, slots=True)
class ErasureDataPackShardObject:
    prefix: str
    records: tuple[ErasureDataPackRecordObject, ...]

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.ERASURE_DATA_PACK_SHARD,
    )

    def __post_init__(self) -> None:
        _require_str("erasure_data_pack_shard.prefix", self.prefix)
        if not re.fullmatch(r"[0-9a-f]{2}", self.prefix):
            raise MetadataObjectError("erasure_data_pack_shard.prefix debe tener dos caracteres hexadecimales lowercase")
        records = _as_tuple("erasure_data_pack_shard.records", self.records)
        object.__setattr__(self, "records", records)
        seen = set()
        previous_hash: str | None = None
        for record in records:
            if not isinstance(record, ErasureDataPackRecordObject):
                raise MetadataObjectError("erasure_data_pack_shard.records debe contener ErasureDataPackRecordObject")
            if not record.pack_hash.startswith(self.prefix):
                raise MetadataObjectError("erasure_data_pack_shard contiene un record fuera de su prefix")
            if record.pack_hash in seen:
                raise MetadataObjectError(f"erasure data pack duplicado: {record.pack_hash}")
            if previous_hash is not None and record.pack_hash < previous_hash:
                raise MetadataObjectError("erasure_data_pack_shard.records debe estar ordenado por pack_hash")
            seen.add(record.pack_hash)
            previous_hash = record.pack_hash


@dataclass(frozen=True, slots=True)
class ErasureDataPackIndexObject:
    shards: tuple[IndexShardRef, ...]
    total_packs: int
    total_chunks: int
    total_shards: int

    object_type: MetadataObjectType = field(
        init=False,
        default=MetadataObjectType.ERASURE_DATA_PACK_INDEX,
    )

    def __post_init__(self) -> None:
        shards = _as_tuple("erasure_data_pack_index.shards", self.shards)
        object.__setattr__(self, "shards", shards)
        _require_int("erasure_data_pack_index.total_packs", self.total_packs, min_value=0)
        _require_int("erasure_data_pack_index.total_chunks", self.total_chunks, min_value=0)
        _require_int("erasure_data_pack_index.total_shards", self.total_shards, min_value=0)
        seen = set()
        previous_prefix: str | None = None
        total = 0
        for shard in shards:
            if not isinstance(shard, IndexShardRef):
                raise MetadataObjectError("erasure_data_pack_index.shards debe contener IndexShardRef")
            if shard.ref.object_type != MetadataObjectType.ERASURE_DATA_PACK_SHARD:
                raise MetadataObjectError("erasure_data_pack_index shard ref debe referenciar erasure_data_pack_shard")
            if shard.prefix in seen:
                raise MetadataObjectError(f"prefix de erasure_data_pack_index duplicado: {shard.prefix}")
            if previous_prefix is not None and shard.prefix < previous_prefix:
                raise MetadataObjectError("erasure_data_pack_index.shards debe estar ordenado por prefix")
            seen.add(shard.prefix)
            previous_prefix = shard.prefix
            total += shard.count
        if total != self.total_packs:
            raise MetadataObjectError("erasure_data_pack_index.total_packs no coincide con los contadores de shards")


@dataclass(frozen=True, slots=True)
class CatalogObject:
    vault_id: str
    snapshot_index: ObjectRef
    known_chunk_index: ObjectRef
    protection_index: ObjectRef | None
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int
    erasure_data_pack_index: ObjectRef | None = None
    erasure_data_pack_count: int = 0
    erasure_data_pack_chunk_count: int = 0
    erasure_data_pack_shard_count: int = 0

    object_type: MetadataObjectType = field(init=False, default=MetadataObjectType.CATALOG)

    def __post_init__(self) -> None:
        _require_vault_id("catalog.vault_id", self.vault_id)
        if not isinstance(self.snapshot_index, ObjectRef):
            raise MetadataObjectError("catalog.snapshot_index debe ser ObjectRef")
        if self.snapshot_index.object_type != MetadataObjectType.SNAPSHOT_INDEX:
            raise MetadataObjectError("catalog.snapshot_index debe referenciar snapshot_index")
        if not isinstance(self.known_chunk_index, ObjectRef):
            raise MetadataObjectError("catalog.known_chunk_index debe ser ObjectRef")
        if self.known_chunk_index.object_type != MetadataObjectType.KNOWN_CHUNK_INDEX:
            raise MetadataObjectError("catalog.known_chunk_index debe referenciar known_chunk_index")
        if self.protection_index is not None:
            if not isinstance(self.protection_index, ObjectRef):
                raise MetadataObjectError("catalog.protection_index debe ser ObjectRef")
            if self.protection_index.object_type != MetadataObjectType.PROTECTION_INDEX:
                raise MetadataObjectError("catalog.protection_index debe referenciar protection_index")
        _require_int("catalog.snapshot_count", self.snapshot_count, min_value=0)
        _require_int("catalog.known_chunk_count", self.known_chunk_count, min_value=0)
        _require_int("catalog.protection_record_count", self.protection_record_count, min_value=0)
        if self.erasure_data_pack_index is not None:
            if not isinstance(self.erasure_data_pack_index, ObjectRef):
                raise MetadataObjectError("catalog.erasure_data_pack_index debe ser ObjectRef")
            if self.erasure_data_pack_index.object_type != MetadataObjectType.ERASURE_DATA_PACK_INDEX:
                raise MetadataObjectError("catalog.erasure_data_pack_index debe referenciar erasure_data_pack_index")
        _require_int("catalog.erasure_data_pack_count", self.erasure_data_pack_count, min_value=0)
        _require_int("catalog.erasure_data_pack_chunk_count", self.erasure_data_pack_chunk_count, min_value=0)
        _require_int("catalog.erasure_data_pack_shard_count", self.erasure_data_pack_shard_count, min_value=0)


MetadataPlainObject = (
    CatalogObject
    | SnapshotIndexObject
    | SnapshotRootObject
    | TreeObject
    | FileObject
    | RecipeObject
    | ChunkListObject
    | KnownChunkIndexObject
    | KnownChunkShardObject
    | ProtectionIndexObject
    | ProtectionShardObject
    | ErasureDataPackIndexObject
    | ErasureDataPackShardObject
)
