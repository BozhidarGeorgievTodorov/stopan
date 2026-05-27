"""
Normalización de registros EC del metadata object graph.

Este módulo concentra la semántica de importación de data packs EC: traversal del
índice, validación estructural y conversión a filas operacionales. No accede a
MetadataDB ni conoce la transacción de importación.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias

from stopan.metadata.objects.models import MetadataObjectType

from .errors import MetadataObjectImportError

if TYPE_CHECKING:
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


ErasurePackRow: TypeAlias = tuple[
    str,
    str,
    int,
    int,
    int,
    int,
    int,
    str,
    str | None,
    float,
    float | None,
    float | None,
    str | None,
]
ErasureChunkRow: TypeAlias = tuple[str, str, int, int, int]
ErasureShardRow: TypeAlias = tuple[
    str,
    int,
    str,
    str,
    int,
    str,
    float | None,
    float | None,
    str | None,
]


@dataclass(frozen=True, slots=True)
class ErasureDataPackImportRows:
    pack: ErasurePackRow
    chunks: tuple[ErasureChunkRow, ...]
    shards: tuple[ErasureShardRow, ...]

    @property
    def pack_hash(self) -> str:
        return self.pack[0]

    @property
    def chunk_count(self) -> int:
        return len(self.chunks)

    @property
    def shard_count(self) -> int:
        return len(self.shards)


def load_erasure_data_pack_rows(
    *,
    reader: MetadataObjectReader,
    catalog: dict[str, Any],
    chunk_sizes: dict[str, int],
) -> list[ErasureDataPackImportRows]:
    index_hash = optional_ref(
        "catalog.erasure_data_pack_index",
        catalog.get("erasure_data_pack_index"),
        MetadataObjectType.ERASURE_DATA_PACK_INDEX,
    )
    if index_hash is None:
        return []

    index = reader.payload(index_hash, MetadataObjectType.ERASURE_DATA_PACK_INDEX)
    shards = index.get("shards")
    if not isinstance(shards, list):
        raise MetadataObjectImportError("erasure_data_pack_index.shards debe ser lista")

    out: list[ErasureDataPackImportRows] = []
    seen_packs: set[str] = set()
    seen_chunks: set[str] = set()
    chunk_count = 0
    shard_count = 0

    for shard_ref in shards:
        if not isinstance(shard_ref, dict):
            raise MetadataObjectImportError("erasure_data_pack_index shard inválido")
        prefix = require_str("erasure_data_pack_index.shard.prefix", shard_ref.get("prefix"))
        shard_hash = require_ref(
            "erasure_data_pack_index.shard.ref",
            shard_ref.get("ref"),
            MetadataObjectType.ERASURE_DATA_PACK_SHARD,
        )
        shard = reader.payload(shard_hash, MetadataObjectType.ERASURE_DATA_PACK_SHARD)
        if shard.get("prefix") != prefix:
            raise MetadataObjectImportError("prefix de erasure_data_pack shard no coincide")
        records = shard.get("records")
        if not isinstance(records, list):
            raise MetadataObjectImportError("erasure_data_pack_shard.records debe ser lista")

        for record in records:
            if not isinstance(record, dict):
                raise MetadataObjectImportError("erasure data pack record inválido")
            pack_hash = require_hash64("erasure_data_pack.pack_hash", record.get("pack_hash"))
            if not pack_hash.startswith(prefix):
                raise MetadataObjectImportError("erasure data pack record fuera de su shard")
            if pack_hash in seen_packs:
                raise MetadataObjectImportError(f"erasure data pack duplicado: {pack_hash}")

            rows = erasure_data_pack_import_rows(
                record,
                pack_hash=pack_hash,
                chunk_sizes=chunk_sizes,
                seen_global_chunks=seen_chunks,
            )
            seen_packs.add(pack_hash)
            chunk_count += rows.chunk_count
            shard_count += rows.shard_count
            out.append(rows)

    expected_packs = require_int(
        "erasure_data_pack_index.total_packs",
        index.get("total_packs"),
        min_value=0,
    )
    expected_chunks = require_int(
        "erasure_data_pack_index.total_chunks",
        index.get("total_chunks"),
        min_value=0,
    )
    expected_shards = require_int(
        "erasure_data_pack_index.total_shards",
        index.get("total_shards"),
        min_value=0,
    )
    if len(out) != expected_packs:
        raise MetadataObjectImportError(
            f"erasure_data_pack_index total_packs no coincide: total={expected_packs} cargados={len(out)}"
        )
    if chunk_count != expected_chunks:
        raise MetadataObjectImportError(
            f"erasure_data_pack_index total_chunks no coincide: total={expected_chunks} cargados={chunk_count}"
        )
    if shard_count != expected_shards:
        raise MetadataObjectImportError(
            f"erasure_data_pack_index total_shards no coincide: total={expected_shards} cargados={shard_count}"
        )
    return out


def erasure_data_pack_import_rows(
    record: dict[str, Any],
    *,
    pack_hash: str,
    chunk_sizes: dict[str, int],
    seen_global_chunks: set[str],
) -> ErasureDataPackImportRows:
    codec = require_str("erasure_data_pack.codec", record.get("codec"))
    data_shards = require_int("erasure_data_pack.data_shards", record.get("data_shards"), min_value=1)
    parity_shards = require_int("erasure_data_pack.parity_shards", record.get("parity_shards"), min_value=0)
    payload_size = require_int("erasure_data_pack.payload_size", record.get("payload_size"), min_value=0)
    padded_size = require_int("erasure_data_pack.padded_size", record.get("padded_size"), min_value=0)
    shard_size = require_int("erasure_data_pack.shard_size", record.get("shard_size"), min_value=1)
    if padded_size != shard_size * data_shards:
        raise MetadataObjectImportError("erasure_data_pack.padded_size debe coincidir con shard_size * data_shards")
    if payload_size > padded_size:
        raise MetadataObjectImportError("erasure_data_pack.payload_size no puede ser mayor que padded_size")

    protection_state = require_protection_state(
        "erasure_data_pack.protection_state",
        record.get("protection_state"),
    )
    placement_epoch = _optional_text(
        "erasure_data_pack.placement_epoch",
        record.get("placement_epoch"),
    )
    created_at = require_number("erasure_data_pack.created_at", record.get("created_at"), min_value=0.0)
    last_push_at = _optional_number("erasure_data_pack.last_push_at", record.get("last_push_at"))
    last_verify_at = _optional_number("erasure_data_pack.last_verify_at", record.get("last_verify_at"))
    last_error = _optional_text("erasure_data_pack.last_error", record.get("last_error"))

    chunks = _erasure_chunk_rows(
        record,
        pack_hash=pack_hash,
        chunk_sizes=chunk_sizes,
        seen_global_chunks=seen_global_chunks,
    )
    shards = _erasure_shard_rows(record, pack_hash=pack_hash)

    return ErasureDataPackImportRows(
        pack=(
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
            last_verify_at,
            last_error,
        ),
        chunks=chunks,
        shards=shards,
    )


def _erasure_chunk_rows(
    record: dict[str, Any],
    *,
    pack_hash: str,
    chunk_sizes: dict[str, int],
    seen_global_chunks: set[str],
) -> tuple[ErasureChunkRow, ...]:
    raw_chunks = record.get("chunks")
    if not isinstance(raw_chunks, list):
        raise MetadataObjectImportError("erasure_data_pack.chunks debe ser lista")

    rows: list[ErasureChunkRow] = []
    previous_end = 0
    seen_pack_chunks: set[str] = set()
    for expected_ordinal, item in enumerate(raw_chunks):
        if not isinstance(item, dict):
            raise MetadataObjectImportError("erasure_data_pack chunk inválido")
        chunk_hash = require_hash64("erasure_pack_chunk.chunk_hash", item.get("chunk_hash"))
        if chunk_hash in seen_pack_chunks:
            raise MetadataObjectImportError(f"erasure_data_pack contiene chunk duplicado: {chunk_hash}")
        if chunk_hash in seen_global_chunks:
            raise MetadataObjectImportError(f"chunk EC duplicado entre data packs: {chunk_hash}")
        if chunk_hash not in chunk_sizes:
            raise MetadataObjectImportError(f"erasure_data_pack referencia chunk inexistente: {chunk_hash}")

        offset = require_int("erasure_pack_chunk.offset", item.get("offset"), min_value=0)
        length = require_int("erasure_pack_chunk.length", item.get("length"), min_value=1)
        ordinal = require_int("erasure_pack_chunk.ordinal", item.get("ordinal"), min_value=0)
        if ordinal != expected_ordinal:
            raise MetadataObjectImportError("chunks EC deben tener ordinales consecutivos desde 0")
        if offset != previous_end:
            raise MetadataObjectImportError("chunks EC deben cubrir el payload de forma contigua")

        previous_end = offset + length
        seen_pack_chunks.add(chunk_hash)
        seen_global_chunks.add(chunk_hash)
        rows.append((chunk_hash, pack_hash, offset, length, ordinal))

    payload_size = require_int("erasure_data_pack.payload_size", record.get("payload_size"), min_value=0)
    if not rows:
        raise MetadataObjectImportError("erasure_data_pack debe contener al menos un chunk")
    if previous_end != payload_size:
        raise MetadataObjectImportError("chunks EC no cubren exactamente payload_size")
    return tuple(rows)


def _erasure_shard_rows(record: dict[str, Any], *, pack_hash: str) -> tuple[ErasureShardRow, ...]:
    raw_shards = record.get("shards")
    if not isinstance(raw_shards, list):
        raise MetadataObjectImportError("erasure_data_pack.shards debe ser lista")

    data_shards = require_int("erasure_data_pack.data_shards", record.get("data_shards"), min_value=1)
    parity_shards = require_int("erasure_data_pack.parity_shards", record.get("parity_shards"), min_value=0)
    expected_indexes = list(range(data_shards + parity_shards))

    rows: list[ErasureShardRow] = []
    indexes: list[int] = []
    node_ids: set[str] = set()
    for item in raw_shards:
        if not isinstance(item, dict):
            raise MetadataObjectImportError("erasure_data_pack shard inválido")
        shard_index = require_int("erasure_pack_shard.shard_index", item.get("shard_index"), min_value=0)
        shard_hash = require_hash64("erasure_pack_shard.shard_hash", item.get("shard_hash"))
        node_id = require_str("erasure_pack_shard.node_id", item.get("node_id"))
        size = require_int("erasure_pack_shard.size", item.get("size"), min_value=1)
        protection_state = require_protection_state(
            "erasure_pack_shard.protection_state",
            item.get("protection_state"),
        )
        last_push_at = _optional_number("erasure_pack_shard.last_push_at", item.get("last_push_at"))
        last_verify_at = _optional_number("erasure_pack_shard.last_verify_at", item.get("last_verify_at"))
        last_error = _optional_text("erasure_pack_shard.last_error", item.get("last_error"))
        indexes.append(shard_index)
        if node_id in node_ids:
            raise MetadataObjectImportError("shards EC de un pack deben ir a nodos distintos")
        node_ids.add(node_id)
        rows.append((
            pack_hash,
            shard_index,
            shard_hash,
            node_id,
            size,
            protection_state,
            last_push_at,
            last_verify_at,
            last_error,
        ))

    if indexes != expected_indexes:
        raise MetadataObjectImportError("shards EC deben cubrir todos los índices esperados")
    return tuple(rows)


def _optional_number(name: str, value: object) -> float | None:
    if value is None:
        return None
    return require_number(name, value, min_value=0.0)


def _optional_text(name: str, value: object) -> str | None:
    if value is None:
        return None
    return require_str(name, value, allow_empty=True)
