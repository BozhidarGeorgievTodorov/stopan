"""
Construcción de data packs post-deduplicación.

El target size es un límite superior operativo. Al finalizar un push, el caller
debe forzar flush del builder aunque el pack abierto sea pequeño.
"""

from __future__ import annotations

from collections.abc import Iterable

from .codec import ZfecErasureCodec, split_primary_blocks
from .manifest import DataPackManifest
from .models import (
    DataPackEntry,
    EncodedDataPack,
    ErasureCodingError,
    ErasureSpec,
    hash_bytes,
    require_hash64,
)


class DataPackBuilder:
    def __init__(
        self,
        *,
        spec: ErasureSpec,
        target_size_bytes: int,
        codec: ZfecErasureCodec | None = None,
    ):
        if not isinstance(spec, ErasureSpec):
            raise ErasureCodingError("spec debe ser ErasureSpec")
        if isinstance(target_size_bytes, bool) or not isinstance(target_size_bytes, int):
            raise ErasureCodingError("target_size_bytes debe ser int")
        if target_size_bytes <= 0:
            raise ErasureCodingError("target_size_bytes debe ser > 0")

        self.spec = spec
        self.target_size_bytes = target_size_bytes
        self.codec = codec or ZfecErasureCodec()
        self._chunks: list[tuple[str, bytes]] = []
        self._current_size = 0

    @property
    def current_size(self) -> int:
        return self._current_size

    @property
    def is_empty(self) -> bool:
        return not self._chunks

    def add_chunk(self, *, chunk_hash: str, data: bytes) -> bool:
        chunk_hash, data = _validated_chunk_payload(chunk_hash=chunk_hash, data=data)
        self._chunks.append((chunk_hash, data))
        self._current_size += len(data)
        return self._current_size >= self.target_size_bytes

    def flush(self) -> EncodedDataPack | None:
        if not self._chunks:
            return None

        encoded = build_data_pack(
            chunks=self._chunks,
            spec=self.spec,
            codec=self.codec,
        )
        self._chunks = []
        self._current_size = 0
        return encoded


def build_data_pack(
    *,
    chunks: Iterable[tuple[str, bytes]],
    spec: ErasureSpec,
    codec: ZfecErasureCodec | None = None,
) -> EncodedDataPack:
    materialized = tuple(chunks)
    if not materialized:
        raise ErasureCodingError("no se puede construir un data pack vacío")

    payload_parts: list[bytes] = []
    entries: list[DataPackEntry] = []
    offset = 0

    for ordinal, (chunk_hash, data) in enumerate(materialized):
        chunk_hash, data = _validated_chunk_payload(chunk_hash=chunk_hash, data=data)
        payload_parts.append(data)
        entries.append(
            DataPackEntry(
                chunk_hash=chunk_hash,
                offset=offset,
                length=len(data),
                ordinal=ordinal,
            )
        )
        offset += len(data)

    payload = b"".join(payload_parts)
    pack_hash = hash_bytes(payload)
    _primary_blocks, shard_size = split_primary_blocks(payload, spec.data_shards)
    padded_size = shard_size * spec.data_shards
    codec = codec or ZfecErasureCodec()
    shards = codec.encode(pack_hash=pack_hash, payload=payload, spec=spec)

    return EncodedDataPack(
        pack_hash=pack_hash,
        payload_size=len(payload),
        padded_size=padded_size,
        shard_size=shard_size,
        spec=spec,
        entries=tuple(entries),
        shards=shards,
    )


def _validated_chunk_payload(*, chunk_hash: str, data: bytes) -> tuple[str, bytes]:
    chunk_hash = require_hash64("chunk_hash", chunk_hash)
    if not isinstance(data, bytes):
        raise ErasureCodingError(f"chunk data debe ser bytes; recibido {type(data).__name__}")

    calculated = hash_bytes(data)
    if calculated != chunk_hash:
        raise ErasureCodingError(
            f"chunk_hash no coincide: esperado={chunk_hash} calculado={calculated}"
        )
    return chunk_hash, data


def reconstruct_payload(
    *,
    manifest: DataPackManifest,
    shards,
    codec: ZfecErasureCodec | None = None,
) -> bytes:
    codec = codec or ZfecErasureCodec()
    return codec.decode(
        shards=shards,
        spec=manifest.spec,
        payload_size=manifest.payload_size,
        expected_pack_hash=manifest.pack_hash,
    )


def extract_pack_chunks(
    *,
    payload: bytes,
    manifest: DataPackManifest,
    wanted_hashes: set[str] | None = None,
) -> dict[str, bytes]:
    if not isinstance(payload, bytes):
        raise ErasureCodingError(f"payload debe ser bytes; recibido {type(payload).__name__}")
    if not isinstance(manifest, DataPackManifest):
        raise ErasureCodingError("manifest debe ser DataPackManifest")

    wanted = {require_hash64("wanted_hash", item) for item in wanted_hashes} if wanted_hashes else None
    chunks: dict[str, bytes] = {}

    for entry in manifest.entries:
        if wanted is not None and entry.chunk_hash not in wanted:
            continue
        data = payload[entry.offset : entry.offset + entry.length]
        calculated = hash_bytes(data)
        if calculated != entry.chunk_hash:
            raise ErasureCodingError(
                f"chunk reconstruido no coincide: esperado={entry.chunk_hash} calculado={calculated}"
            )
        chunks[entry.chunk_hash] = data

    return chunks
