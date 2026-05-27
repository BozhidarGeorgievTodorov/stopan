from __future__ import annotations

from collections.abc import Sequence

from stopan.metadata.database import ErasureDataPackChunkRecord, ErasureDataPackRecord
from stopan.protection.ec.manifest import DataPackManifest
from stopan.protection.ec.models import DataPackEntry, ErasureSpec


def spec_from_erasure_metadata(pack: ErasureDataPackRecord) -> ErasureSpec:
    return ErasureSpec(
        data_shards=pack.data_shards,
        parity_shards=pack.parity_shards,
        codec=pack.codec,
    )


def manifest_entries_from_erasure_metadata(
    chunks: Sequence[ErasureDataPackChunkRecord],
) -> tuple[DataPackEntry, ...]:
    return tuple(
        DataPackEntry(
            chunk_hash=item.chunk_hash,
            offset=item.offset,
            length=item.length,
            ordinal=item.ordinal,
        )
        for item in chunks
    )


def manifest_from_erasure_metadata(
    *,
    pack: ErasureDataPackRecord,
    chunks: Sequence[ErasureDataPackChunkRecord],
) -> DataPackManifest:
    return DataPackManifest(
        pack_hash=pack.pack_hash,
        payload_size=pack.payload_size,
        padded_size=pack.padded_size,
        shard_size=pack.shard_size,
        spec=spec_from_erasure_metadata(pack),
        entries=manifest_entries_from_erasure_metadata(chunks),
    )
