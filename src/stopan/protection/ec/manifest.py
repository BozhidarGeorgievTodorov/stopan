"""
Manifest canónico de un data pack protegido por erasure coding.

El manifest describe el payload reconstruido, no los nodos remotos. La relación
pack -> shard -> nodo debe vivir en metadata de protección para no duplicar una
fila por chunk y shard.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .models import DataPackEntry, ErasureCodingError, ErasureSpec, require_hash64


DATA_PACK_MANIFEST_FORMAT = "stopan.ec.data_pack_manifest"
DATA_PACK_MANIFEST_VERSION = 1


@dataclass(frozen=True, slots=True)
class DataPackManifest:
    pack_hash: str
    payload_size: int
    padded_size: int
    shard_size: int
    spec: ErasureSpec
    entries: tuple[DataPackEntry, ...]

    def __post_init__(self) -> None:
        require_hash64("pack_hash", self.pack_hash)
        if not isinstance(self.spec, ErasureSpec):
            raise ErasureCodingError("spec debe ser ErasureSpec")
        if self.payload_size < 0 or self.padded_size < 0 or self.shard_size < 0:
            raise ErasureCodingError("los tamaños del manifest deben ser >= 0")
        if self.padded_size != self.shard_size * self.spec.data_shards:
            raise ErasureCodingError("padded_size debe coincidir con shard_size * data_shards")
        object.__setattr__(self, "entries", tuple(self.entries))

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": DATA_PACK_MANIFEST_FORMAT,
            "version": DATA_PACK_MANIFEST_VERSION,
            "pack_hash": self.pack_hash,
            "payload_size": self.payload_size,
            "padded_size": self.padded_size,
            "shard_size": self.shard_size,
            "codec": self.spec.codec,
            "data_shards": self.spec.data_shards,
            "parity_shards": self.spec.parity_shards,
            "entries": [
                {
                    "chunk_hash": entry.chunk_hash,
                    "offset": entry.offset,
                    "length": entry.length,
                    "ordinal": entry.ordinal,
                }
                for entry in self.entries
            ],
        }


def encode_manifest(manifest: DataPackManifest) -> bytes:
    if not isinstance(manifest, DataPackManifest):
        raise ErasureCodingError("manifest debe ser DataPackManifest")
    return json.dumps(
        manifest.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_manifest(data: bytes) -> DataPackManifest:
    if not isinstance(data, bytes):
        raise ErasureCodingError(f"manifest data debe ser bytes; recibido {type(data).__name__}")

    try:
        raw = json.loads(data.decode("utf-8"))
    except Exception as exc:
        raise ErasureCodingError(f"manifest JSON inválido: {exc}") from exc

    if not isinstance(raw, dict):
        raise ErasureCodingError("manifest debe ser un objeto JSON")
    if raw.get("format") != DATA_PACK_MANIFEST_FORMAT:
        raise ErasureCodingError("formato de manifest EC no soportado")
    if raw.get("version") != DATA_PACK_MANIFEST_VERSION:
        raise ErasureCodingError("versión de manifest EC no soportada")

    spec = ErasureSpec(
        data_shards=int(raw["data_shards"]),
        parity_shards=int(raw["parity_shards"]),
        codec=str(raw["codec"]),
    )
    entries_raw = raw.get("entries")
    if not isinstance(entries_raw, list):
        raise ErasureCodingError("manifest.entries debe ser una lista")

    entries = tuple(
        DataPackEntry(
            chunk_hash=str(item["chunk_hash"]),
            offset=int(item["offset"]),
            length=int(item["length"]),
            ordinal=int(item["ordinal"]),
        )
        for item in entries_raw
    )

    return DataPackManifest(
        pack_hash=str(raw["pack_hash"]),
        payload_size=int(raw["payload_size"]),
        padded_size=int(raw["padded_size"]),
        shard_size=int(raw["shard_size"]),
        spec=spec,
        entries=entries,
    )
