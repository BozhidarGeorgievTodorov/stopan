"""
Modelos puros para protection packs con erasure coding.

ec_m representa shards extra de redundancia. Puede ser 0, en ese
caso el modo EC actúa como striping sin tolerancia a pérdida de shards. La
librería zfec llama m al número total de shards, así que el código interno usa
total_shards para evitar mezclar ambas convenciones.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from stopan.common.validators import (
    require_strict_non_negative_int,
    require_strict_positive_int,
)
from stopan.common.hashes import (
    BLAKE3_HEX_LENGTH,
    blake3_hex_digest,
    is_valid_blake3_hex,
)
from stopan.errors import (
    StopanConfigError,
    StopanDataError,
    StopanDependencyError,
    StopanNetworkError,
)


class ErasureCodingError(StopanDataError, RuntimeError):
    pass


class ErasureCodingConfigError(StopanConfigError, ErasureCodingError):
    pass


class ErasureCodingNetworkError(StopanNetworkError, ErasureCodingError):
    pass


class ErasureCodingDependencyError(StopanDependencyError, ErasureCodingError):
    pass


def require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ErasureCodingError(f"{name} debe ser str; recibido {type(value).__name__}")
    text = value.strip()
    if not is_valid_blake3_hex(text):
        raise ErasureCodingError(
            f"{name} debe tener {BLAKE3_HEX_LENGTH} caracteres hexadecimales lowercase"
        )
    return text


def require_non_negative_int(name: str, value: object) -> int:
    return require_strict_non_negative_int(name, value, error_factory=ErasureCodingError)


def require_positive_int(name: str, value: object) -> int:
    return require_strict_positive_int(name, value, error_factory=ErasureCodingError)


def require_config_non_negative_int(name: str, value: object) -> int:
    return require_strict_non_negative_int(name, value, error_factory=ErasureCodingConfigError)


def require_config_positive_int(name: str, value: object) -> int:
    return require_strict_positive_int(name, value, error_factory=ErasureCodingConfigError)


def hash_bytes(data: bytes) -> str:
    if not isinstance(data, bytes):
        raise ErasureCodingError(f"data debe ser bytes; recibido {type(data).__name__}")
    return blake3_hex_digest(data)


@dataclass(frozen=True, slots=True)
class ErasureSpec:
    data_shards: int
    parity_shards: int
    codec: str = "zfec"

    def __post_init__(self) -> None:
        data_shards = require_config_positive_int("ec.data_shards", self.data_shards)
        parity_shards = require_config_non_negative_int("ec.parity_shards", self.parity_shards)
        total_shards = data_shards + parity_shards

        if total_shards > 256:
            raise ErasureCodingConfigError("ec.total_shards debe ser <= 256 para zfec")
        if self.codec != "zfec":
            raise ErasureCodingConfigError(f"codec EC no soportado: {self.codec!r}")

        object.__setattr__(self, "data_shards", data_shards)
        object.__setattr__(self, "parity_shards", parity_shards)

    @property
    def total_shards(self) -> int:
        return self.data_shards + self.parity_shards

    @property
    def recoverable_missing_shards(self) -> int:
        return self.parity_shards


@dataclass(frozen=True, slots=True)
class DataPackEntry:
    chunk_hash: str
    offset: int
    length: int
    ordinal: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "chunk_hash", require_hash64("chunk_hash", self.chunk_hash))
        object.__setattr__(self, "offset", require_non_negative_int("offset", self.offset))
        object.__setattr__(self, "length", require_positive_int("length", self.length))
        object.__setattr__(self, "ordinal", require_non_negative_int("ordinal", self.ordinal))


@dataclass(frozen=True, slots=True)
class DataPackShard:
    pack_hash: str
    shard_index: int
    data: bytes
    shard_hash: str

    def __post_init__(self) -> None:
        pack_hash = require_hash64("pack_hash", self.pack_hash)
        shard_index = require_non_negative_int("shard_index", self.shard_index)
        if not isinstance(self.data, bytes):
            raise ErasureCodingError(f"shard.data debe ser bytes; recibido {type(self.data).__name__}")

        shard_hash = require_hash64("shard_hash", self.shard_hash)
        calculated = hash_bytes(self.data)
        if calculated != shard_hash:
            raise ErasureCodingError(
                f"shard_hash no coincide: esperado={shard_hash} calculado={calculated}"
            )

        object.__setattr__(self, "pack_hash", pack_hash)
        object.__setattr__(self, "shard_index", shard_index)

    @classmethod
    def _from_prehashed(
        cls,
        *,
        pack_hash: str,
        shard_index: int,
        data: bytes,
        shard_hash: str,
    ) -> "DataPackShard":
        """Construye un shard cuyo hash acaba de calcular el codec local."""
        pack_hash = require_hash64("pack_hash", pack_hash)
        shard_index = require_non_negative_int("shard_index", shard_index)
        if not isinstance(data, bytes):
            raise ErasureCodingError(
                f"shard.data debe ser bytes; recibido {type(data).__name__}"
            )
        shard_hash = require_hash64("shard_hash", shard_hash)

        shard = object.__new__(cls)
        object.__setattr__(shard, "pack_hash", pack_hash)
        object.__setattr__(shard, "shard_index", shard_index)
        object.__setattr__(shard, "data", data)
        object.__setattr__(shard, "shard_hash", shard_hash)
        return shard


@dataclass(frozen=True, slots=True)
class EncodedDataPack:
    pack_hash: str
    payload_size: int
    padded_size: int
    shard_size: int
    spec: ErasureSpec
    entries: tuple[DataPackEntry, ...]
    shards: tuple[DataPackShard, ...]

    def __post_init__(self) -> None:
        pack_hash = require_hash64("pack_hash", self.pack_hash)
        payload_size = require_non_negative_int("payload_size", self.payload_size)
        padded_size = require_non_negative_int("padded_size", self.padded_size)
        shard_size = require_non_negative_int("shard_size", self.shard_size)

        if not isinstance(self.spec, ErasureSpec):
            raise ErasureCodingError("spec debe ser ErasureSpec")
        if padded_size != shard_size * self.spec.data_shards:
            raise ErasureCodingError("padded_size debe coincidir con shard_size * data_shards")
        if payload_size > padded_size:
            raise ErasureCodingError("payload_size no puede ser mayor que padded_size")
        if len(self.shards) != self.spec.total_shards:
            raise ErasureCodingError("el número de shards no coincide con la especificación EC")

        entries = tuple(self.entries)
        previous_end = 0
        for ordinal, entry in enumerate(entries):
            if not isinstance(entry, DataPackEntry):
                raise ErasureCodingError("entries debe contener DataPackEntry")
            if entry.ordinal != ordinal:
                raise ErasureCodingError("entries debe estar ordenado por ordinal consecutivo")
            if entry.offset != previous_end:
                raise ErasureCodingError("entries debe cubrir el payload de forma contigua")
            previous_end = entry.offset + entry.length

        if previous_end != payload_size:
            raise ErasureCodingError("entries no cubre exactamente payload_size")

        seen_indexes: set[int] = set()
        for shard in self.shards:
            if not isinstance(shard, DataPackShard):
                raise ErasureCodingError("shards debe contener DataPackShard")
            if shard.pack_hash != pack_hash:
                raise ErasureCodingError("shard.pack_hash debe coincidir con pack_hash")
            if shard.shard_index >= self.spec.total_shards:
                raise ErasureCodingError("shard_index fuera de rango")
            if shard.shard_index in seen_indexes:
                raise ErasureCodingError("shard_index duplicado")
            if len(shard.data) != shard_size:
                raise ErasureCodingError("todos los shards deben tener shard_size bytes")
            seen_indexes.add(shard.shard_index)

        object.__setattr__(self, "pack_hash", pack_hash)
        object.__setattr__(self, "payload_size", payload_size)
        object.__setattr__(self, "padded_size", padded_size)
        object.__setattr__(self, "shard_size", shard_size)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(
            self,
            "shards",
            tuple(sorted(self.shards, key=lambda item: item.shard_index)),
        )

    @property
    def entry_by_chunk_hash(self) -> Mapping[str, DataPackEntry]:
        return MappingProxyType({entry.chunk_hash: entry for entry in self.entries})
