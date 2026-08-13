"""
Adaptador de zfec para protection packs.

zfec trabaja con K bloques primarios y un total N de bloques. El adaptador
oculta esa convención y expone la semántica del proyecto: K data shards y M
shards de paridad.
"""

from __future__ import annotations

from collections.abc import Iterable

from .models import (
    DataPackShard,
    ErasureCodingDependencyError,
    ErasureCodingError,
    ErasureSpec,
    hash_bytes,
)


class ZfecErasureCodec:
    def encode(
        self,
        *,
        pack_hash: str,
        payload: bytes,
        spec: ErasureSpec,
    ) -> tuple[DataPackShard, ...]:
        if not isinstance(payload, bytes):
            raise ErasureCodingError(f"payload debe ser bytes; recibido {type(payload).__name__}")
        if not isinstance(spec, ErasureSpec):
            raise ErasureCodingError("spec debe ser ErasureSpec")

        zfec = _import_zfec()
        primary_blocks, shard_size = split_primary_blocks(payload, spec.data_shards)
        wanted_indexes = list(range(spec.total_shards))
        raw_shards = zfec.Encoder(spec.data_shards, spec.total_shards).encode(
            primary_blocks,
            wanted_indexes,
        )

        shards = []
        for shard_index, raw in zip(wanted_indexes, raw_shards, strict=True):
            data = bytes(raw)
            if len(data) != shard_size:
                raise ErasureCodingError("zfec devolvió un shard con tamaño inesperado")
            shard_hash = hash_bytes(data)
            shards.append(
                DataPackShard._from_prehashed(
                    pack_hash=pack_hash,
                    shard_index=shard_index,
                    data=data,
                    shard_hash=shard_hash,
                )
            )

        return tuple(shards)

    def decode(
        self,
        *,
        shards: Iterable[DataPackShard],
        spec: ErasureSpec,
        payload_size: int,
        expected_pack_hash: str,
    ) -> bytes:
        if not isinstance(spec, ErasureSpec):
            raise ErasureCodingError("spec debe ser ErasureSpec")

        selected = tuple(sorted(shards, key=lambda item: item.shard_index))[: spec.data_shards]
        if len(selected) < spec.data_shards:
            raise ErasureCodingError(
                f"se necesitan {spec.data_shards} shards para reconstruir el pack"
            )

        seen: set[int] = set()
        shard_payloads: list[bytes] = []
        shard_indexes: list[int] = []

        for shard in selected:
            if not isinstance(shard, DataPackShard):
                raise ErasureCodingError("shards debe contener DataPackShard")
            if shard.pack_hash != expected_pack_hash:
                raise ErasureCodingError("shard.pack_hash no coincide con el pack solicitado")
            if shard.shard_index >= spec.total_shards:
                raise ErasureCodingError("shard_index fuera de rango")
            if shard.shard_index in seen:
                raise ErasureCodingError("shard_index duplicado")
            seen.add(shard.shard_index)
            shard_indexes.append(shard.shard_index)
            shard_payloads.append(shard.data)

        zfec = _import_zfec()
        primary_blocks = zfec.Decoder(spec.data_shards, spec.total_shards).decode(
            shard_payloads,
            shard_indexes,
        )
        payload = b"".join(bytes(block) for block in primary_blocks)[:payload_size]

        calculated = hash_bytes(payload)
        if calculated != expected_pack_hash:
            raise ErasureCodingError(
                f"pack_hash no coincide tras reconstrucción: "
                f"esperado={expected_pack_hash} calculado={calculated}"
            )

        return payload


def split_primary_blocks(payload: bytes, data_shards: int) -> tuple[list[bytes], int]:
    if not isinstance(payload, bytes):
        raise ErasureCodingError(f"payload debe ser bytes; recibido {type(payload).__name__}")

    data_shards = spec_data_shards(data_shards)
    shard_size = (len(payload) + data_shards - 1) // data_shards
    if shard_size == 0:
        shard_size = 1

    padded_size = shard_size * data_shards
    padded = payload + b"\\x00" * (padded_size - len(payload))
    blocks = [
        padded[index * shard_size : (index + 1) * shard_size]
        for index in range(data_shards)
    ]
    return blocks, shard_size


def spec_data_shards(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ErasureCodingError(f"data_shards debe ser int; recibido {type(value).__name__}")
    if value <= 0:
        raise ErasureCodingError(f"data_shards debe ser > 0; recibido {value}")
    return value


def _import_zfec():
    try:
        import zfec
    except ImportError as exc:
        raise ErasureCodingDependencyError(
            "zfec no está instalado; añade zfec a requirements.txt para usar erasure coding"
        ) from exc
    return zfec
