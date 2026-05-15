"""
Almacén local de shards EC para data packs.

Los shards EC se guardan separados del CAS de chunks. Un shard se identifica por
(pack_hash, shard_index, shard_hash) y se valida con BLAKE3 antes de persistirlo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import blake3

_HASH_ALPHABET = set("0123456789abcdef")
_TMP_SUFFIX = ".tmp"
_SHARD_SUFFIX = ".stec"


class DataPackShardStoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StoredDataPackShard:
    pack_hash: str
    shard_index: int
    shard_hash: str
    data: bytes


def is_valid_hash64(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in _HASH_ALPHABET for char in value)
    )


def hash_bytes(data: bytes) -> str:
    return blake3.blake3(data).hexdigest()


class DataPackShardStore:
    def __init__(self, root_dir: str | Path, *, max_shard_size: int):
        self.root = Path(root_dir).expanduser().resolve() / "ec_shards"
        self.max_shard_size = _require_positive_int("max_shard_size", max_shard_size)
        self.root.mkdir(parents=True, exist_ok=True)

    def exists(self, *, pack_hash: str, shard_index: int, shard_hash: str) -> bool:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        shard_hash = _require_hash64("shard_hash", shard_hash)
        shard_index = _require_non_negative_int("shard_index", shard_index)
        return self._path(pack_hash, shard_index, shard_hash).is_file()

    def put(
        self,
        *,
        pack_hash: str,
        shard_index: int,
        shard_hash: str,
        data: bytes,
    ) -> bool:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        shard_hash = _require_hash64("shard_hash", shard_hash)
        shard_index = _require_non_negative_int("shard_index", shard_index)
        if not isinstance(data, bytes):
            raise DataPackShardStoreError(
                f"shard_data debe ser bytes; recibido {type(data).__name__}"
            )
        if len(data) > self.max_shard_size:
            raise DataPackShardTooLargeError(
                f"shard demasiado grande: {len(data)} > {self.max_shard_size}"
            )

        calculated = hash_bytes(data)
        if calculated != shard_hash:
            raise DataPackShardHashMismatchError(
                f"shard_hash no coincide: esperado={shard_hash} calculado={calculated}"
            )

        final_path = self._path(pack_hash, shard_index, shard_hash)
        if final_path.exists():
            return False

        final_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = final_path.with_name(final_path.name + _TMP_SUFFIX)
        with tmp_path.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return True

    def get(
        self,
        *,
        pack_hash: str,
        shard_index: int,
        shard_hash: str,
    ) -> StoredDataPackShard:
        pack_hash = _require_hash64("pack_hash", pack_hash)
        shard_hash = _require_hash64("shard_hash", shard_hash)
        shard_index = _require_non_negative_int("shard_index", shard_index)
        path = self._path(pack_hash, shard_index, shard_hash)

        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise

        calculated = hash_bytes(data)
        if calculated != shard_hash:
            raise DataPackShardHashMismatchError(
                f"shard_hash no coincide al leer: esperado={shard_hash} calculado={calculated}"
            )

        return StoredDataPackShard(
            pack_hash=pack_hash,
            shard_index=shard_index,
            shard_hash=shard_hash,
            data=data,
        )

    def _path(self, pack_hash: str, shard_index: int, shard_hash: str) -> Path:
        return (
            self.root
            / pack_hash[:2]
            / pack_hash
            / f"{shard_index:03d}-{shard_hash}{_SHARD_SUFFIX}"
        )


class DataPackShardHashMismatchError(DataPackShardStoreError):
    pass


class DataPackShardTooLargeError(DataPackShardStoreError):
    pass


def _require_hash64(name: str, value: object) -> str:
    if not is_valid_hash64(value):
        raise DataPackShardStoreError(
            f"{name} debe tener 64 caracteres hexadecimales lowercase"
        )
    return str(value)


def _require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DataPackShardStoreError(f"{name} debe ser int; recibido {type(value).__name__}")
    if value < 0:
        raise DataPackShardStoreError(f"{name} debe ser >= 0; recibido {value}")
    return value


def _require_positive_int(name: str, value: object) -> int:
    number = _require_non_negative_int(name, value)
    if number <= 0:
        raise DataPackShardStoreError(f"{name} debe ser > 0; recibido {number}")
    return number


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
