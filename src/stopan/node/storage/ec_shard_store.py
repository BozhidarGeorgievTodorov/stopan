"""
Almacén local de shards EC para data packs.

Los shards EC se guardan separados del CAS de chunks. Un shard se identifica por
(pack_hash, shard_index, shard_hash) y se valida con BLAKE3 antes de persistirlo.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from stopan.common.fs import atomic_write_bytes
from stopan.common.hashes import blake3_hex_digest, is_valid_blake3_hex
from stopan.common.validators import (
    require_strict_non_negative_int,
    require_strict_positive_int,
)
from stopan.errors import StopanDataError, StopanStorageError

_SHARD_SUFFIX = ".stec"


class DataPackShardStoreError(StopanStorageError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StoredDataPackShard:
    pack_hash: str
    shard_index: int
    shard_hash: str
    data: bytes



class DataPackShardStore:
    def __init__(self, root_dir: str | Path, *, max_shard_size: int):
        self.root = Path(root_dir).expanduser().resolve()
        self.max_shard_size = _require_positive_int("max_shard_size", max_shard_size)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise DataPackShardStoreError(f"No se pudo preparar el almacén EC {self.root}: {exc}") from exc

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

        calculated = blake3_hex_digest(data)
        if calculated != shard_hash:
            raise DataPackShardHashMismatchError(
                f"shard_hash no coincide: esperado={shard_hash} calculado={calculated}"
            )

        final_path = self._path(pack_hash, shard_index, shard_hash)
        if final_path.exists():
            return False

        try:
            atomic_write_bytes(final_path, data, mode=0o600)
        except StopanStorageError as exc:
            raise DataPackShardStoreError(f"No se pudo escribir shard EC {final_path}: {exc}") from exc
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
        except OSError as exc:
            raise DataPackShardStoreError(f"No se pudo leer shard EC {path}: {exc}") from exc

        calculated = blake3_hex_digest(data)
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


class DataPackShardHashMismatchError(StopanDataError, DataPackShardStoreError):
    pass


class DataPackShardTooLargeError(DataPackShardStoreError):
    pass


def _require_hash64(name: str, value: object) -> str:
    if not is_valid_blake3_hex(value):
        raise DataPackShardStoreError(
            f"{name} debe tener 64 caracteres hexadecimales lowercase"
        )
    return str(value)


def _require_non_negative_int(name: str, value: object) -> int:
    return require_strict_non_negative_int(name, value, error_factory=DataPackShardStoreError)


def _require_positive_int(name: str, value: object) -> int:
    return require_strict_positive_int(name, value, error_factory=DataPackShardStoreError)

