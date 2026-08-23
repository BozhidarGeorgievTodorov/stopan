"""
Cliente gRPC para shards EC de data packs.

Este módulo no decide placement ni reconstruye packs. Solo adapta la API P2P de
shards EC a objetos Python y mantiene canales reutilizables por dirección.
"""

from __future__ import annotations

from dataclasses import dataclass

from stopan.common.sequences import ordered_unique_by
from stopan.protection.ec.models import (
    ErasureCodingError,
    ErasureCodingConfigError,
    require_hash64,
    require_non_negative_int,
)
from stopan.protection.remote_client_base import P2PStorageProtectionClient
from stopan.rpc.errors import format_remote_error
from stopan.rpc.p2p_storage_client import run_adaptive_batch_call


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardRef:
    pack_hash: str
    shard_index: int
    shard_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "pack_hash", require_hash64("pack_hash", self.pack_hash))
        object.__setattr__(self, "shard_hash", require_hash64("shard_hash", self.shard_hash))
        object.__setattr__(
            self,
            "shard_index",
            require_non_negative_int("shard_index", self.shard_index),
        )

    @property
    def identity_key(self) -> tuple[str, int, str]:
        return self.pack_hash, self.shard_index, self.shard_hash


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardPayload:
    ref: RemoteDataPackShardRef
    data: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.ref, RemoteDataPackShardRef):
            raise ErasureCodingError("ref debe ser RemoteDataPackShardRef")
        if not isinstance(self.data, bytes):
            raise ErasureCodingError(f"data debe ser bytes; recibido {type(self.data).__name__}")
        if not self.data:
            raise ErasureCodingError("data no puede estar vacío")


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardStoreResult:
    ref: RemoteDataPackShardRef
    status: int
    detail: str

    def is_success(self, stored_status: int, already_present_status: int) -> bool:
        return self.status in {stored_status, already_present_status}


class RemoteDataPackShardReplicationError(ErasureCodingError):
    def __init__(
        self,
        message: str,
        *,
        results: tuple[RemoteDataPackShardStoreResult, ...],
    ):
        super().__init__(message)
        self.results = results


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardRetrieveResult:
    ref: RemoteDataPackShardRef
    status: int
    data: bytes
    detail: str

    def is_found(self, found_status: int) -> bool:
        return self.status == found_status


class RemoteDataPackShardClientPool(P2PStorageProtectionClient):
    def __init__(
        self,
        *,
        cluster_token: str,
        timeout_s: float,
        max_message_bytes: int,
    ):
        self.timeout_s = float(timeout_s)
        super().__init__(
            cluster_token=cluster_token,
            max_message_bytes=max_message_bytes,
            closed_message="RemoteDataPackShardClientPool cerrado",
            closed_error_factory=ErasureCodingError,
        )

    @property
    def store_status_stored(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_STORE_STATUS_STORED

    @property
    def store_status_already_present(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_STORE_STATUS_ALREADY_PRESENT

    @property
    def store_status_error(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_STORE_STATUS_ERROR

    @property
    def retrieve_status_found(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_RETRIEVE_STATUS_FOUND

    @property
    def retrieve_status_not_found(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_RETRIEVE_STATUS_NOT_FOUND

    def probe_missing_shards(
        self,
        *,
        addr: str,
        refs: list[RemoteDataPackShardRef],
    ) -> list[RemoteDataPackShardRef]:
        self._ensure_open()
        self._ensure_runtime()

        ordered_refs = _dedupe_refs(refs)
        if not ordered_refs:
            return []

        stub = self._get_stub(addr)
        response = stub.ProbeMissingDataPackShards(
            self._pb.ProbeMissingDataPackShardsRequest(
                shards=[self._pb_ref(ref) for ref in ordered_refs],
            ),
            timeout=self.timeout_s,
            metadata=self._call_metadata,
        )
        return [
            RemoteDataPackShardRef(
                pack_hash=item.pack_hash,
                shard_index=int(item.shard_index),
                shard_hash=item.shard_hash,
            )
            for item in response.missing_shards
        ]

    def replicate_shards(
        self,
        *,
        addr: str,
        shards: list[RemoteDataPackShardPayload],
    ) -> list[RemoteDataPackShardStoreResult]:
        self._ensure_open()
        self._ensure_runtime()

        ordered = _dedupe_payloads(shards)
        if not ordered:
            return []

        for item in ordered:
            ensure_data_pack_shard_fits_transport(
                item,
                max_message_bytes=self.max_message_bytes,
            )

        stub = self._get_stub(addr)
        response_iter = stub.ReplicateDataPackShards(
            (self._pb_replicate_request(item) for item in ordered),
            timeout=self.timeout_s,
            metadata=self._call_metadata,
        )

        results: list[RemoteDataPackShardStoreResult] = []
        try:
            for item in response_iter:
                results.append(
                    RemoteDataPackShardStoreResult(
                        ref=RemoteDataPackShardRef(
                            pack_hash=item.pack_hash,
                            shard_index=int(item.shard_index),
                            shard_hash=item.shard_hash,
                        ),
                        status=item.status,
                        detail=item.detail or "",
                    )
                )
        except Exception as exc:
            raise RemoteDataPackShardReplicationError(
                f"ReplicateDataPackShards falló: {format_remote_error(exc)}",
                results=tuple(results),
            ) from exc

        return results

    def retrieve_shard_batch(
        self,
        *,
        addr: str,
        refs: list[RemoteDataPackShardRef],
    ) -> dict[tuple[str, int, str], RemoteDataPackShardRetrieveResult]:
        self._ensure_open()
        self._ensure_runtime()

        ordered_refs = _dedupe_refs(refs)
        if not ordered_refs:
            return {}

        return run_adaptive_batch_call(
            items=ordered_refs,
            call_once=lambda batch: self._retrieve_shard_batch_once(addr=addr, refs=batch),
        )

    def _retrieve_shard_batch_once(
        self,
        *,
        addr: str,
        refs: list[RemoteDataPackShardRef],
    ) -> dict[tuple[str, int, str], RemoteDataPackShardRetrieveResult]:
        stub = self._get_stub(addr)
        response = stub.RetrieveDataPackShardBatch(
            self._pb.RetrieveDataPackShardBatchRequest(
                shards=[self._pb_ref(ref) for ref in refs],
            ),
            timeout=self.timeout_s,
            metadata=self._call_metadata,
        )

        results: dict[tuple[str, int, str], RemoteDataPackShardRetrieveResult] = {}
        for item in response.results:
            if not item.pack_hash or not item.shard_hash:
                continue
            ref = RemoteDataPackShardRef(
                pack_hash=item.pack_hash,
                shard_index=int(item.shard_index),
                shard_hash=item.shard_hash,
            )
            results[ref.identity_key] = RemoteDataPackShardRetrieveResult(
                ref=ref,
                status=item.status,
                data=bytes(item.shard_data),
                detail=item.detail or "",
            )

        for ref in refs:
            key = ref.identity_key
            if key not in results:
                results[key] = RemoteDataPackShardRetrieveResult(
                    ref=ref,
                    status=self._pb.DATA_PACK_SHARD_RETRIEVE_STATUS_ERROR,
                    data=b"",
                    detail="Sin resultado en RetrieveDataPackShardBatch",
                )

        return results

    def _pb_ref(self, ref: RemoteDataPackShardRef):
        return self._pb.DataPackShardRef(
            pack_hash=ref.pack_hash,
            shard_index=ref.shard_index,
            shard_hash=ref.shard_hash,
        )

    def _pb_replicate_request(self, item: RemoteDataPackShardPayload):
        return self._pb.ReplicateDataPackShardRequest(
            pack_hash=item.ref.pack_hash,
            shard_index=item.ref.shard_index,
            shard_hash=item.ref.shard_hash,
            shard_data=item.data,
        )


def _dedupe_refs(refs: list[RemoteDataPackShardRef]) -> list[RemoteDataPackShardRef]:
    for ref in refs:
        if not isinstance(ref, RemoteDataPackShardRef):
            raise ErasureCodingError("refs debe contener RemoteDataPackShardRef")
    return ordered_unique_by(refs, key=lambda ref: ref.identity_key)


def _dedupe_payloads(
    shards: list[RemoteDataPackShardPayload],
) -> list[RemoteDataPackShardPayload]:
    ordered: dict[tuple[str, int, str], RemoteDataPackShardPayload] = {}
    for shard in shards:
        if not isinstance(shard, RemoteDataPackShardPayload):
            raise ErasureCodingError("shards debe contener RemoteDataPackShardPayload")
        key = shard.ref.identity_key
        existing = ordered.get(key)
        if existing is not None and existing.data != shard.data:
            raise ErasureCodingError("payload duplicado con datos distintos")
        ordered.setdefault(key, shard)
    return list(ordered.values())


def replicate_data_pack_shard_request_size_bytes(
    item: RemoteDataPackShardPayload,
) -> int:
    """Devuelve el tamaño protobuf del mensaje ReplicateDataPackShardRequest."""
    if not isinstance(item, RemoteDataPackShardPayload):
        raise ErasureCodingError("item debe ser RemoteDataPackShardPayload")

    # Campos 1 y 3 son hashes ASCII de 64 bytes. Sus tags y longitudes ocupan
    # un byte cada uno. shard_index se omite en proto3 cuando vale 0.
    size = (1 + 1 + 64) + (1 + 1 + 64)
    if item.ref.shard_index != 0:
        size += 1 + _protobuf_varint_size(item.ref.shard_index)

    data_size = len(item.data)
    size += 1 + _protobuf_varint_size(data_size) + data_size
    return size


def ensure_replicate_data_pack_shard_fits_message(
    item: RemoteDataPackShardPayload,
    *,
    max_message_bytes: int,
) -> None:
    max_message_bytes = int(max_message_bytes)
    if max_message_bytes <= 0:
        raise ErasureCodingConfigError("grpc.max_message_bytes debe ser > 0")

    request_size = replicate_data_pack_shard_request_size_bytes(item)
    if request_size > max_message_bytes:
        raise ErasureCodingConfigError(
            "shard EC demasiado grande para un mensaje gRPC: "
            f"request_size={request_size} > max_message_bytes={max_message_bytes}; "
            f"shard_size={len(item.data)} shard_index={item.ref.shard_index}"
        )


def retrieve_data_pack_shard_response_size_bytes(
    item: RemoteDataPackShardPayload,
) -> int:
    """Tamaño protobuf de una respuesta batch que contiene únicamente este shard."""
    if not isinstance(item, RemoteDataPackShardPayload):
        raise ErasureCodingError("item debe ser RemoteDataPackShardPayload")

    # RetrievedDataPackShard: hashes, índice, status=FOUND, shard_data y detail="ok".
    inner_size = (1 + 1 + 64) + (1 + 1 + 64)
    if item.ref.shard_index != 0:
        inner_size += 1 + _protobuf_varint_size(item.ref.shard_index)
    inner_size += 1 + 1  # status enum FOUND=1
    inner_size += 1 + _protobuf_varint_size(len(item.data)) + len(item.data)
    inner_size += 1 + 1 + 2  # detail="ok"

    # RetrieveDataPackShardBatchResponse.results es un campo repeated message.
    return 1 + _protobuf_varint_size(inner_size) + inner_size


def ensure_data_pack_shard_fits_transport(
    item: RemoteDataPackShardPayload,
    *,
    max_message_bytes: int,
) -> None:
    ensure_replicate_data_pack_shard_fits_message(
        item,
        max_message_bytes=max_message_bytes,
    )

    response_size = retrieve_data_pack_shard_response_size_bytes(item)
    if response_size > int(max_message_bytes):
        raise ErasureCodingConfigError(
            "shard EC demasiado grande para recuperarse en un mensaje gRPC: "
            f"response_size={response_size} > max_message_bytes={int(max_message_bytes)}; "
            f"shard_size={len(item.data)} shard_index={item.ref.shard_index}"
        )


def _protobuf_varint_size(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ErasureCodingError("varint protobuf requiere entero >= 0")
    size = 1
    while value >= 0x80:
        value >>= 7
        size += 1
    return size
