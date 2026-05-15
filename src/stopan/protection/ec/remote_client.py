"""
Cliente gRPC para shards EC de data packs.

Este módulo no decide placement ni reconstruye packs. Solo adapta la API P2P de
shards EC a objetos Python y mantiene canales reutilizables por dirección.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from stopan.rpc.errors import is_message_too_large_error
from stopan.rpc.options import grpc_channel_options

from .models import ErasureCodingError, require_hash64


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardRef:
    pack_hash: str
    shard_index: int
    shard_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "pack_hash", require_hash64("pack_hash", self.pack_hash))
        object.__setattr__(self, "shard_hash", require_hash64("shard_hash", self.shard_hash))
        object.__setattr__(self, "shard_index", _require_non_negative_int("shard_index", self.shard_index))


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


@dataclass(frozen=True, slots=True)
class RemoteDataPackShardRetrieveResult:
    ref: RemoteDataPackShardRef
    status: int
    data: bytes
    detail: str

    def is_found(self, found_status: int) -> bool:
        return self.status == found_status


class RemoteDataPackShardClientPool:
    def __init__(self, *, timeout_s: float, max_message_bytes: int):
        self.timeout_s = float(timeout_s)
        self.max_message_bytes = max(int(max_message_bytes), 1)
        self._channels = {}
        self._stubs = {}
        self._grpc = None
        self._pb = None
        self._pb_grpc = None
        self._lock = threading.Lock()
        self._closed = threading.Event()

    def _ensure_open(self) -> None:
        if self._closed.is_set():
            raise ErasureCodingError("RemoteDataPackShardClientPool cerrado")

    def _ensure_runtime(self) -> None:
        if self._grpc is not None:
            return

        import grpc
        from stopan.protos import p2p_storage_pb2
        from stopan.protos import p2p_storage_pb2_grpc

        self._grpc = grpc
        self._pb = p2p_storage_pb2
        self._pb_grpc = p2p_storage_pb2_grpc

    def _get_stub(self, addr: str):
        self._ensure_runtime()

        with self._lock:
            self._ensure_open()
            if addr not in self._stubs:
                channel = self._grpc.insecure_channel(
                    addr,
                    options=grpc_channel_options(self.max_message_bytes),
                )
                self._channels[addr] = channel
                self._stubs[addr] = self._pb_grpc.P2PStorageStub(channel)

            return self._stubs[addr]

    @property
    def store_status_stored(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_STORE_STATUS_STORED

    @property
    def store_status_already_present(self) -> int:
        self._ensure_runtime()
        return self._pb.DATA_PACK_SHARD_STORE_STATUS_ALREADY_PRESENT

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

        stub = self._get_stub(addr)
        response_iter = stub.ReplicateDataPackShards(
            (self._pb_replicate_request(item) for item in ordered),
            timeout=self.timeout_s,
        )

        return [
            RemoteDataPackShardStoreResult(
                ref=RemoteDataPackShardRef(
                    pack_hash=item.pack_hash,
                    shard_index=int(item.shard_index),
                    shard_hash=item.shard_hash,
                ),
                status=item.status,
                detail=item.detail or "",
            )
            for item in response_iter
        ]

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

        return self._retrieve_shard_batch_adaptive(addr=addr, refs=ordered_refs)

    def _retrieve_shard_batch_adaptive(
        self,
        *,
        addr: str,
        refs: list[RemoteDataPackShardRef],
    ) -> dict[tuple[str, int, str], RemoteDataPackShardRetrieveResult]:
        try:
            return self._retrieve_shard_batch_once(addr=addr, refs=refs)
        except Exception as exc:
            if len(refs) > 1 and is_message_too_large_error(exc):
                mid = max(1, len(refs) // 2)
                left = self._retrieve_shard_batch_adaptive(addr=addr, refs=refs[:mid])
                right = self._retrieve_shard_batch_adaptive(addr=addr, refs=refs[mid:])
                left.update(right)
                return left

            raise

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
            results[_ref_key(ref)] = RemoteDataPackShardRetrieveResult(
                ref=ref,
                status=item.status,
                data=bytes(item.shard_data),
                detail=item.detail or "",
            )

        for ref in refs:
            key = _ref_key(ref)
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

    def close(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return

            self._closed.set()

            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()


def _dedupe_refs(refs: list[RemoteDataPackShardRef]) -> list[RemoteDataPackShardRef]:
    ordered: dict[tuple[str, int, str], RemoteDataPackShardRef] = {}
    for ref in refs:
        if not isinstance(ref, RemoteDataPackShardRef):
            raise ErasureCodingError("refs debe contener RemoteDataPackShardRef")
        ordered.setdefault(_ref_key(ref), ref)
    return list(ordered.values())


def _dedupe_payloads(
    shards: list[RemoteDataPackShardPayload],
) -> list[RemoteDataPackShardPayload]:
    ordered: dict[tuple[str, int, str], RemoteDataPackShardPayload] = {}
    for shard in shards:
        if not isinstance(shard, RemoteDataPackShardPayload):
            raise ErasureCodingError("shards debe contener RemoteDataPackShardPayload")
        key = _ref_key(shard.ref)
        existing = ordered.get(key)
        if existing is not None and existing.data != shard.data:
            raise ErasureCodingError("payload duplicado con datos distintos")
        ordered.setdefault(key, shard)
    return list(ordered.values())


def _ref_key(ref: RemoteDataPackShardRef) -> tuple[str, int, str]:
    return ref.pack_hash, ref.shard_index, ref.shard_hash


def _require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ErasureCodingError(f"{name} debe ser int; recibido {type(value).__name__}")
    if value < 0:
        raise ErasureCodingError(f"{name} debe ser >= 0; recibido {value}")
    return value
