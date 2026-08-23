from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import time

from stopan.metadata.database import ErasureDataPackPushUpdate, MetadataDB
from stopan.protection.concurrency import iter_completed_keyed_tasks
from stopan.protection.ec.models import (
    EncodedDataPack,
    ErasureCodingConfigError,
)
from stopan.protection.ec.placement import DataPackShardPlacement
from stopan.protection.ec.remote_client import (
    RemoteDataPackShardClientPool,
    RemoteDataPackShardPayload,
    RemoteDataPackShardRef,
    RemoteDataPackShardReplicationError,
    RemoteDataPackShardStoreResult,
    ensure_data_pack_shard_fits_transport,
)
from stopan.rpc.errors import format_remote_error
from stopan.protection.policy import ProtectionState


@dataclass(frozen=True)
class DataPackShardPushResult:
    successful_indexes: frozenset[int]
    stored_shards: int = 0
    already_present_shards: int = 0
    failed_results: tuple[tuple[str, RemoteDataPackShardStoreResult], ...] = ()

    @property
    def failed_shards(self) -> int:
        return len(self.failed_results)


@dataclass(frozen=True)
class _ShardTargetBatch:
    address: str
    payloads: tuple[RemoteDataPackShardPayload, ...]


@dataclass(frozen=True)
class _DataPackMetadataRows:
    chunks: list[tuple[str, int, int, int]]
    shards: list[tuple[int, str, str, int]]


def push_data_pack_shards(
    *,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
    pool: RemoteDataPackShardClientPool,
    target_parallelism: int,
) -> DataPackShardPushResult:
    successful_indexes: set[int] = set()
    stored_shards = 0
    already_present_shards = 0
    failed_results: list[tuple[str, RemoteDataPackShardStoreResult]] = []

    batches = _target_batches(pack=pack, placements=placements)
    tasks = {
        batch.address: (
            lambda batch=batch: _push_target_batch(pool=pool, batch=batch)
        )
        for batch in batches
    }
    batch_by_address = {batch.address: batch for batch in batches}

    for completed in iter_completed_keyed_tasks(
        tasks=tasks,
        max_workers=target_parallelism,
        thread_name_prefix="ec-target",
    ):
        if completed.error is not None:
            batch = batch_by_address[completed.key]
            result = DataPackShardPushResult(
                successful_indexes=frozenset(),
                failed_results=tuple(
                    _failed_results_for_refs(
                        address=batch.address,
                        refs=[item.ref for item in batch.payloads],
                        status=pool.store_status_error,
                        detail=(
                            "ejecución del target falló: "
                            f"{format_remote_error(completed.error)}"
                        ),
                    )
                ),
            )
        else:
            result = completed.result

        successful_indexes.update(result.successful_indexes)
        stored_shards += result.stored_shards
        already_present_shards += result.already_present_shards
        failed_results.extend(result.failed_results)

    return DataPackShardPushResult(
        successful_indexes=frozenset(successful_indexes),
        stored_shards=stored_shards,
        already_present_shards=already_present_shards,
        failed_results=tuple(
            sorted(
                failed_results,
                key=lambda item: (
                    item[0],
                    item[1].ref.shard_index,
                    item[1].ref.shard_hash,
                ),
            )
        ),
    )


def validate_data_pack_delivery_limits(
    *,
    pack: EncodedDataPack,
    max_shard_size: int,
    max_message_bytes: int,
) -> None:
    max_shard_size = int(max_shard_size)
    if max_shard_size <= 0:
        raise ErasureCodingConfigError("storage.max_chunk_size debe ser > 0")

    for shard in pack.shards:
        if len(shard.data) > max_shard_size:
            raise ErasureCodingConfigError(
                "shard EC excede el límite de almacenamiento remoto: "
                f"shard_size={len(shard.data)} > max_shard_size={max_shard_size}; "
                f"pack_hash={pack.pack_hash} shard_index={shard.shard_index}"
            )

        ensure_data_pack_shard_fits_transport(
            RemoteDataPackShardPayload(
                ref=RemoteDataPackShardRef(
                    pack_hash=pack.pack_hash,
                    shard_index=shard.shard_index,
                    shard_hash=shard.shard_hash,
                ),
                data=shard.data,
            ),
            max_message_bytes=max_message_bytes,
        )


def _push_target_batch(
    *,
    pool: RemoteDataPackShardClientPool,
    batch: _ShardTargetBatch,
) -> DataPackShardPushResult:
    successful_indexes: set[int] = set()
    stored_shards = 0
    already_present_shards = 0
    failed_results: list[tuple[str, RemoteDataPackShardStoreResult]] = []
    refs = [item.ref for item in batch.payloads]

    try:
        missing_refs = pool.probe_missing_shards(addr=batch.address, refs=refs)
    except Exception as exc:
        return DataPackShardPushResult(
            successful_indexes=frozenset(),
            failed_results=tuple(
                _failed_results_for_refs(
                    address=batch.address,
                    refs=refs,
                    status=pool.store_status_error,
                    detail=f"probe remoto falló: {format_remote_error(exc)}",
                )
            ),
        )

    requested_keys = {ref.identity_key for ref in refs}
    missing_keys = {
        ref.identity_key
        for ref in missing_refs
        if ref.identity_key in requested_keys
    }

    already_present_shards += len(refs) - len(missing_keys)
    for ref in refs:
        if ref.identity_key not in missing_keys:
            successful_indexes.add(ref.shard_index)

    if not missing_keys:
        return DataPackShardPushResult(
            successful_indexes=frozenset(successful_indexes),
            already_present_shards=already_present_shards,
        )

    payloads_to_send = [
        item for item in batch.payloads
        if item.ref.identity_key in missing_keys
    ]
    expected_ack_keys = {item.ref.identity_key for item in payloads_to_send}

    try:
        store_results = pool.replicate_shards(addr=batch.address, shards=payloads_to_send)
        stream_failure_detail = None
    except RemoteDataPackShardReplicationError as exc:
        store_results = list(exc.results)
        stream_failure_detail = str(exc)
    except Exception as exc:
        store_results = []
        stream_failure_detail = f"replicación remota falló: {format_remote_error(exc)}"

    acknowledged_keys: set[tuple[str, int, str]] = set()
    for result in store_results:
        key = result.ref.identity_key
        if key not in expected_ack_keys or key in acknowledged_keys:
            continue
        acknowledged_keys.add(key)
        if result.is_success(pool.store_status_stored, pool.store_status_already_present):
            successful_indexes.add(result.ref.shard_index)
            if result.status == pool.store_status_stored:
                stored_shards += 1
            else:
                already_present_shards += 1
        else:
            failed_results.append((batch.address, result))

    failed_results.extend(
        _missing_ack_results(
            address=batch.address,
            payloads=payloads_to_send,
            acknowledged_keys=acknowledged_keys,
            status=pool.store_status_error,
            detail=stream_failure_detail or "sin ACK remoto para el shard",
        )
    )

    return DataPackShardPushResult(
        successful_indexes=frozenset(successful_indexes),
        stored_shards=stored_shards,
        already_present_shards=already_present_shards,
        failed_results=tuple(failed_results),
    )


def register_data_pack_metadata(
    *,
    db: MetadataDB,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
    protection_state: ProtectionState,
    placement_epoch: str,
) -> None:
    _write_data_pack_push_metadata(
        db=db,
        pack=pack,
        placements=placements,
        protection_state=protection_state,
        placement_epoch=placement_epoch,
        refresh_existing=False,
    )


def refresh_data_pack_push_metadata(
    *,
    db: MetadataDB,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
    protection_state: ProtectionState,
    placement_epoch: str,
) -> None:
    _write_data_pack_push_metadata(
        db=db,
        pack=pack,
        placements=placements,
        protection_state=protection_state,
        placement_epoch=placement_epoch,
        refresh_existing=True,
    )


def build_data_pack_push_update(
    *,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
    protection_state: ProtectionState,
    placement_epoch: str,
) -> ErasureDataPackPushUpdate:
    metadata_rows = _data_pack_metadata_rows(pack=pack, placements=placements)
    return ErasureDataPackPushUpdate(
        pack_hash=pack.pack_hash,
        codec=pack.spec.codec,
        data_shards=pack.spec.data_shards,
        parity_shards=pack.spec.parity_shards,
        payload_size=pack.payload_size,
        padded_size=pack.padded_size,
        shard_size=pack.shard_size,
        chunks=tuple(metadata_rows.chunks),
        shards=tuple(metadata_rows.shards),
        protection_state=protection_state,
        placement_epoch=placement_epoch,
        pushed_at=time.time(),
    )


def _write_data_pack_push_metadata(
    *,
    db: MetadataDB,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
    protection_state: ProtectionState,
    placement_epoch: str,
    refresh_existing: bool,
) -> None:
    metadata_rows = _data_pack_metadata_rows(pack=pack, placements=placements)
    writer = (
        db.refresh_erasure_data_pack_push
        if refresh_existing
        else db.register_erasure_data_pack
    )
    writer(
        pack_hash=pack.pack_hash,
        codec=pack.spec.codec,
        data_shards=pack.spec.data_shards,
        parity_shards=pack.spec.parity_shards,
        payload_size=pack.payload_size,
        padded_size=pack.padded_size,
        shard_size=pack.shard_size,
        chunks=metadata_rows.chunks,
        shards=metadata_rows.shards,
        protection_state=protection_state,
        placement_epoch=placement_epoch,
    )


def _data_pack_metadata_rows(
    *,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
) -> _DataPackMetadataRows:
    placement_by_index = {item.shard_index: item for item in placements}
    return _DataPackMetadataRows(
        chunks=[
            (entry.chunk_hash, entry.offset, entry.length, entry.ordinal)
            for entry in pack.entries
        ],
        shards=[
            (
                shard.shard_index,
                shard.shard_hash,
                placement_by_index[shard.shard_index].node_id,
                len(shard.data),
            )
            for shard in pack.shards
        ],
    )


def _target_batches(
    *,
    pack: EncodedDataPack,
    placements: tuple[DataPackShardPlacement, ...],
) -> tuple[_ShardTargetBatch, ...]:
    placement_by_index = {item.shard_index: item for item in placements}
    payloads_by_address: dict[str, list[RemoteDataPackShardPayload]] = defaultdict(list)

    for shard in pack.shards:
        placement = placement_by_index[shard.shard_index]
        ref = RemoteDataPackShardRef(
            pack_hash=pack.pack_hash,
            shard_index=shard.shard_index,
            shard_hash=shard.shard_hash,
        )
        payloads_by_address[placement.address].append(
            RemoteDataPackShardPayload(ref=ref, data=shard.data)
        )

    return tuple(
        _ShardTargetBatch(address=address, payloads=tuple(payloads))
        for address, payloads in sorted(payloads_by_address.items())
    )



def _failed_results_for_refs(
    *,
    address: str,
    refs: list[RemoteDataPackShardRef],
    status: int,
    detail: str,
) -> list[tuple[str, RemoteDataPackShardStoreResult]]:
    return [
        (
            address,
            RemoteDataPackShardStoreResult(
                ref=ref,
                status=status,
                detail=detail,
            ),
        )
        for ref in refs
    ]


def _missing_ack_results(
    *,
    address: str,
    payloads: list[RemoteDataPackShardPayload],
    acknowledged_keys: set[tuple[str, int, str]],
    status: int,
    detail: str,
) -> list[tuple[str, RemoteDataPackShardStoreResult]]:
    return [
        (
            address,
            RemoteDataPackShardStoreResult(
                ref=item.ref,
                status=status,
                detail=detail,
            ),
        )
        for item in payloads
        if item.ref.identity_key not in acknowledged_keys
    ]

