"""Handlers RPC para shards EC dentro del servicio P2PStorage."""

from __future__ import annotations

import grpc

from stopan.common.hashes import is_valid_blake3_hex
from stopan.protos import p2p_storage_pb2

from stopan.node.storage.ec_shard_store import (
    DataPackShardHashMismatchError,
    DataPackShardStore,
    DataPackShardStoreError,
    DataPackShardTooLargeError,
    StoredDataPackShard,
)


_INVALID_HASH_PREVIEW_CHARS = 32


def _invalid_pack_hash_detail(name: str, value: object) -> str:
    visible = str(value)[:_INVALID_HASH_PREVIEW_CHARS]
    return f"{name} inválido: {visible!r}; se esperaba BLAKE3 hex lowercase de 64 caracteres"


class DataPackShardRpcHandler:
    """Operaciones RPC de almacenamiento y lectura de shards EC."""

    def __init__(self, shard_store: DataPackShardStore):
        self.shard_store = shard_store

    def probe_missing(self, request, context):
        """
        Devuelve qué shards EC no existen localmente.

        Este método solo comprueba presencia por identificador canónico. La
        integridad se valida al aceptar y al leer cada shard mediante BLAKE3.
        """
        try:
            invalid_detail = _first_invalid_shard_ref_detail(request.shards)
            if invalid_detail is not None:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(invalid_detail)
                return p2p_storage_pb2.ProbeMissingDataPackShardsResponse()

            missing = [
                shard
                for shard in request.shards
                if not self.shard_store.exists(
                    pack_hash=shard.pack_hash,
                    shard_index=int(shard.shard_index),
                    shard_hash=shard.shard_hash,
                )
            ]
            return p2p_storage_pb2.ProbeMissingDataPackShardsResponse(
                missing_shards=missing,
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.ProbeMissingDataPackShardsResponse()

    def replicate(self, request_iterator, context):
        """
        Recibe shards EC por stream y devuelve un ACK por shard.

        Los shards no pasan por el CAS de chunks ni por StorageCommitEngine:
        tienen formato, validación y ruta de almacenamiento propios.
        """
        try:
            for item in request_iterator:
                yield self._store_data_pack_shard(item)
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return

    def retrieve_batch(self, request, context):
        """Devuelve shards EC por batch para reconstrucción de data packs."""
        try:
            results = []

            for shard in request.shards:
                detail = _invalid_shard_ref_detail(shard)
                if detail is not None:
                    results.append(
                        _retrieved_shard_from_ref(
                            shard,
                            status=p2p_storage_pb2.DATA_PACK_SHARD_RETRIEVE_STATUS_ERROR,
                            detail=detail,
                        )
                    )
                    continue

                try:
                    stored = self.shard_store.get(
                        pack_hash=shard.pack_hash,
                        shard_index=int(shard.shard_index),
                        shard_hash=shard.shard_hash,
                    )
                    results.append(_retrieved_stored_shard(stored))
                except FileNotFoundError:
                    results.append(
                        _retrieved_shard_from_ref(
                            shard,
                            status=p2p_storage_pb2.DATA_PACK_SHARD_RETRIEVE_STATUS_NOT_FOUND,
                            detail="shard EC no encontrado",
                        )
                    )
                except DataPackShardHashMismatchError as exc:
                    results.append(
                        _retrieved_shard_from_ref(
                            shard,
                            status=p2p_storage_pb2.DATA_PACK_SHARD_RETRIEVE_STATUS_HASH_MISMATCH,
                            detail=str(exc),
                        )
                    )
                except Exception as exc:
                    results.append(
                        _retrieved_shard_from_ref(
                            shard,
                            status=p2p_storage_pb2.DATA_PACK_SHARD_RETRIEVE_STATUS_ERROR,
                            detail=str(exc),
                        )
                    )

            return p2p_storage_pb2.RetrieveDataPackShardBatchResponse(results=results)
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.RetrieveDataPackShardBatchResponse()

    def _store_data_pack_shard(self, item):
        detail = _invalid_shard_request_detail(item)
        if detail is not None:
            return _replicated_shard_result_from_item(
                item,
                status=p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                detail=detail,
            )

        try:
            stored = self.shard_store.put(
                pack_hash=item.pack_hash,
                shard_index=int(item.shard_index),
                shard_hash=item.shard_hash,
                data=bytes(item.shard_data),
            )
            status = (
                p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_STORED
                if stored
                else p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_ALREADY_PRESENT
            )
            detail = "almacenado" if stored else "ya presente"
        except DataPackShardHashMismatchError as exc:
            status = p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_REJECTED_HASH_MISMATCH
            detail = str(exc)
        except DataPackShardTooLargeError as exc:
            status = p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_REJECTED_TOO_LARGE
            detail = str(exc)
        except DataPackShardStoreError as exc:
            status = p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_REJECTED_INVALID_ARGUMENT
            detail = str(exc)
        except Exception as exc:
            status = p2p_storage_pb2.DATA_PACK_SHARD_STORE_STATUS_ERROR
            detail = str(exc)

        return _replicated_shard_result_from_item(item, status=status, detail=detail)


def _retrieved_stored_shard(stored: StoredDataPackShard):
    return p2p_storage_pb2.RetrievedDataPackShard(
        pack_hash=stored.pack_hash,
        shard_index=stored.shard_index,
        shard_hash=stored.shard_hash,
        status=p2p_storage_pb2.DATA_PACK_SHARD_RETRIEVE_STATUS_FOUND,
        shard_data=stored.data,
        detail="ok",
    )


def _retrieved_shard_from_ref(shard, *, status: int, detail: str):
    return p2p_storage_pb2.RetrievedDataPackShard(
        pack_hash=str(shard.pack_hash),
        shard_index=int(shard.shard_index),
        shard_hash=str(shard.shard_hash),
        status=status,
        detail=detail,
    )


def _replicated_shard_result_from_item(item, *, status: int, detail: str):
    return p2p_storage_pb2.ReplicateDataPackShardResult(
        pack_hash=str(item.pack_hash),
        shard_index=int(item.shard_index),
        shard_hash=str(item.shard_hash),
        status=status,
        detail=detail,
    )


def _first_invalid_shard_ref_detail(shards) -> str | None:
    for shard in shards:
        detail = _invalid_shard_ref_detail(shard)
        if detail is not None:
            return detail
    return None


def _invalid_shard_ref_detail(shard) -> str | None:
    if not is_valid_blake3_hex(shard.pack_hash):
        return _invalid_pack_hash_detail("pack_hash", shard.pack_hash)
    if not is_valid_blake3_hex(shard.shard_hash):
        return _invalid_pack_hash_detail("shard_hash", shard.shard_hash)
    if int(shard.shard_index) < 0:
        return "shard_index debe ser >= 0"
    return None


def _invalid_shard_request_detail(item) -> str | None:
    detail = _invalid_shard_ref_detail(item)
    if detail is not None:
        return detail
    if not item.shard_data:
        return "shard_data no puede estar vacío"
    return None
