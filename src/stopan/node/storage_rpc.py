"""
Servicer gRPC de almacenamiento P2P.

Expone operaciones de presencia, replicación y recuperación de chunks:
ProbeMissingChunks, ReplicateChunks y RetrieveChunkBatch.
"""

from __future__ import annotations

import os
import queue
import threading
from concurrent import futures

import grpc
from stopan.protos import p2p_storage_pb2
from stopan.protos import p2p_storage_pb2_grpc

from stopan.cas.hashes import is_valid_chunk_hash
from stopan.cas.repository import CASRepository
from .commit_engine import StorageCommitEngine
from .ec_shard_store import (
    DataPackShardHashMismatchError,
    DataPackShardStore,
    DataPackShardStoreError,
    DataPackShardTooLargeError,
    is_valid_hash64,
)


_INVALID_HASH_PREVIEW_CHARS = 32
_STREAM_QUEUE_POLL_TIMEOUT_S = 0.05
_STREAM_READER_JOIN_TIMEOUT_S = 2.0


def _invalid_chunk_hash_detail(chunk_hash: str) -> str:
    visible = str(chunk_hash)[:_INVALID_HASH_PREVIEW_CHARS]
    return f"chunk_hash inválido: {visible!r}; se esperaba BLAKE3 hex lowercase de 64 caracteres"


def _invalid_pack_hash_detail(name: str, value: object) -> str:
    visible = str(value)[:_INVALID_HASH_PREVIEW_CHARS]
    return f"{name} inválido: {visible!r}; se esperaba BLAKE3 hex lowercase de 64 caracteres"


class StorageNodeServicer(p2p_storage_pb2_grpc.P2PStorageServicer):
    def __init__(
        self,
        repo_store_dir: str,
        *,
        commit_workers: int,
        commit_queue_items: int,
        max_chunk_size: int,
    ):
        """
        Servidor P2PStorage backed by CAS local.

        Los chunks entrantes se validan mediante StorageCommitEngine. Los chunks
        salientes se devuelven comprimidos y el cliente restore valida integridad.
        """
        self.repo = CASRepository(repo_store_dir)
        self.commit_engine = StorageCommitEngine(
            self.repo,
            max_chunk_size=max_chunk_size,
            worker_count=commit_workers,
            max_pending=commit_queue_items,
        )
        self.ec_shard_store = DataPackShardStore(
            repo_store_dir,
            max_shard_size=max_chunk_size,
        )
        print(f"Nodo P2P listo. Almacenando en: {os.path.abspath(repo_store_dir)}")
        print(
            f"Commit engine: workers={self.commit_engine.worker_count} "
            f"queue={self.commit_engine.max_pending} "
            f"max_chunk_size={self.commit_engine.max_chunk_size}"
        )

    def close(self) -> None:
        self.commit_engine.close()

    def ProbeMissingChunks(self, request, context):
        """
        Devuelve qué chunks no existen localmente en el CAS.

        Nota:
        - este método verifica presencia, no integridad profunda;
        - la integridad se valida al aceptar blobs nuevos en ReplicateChunks
            y al consumir blobs en restore/retrieve mediante BLAKE3.
        """
        try:
            invalid = [
                chunk_hash
                for chunk_hash in request.chunk_hashes
                if chunk_hash and not is_valid_chunk_hash(chunk_hash)
            ]
            if invalid:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(_invalid_chunk_hash_detail(invalid[0]))
                return p2p_storage_pb2.ProbeMissingChunksResponse()

            missing = [
                chunk_hash
                for chunk_hash in request.chunk_hashes
                if chunk_hash and not self.repo.exists_local(chunk_hash)
            ]
            return p2p_storage_pb2.ProbeMissingChunksResponse(missing_hashes=missing)
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.ProbeMissingChunksResponse()

    def ReplicateChunks(self, request_iterator, context):
        """
        Recibe un stream de chunks comprimidos y devuelve un ACK por chunk.

        Propiedades importantes:
          - backpressure explícito mediante commit_engine.max_pending;
          - si el cliente cancela o desaparece, el reader no queda bloqueado
            indefinidamente en submit()/put();
          - no se inventan ACKs: solo se emiten resultados de commits completados;
          - en cancelación se pueden descartar futures pendientes de la cola de
            salida porque el cliente ya no debe contarlos como confirmados.
        """
        future_queue: queue.Queue = queue.Queue(maxsize=max(1, self.commit_engine.max_pending))
        sentinel = object()
        reader_errors: list[Exception] = []
        stop_event = threading.Event()

        try:
            context.add_callback(stop_event.set)
        except Exception:
            pass

        def enqueue(item) -> bool:
            while not stop_event.is_set():
                if not context.is_active():
                    stop_event.set()
                    return False
                try:
                    future_queue.put(item, timeout=_STREAM_QUEUE_POLL_TIMEOUT_S)
                    return True
                except queue.Full:
                    continue
            return False

        def enqueue_sentinel() -> None:
            if stop_event.is_set() or not context.is_active():
                while True:
                    try:
                        future_queue.get_nowait()
                    except queue.Empty:
                        break

                try:
                    future_queue.put_nowait(sentinel)
                except queue.Full:
                    pass
                return

            while True:
                try:
                    future_queue.put(sentinel, timeout=_STREAM_QUEUE_POLL_TIMEOUT_S)
                    return
                except queue.Full:
                    if stop_event.is_set() or not context.is_active():
                        stop_event.set()
                        continue

        def reader() -> None:
            try:
                for item in request_iterator:
                    if stop_event.is_set() or not context.is_active():
                        stop_event.set()
                        break
                    if not item.chunk_hash:
                        continue

                    future = self.commit_engine.submit(
                        item.chunk_hash,
                        item.chunk_data,
                        cancel_event=stop_event,
                    )
                    if not enqueue(future):
                        break
            except Exception as exc:
                if not stop_event.is_set():
                    reader_errors.append(exc)
            finally:
                enqueue_sentinel()

        reader_thread = threading.Thread(target=reader, name="storage-stream-reader", daemon=True)
        reader_thread.start()

        inflight = set()
        reader_finished = False

        try:
            while True:
                while True:
                    try:
                        item = future_queue.get_nowait()
                    except queue.Empty:
                        break

                    if item is sentinel:
                        reader_finished = True
                    else:
                        inflight.add(item)

                if not inflight and reader_finished:
                    break

                if not context.is_active():
                    stop_event.set()
                    break

                if not inflight:
                    try:
                        item = future_queue.get(timeout=_STREAM_QUEUE_POLL_TIMEOUT_S)
                    except queue.Empty:
                        continue

                    if item is sentinel:
                        reader_finished = True
                        continue
                    inflight.add(item)
                    continue

                done, not_done = futures.wait(
                    inflight,
                    timeout=_STREAM_QUEUE_POLL_TIMEOUT_S,
                    return_when=futures.FIRST_COMPLETED,
                )
                inflight = set(not_done)

                for future in done:
                    result = future.result()
                    yield p2p_storage_pb2.ReplicateChunkResult(
                        chunk_hash=result.chunk_hash,
                        status=result.status,
                        detail=result.detail,
                    )

            if reader_errors:
                raise reader_errors[0]

        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return
        finally:
            stop_event.set()
            reader_thread.join(timeout=_STREAM_READER_JOIN_TIMEOUT_S)
            if reader_thread.is_alive():
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("el reader del stream de storage no se detuvo tras la cancelación")

    def RetrieveChunkBatch(self, request, context):
        """
        Devuelve blobs comprimidos del CAS local por batch.

        La validación de integridad extremo-a-extremo la realiza el cliente restore
        al descomprimir y comprobar BLAKE3 contra chunk_hash.
        """
        try:
            results = []

            for chunk_hash in request.chunk_hashes:
                if not chunk_hash:
                    continue

                if not is_valid_chunk_hash(chunk_hash):
                    results.append(
                        p2p_storage_pb2.RetrievedChunk(
                            chunk_hash=str(chunk_hash),
                            status=p2p_storage_pb2.RETRIEVE_STATUS_ERROR,
                            detail=_invalid_chunk_hash_detail(chunk_hash),
                        )
                    )
                    continue

                try:
                    chunk_data = self.repo.get_compressed(chunk_hash)
                    results.append(
                        p2p_storage_pb2.RetrievedChunk(
                            chunk_hash=chunk_hash,
                            status=p2p_storage_pb2.RETRIEVE_STATUS_FOUND,
                            chunk_data=chunk_data,
                            detail="ok",
                        )
                    )
                except FileNotFoundError:
                    results.append(
                        p2p_storage_pb2.RetrievedChunk(
                            chunk_hash=chunk_hash,
                            status=p2p_storage_pb2.RETRIEVE_STATUS_NOT_FOUND,
                            detail="chunk no encontrado",
                        )
                    )
                except Exception as exc:
                    results.append(
                        p2p_storage_pb2.RetrievedChunk(
                            chunk_hash=chunk_hash,
                            status=p2p_storage_pb2.RETRIEVE_STATUS_ERROR,
                            detail=str(exc),
                        )
                    )

            return p2p_storage_pb2.RetrieveChunkBatchResponse(results=results)

        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.RetrieveChunkBatchResponse()

    def ProbeMissingDataPackShards(self, request, context):
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
                if not self.ec_shard_store.exists(
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

    def ReplicateDataPackShards(self, request_iterator, context):
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

    def RetrieveDataPackShardBatch(self, request, context):
        """Devuelve shards EC por batch para reconstrucción de data packs."""
        try:
            results = []

            for shard in request.shards:
                detail = _invalid_shard_ref_detail(shard)
                if detail is not None:
                    results.append(
                        p2p_storage_pb2.RetrievedDataPackShard(
                            pack_hash=str(shard.pack_hash),
                            shard_index=int(shard.shard_index),
                            shard_hash=str(shard.shard_hash),
                            status=(
                                p2p_storage_pb2
                                .DATA_PACK_SHARD_RETRIEVE_STATUS_ERROR
                            ),
                            detail=detail,
                        )
                    )
                    continue

                try:
                    stored = self.ec_shard_store.get(
                        pack_hash=shard.pack_hash,
                        shard_index=int(shard.shard_index),
                        shard_hash=shard.shard_hash,
                    )
                    results.append(
                        p2p_storage_pb2.RetrievedDataPackShard(
                            pack_hash=stored.pack_hash,
                            shard_index=stored.shard_index,
                            shard_hash=stored.shard_hash,
                            status=(
                                p2p_storage_pb2
                                .DATA_PACK_SHARD_RETRIEVE_STATUS_FOUND
                            ),
                            shard_data=stored.data,
                            detail="ok",
                        )
                    )
                except FileNotFoundError:
                    results.append(
                        p2p_storage_pb2.RetrievedDataPackShard(
                            pack_hash=shard.pack_hash,
                            shard_index=int(shard.shard_index),
                            shard_hash=shard.shard_hash,
                            status=(
                                p2p_storage_pb2
                                .DATA_PACK_SHARD_RETRIEVE_STATUS_NOT_FOUND
                            ),
                            detail="shard EC no encontrado",
                        )
                    )
                except DataPackShardHashMismatchError as exc:
                    results.append(
                        p2p_storage_pb2.RetrievedDataPackShard(
                            pack_hash=shard.pack_hash,
                            shard_index=int(shard.shard_index),
                            shard_hash=shard.shard_hash,
                            status=(
                                p2p_storage_pb2
                                .DATA_PACK_SHARD_RETRIEVE_STATUS_HASH_MISMATCH
                            ),
                            detail=str(exc),
                        )
                    )
                except Exception as exc:
                    results.append(
                        p2p_storage_pb2.RetrievedDataPackShard(
                            pack_hash=shard.pack_hash,
                            shard_index=int(shard.shard_index),
                            shard_hash=shard.shard_hash,
                            status=(
                                p2p_storage_pb2
                                .DATA_PACK_SHARD_RETRIEVE_STATUS_ERROR
                            ),
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
            return p2p_storage_pb2.ReplicateDataPackShardResult(
                pack_hash=str(item.pack_hash),
                shard_index=int(item.shard_index),
                shard_hash=str(item.shard_hash),
                status=(
                    p2p_storage_pb2
                    .DATA_PACK_SHARD_STORE_STATUS_REJECTED_INVALID_ARGUMENT
                ),
                detail=detail,
            )

        try:
            stored = self.ec_shard_store.put(
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

        return p2p_storage_pb2.ReplicateDataPackShardResult(
            pack_hash=item.pack_hash,
            shard_index=int(item.shard_index),
            shard_hash=item.shard_hash,
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
    if not is_valid_hash64(shard.pack_hash):
        return _invalid_pack_hash_detail("pack_hash", shard.pack_hash)
    if not is_valid_hash64(shard.shard_hash):
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

