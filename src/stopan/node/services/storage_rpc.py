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
from stopan.node.storage.commit_engine import StorageCommitEngine
from stopan.node.storage.ec_shard_store import DataPackShardStore
from stopan.node.lifecycle import NodeDrainController
from stopan.node.services.ec_storage_rpc import DataPackShardRpcHandler
from stopan.rpc.auth import require_cluster_token_metadata


_INVALID_HASH_PREVIEW_CHARS = 32
_STREAM_QUEUE_POLL_TIMEOUT_S = 0.05
_STREAM_READER_JOIN_TIMEOUT_S = 2.0


def _invalid_chunk_hash_detail(chunk_hash: str) -> str:
    visible = str(chunk_hash)[:_INVALID_HASH_PREVIEW_CHARS]
    return f"chunk_hash inválido: {visible!r}; se esperaba BLAKE3 hex lowercase de 64 caracteres"


class StorageNodeServicer(p2p_storage_pb2_grpc.P2PStorageServicer):
    def __init__(
        self,
        custody_dir: str,
        *,
        cluster_token: str,
        commit_workers: int,
        commit_queue_items: int,
        max_chunk_size: int,
        drain_controller: NodeDrainController | None = None,
    ):
        """
        Servidor P2PStorage backed by CAS local.

        Los chunks entrantes se validan mediante StorageCommitEngine. Los chunks
        salientes se devuelven comprimidos y el cliente restore valida integridad.
        """
        self._cluster_token = str(cluster_token or "")
        self._drain_controller = drain_controller
        custody_root = os.path.abspath(custody_dir)
        self.repo = CASRepository(os.path.join(custody_root, "chunks"))
        self.commit_engine = StorageCommitEngine(
            self.repo,
            max_chunk_size=max_chunk_size,
            worker_count=commit_workers,
            max_pending=commit_queue_items,
        )
        self.ec_shard_store = DataPackShardStore(
            os.path.join(custody_root, "ec_shards"),
            max_shard_size=max_chunk_size,
        )
        self.ec_shard_rpc = DataPackShardRpcHandler(self.ec_shard_store)
        print(f"Nodo P2P listo. Custodia en: {custody_root}")
        print(
            f"Commit engine: workers={self.commit_engine.worker_count} "
            f"queue={self.commit_engine.max_pending} "
            f"max_chunk_size={self.commit_engine.max_chunk_size}"
        )

    def close(self) -> None:
        self.commit_engine.close()

    def _admit_work(self, context) -> None:
        if self._drain_controller is not None:
            self._drain_controller.admit_rpc(context)

    def ProbeMissingChunks(self, request, context):
        """
        Devuelve qué chunks no existen localmente en el CAS.

        Nota:
        - este método verifica presencia, no integridad profunda;
        - la integridad se valida al aceptar blobs nuevos en ReplicateChunks
            y al consumir blobs en restore/retrieve mediante BLAKE3.
        """
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)

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
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)

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

        El custodio valida el contenido antes de entregarlo. El consumidor remoto
        vuelve a comprobar la integridad extremo-a-extremo al descomprimirlo.
        """
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)

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
                    chunk_data = self.repo.get_validated_compressed(chunk_hash)
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
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)
        return self.ec_shard_rpc.probe_missing(request, context)

    def ReplicateDataPackShards(self, request_iterator, context):
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)
        yield from self.ec_shard_rpc.replicate(request_iterator, context)

    def RetrieveDataPackShardBatch(self, request, context):
        require_cluster_token_metadata(self._cluster_token, context)
        self._admit_work(context)
        return self.ec_shard_rpc.retrieve_batch(request, context)

