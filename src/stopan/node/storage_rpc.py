from __future__ import annotations

import os
import queue
import threading
from concurrent import futures

import grpc

from stopan.cas.repository import CASRepository
from stopan.node.commit_engine import StorageCommitEngine
from stopan.protos import p2p_storage_pb2
from stopan.protos import p2p_storage_pb2_grpc


class StorageNodeServicer(p2p_storage_pb2_grpc.P2PStorageServicer):
    """Servicio gRPC que recibe y sirve chunks comprimidos."""

    def __init__(self, repo_store_dir: str):
        self.repo = CASRepository(repo_store_dir)
        self.commit_engine = StorageCommitEngine(self.repo)
        print(f"P2P storage node ready. Store: {os.path.abspath(repo_store_dir)}")
        print(
            f"Commit engine: workers={self.commit_engine.worker_count} "
            f"queue={self.commit_engine.max_pending}"
        )

    def close(self) -> None:
        self.commit_engine.close()

    def ProbeMissingChunks(self, request, context):
        """Devuelve solo los hashes que este nodo no tiene en local."""
        try:
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
        """Guarda chunks recibidos por stream y devuelve una respuesta por item."""
        future_queue: queue.Queue = queue.Queue(maxsize=max(1, self.commit_engine.max_pending))
        sentinel = object()
        reader_errors: list[Exception] = []

        def reader() -> None:
            try:
                for item in request_iterator:
                    if not context.is_active():
                        break
                    if not item.chunk_hash:
                        continue
                    future = self.commit_engine.submit(item.chunk_hash, item.chunk_data)
                    future_queue.put(future)
            except Exception as exc:
                reader_errors.append(exc)
            finally:
                future_queue.put(sentinel)

        reader_thread = threading.Thread(
            target=reader,
            name="storage-stream-reader",
            daemon=True,
        )
        reader_thread.start()

        inflight = set()
        reader_finished = False

        try:
            while True:
                drained_any = False

                while True:
                    try:
                        item = future_queue.get_nowait()
                    except queue.Empty:
                        break

                    drained_any = True
                    if item is sentinel:
                        reader_finished = True
                    else:
                        inflight.add(item)

                if not inflight and reader_finished:
                    break

                if not inflight:
                    item = future_queue.get()
                    if item is sentinel:
                        reader_finished = True
                        if not inflight:
                            break
                    else:
                        inflight.add(item)
                    continue

                done, not_done = futures.wait(
                    inflight,
                    timeout=0.05,
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

                if reader_finished and not inflight:
                    break

                if (not drained_any) and (not done) and (not context.is_active()):
                    break

            if reader_errors:
                raise reader_errors[0]

        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return
        finally:
            reader_thread.join(timeout=2.0)

    def RetrieveChunkBatch(self, request, context):
        """Devuelve varios chunks comprimidos en una sola llamada gRPC."""
        try:
            results = []

            for chunk_hash in request.chunk_hashes:
                if not chunk_hash:
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
                            detail="missing chunk",
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
