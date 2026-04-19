from __future__ import annotations

import os
import threading
from concurrent import futures
from dataclasses import dataclass

import blake3
import zstandard as zstd

from stopan.cas.repository import CASRepository
from stopan.protos import p2p_storage_pb2


MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", str(8 * 1024 * 1024)))
STORAGE_COMMIT_WORKERS = int(os.getenv("STORAGE_COMMIT_WORKERS", str(max(4, (os.cpu_count() or 4)))))
STORAGE_COMMIT_QUEUE_ITEMS = int(os.getenv("STORAGE_COMMIT_QUEUE_ITEMS", "256"))


@dataclass(frozen=True)
class StorageCommitResult:
    chunk_hash: str
    status: int
    detail: str

    @property
    def is_success(self) -> bool:
        return self.status in (
            p2p_storage_pb2.STORE_STATUS_STORED,
            p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
        )


class StorageCommitEngine:
    """
    Motor interno de ingestión de chunks.

    Desacopla la recepción por gRPC del coste de validar, descomprimir y
    persistir bloques en el CAS. La cola acotada aplica backpressure cuando
    el disco o la CPU no siguen el ritmo del stream.
    """

    def __init__(self, repo: CASRepository):
        self.repo = repo
        self._max_chunk_size = MAX_CHUNK_SIZE
        self._slots = threading.Semaphore(max(1, STORAGE_COMMIT_QUEUE_ITEMS))
        self._executor = futures.ThreadPoolExecutor(
            max_workers=STORAGE_COMMIT_WORKERS,
            thread_name_prefix="storage-commit",
        )
        self._tls = threading.local()

    @property
    def worker_count(self) -> int:
        return STORAGE_COMMIT_WORKERS

    @property
    def max_pending(self) -> int:
        return STORAGE_COMMIT_QUEUE_ITEMS

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _decompressor(self) -> zstd.ZstdDecompressor:
        decompressor = getattr(self._tls, "decompressor", None)
        if decompressor is None:
            decompressor = zstd.ZstdDecompressor()
            self._tls.decompressor = decompressor
        return decompressor

    def process_one(self, chunk_hash: str, chunk_data: bytes) -> StorageCommitResult:
        if self.repo.exists_local(chunk_hash):
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
                detail="already present",
            )

        try:
            raw = self._decompressor().decompress(
                chunk_data,
                max_output_size=self._max_chunk_size,
            )
        except zstd.ZstdError:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_CORRUPT,
                detail="rejected: corrupt zstd data",
            )
        except Exception:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_CORRUPT,
                detail="rejected: decompressed output too large",
            )

        calculated_hash = blake3.blake3(raw).hexdigest()
        if calculated_hash != chunk_hash:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_HASH_MISMATCH,
                detail="rejected: hash mismatch",
            )

        try:
            is_new = self.repo.put_compressed(chunk_hash, chunk_data)
        except Exception as exc:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_ERROR,
                detail=str(exc),
            )

        if is_new:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_STORED,
                detail="stored",
            )

        return StorageCommitResult(
            chunk_hash=chunk_hash,
            status=p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
            detail="already present",
        )

    def submit(self, chunk_hash: str, chunk_data: bytes):
        self._slots.acquire()
        future = self._executor.submit(self.process_one, chunk_hash, chunk_data)

        def release_slot(_):
            self._slots.release()

        future.add_done_callback(release_slot)
        return future
