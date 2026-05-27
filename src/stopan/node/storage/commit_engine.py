"""
Motor de commit para chunks recibidos por gRPC.

Valida chunks comprimidos antes de incorporarlos al CAS local: formato de hash,
Zstandard, tamaño máximo descomprimido y BLAKE3 del contenido raw.
"""

from __future__ import annotations

import threading
from concurrent import futures
from dataclasses import dataclass

import blake3
from stopan.protos import p2p_storage_pb2
import zstandard as zstd

from stopan.cas.hashes import is_valid_chunk_hash
from stopan.cas.repository import CASRepository


_SUBMIT_SLOT_ACQUIRE_TIMEOUT_S = 0.05


@dataclass(frozen=True, slots=True)
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
    def __init__(
        self,
        repo: CASRepository,
        *,
        max_chunk_size: int,
        worker_count: int,
        max_pending: int,
    ):
        self.repo = repo
        self._max_chunk_size = max(1, int(max_chunk_size))
        self._worker_count = max(1, int(worker_count))
        self._max_pending = max(1, int(max_pending))
        self._slots = threading.Semaphore(self._max_pending)
        self._executor = futures.ThreadPoolExecutor(
            max_workers=self._worker_count,
            thread_name_prefix="storage-commit",
        )
        self._tls = threading.local()
        self._closed = threading.Event()
        self._lifecycle_lock = threading.Lock()

    @property
    def worker_count(self) -> int:
        return self._worker_count

    @property
    def max_pending(self) -> int:
        return self._max_pending

    @property
    def max_chunk_size(self) -> int:
        return self._max_chunk_size

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed.is_set():
                return
            self._closed.set()
            executor = self._executor

        executor.shutdown(wait=True, cancel_futures=False)

    def _decompressor(self) -> zstd.ZstdDecompressor:
        value = getattr(self._tls, "decompressor", None)
        if value is None:
            value = zstd.ZstdDecompressor()
            self._tls.decompressor = value
        return value

    def process_one(self, chunk_hash: str, chunk_data: bytes) -> StorageCommitResult:
        """Valida un chunk comprimido y lo incorpora al CAS si es correcto."""

        if not is_valid_chunk_hash(chunk_hash):
            return StorageCommitResult(
                chunk_hash=str(chunk_hash),
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_HASH_MISMATCH,
                detail="chunk_hash inválido: se esperaba BLAKE3 hex lowercase de 64 caracteres",
            )

        if self.repo.exists_local(chunk_hash):
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
                detail="Chunk ya presente",
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
                detail="zstd corrupto",
            )
        except Exception:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_CORRUPT,
                detail="salida descomprimida demasiado grande",
            )

        calculated_hash = blake3.blake3(raw).hexdigest()
        if calculated_hash != chunk_hash:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_REJECTED_HASH_MISMATCH,
                detail="hash no coincide",
            )

        try:
            written = self.repo.put_compressed(chunk_hash, chunk_data)
        except Exception as exc:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_ERROR,
                detail=str(exc),
            )

        if written:
            return StorageCommitResult(
                chunk_hash=chunk_hash,
                status=p2p_storage_pb2.STORE_STATUS_STORED,
                detail="guardado",
            )

        return StorageCommitResult(
            chunk_hash=chunk_hash,
            status=p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
            detail="chunk ya presente",
        )

    def submit(
        self,
        chunk_hash: str,
        chunk_data: bytes,
        *,
        cancel_event: threading.Event | None = None,
    ):
        """Agenda un commit aplicando backpressure mediante slots acotados."""

        while True:
            if self._closed.is_set():
                raise RuntimeError("commit engine cerrado")

            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("submit de commit cancelado")

            if self._slots.acquire(timeout=_SUBMIT_SLOT_ACQUIRE_TIMEOUT_S):
                break

        try:
            with self._lifecycle_lock:
                if self._closed.is_set():
                    raise RuntimeError("commit engine cerrado")
                future = self._executor.submit(self.process_one, chunk_hash, chunk_data)
        except Exception:
            self._slots.release()
            raise

        def release_slot(_):
            self._slots.release()

        future.add_done_callback(release_slot)
        return future
