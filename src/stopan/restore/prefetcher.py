from __future__ import annotations

from collections.abc import Iterable, Iterator

from stopan.restore.fetch import ChunkFetchService


class OrderedBatchChunkPrefetcher:
    """
    Lee chunks por ventanas y conserva el orden de escritura del archivo.

    Cada ventana se resuelve local-first. Si faltan chunks, se agrupan por rank
    HRW y por nodo remoto para reducir llamadas gRPC durante restore.
    """

    def __init__(self, fetch_service: ChunkFetchService, *, target_parallelism: int, window: int):
        self.fetch_service = fetch_service
        self.target_parallelism = max(int(target_parallelism), 1)
        self.window = max(int(window), 1)

    def iter_raw_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[tuple[str, bytes]]:
        pending_window: list[str] = []

        for chunk_hash in chunk_hashes:
            pending_window.append(chunk_hash)
            if len(pending_window) >= self.window:
                yield from self._drain_window(pending_window)
                pending_window = []

        if pending_window:
            yield from self._drain_window(pending_window)

    def _drain_window(self, window_hashes: list[str]) -> Iterator[tuple[str, bytes]]:
        result_map = self.fetch_service.fetch_many_raw_chunks(
            window_hashes,
            target_parallelism=self.target_parallelism,
        )

        for chunk_hash in window_hashes:
            value = result_map[chunk_hash]
            if isinstance(value, Exception):
                raise value
            yield chunk_hash, value
