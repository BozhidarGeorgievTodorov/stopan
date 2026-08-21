"""
Prefetch ordenado de chunks para restore.

El prefetcher agrupa hashes en ventanas para aprovechar lecturas por lote, pero
entrega los chunks en el mismo orden en el que deben escribirse al archivo.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from stopan.restore.fetch import ChunkFetchService
from stopan.restore.errors import ChunkUnavailableError


class OrderedBatchChunkPrefetcher:
    """
    Resuelve chunks por ventanas y los entrega en orden de escritura.

    La resolución puede usar CAS local, CAS P2P local o red remota, según la
    política de ChunkFetchService. Este objeto solo controla el tamaño de
    ventana y preserva el orden.
    """

    def __init__(self, fetch_service: ChunkFetchService, *, target_parallelism: int, window: int):
        self.fetch_service = fetch_service
        self.target_parallelism = max(int(target_parallelism), 1)
        self.window = max(int(window), 1)

    def iter_raw_chunks(self, chunk_hashes: Iterable[str]) -> Iterator[tuple[str, bytes]]:
        """
        Itera chunks raw en el mismo orden que chunk_hashes.

        Cada ventana se resuelve en lote, pero los resultados se emiten según el
        orden original para que el restorer pueda escribir secuencialmente.
        """

        records = ((index, chunk_hash, 0) for index, chunk_hash in enumerate(chunk_hashes))
        for _order, chunk_hash, _size, raw_chunk in self.iter_raw_chunk_records(records):
            yield chunk_hash, raw_chunk

    def iter_raw_chunk_records(
        self,
        records: Iterable[tuple[int, str, int]],
    ) -> Iterator[tuple[int, str, int, bytes]]:
        """Resuelve registros de receta por ventanas preservando orden y tamaño esperado."""

        window_records: list[tuple[int, str, int]] = []
        for record in records:
            window_records.append(record)
            if len(window_records) >= self.window:
                yield from self._flush_record_window(window_records)
                window_records = []

        if window_records:
            yield from self._flush_record_window(window_records)

    def _flush_record_window(
        self,
        window_records: list[tuple[int, str, int]],
    ) -> Iterator[tuple[int, str, int, bytes]]:
        """Resuelve una ventana de registros y devuelve cada resultado en orden."""

        window_hashes = [chunk_hash for _order, chunk_hash, _size in window_records]
        results = self.fetch_service.fetch_many_raw_chunks(
            window_hashes,
            target_parallelism=self.target_parallelism,
        )
        for chunk_order, chunk_hash, chunk_size in window_records:
            value = results.get(chunk_hash)
            if value is None:
                raise ChunkUnavailableError(
                    f"Chunk {chunk_hash[:8]} no resuelto en ventana de restore"
                )
            if isinstance(value, Exception):
                raise value
            yield chunk_order, chunk_hash, chunk_size, value
