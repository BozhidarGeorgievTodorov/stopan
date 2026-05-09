"""
Prefetch ordenado de chunks para restore.

El prefetcher agrupa hashes en ventanas para aprovechar lecturas por lote, pero
entrega los chunks en el mismo orden en el que deben escribirse al archivo.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from stopan.restore.fetch import ChunkFetchService


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
                
        window_hashes: list[str] = []

        for chunk_hash in chunk_hashes:
            window_hashes.append(chunk_hash)
            if len(window_hashes) >= self.window:
                yield from self._flush_window(window_hashes)
                window_hashes = []

        if window_hashes:
            yield from self._flush_window(window_hashes)

    def _flush_window(self, window_hashes: list[str]) -> Iterator[tuple[str, bytes]]:
        """
        Resuelve una ventana y produce sus chunks en el orden solicitado.

        Si algún chunk no se resuelve, propaga el error asociado para abortar el
        restore de forma explícita.
        """
        
        results = self.fetch_service.fetch_many_raw_chunks(
            window_hashes,
            target_parallelism=self.target_parallelism,
        )
        for chunk_hash in window_hashes:
            value = results.get(chunk_hash)
            if value is None:
                raise FileNotFoundError(f"Chunk {chunk_hash[:8]} no resuelto en ventana de restore")
            if isinstance(value, Exception):
                raise value
            yield chunk_hash, value
