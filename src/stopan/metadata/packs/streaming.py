from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path


_DEFAULT_CHUNK_BYTES = 1024 * 1024
_MESSAGE_OVERHEAD_BYTES = 1024


def metadata_pack_stream_chunk_bytes(max_message_bytes: int) -> int:
    """Devuelve un bloque que cabe holgadamente en un mensaje gRPC."""

    limit = max(1, int(max_message_bytes))
    if limit <= 2 * _MESSAGE_OVERHEAD_BYTES:
        return max(1, limit // 2)
    return min(_DEFAULT_CHUNK_BYTES, limit - _MESSAGE_OVERHEAD_BYTES)


def iter_file_chunks(path: str | Path, *, chunk_bytes: int) -> Iterator[bytes]:
    source = Path(path).expanduser().resolve()
    size = int(chunk_bytes)
    if size <= 0:
        raise ValueError("chunk_bytes debe ser > 0")

    with source.open("rb") as fh:
        while True:
            block = fh.read(size)
            if not block:
                break
            yield block
