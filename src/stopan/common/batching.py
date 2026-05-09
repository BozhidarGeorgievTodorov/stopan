from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")


def iter_batches(items: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    batch_size = max(1, int(batch_size))
    current: list[T] = []

    for item in items:
        current.append(item)
        if len(current) >= batch_size:
            yield current
            current = []

    if current:
        yield current
