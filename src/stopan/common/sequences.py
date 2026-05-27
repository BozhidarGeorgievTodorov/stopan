"""Utilidades pequeñas para secuencias ordenadas."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
K = TypeVar("K")


def ordered_unique(items: Iterable[T]) -> list[T]:
    """Devuelve los elementos sin duplicados preservando la primera aparición."""
    return list(dict.fromkeys(items))


def ordered_unique_by(items: Iterable[T], key: Callable[[T], K]) -> list[T]:
    """Deduplica preservando orden usando una clave derivada por elemento."""
    seen: dict[K, T] = {}
    for item in items:
        item_key = key(item)
        if item_key not in seen:
            seen[item_key] = item
    return list(seen.values())
