"""Helpers pequeños para ejecutar trabajo concurrente por clave."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Generic, TypeVar


K = TypeVar("K")
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CompletedKeyedTask(Generic[K, T]):
    key: K
    result: T | None = None
    error: Exception | None = None


def iter_completed_keyed_tasks(
    *,
    tasks: Mapping[K, Callable[[], T]],
    max_workers: int,
    thread_name_prefix: str,
) -> Iterator[CompletedKeyedTask[K, T]]:
    """
    Ejecuta tareas indexadas por clave y devuelve resultados según completan.

    Si la iteración se interrumpe, cancela los futures pendientes antes de
    propagar la excepción. Las excepciones normales de una tarea se devuelven en
    ``CompletedKeyedTask.error`` para que el caller conserve semántica de dominio.
    """

    if not tasks:
        return

    executor = ThreadPoolExecutor(
        max_workers=max(1, int(max_workers)),
        thread_name_prefix=thread_name_prefix,
    )
    future_map = {}

    try:
        future_map = {
            executor.submit(task): key
            for key, task in tasks.items()
        }

        for future in as_completed(future_map):
            key = future_map[future]
            try:
                yield CompletedKeyedTask(key=key, result=future.result())
            except Exception as exc:
                yield CompletedKeyedTask(key=key, error=exc)

    except BaseException:
        for future in future_map:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise

    else:
        executor.shutdown(wait=True, cancel_futures=False)
