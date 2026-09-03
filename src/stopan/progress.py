from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol


class ProgressReporter(Protocol):
    """Interfaz mínima para informar progreso sin acoplar servicios a una UI."""

    def start(
        self,
        label: str,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        detail: str | None = None,
    ) -> None: ...

    def update(
        self,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        detail: str | None = None,
    ) -> None: ...

    def finish(self) -> None: ...


@contextmanager
def suspend_progress(progress: ProgressReporter | None) -> Iterator[None]:
    """Pausa, si existe, el render interactivo mientras se imprimen mensajes normales."""
    if progress is None:
        yield
        return
    suspend = getattr(progress, "suspend", None)
    if not callable(suspend):
        yield
        return
    with suspend():
        yield
