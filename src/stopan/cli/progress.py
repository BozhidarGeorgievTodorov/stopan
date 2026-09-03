from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TextIO


_TRUTHY = {"1", "true", "yes", "on", "si", "sí"}
_UNICODE_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
_ASCII_FRAMES = ("|", "/", "-", "\\")


def interactive_progress_enabled(stream: TextIO | None = None) -> bool:
    """Activa el feedback solo en una terminal interactiva y si no se ha deshabilitado."""
    target = stream or sys.stderr
    disabled = os.getenv("STOPAN_NO_PROGRESS", "").strip().lower()
    if disabled in _TRUTHY:
        return False
    try:
        return bool(target.isatty())
    except (AttributeError, OSError, ValueError):
        return False


def _stream_supports_unicode(stream: TextIO) -> bool:
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        "⠹█░·…".encode(encoding)
    except (LookupError, UnicodeEncodeError):
        return False
    return True


@dataclass(slots=True)
class _ProgressState:
    label: str = ""
    current: int | None = None
    total: int | None = None
    unit: str | None = None
    detail: str | None = None
    started_at: float = 0.0


class TerminalProgress:
    """Spinner de una sola línea con barra opcional para sesiones TTY."""

    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        delay_s: float = 0.35,
        interval_s: float = 0.08,
        bar_width: int = 14,
    ) -> None:
        self.stream = stream or sys.stderr
        self.enabled = interactive_progress_enabled(self.stream)
        self.delay_s = max(0.0, float(delay_s))
        self.interval_s = max(0.02, float(interval_s))
        self.bar_width = max(6, int(bar_width))
        self._unicode = _stream_supports_unicode(self.stream)
        self._frames = _UNICODE_FRAMES if self._unicode else _ASCII_FRAMES
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._suspended = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = _ProgressState()
        self._rendered = False
        self._previous_width = 0

    def start(
        self,
        label: str,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        detail: str | None = None,
    ) -> None:
        if not self.enabled:
            return

        self.finish()
        with self._lock:
            self._state = _ProgressState(
                label=str(label),
                current=current,
                total=total,
                unit=unit,
                detail=detail,
                started_at=time.monotonic(),
            )
            self._rendered = False
            self._previous_width = 0
            self._stop_event = threading.Event()
            self._suspended.clear()
            self._thread = threading.Thread(
                target=self._render_loop,
                name="stopan-cli-progress",
                daemon=True,
            )
            self._thread.start()

    def update(
        self,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        detail: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        with self._lock:
            if current is not None:
                self._state.current = int(current)
            if total is not None:
                self._state.total = int(total)
            if unit is not None:
                self._state.unit = str(unit)
            self._state.detail = detail

    @contextmanager
    def task(
        self,
        label: str,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        detail: str | None = None,
    ) -> Iterator["TerminalProgress"]:
        """Ejecuta una fase asegurando que la línea interactiva se limpia al salir."""
        self.start(
            label,
            current=current,
            total=total,
            unit=unit,
            detail=detail,
        )
        try:
            yield self
        finally:
            self.finish()

    def clear(self) -> None:
        """Limpia temporalmente la línea sin detener el progreso activo."""
        if not self.enabled:
            return
        with self._lock:
            self._clear_locked()

    @contextmanager
    def suspend(self) -> Iterator[None]:
        """Pausa el render para poder escribir mensajes normales sin solaparlos."""
        if not self.enabled:
            yield
            return

        self._suspended.set()
        with self._lock:
            self._clear_locked()
        try:
            yield
        finally:
            self._suspended.clear()

    def _clear_locked(self) -> None:
        if not self._rendered:
            return
        try:
            self.stream.write("\r" + (" " * self._previous_width) + "\r")
            self.stream.flush()
        except (OSError, ValueError):
            return
        self._rendered = False
        self._previous_width = 0

    def finish(self) -> None:
        if not self.enabled:
            return

        thread = self._thread
        if thread is None:
            return

        self._stop_event.set()
        if thread is not threading.current_thread():
            thread.join(timeout=max(0.25, self.interval_s * 4))

        with self._lock:
            if self._rendered:
                try:
                    self.stream.write("\r" + (" " * self._previous_width) + "\r")
                    self.stream.flush()
                except (OSError, ValueError):
                    pass
            self._thread = None
            self._rendered = False
            self._previous_width = 0

    def _render_loop(self) -> None:
        if self._stop_event.wait(self.delay_s):
            return

        frame_index = 0
        while not self._stop_event.is_set():
            if self._suspended.is_set():
                if self._stop_event.wait(self.interval_s):
                    return
                continue
            self._render(frame_index)
            frame_index = (frame_index + 1) % len(self._frames)
            if self._stop_event.wait(self.interval_s):
                return

    def _render(self, frame_index: int) -> None:
        with self._lock:
            elapsed = max(0.0, time.monotonic() - self._state.started_at)
            line = self._format_line(self._frames[frame_index], elapsed)
            line = self._truncate_to_terminal(line)
            padding = max(0, self._previous_width - len(line))
            try:
                self.stream.write("\r" + line + (" " * padding))
                self.stream.flush()
            except UnicodeEncodeError:
                self._unicode = False
                self._frames = _ASCII_FRAMES
                return
            except (OSError, ValueError):
                return
            self._rendered = True
            self._previous_width = len(line)

    def _format_line(self, frame: str, elapsed: float) -> str:
        parts = [f"{frame} {self._state.label}"]
        current = self._state.current
        total = self._state.total
        unit = self._state.unit or ""

        if current is not None and total is not None and total > 0:
            bounded = min(max(current, 0), total)
            ratio = bounded / total
            filled = min(self.bar_width, int(ratio * self.bar_width))
            empty = self.bar_width - filled
            if self._unicode:
                bar = ("█" * filled) + ("░" * empty)
            else:
                bar = ("#" * filled) + ("-" * empty)
            percent = int(ratio * 100)
            count = f"{current}/{total}"
            if unit:
                count += f" {unit}"
            parts.append(f"[{bar}] {count} ({percent}%)")
        elif current is not None:
            count = str(current)
            if unit:
                count += f" {unit}"
            parts.append(count)

        if self._state.detail:
            parts.append(str(self._state.detail))
        parts.append(f"{elapsed:.1f} s")
        separator = " · " if self._unicode else " | "
        return separator.join(parts)

    def _truncate_to_terminal(self, line: str) -> str:
        try:
            columns = os.get_terminal_size(self.stream.fileno()).columns
        except (AttributeError, OSError, ValueError):
            columns = 100
        if columns < 40:
            columns = 100
        max_width = max(20, columns - 1)
        if len(line) <= max_width:
            return line
        suffix = "…" if self._unicode else "~"
        return line[: max_width - 1] + suffix
