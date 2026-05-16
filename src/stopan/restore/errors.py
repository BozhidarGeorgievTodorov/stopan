from __future__ import annotations

from stopan.errors import StopanDataError


class RestoreDataError(StopanDataError, RuntimeError):
    """Metadata o chunks necesarios para restore están corruptos o incompletos."""


class RestorePathError(StopanDataError, ValueError):
    """Una ruta persistida para restore intenta escapar del árbol destino."""


class ChunkUnavailableError(RestoreDataError):
    """Un chunk requerido por restore no está disponible en las fuentes habilitadas."""
