from __future__ import annotations


class StopanError(Exception):
    """Error controlado que puede mostrarse al usuario sin traceback por defecto."""

    exit_code = 1
    prefix = "Error de Stopan"

    def __init__(self, message: str, *, exit_code: int | None = None, prefix: str | None = None) -> None:
        super().__init__(message)
        if exit_code is not None:
            self.exit_code = int(exit_code)
        if prefix is not None:
            self.prefix = prefix


class StopanUsageError(StopanError):
    """Uso inválido de CLI o combinación inválida de configuración efectiva."""

    exit_code = 2
    prefix = "Error de uso de Stopan"


class StopanConfigError(StopanError):
    """Configuración ausente, inválida o insuficiente para ejecutar el comando."""

    exit_code = 2
    prefix = "Error de configuración de Stopan"


class StopanConfigTypeError(StopanConfigError, TypeError):
    """Tipo inválido en configuración o secretos, preservando compatibilidad con TypeError."""


class StopanConfigValueError(StopanConfigError, ValueError):
    """Valor inválido en configuración o secretos, preservando compatibilidad con ValueError."""


class StopanConfigRuntimeError(StopanConfigError, RuntimeError):
    """Configuración ausente o irresoluble, preservando compatibilidad con RuntimeError."""


class StopanDataError(StopanError):
    """Estado persistido, packs, hashes o metadata inconsistentes o no recuperables."""

    exit_code = 1
    prefix = "Error de datos de Stopan"


class StopanStorageError(StopanError):
    """Repositorios, stores locales o rutas persistentes no accesibles o inconsistentes."""

    exit_code = 1
    prefix = "Error de almacenamiento de Stopan"


class StopanStorageOSError(StopanStorageError, OSError):
    """Fallo de filesystem clasificado como almacenamiento, compatible con OSError."""


class StopanNetworkError(StopanError):
    """Comunicación remota, membership, RPC o streams P2P no completados correctamente."""

    exit_code = 1
    prefix = "Error de red de Stopan"


class StopanDependencyError(StopanError):
    """Dependencia opcional o de runtime no disponible."""

    exit_code = 1
    prefix = "Error de dependencias de Stopan"
