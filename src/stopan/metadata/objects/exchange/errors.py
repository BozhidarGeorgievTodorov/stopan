from __future__ import annotations

from stopan.errors import StopanDataError


class MetadataObjectExchangeError(StopanDataError, RuntimeError):
    pass


class MetadataObjectImportError(MetadataObjectExchangeError):
    pass
