from __future__ import annotations


class MetadataObjectExchangeError(RuntimeError):
    pass


class MetadataObjectExportError(MetadataObjectExchangeError):
    pass


class MetadataObjectImportError(MetadataObjectExchangeError):
    pass
