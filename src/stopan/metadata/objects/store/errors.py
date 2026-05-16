from __future__ import annotations

from stopan.errors import StopanDataError, StopanStorageError


class MetadataObjectStoreError(StopanStorageError, RuntimeError):
    pass


class MetadataObjectStoreAuthenticationError(StopanDataError, MetadataObjectStoreError):
    pass


class MetadataObjectStoreMissingError(MetadataObjectStoreError):
    pass
