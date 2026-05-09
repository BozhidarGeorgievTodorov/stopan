from __future__ import annotations


class MetadataObjectStoreError(RuntimeError):
    pass


class MetadataObjectStoreAuthenticationError(MetadataObjectStoreError):
    pass
