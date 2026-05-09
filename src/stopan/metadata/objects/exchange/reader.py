"""
Lectura cacheada de metadata objects desde el object store.

El reader valida tipo esperado y hash al cargar objetos, y reutiliza payloads ya
leídos durante la importación del grafo.
"""

from __future__ import annotations

from typing import Any

from .errors import MetadataObjectImportError
from .validation import require_hash64
from stopan.metadata.objects.graph import decode_object_envelope
from stopan.metadata.objects.models import MetadataObjectType
from stopan.metadata.objects.store import MetadataObjectStore


class MetadataObjectReader:
    def __init__(self, store: MetadataObjectStore):
        self.store = store
        self._payload_cache: dict[str, dict[str, Any]] = {}
        self._type_cache: dict[str, MetadataObjectType] = {}
        self._size_by_hash: dict[str, int] = {}

    @property
    def object_hashes_read(self) -> tuple[str, ...]:
        return tuple(self._payload_cache.keys())

    @property
    def total_canonical_bytes(self) -> int:
        return sum(self._size_by_hash.values())

    def payload(self, object_hash: str, expected_type: MetadataObjectType) -> dict[str, Any]:
        object_hash = require_hash64("object_hash", object_hash)

        cached = self._payload_cache.get(object_hash)
        if cached is not None:
            cached_type = self._type_cache[object_hash]
            if cached_type != expected_type:
                raise MetadataObjectImportError(
                    f"tipo de metadata object no coincide para {object_hash}: "
                    f"cache={cached_type.value} esperado={expected_type.value}"
                )
            return cached

        plaintext = self.store.get_object_bytes(
            object_hash=object_hash,
            expected_type=expected_type,
        )
        try:
            object_type, payload = decode_object_envelope(
                plaintext,
                expected_hash=object_hash,
            )
        except Exception as exc:
            raise MetadataObjectImportError(str(exc)) from exc

        if object_type != expected_type:
            raise MetadataObjectImportError(
                f"tipo de metadata object no coincide para {object_hash}: "
                f"esperado={expected_type.value} recibido={object_type.value}"
            )

        self._payload_cache[object_hash] = payload
        self._type_cache[object_hash] = expected_type
        self._size_by_hash[object_hash] = len(plaintext)
        return payload
