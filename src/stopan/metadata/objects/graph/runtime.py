"""
Representación runtime de un metadata object graph.

El grafo mantiene el catalog, el digest de estado y el mapa inmutable de objetos
codificados, validando que las claves coinciden con los hashes de cada objeto.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from stopan.metadata.objects.codec import EncodedMetadataObject
from stopan.metadata.objects.models import MetadataObjectError, MetadataObjectType, ObjectRef


def _require_hash64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectError(f"{name} debe ser str; recibido {type(value).__name__}")

    text = value.strip()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectError(f"{name} debe tener 64 caracteres hexadecimales lowercase")

    return text


def _require_vault_id(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise MetadataObjectError(f"{name} debe ser str; recibido {type(value).__name__}")

    text = value.strip()
    if len(text) != 32 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectError(f"{name} debe tener 32 caracteres hexadecimales lowercase")

    return text

def _require_int(name: str, value: object, *, min_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataObjectError(f"{name} debe ser int; recibido {type(value).__name__}")
    if value < min_value:
        raise MetadataObjectError(f"{name} debe ser >= {min_value}; recibido {value}")
    return value


@dataclass(frozen=True, slots=True)
class MetadataObjectGraph:
    vault_id: str
    catalog_hash: str
    catalog_ref: ObjectRef
    state_digest: str
    objects: Mapping[str, EncodedMetadataObject]
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int

    def __post_init__(self) -> None:
        vault_id = _require_vault_id("metadata_graph.vault_id", self.vault_id)
        catalog_hash = _require_hash64("metadata_graph.catalog_hash", self.catalog_hash)
        state_digest = _require_hash64(
            "metadata_graph.state_digest",
            self.state_digest,
        )

        if not isinstance(self.catalog_ref, ObjectRef):
            raise MetadataObjectError("metadata_graph.catalog_ref debe ser ObjectRef")
        if self.catalog_ref.object_type != MetadataObjectType.CATALOG:
            raise MetadataObjectError("metadata_graph.catalog_ref debe referenciar catalog")
        if self.catalog_ref.object_hash != catalog_hash:
            raise MetadataObjectError("metadata_graph.catalog_ref.object_hash debe coincidir con catalog_hash")

        if not isinstance(self.objects, Mapping):
            raise MetadataObjectError("metadata_graph.objects debe ser un mapping")

        normalized: dict[str, EncodedMetadataObject] = {}
        for key, encoded in self.objects.items():
            object_hash = _require_hash64("metadata_graph.objects key", key)
            if not isinstance(encoded, EncodedMetadataObject):
                raise MetadataObjectError("metadata_graph.objects debe contener EncodedMetadataObject")
            if encoded.object_hash != object_hash:
                raise MetadataObjectError(
                    "metadata_graph object key mismatch: "
                    f"key={object_hash} object_hash={encoded.object_hash}"
                )
            normalized[object_hash] = encoded

        if catalog_hash not in normalized:
            raise MetadataObjectError("metadata_graph.objects debe contener catalog_hash")

        object.__setattr__(self, "vault_id", vault_id)
        object.__setattr__(self, "catalog_hash", catalog_hash)
        object.__setattr__(self, "state_digest", state_digest)
        object.__setattr__(
            self,
            "objects",
            MappingProxyType(dict(sorted(normalized.items()))),
        )

        _require_int("metadata_graph.snapshot_count", self.snapshot_count, min_value=0)
        _require_int("metadata_graph.known_chunk_count", self.known_chunk_count, min_value=0)
        _require_int(
            "metadata_graph.protection_record_count",
            self.protection_record_count,
            min_value=0,
        )

    @property
    def object_count(self) -> int:
        return len(self.objects)

    @property
    def total_canonical_bytes(self) -> int:
        return sum(item.size for item in self.objects.values())
