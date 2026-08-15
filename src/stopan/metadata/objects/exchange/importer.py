"""
Importación de metadata object graphs a MetadataDB.

El importer reconstruye una DB operacional nueva o vacía desde el latest pointer
cifrado, validando el digest del estado alcanzable antes de cerrar la importación.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stopan.metadata.database import (
    MetadataDB,
    MetadataDBAccessMode,
    MetadataDBTransactionMode,
)
from stopan.metadata.objects.codec import canonical_state_digest
from stopan.metadata.objects.graph.validation import (
    GraphObjectPayload,
    validate_metadata_object_graph_semantics,
)
from stopan.metadata.objects.graph.walk import (
    collect_reachable_object_bytes,
    decode_object_envelope,
)
from stopan.metadata.objects.models import MetadataObjectType
from stopan.metadata.objects.store import MetadataObjectStore

from .erasure_records import ErasureDataPackImportRows, load_erasure_data_pack_rows
from .errors import MetadataObjectImportError
from .reader import MetadataObjectReader
from .validation import (
    optional_ref,
    require_hash64,
    require_int,
    require_ref,
    require_str,
)
from .writer import MetadataDBImportWriter



def _require_vault_id(name: str, value: object) -> str:
    text = require_str(name, value)
    if len(text) != 32 or any(char not in "0123456789abcdef" for char in text):
        raise MetadataObjectImportError(f"{name} debe tener 32 caracteres hexadecimales lowercase")
    return text

@dataclass(frozen=True, slots=True)
class MetadataObjectGraphImportStats:
    objects_read: int
    total_canonical_bytes: int
    snapshots_imported: int
    complete_snapshots_imported: int
    items_imported: int
    recipes_imported: int
    recipes_reused: int
    chunks_imported: int
    protection_records_imported: int
    pending_protection_records_created: int
    erasure_data_packs_imported: int
    erasure_pack_chunks_imported: int
    erasure_pack_shards_imported: int


@dataclass(frozen=True, slots=True)
class MetadataObjectGraphImportResult:
    db_file: str
    object_store_dir: Path
    vault_id: str
    catalog_hash: str
    state_digest: str
    stats: MetadataObjectGraphImportStats


class MetadataObjectGraphImporter:
    """Importa el latest graph en una DB operacional nueva o vacía."""

    def __init__(
        self,
        *,
        db_file: str,
        object_store_dir: str | Path,
        passphrase: str | bytes,
        default_desired_rf: int,
    ):
        self.db_file = str(db_file)
        self.object_store_dir = Path(object_store_dir).expanduser().resolve()
        self.default_desired_rf = int(default_desired_rf)
        self.store = MetadataObjectStore.open_existing(
            self.object_store_dir,
            passphrase=passphrase,
        )
        self.reader = MetadataObjectReader(self.store)
        self._chunk_sizes: dict[str, int] = {}

    def import_latest(self, *, include_protection: bool = True) -> MetadataObjectGraphImportResult:
        latest = self.store.read_latest_pointer()
        self._validate_latest_graph_semantics(latest)
        catalog = self.reader.payload(latest.catalog_hash, MetadataObjectType.CATALOG)
        catalog_vault_id = _require_vault_id("catalog.vault_id", catalog.get("vault_id"))
        if catalog_vault_id != latest.vault_id:
            raise MetadataObjectImportError(
                f"catalog.vault_id={catalog_vault_id} no coincide con latest.vault_id={latest.vault_id}"
            )

        chunks = self._load_chunks(catalog)
        # La protección se lee siempre si existe para validar latest.state_digest
        # contra el grafo alcanzable completo, aunque el caller no quiera
        # materializar filas de chunk_protection en la DB destino.
        protection_records = self._load_protection_records(catalog)
        erasure_data_packs = self._load_erasure_data_packs(catalog)
        snapshot_entries = self._snapshot_entries(catalog)

        writer = MetadataDBImportWriter(
            reader=self.reader,
            default_desired_rf=self.default_desired_rf,
            chunk_sizes=self._chunk_sizes,
        )

        db = MetadataDB(
            self.db_file,
            access_mode=MetadataDBAccessMode.REBUILD,
        )
        try:
            with db.transaction(mode=MetadataDBTransactionMode.EXCLUSIVE):
                db.set_vault_id(latest.vault_id)
                writer.require_empty_operational_db(db)
                chunks_inserted = writer.insert_chunks(db, chunks)
                snapshot_stats = writer.insert_snapshots(db, snapshot_entries)

                if include_protection:
                    if protection_records:
                        protection_inserted = writer.insert_protection_records(
                            db,
                            protection_records,
                        )
                        pending_created = 0
                    else:
                        protection_inserted = 0
                        pending_created = writer.insert_pending_protection_for_chunks(db)
                    erasure_packs_inserted = writer.insert_erasure_data_packs(db, erasure_data_packs)
                    erasure_chunks_inserted = sum(item.chunk_count for item in erasure_data_packs)
                    erasure_shards_inserted = sum(item.shard_count for item in erasure_data_packs)
                else:
                    protection_inserted = 0
                    pending_created = writer.insert_pending_protection_for_chunks(db)
                    erasure_packs_inserted = 0
                    erasure_chunks_inserted = 0
                    erasure_shards_inserted = 0

                self._verify_latest_digest(
                    expected_catalog_hash=latest.catalog_hash,
                    expected_state_digest=latest.state_digest,
                    expected_object_count=latest.object_count,
                    expected_total_bytes=latest.total_canonical_bytes,
                )
        finally:
            db.close()

        return MetadataObjectGraphImportResult(
            db_file=self.db_file,
            object_store_dir=self.object_store_dir,
            vault_id=latest.vault_id,
            catalog_hash=latest.catalog_hash,
            state_digest=latest.state_digest,
            stats=MetadataObjectGraphImportStats(
                objects_read=len(self.reader.object_hashes_read),
                total_canonical_bytes=self.reader.total_canonical_bytes,
                snapshots_imported=snapshot_stats.snapshots_imported,
                complete_snapshots_imported=snapshot_stats.complete_snapshots_imported,
                items_imported=snapshot_stats.items_imported,
                recipes_imported=snapshot_stats.recipes_created,
                recipes_reused=snapshot_stats.recipes_reused,
                chunks_imported=chunks_inserted,
                protection_records_imported=protection_inserted,
                pending_protection_records_created=pending_created,
                erasure_data_packs_imported=erasure_packs_inserted,
                erasure_pack_chunks_imported=erasure_chunks_inserted,
                erasure_pack_shards_imported=erasure_shards_inserted,
            ),
        )

    def _validate_latest_graph_semantics(self, latest) -> None:
        """Valida el grafo completo antes de abrir la transacción de reconstrucción."""

        try:
            objects = collect_reachable_object_bytes(
                catalog_hash=latest.catalog_hash,
                read_object_bytes=lambda object_hash: self.store.get_object_bytes(
                    object_hash=object_hash,
                ),
            )
            object_hashes = tuple(objects)
            digest = canonical_state_digest(latest.catalog_hash, object_hashes)
            if digest != latest.state_digest:
                raise MetadataObjectImportError(
                    "state_digest no coincide antes de reconstruir SQLite: "
                    f"esperado={latest.state_digest} calculado={digest}"
                )
            if len(objects) != latest.object_count:
                raise MetadataObjectImportError(
                    "object_count no coincide antes de reconstruir SQLite: "
                    f"latest={latest.object_count} alcanzables={len(objects)}"
                )
            total_bytes = sum(len(canonical) for canonical in objects.values())
            if total_bytes != latest.total_canonical_bytes:
                raise MetadataObjectImportError(
                    "canonical_bytes no coincide antes de reconstruir SQLite: "
                    f"latest={latest.total_canonical_bytes} alcanzables={total_bytes}"
                )

            payloads: dict[str, GraphObjectPayload] = {}
            for object_hash, canonical in objects.items():
                object_type, payload = decode_object_envelope(
                    canonical,
                    expected_hash=object_hash,
                )
                payloads[object_hash] = GraphObjectPayload(
                    object_type=object_type,
                    payload=payload,
                )

            validate_metadata_object_graph_semantics(
                catalog_hash=latest.catalog_hash,
                objects_by_hash=payloads,
                expected_snapshot_count=latest.snapshot_count,
                expected_known_chunk_count=latest.known_chunk_count,
                expected_protection_record_count=latest.protection_record_count,
            )
        except MetadataObjectImportError:
            raise
        except Exception as exc:
            raise MetadataObjectImportError(
                f"grafo de metadata no es semánticamente recuperable: {exc}"
            ) from exc

    def _verify_latest_digest(
        self,
        *,
        expected_catalog_hash: str,
        expected_state_digest: str,
        expected_object_count: int,
        expected_total_bytes: int,
    ) -> None:
        object_hashes = self.reader.object_hashes_read
        digest = canonical_state_digest(expected_catalog_hash, object_hashes)
        if digest != expected_state_digest:
            raise MetadataObjectImportError(
                "state_digest no coincide: "
                f"esperado={expected_state_digest} calculado={digest}"
            )
        if len(object_hashes) != int(expected_object_count):
            raise MetadataObjectImportError(
                "object_count no coincide: "
                f"latest={expected_object_count} alcanzables={len(object_hashes)}"
            )
        if self.reader.total_canonical_bytes != int(expected_total_bytes):
            raise MetadataObjectImportError(
                "canonical_bytes no coincide: "
                f"latest={expected_total_bytes} "
                f"alcanzables={self.reader.total_canonical_bytes}"
            )

    def _snapshot_entries(self, catalog: dict[str, Any]) -> list[dict[str, Any]]:
        snapshot_index_hash = require_ref(
            "catalog.snapshot_index",
            catalog.get("snapshot_index"),
            MetadataObjectType.SNAPSHOT_INDEX,
        )
        index = self.reader.payload(snapshot_index_hash, MetadataObjectType.SNAPSHOT_INDEX)
        snapshots = index.get("snapshots")
        if not isinstance(snapshots, list):
            raise MetadataObjectImportError("snapshot_index.snapshots debe ser lista")
        return snapshots

    def _load_chunks(self, catalog: dict[str, Any]) -> dict[str, int]:
        index_hash = require_ref(
            "catalog.known_chunk_index",
            catalog.get("known_chunk_index"),
            MetadataObjectType.KNOWN_CHUNK_INDEX,
        )
        index = self.reader.payload(index_hash, MetadataObjectType.KNOWN_CHUNK_INDEX)
        shards = index.get("shards")
        if not isinstance(shards, list):
            raise MetadataObjectImportError("known_chunk_index.shards debe ser lista")

        out: dict[str, int] = {}
        for shard_ref in shards:
            if not isinstance(shard_ref, dict):
                raise MetadataObjectImportError("known_chunk_index shard inválido")
            prefix = require_str("known_chunk_index.shard.prefix", shard_ref.get("prefix"))
            shard_hash = require_ref(
                "known_chunk_index.shard.ref",
                shard_ref.get("ref"),
                MetadataObjectType.KNOWN_CHUNK_SHARD,
            )
            shard = self.reader.payload(shard_hash, MetadataObjectType.KNOWN_CHUNK_SHARD)
            if shard.get("prefix") != prefix:
                raise MetadataObjectImportError("prefix de known_chunk shard no coincide")
            chunks = shard.get("chunks")
            if not isinstance(chunks, list):
                raise MetadataObjectImportError("known_chunk_shard.chunks debe ser lista")
            for record in chunks:
                if not isinstance(record, dict):
                    raise MetadataObjectImportError("known_chunk record inválido")
                chunk_hash = require_hash64("known_chunk.chunk_hash", record.get("chunk_hash"))
                size = require_int("known_chunk.size", record.get("size"), min_value=1)
                if not chunk_hash.startswith(prefix):
                    raise MetadataObjectImportError("known_chunk fuera de su shard")
                existing = out.get(chunk_hash)
                if existing is not None and existing != size:
                    raise MetadataObjectImportError(f"known_chunk duplicado con tamaño distinto: {chunk_hash}")
                out[chunk_hash] = size

        expected_total = require_int("known_chunk_index.total_chunks", index.get("total_chunks"), min_value=0)
        if len(out) != expected_total:
            raise MetadataObjectImportError(
                f"known_chunk_index total no coincide: total={expected_total} cargados={len(out)}"
            )
        self._chunk_sizes = dict(out)
        return out

    def _load_protection_records(self, catalog: dict[str, Any]) -> list[dict[str, Any]]:
        protection_index_hash = optional_ref(
            "catalog.protection_index",
            catalog.get("protection_index"),
            MetadataObjectType.PROTECTION_INDEX,
        )
        if protection_index_hash is None:
            return []

        index = self.reader.payload(protection_index_hash, MetadataObjectType.PROTECTION_INDEX)
        shards = index.get("shards")
        if not isinstance(shards, list):
            raise MetadataObjectImportError("protection_index.shards debe ser lista")

        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for shard_ref in shards:
            if not isinstance(shard_ref, dict):
                raise MetadataObjectImportError("protection_index shard inválido")
            prefix = require_str("protection_index.shard.prefix", shard_ref.get("prefix"))
            shard_hash = require_ref(
                "protection_index.shard.ref",
                shard_ref.get("ref"),
                MetadataObjectType.PROTECTION_SHARD,
            )
            shard = self.reader.payload(shard_hash, MetadataObjectType.PROTECTION_SHARD)
            if shard.get("prefix") != prefix:
                raise MetadataObjectImportError("prefix de protection shard no coincide")
            records = shard.get("records")
            if not isinstance(records, list):
                raise MetadataObjectImportError("protection_shard.records debe ser lista")
            for record in records:
                if not isinstance(record, dict):
                    raise MetadataObjectImportError("protection record inválido")
                chunk_hash = require_hash64("protection.chunk_hash", record.get("chunk_hash"))
                if not chunk_hash.startswith(prefix):
                    raise MetadataObjectImportError("protection record fuera de su shard")
                if chunk_hash in seen:
                    raise MetadataObjectImportError(f"protection record duplicado: {chunk_hash}")
                if chunk_hash not in self._chunk_sizes:
                    raise MetadataObjectImportError(f"chunk_protection referencia chunk inexistente: {chunk_hash}")
                seen.add(chunk_hash)
                out.append(record)

        expected_total = require_int("protection_index.total_records", index.get("total_records"), min_value=0)
        if len(out) != expected_total:
            raise MetadataObjectImportError(
                f"protection_index total no coincide: total={expected_total} cargados={len(out)}"
            )
        return out


    def _load_erasure_data_packs(self, catalog: dict[str, Any]) -> list[ErasureDataPackImportRows]:
        return load_erasure_data_pack_rows(
            reader=self.reader,
            catalog=catalog,
            chunk_sizes=self._chunk_sizes,
        )
