"""
Servicio de alto nivel para metadata object graphs.

Exporta el estado operacional desde MetadataDB a un object store cifrado e
importa el último grafo cifrado para reconstruir una base de metadata vacía.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from stopan.errors import StopanUsageError
from stopan.metadata.database import MetadataDB, MetadataDBAccessMode
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.exchange.importer import (
    MetadataObjectGraphImporter,
    MetadataObjectGraphImportResult,
)
from stopan.metadata.objects.exchange.exporter import MetadataObjectGraphExporter
from stopan.metadata.objects.store import (
    LatestMetadataPointer,
    MetadataObjectStore,
    MetadataObjectStoreInspection,
    MetadataObjectStoreWriteResult,
    inspect_object_store_header,
    object_store_lock,
)


@dataclass(frozen=True, slots=True)
class MetadataObjectGraphExportStats:
    objects_total: int
    objects_written: int
    objects_reused: int
    total_canonical_bytes: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataObjectGraphExportResult:
    root_dir: Path
    vault_id: str
    catalog_hash: str
    state_digest: str
    stats: MetadataObjectGraphExportStats


class MetadataObjectGraphStoreService:
    """Fachada de export/import para metadata object graphs cifrados."""

    def __init__(self, *, db_file: str, scrypt_cost: ScryptCost):
        self.db_file = str(db_file)
        self.scrypt_cost = scrypt_cost

    def export_current_state(
        self,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes,
        include_protection: bool = True,
    ) -> MetadataObjectGraphExportResult:
        initializer = MetadataDB(
            self.db_file,
            access_mode=MetadataDBAccessMode.READ_WRITE,
        )
        try:
            initializer.get_or_create_vault_id()
        finally:
            initializer.close()

        db = MetadataDB(
            self.db_file,
            init_schema=False,
            access_mode=MetadataDBAccessMode.READ_ONLY,
        )
        try:
            with db.read_snapshot():
                graph = MetadataObjectGraphExporter(db).export_current_state(
                    include_protection=bool(include_protection),
                )
        finally:
            db.close()

        with object_store_lock(object_store_dir):
            store = MetadataObjectStore.open_or_create(
                object_store_dir,
                passphrase=passphrase,
                scrypt_cost=self.scrypt_cost,
            )
            write_result = store.put_graph(graph)

        return self._to_export_result(write_result)

    def inspect_store(
        self,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes | None = None,
        decrypt_latest: bool = False,
    ) -> MetadataObjectStoreInspection:
        if decrypt_latest:
            if passphrase is None:
                raise StopanUsageError("decrypt_latest requiere passphrase")
            store = MetadataObjectStore.open_existing(
                object_store_dir,
                passphrase=passphrase,
            )
            return store.inspect(decrypt_latest=True)

        header = inspect_object_store_header(object_store_dir)
        return MetadataObjectStoreInspection(header=header, latest=None)

    def read_latest(
        self,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes,
    ) -> LatestMetadataPointer:
        store = MetadataObjectStore.open_existing(
            object_store_dir,
            passphrase=passphrase,
        )
        return store.read_latest_pointer()

    def import_latest_state(
        self,
        *,
        object_store_dir: str | Path,
        passphrase: str | bytes,
        include_protection: bool = True,
        default_desired_rf: int = 1,
    ) -> MetadataObjectGraphImportResult:
        with object_store_lock(object_store_dir):
            importer = MetadataObjectGraphImporter(
                db_file=self.db_file,
                object_store_dir=object_store_dir,
                passphrase=passphrase,
                default_desired_rf=default_desired_rf,
            )
            return importer.import_latest(include_protection=bool(include_protection))

    def _to_export_result(
        self,
        result: MetadataObjectStoreWriteResult,
    ) -> MetadataObjectGraphExportResult:
        return MetadataObjectGraphExportResult(
            root_dir=result.root_dir,
            vault_id=result.vault_id,
            catalog_hash=result.catalog_hash,
            state_digest=result.state_digest,
            stats=MetadataObjectGraphExportStats(
                objects_total=result.objects_total,
                objects_written=result.objects_written,
                objects_reused=result.objects_reused,
                total_canonical_bytes=result.total_canonical_bytes,
                snapshot_count=result.snapshot_count,
                known_chunk_count=result.known_chunk_count,
                protection_record_count=result.protection_record_count,
            ),
        )
