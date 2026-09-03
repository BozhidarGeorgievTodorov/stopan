"""
Auto-export del metadata object graph tras cambios de metadata.

Este hook se ejecuta después de una mutación ya confirmada en MetadataDB. Si el
export o auto-pack falla, la metadata operacional sigue persistida; el fallo
solo deja el object graph o el pack cifrado desactualizado.
"""

from __future__ import annotations

from dataclasses import dataclass
import sys
from pathlib import Path

from stopan.errors import (
    StopanConfigTypeError,
    StopanConfigValueError,
    StopanDataError,
)
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.progress import ProgressReporter


class MetadataAutoExportError(StopanDataError, RuntimeError):
    """Fallo al materializar el metadata object graph o el pack tras una mutación."""


@dataclass(frozen=True, slots=True)
class MetadataObjectGraphAutoExport:
    enabled: bool
    object_store_dir: str
    passphrase_file: str
    scrypt_cost: ScryptCost
    include_protection: bool = True
    auto_pack: bool = False
    pack_dir: str | None = None
    identity_file: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise StopanConfigTypeError("metadata auto-export enabled debe ser bool")
        if not isinstance(self.include_protection, bool):
            raise StopanConfigTypeError(
                "metadata auto-export include_protection debe ser bool"
            )
        if not isinstance(self.auto_pack, bool):
            raise StopanConfigTypeError("metadata auto-export auto_pack debe ser bool")
        if not isinstance(self.scrypt_cost, ScryptCost):
            raise StopanConfigTypeError(
                "metadata auto-export scrypt_cost debe ser ScryptCost"
            )

        if not self.enabled:
            return

        if (
            not isinstance(self.object_store_dir, str)
            or not self.object_store_dir.strip()
        ):
            raise StopanConfigValueError(
                "metadata auto-export requiere object_store_dir cuando está activado"
            )
        if (
            not isinstance(self.passphrase_file, str)
            or not self.passphrase_file.strip()
        ):
            raise StopanConfigValueError(
                "metadata auto-export requiere passphrase_file cuando está activado"
            )
        if self.pack_dir is not None and not isinstance(self.pack_dir, str):
            raise StopanConfigTypeError(
                "metadata auto-export pack_dir debe ser str o None"
            )
        if self.auto_pack and (
            not isinstance(self.identity_file, str)
            or not self.identity_file.strip()
        ):
            raise StopanConfigValueError("metadata auto-pack requiere identity_file")


@dataclass(frozen=True, slots=True)
class MetadataObjectGraphAutoExportResult:
    root_dir: Path
    catalog_hash: str
    state_digest: str
    objects_total: int
    objects_written: int
    objects_reused: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int
    pack_path: Path | None = None
    pack_hash: str | None = None
    objects_packed: int | None = None


def export_metadata_object_graph_after_metadata_change(
    *,
    db_file: str,
    settings: MetadataObjectGraphAutoExport | None,
    context_label: str,
    progress: ProgressReporter | None = None,
) -> MetadataObjectGraphAutoExportResult | None:
    if settings is None or not settings.enabled:
        return None

    if progress is not None:
        progress.start("Actualizando metadata")
    try:
        from stopan.metadata.identity.passphrase import read_passphrase_file
        from stopan.metadata.objects.service import MetadataObjectGraphStoreService

        passphrase = read_passphrase_file(settings.passphrase_file)
        service = MetadataObjectGraphStoreService(
            db_file=db_file,
            scrypt_cost=settings.scrypt_cost,
        )
        result = service.export_current_state(
            object_store_dir=settings.object_store_dir,
            passphrase=passphrase,
            include_protection=settings.include_protection,
        )
    except Exception as exc:
        if progress is not None:
            progress.finish()
        raise MetadataAutoExportError(
            f"{context_label} completado, pero falló el export del metadata "
            f"object graph cifrado: {exc}"
        ) from exc

    if progress is not None:
        progress.finish()

    print(f"Metadata object graph actualizado tras {context_label.lower()}")
    print(f"   object_store: {result.root_dir}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    stats = result.stats
    print(f"   objects_total: {stats.objects_total}")
    print(f"   objects_written: {stats.objects_written}")
    print(f"   objects_reused: {stats.objects_reused}")
    print(f"   snapshots: {stats.snapshot_count}")
    print(f"   known_chunks: {stats.known_chunk_count}")
    print(f"   protection_records: {stats.protection_record_count}")

    pack_path = None
    pack_hash = None
    objects_packed = None

    if settings.auto_pack:
        if progress is not None:
            progress.start("Creando metadata object pack")
        try:
            from stopan.metadata.packs.object_pack import MetadataObjectPackService

            pack_result = MetadataObjectPackService(
                scrypt_cost=settings.scrypt_cost,
            ).export_latest_pack(
                object_store_dir=settings.object_store_dir,
                passphrase=passphrase,
                identity_file=settings.identity_file or "",
                pack_dir=settings.pack_dir,
            )
            pack_path = pack_result.path
            pack_hash = pack_result.pack_hash
            objects_packed = pack_result.stats.objects_packed
        except Exception as exc:
            if progress is not None:
                progress.finish()
            raise MetadataAutoExportError(
                f"{context_label} completado y object graph actualizado, "
                f"pero falló metadata auto-pack: {exc}"
            ) from exc

        if progress is not None:
            progress.finish()

        print(f"Metadata object pack creado tras {context_label.lower()}")
        print(f"   path: {pack_result.path}")
        print(f"   pack_hash: {pack_result.pack_hash}")
        print(f"   objects_packed: {pack_result.stats.objects_packed}")

    return MetadataObjectGraphAutoExportResult(
        root_dir=Path(result.root_dir),
        catalog_hash=result.catalog_hash,
        state_digest=result.state_digest,
        objects_total=stats.objects_total,
        objects_written=stats.objects_written,
        objects_reused=stats.objects_reused,
        snapshot_count=stats.snapshot_count,
        known_chunk_count=stats.known_chunk_count,
        protection_record_count=stats.protection_record_count,
        pack_path=Path(pack_path) if pack_path is not None else None,
        pack_hash=pack_hash,
        objects_packed=objects_packed,
    )


def export_after_successful_metadata_change(
    *,
    metadata_changed: bool,
    db_file: str,
    settings: MetadataObjectGraphAutoExport | None,
    context_label: str,
    progress: ProgressReporter | None = None,
) -> MetadataObjectGraphAutoExportResult | None:
    if not metadata_changed or sys.exc_info()[0] is not None:
        return None
    return export_metadata_object_graph_after_metadata_change(
        db_file=db_file,
        settings=settings,
        context_label=context_label,
        progress=progress,
    )

