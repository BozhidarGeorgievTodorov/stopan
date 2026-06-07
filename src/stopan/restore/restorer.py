"""
Reconstrucción de snapshots en disco.

SnapshotRestorer reconstruye la jerarquía de directorios y archivos a partir de
metadata y chunks raw obtenidos mediante ChunkFetchService. La escritura se hace
en un directorio .incomplete y cada archivo pasa primero por un .tmp.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

from stopan.cli.output import format_duration, format_speed
from stopan.metadata.database import MetadataDB
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.models import RestoreRunStats
from stopan.restore.paths import RestorePaths, safe_restore_path
from stopan.restore.prefetcher import OrderedBatchChunkPrefetcher
from stopan.errors import StopanStorageError
from stopan.restore.errors import RestoreDataError


RESTORE_PROGRESS_EVERY_ITEMS = 100


@dataclass(frozen=True, slots=True)
class RestoreResult:
    snapshot_id: int
    completed: bool
    already_restored: bool = False
    interrupted: bool = False
    processed_items: int = 0
    successful_items: int = 0
    final_dir: str | None = None
    work_dir: str | None = None
    error: str | None = None
    stats: RestoreRunStats | None = None


class SnapshotRestorer:
    """
    Restaura un snapshot completo en un directorio destino.

    El restorer no decide de dónde salen los chunks. Esa política queda en
    ChunkFetchService. Aquí solo se reconstruyen rutas, archivos, directorios y
    metadata del filesystem.
    """

    def __init__(
        self,
        *,
        db: MetadataDB,
        fetch_service: ChunkFetchService,
        base_output_dir: str,
        batch_target_parallelism: int,
        prefetch_window: int,
    ):
        self.db = db
        self.fetch_service = fetch_service
        self.base_output_dir = base_output_dir
        self.batch_target_parallelism = max(int(batch_target_parallelism), 1)
        self.prefetch_window = max(int(prefetch_window), 1)

    def restore(self, snapshot_id: int) -> RestoreResult:
        """
        Ejecuta el restore de snapshot_id.

        Si el directorio final ya existe, no sobrescribe. Si existe el directorio
        .incomplete, continúa trabajando sobre él para permitir reintentos.
        """
        started_at = time.perf_counter()
        stats = self.fetch_service.stats

        status, error = self.db.get_snapshot_status(snapshot_id)
        if status is None:
            message = f"No existe el Snapshot ID {snapshot_id}."
            print(f"{message}")
            return RestoreResult(snapshot_id=snapshot_id, completed=False, error=message, stats=stats)

        if status != "COMPLETE":
            message = f"Snapshot {snapshot_id} no es restaurable (status={status})."
            print(f"{message}")
            if error:
                print(f"   Motivo registrado: {error}")
                message = f"{message} {error}"
            return RestoreResult(snapshot_id=snapshot_id, completed=False, error=message, stats=stats)

        snapshot_uuid = self.db.get_snapshot_uuid(snapshot_id)
        if not snapshot_uuid:
            raise RestoreDataError("Snapshot sin UUID")

        paths = RestorePaths.for_snapshot(self.base_output_dir, snapshot_uuid)

        if os.path.exists(paths.final_dir):
            print(f"Snapshot {snapshot_id} ya restaurado en: {paths.final_dir}")
            return RestoreResult(
                snapshot_id=snapshot_id,
                completed=True,
                already_restored=True,
                final_dir=paths.final_dir,
                stats=stats,
            )

        items_gen = self.db.get_snapshot_items(snapshot_id)
        first_item = next(items_gen, None)
        if first_item is None:
            message = f"Snapshot {snapshot_id} está vacío."
            print(f"{message}")
            return RestoreResult(
                snapshot_id=snapshot_id,
                completed=False,
                work_dir=paths.incomplete_dir,
                error=message,
                stats=stats,
            )

        try:
            os.makedirs(paths.incomplete_dir, exist_ok=True)
        except OSError as exc:
            raise StopanStorageError(f"No se pudo preparar el directorio de restore {paths.incomplete_dir}: {exc}") from exc

        print(f"Restaurando Snapshot {snapshot_id} en '{paths.incomplete_dir}/'...")
        print(
            "Lectura por lotes: "
            f"target_parallelism={self.batch_target_parallelism} "
            f"window={self.prefetch_window}"
        )

        def iter_items():
            yield first_item
            yield from items_gen

        directories: list[tuple[str, dict]] = []
        current_tmp_path: str | None = None

        try:
            for item in iter_items():
                stats.processed_items += 1

                try:
                    full_path = safe_restore_path(paths.incomplete_dir, item["path"])
                except ValueError as exc:
                    print(f"   {exc}")
                    continue

                item_type = item.get("item_type")
                if item_type == "dir":
                    os.makedirs(full_path, exist_ok=True)
                    directories.append((full_path, item))
                    stats.directories_created += 1
                    stats.successful_items += 1
                    continue

                if item_type != "file":
                    print(f"   Tipo de item no soportado en {item['path']}: {item_type!r}")
                    continue

                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                if os.path.isdir(full_path):
                    print(f"   Se esperaba archivo pero existe directorio en {item['path']}")
                    stats.files_failed += 1
                    continue

                if stats.processed_items % RESTORE_PROGRESS_EVERY_ITEMS == 0:
                    self._print_progress(stats)

                current_tmp_path = full_path + ".tmp"
                success_file = True

                try:
                    chunk_hashes = list(self.db.get_item_chunks(item["id"]))
                    prefetcher = OrderedBatchChunkPrefetcher(
                        self.fetch_service,
                        target_parallelism=self.batch_target_parallelism,
                        window=self.prefetch_window,
                    )

                    with open(current_tmp_path, "wb") as handle:
                        for chunk_hash, raw_chunk in prefetcher.iter_raw_chunks(chunk_hashes):
                            try:
                                handle.write(raw_chunk)
                                stats.bytes_written += len(raw_chunk)
                            except Exception as exc:
                                print(
                                    f"   Error escribiendo chunk {chunk_hash[:8]} "
                                    f"de {item['path']}: {exc}"
                                )
                                success_file = False
                                break

                except Exception as exc:
                    print(f"   Error crítico en archivo {item['path']}: {exc}")
                    success_file = False

                if success_file:
                    try:
                        os.replace(current_tmp_path, full_path)
                        current_tmp_path = None
                        self.apply_item_metadata(full_path, item, is_dir=False)
                        stats.files_restored += 1
                        stats.successful_items += 1
                    except Exception as exc:
                        print(f"   Error finalizando archivo {item['path']}: {exc}")
                        if current_tmp_path and os.path.exists(current_tmp_path):
                            os.remove(current_tmp_path)
                        current_tmp_path = None
                        stats.files_failed += 1
                        print(
                            f"   Archivo {item['path']} no restaurado. "
                            "Se reintentará en la próxima ejecución."
                        )
                else:
                    if current_tmp_path and os.path.exists(current_tmp_path):
                        os.remove(current_tmp_path)
                    current_tmp_path = None
                    stats.files_failed += 1
                    print(f"   Archivo {item['path']} no restaurado. Se reintentará en la próxima ejecución.")

        except KeyboardInterrupt:
            print("\n\nRestauración cancelada (Ctrl+C).")
            if current_tmp_path and os.path.exists(current_tmp_path):
                os.remove(current_tmp_path)
                print("Limpieza de archivo temporal completada.")
            print(f"Progreso guardado en {paths.incomplete_dir}. Repite el comando para continuar.")
            return RestoreResult(
                snapshot_id=snapshot_id,
                completed=False,
                interrupted=True,
                processed_items=stats.processed_items,
                successful_items=stats.successful_items,
                work_dir=paths.incomplete_dir,
                error="restore interrumpido",
                stats=stats,
            )

        finally:
            directories.sort(key=lambda item: len(item[0]), reverse=True)
            for directory_path, item in directories:
                self.apply_item_metadata(directory_path, item, is_dir=True)

        if stats.successful_items == stats.processed_items and stats.processed_items > 0:
            try:
                os.replace(paths.incomplete_dir, paths.final_dir)
                print("-" * 40)
                print(f"Restauración completa del Snapshot {snapshot_id}.")
                print(f"Directorio final: {paths.final_dir}")
                self._print_summary(stats, elapsed=time.perf_counter() - started_at)
                return RestoreResult(
                    snapshot_id=snapshot_id,
                    completed=True,
                    processed_items=stats.processed_items,
                    successful_items=stats.successful_items,
                    final_dir=paths.final_dir,
                    stats=stats,
                )
            except OSError as exc:
                message = f"Error al renombrar carpeta final: {exc}"
                print(f"{message}")
                return RestoreResult(
                    snapshot_id=snapshot_id,
                    completed=False,
                    processed_items=stats.processed_items,
                    successful_items=stats.successful_items,
                    work_dir=paths.incomplete_dir,
                    error=message,
                    stats=stats,
                )

        print("-" * 40)
        print(f"Restauración incompleta: {stats.successful_items}/{stats.processed_items} ítems.")
        print(f"Carpeta de trabajo: {paths.incomplete_dir}")
        self._print_summary(stats, elapsed=time.perf_counter() - started_at)
        return RestoreResult(
            snapshot_id=snapshot_id,
            completed=False,
            processed_items=stats.processed_items,
            successful_items=stats.successful_items,
            work_dir=paths.incomplete_dir,
            error=f"restore incompleto: {stats.successful_items}/{stats.processed_items} items",
            stats=stats,
        )

    @staticmethod
    def _print_progress(stats: RestoreRunStats) -> None:
        print(
            "   progreso: "
            f"ítems={stats.processed_items} "
            f"archivos={stats.files_restored} "
            f"directorios={stats.directories_created} "
            f"fallidos={stats.files_failed} "
            f"bytes={stats.bytes_written}"
        )

    @staticmethod
    def _print_summary(stats: RestoreRunStats, *, elapsed: float | None = None) -> None:
        print(
            "Resumen: "
            f"ítems={stats.successful_items}/{stats.processed_items} | "
            f"archivos={stats.files_restored} | "
            f"directorios={stats.directories_created} | "
            f"archivos_fallidos={stats.files_failed} | "
            f"bytes={stats.bytes_written}"
        )
        if elapsed is not None:
            print(f"Tiempo: {format_duration(elapsed)}")
            print(f"Velocidad: {format_speed(stats.bytes_written, elapsed)}")
        print(
            "Chunks: "
            f"requested={stats.chunks_requested} | "
            f"local_cas={stats.chunks_from_local_cas} | "
            f"local_p2p_cas={stats.chunks_from_local_p2p_cas} | "
            f"remote_replication={stats.chunks_from_remote_replication} | "
            f"ec={stats.chunks_from_ec} | "
            f"failed={stats.chunks_failed}"
        )

    @staticmethod
    def apply_item_metadata(path: str, item: dict, *, is_dir: bool) -> None:
        """
        Restaura permisos y mtime del item.

        Los fallos no abortan el restore porque el contenido ya fue reconstruido.
        """
        kind = "directorio" if is_dir else "archivo"
        try:
            os.chmod(path, item["mode"])
            os.utime(path, (item["mtime"], item["mtime"]))
        except OSError:
            print(f"   No se pudieron restaurar permisos/fechas de {kind}: {item['path']}")

