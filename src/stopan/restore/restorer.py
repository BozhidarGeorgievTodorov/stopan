"""
Reconstrucción de snapshots en disco.

SnapshotRestorer reconstruye la jerarquía de directorios y archivos a partir de
metadata y chunks raw obtenidos mediante ChunkFetchService. La escritura se hace
en un directorio .incomplete y cada archivo pasa primero por un temporal privado.
"""

from __future__ import annotations

import os
import stat
import tempfile
import time
from dataclasses import dataclass

from stopan.cli.output import format_duration, format_speed
from stopan.common.fs import atomic_rename_noreplace
from stopan.metadata.database import MetadataDB
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.models import RestoreRunStats
from stopan.restore.paths import RestorePaths, safe_restore_path, validate_restore_root
from stopan.restore.prefetcher import OrderedBatchChunkPrefetcher
from stopan.errors import StopanStorageError
from stopan.restore.errors import RestoreDataError, RestorePathError


RESTORE_PROGRESS_EVERY_ITEMS = 100


@dataclass(frozen=True, slots=True)
class RestoreResult:
    snapshot_id: int
    completed: bool
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

        Si el directorio final ya existe, rechaza la operación para no asumir que
        contiene una restauración válida. Si existe el directorio .incomplete,
        continúa trabajando sobre él para permitir reintentos.
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

        if os.path.lexists(paths.final_dir):
            message = (
                f"El destino final ya existe y no se sobrescribirá: {paths.final_dir}. "
                "Utiliza otro directorio base o retira el destino existente antes de reintentar."
            )
            print(message)
            return RestoreResult(
                snapshot_id=snapshot_id,
                completed=False,
                final_dir=paths.final_dir,
                error=message,
                stats=stats,
            )

        items_gen = self.db.iter_snapshot_restore_items(snapshot_id)
        first_entry = next(items_gen, None)
        if first_entry is None:
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
            validate_restore_root(paths.incomplete_dir)
            self._ensure_work_directory_permissions(paths.incomplete_dir)
        except (OSError, RestorePathError) as exc:
            raise StopanStorageError(
                f"No se pudo preparar el directorio de restore {paths.incomplete_dir}: {exc}"
            ) from exc

        print(f"Restaurando Snapshot {snapshot_id} en '{paths.incomplete_dir}/'...")
        print(
            "Lectura por lotes: "
            f"target_parallelism={self.batch_target_parallelism} "
            f"window={self.prefetch_window}"
        )

        def iter_items():
            yield first_entry
            yield from items_gen

        directories: list[tuple[str, dict]] = []
        prepared_dirs: set[str] = {paths.incomplete_dir}
        prefetcher = OrderedBatchChunkPrefetcher(
            self.fetch_service,
            target_parallelism=self.batch_target_parallelism,
            window=self.prefetch_window,
        )
        current_tmp_path: str | None = None
        try:
            temp_workspace = tempfile.TemporaryDirectory(
                prefix=f".stopan-restore-{snapshot_uuid[:12]}-",
                dir=os.path.dirname(paths.incomplete_dir),
                ignore_cleanup_errors=True,
            )
        except OSError as exc:
            raise StopanStorageError(
                f"No se pudo preparar el espacio temporal de restore junto a {paths.incomplete_dir}: {exc}"
            ) from exc

        try:
            for item, recipe_chunk_hashes in iter_items():
                stats.processed_items += 1

                try:
                    full_path = safe_restore_path(paths.incomplete_dir, item["path"])
                except RestorePathError as exc:
                    print(f"   {exc}")
                    continue

                item_type = item.get("item_type")
                if item_type == "dir":
                    os.makedirs(full_path, exist_ok=True)
                    self._ensure_work_directory_permissions(full_path)
                    prepared_dirs.add(full_path)
                    directories.append((full_path, item))
                    stats.directories_created += 1
                    stats.successful_items += 1
                    continue

                if item_type != "file":
                    print(f"   Tipo de item no soportado en {item['path']}: {item_type!r}")
                    continue

                parent_dir = os.path.dirname(full_path)
                if parent_dir not in prepared_dirs:
                    os.makedirs(parent_dir, exist_ok=True)
                    self._ensure_work_directory_permissions(parent_dir)
                    prepared_dirs.add(parent_dir)

                if os.path.isdir(full_path):
                    print(f"   Se esperaba archivo pero existe directorio en {item['path']}")
                    stats.files_failed += 1
                    continue

                if stats.processed_items % RESTORE_PROGRESS_EVERY_ITEMS == 0:
                    self._print_progress(stats)

                current_tmp_path = None
                success_file = True

                try:
                    recipe_id = item.get("recipe_id")
                    if recipe_id is None:
                        raise RestoreDataError(
                            f"Archivo {item['path']} sin receta asociada"
                        )
                    chunk_hashes = recipe_chunk_hashes
                    current_tmp_path = os.path.join(
                        temp_workspace.name,
                        f"{int(item['id'])}.tmp",
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
                        # La obtención de chunks puede ser larga. Revalida la ruta
                        # inmediatamente antes de publicar para detectar cambios
                        # simbólicos introducidos desde la resolución inicial.
                        full_path = safe_restore_path(paths.incomplete_dir, item["path"])
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
            temp_workspace.cleanup()

        if stats.successful_items == stats.processed_items and stats.processed_items > 0:
            if os.path.lexists(paths.final_dir):
                message = (
                    f"El destino final apareció durante la restauración y no se sobrescribirá: "
                    f"{paths.final_dir}. El resultado permanece en {paths.incomplete_dir}."
                )
                print(message)
                return RestoreResult(
                    snapshot_id=snapshot_id,
                    completed=False,
                    processed_items=stats.processed_items,
                    successful_items=stats.successful_items,
                    final_dir=paths.final_dir,
                    work_dir=paths.incomplete_dir,
                    error=message,
                    stats=stats,
                )

            directories.sort(key=lambda item: len(item[0]), reverse=True)
            for directory_path, item in directories:
                self.apply_item_metadata(directory_path, item, is_dir=True)

            try:
                atomic_rename_noreplace(paths.incomplete_dir, paths.final_dir)
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
            except FileExistsError:
                self._restore_work_directory_permissions(paths.incomplete_dir, directories)
                message = (
                    f"El destino final apareció durante la restauración y no se sobrescribirá: "
                    f"{paths.final_dir}. El resultado permanece en {paths.incomplete_dir}."
                )
                print(message)
                return RestoreResult(
                    snapshot_id=snapshot_id,
                    completed=False,
                    processed_items=stats.processed_items,
                    successful_items=stats.successful_items,
                    final_dir=paths.final_dir,
                    work_dir=paths.incomplete_dir,
                    error=message,
                    stats=stats,
                )
            except OSError as exc:
                self._restore_work_directory_permissions(paths.incomplete_dir, directories)
                message = f"Error al publicar la carpeta final: {exc}"
                print(message)
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
    def _ensure_work_directory_permissions(path: str) -> None:
        """
        Mantiene un directorio de ``.incomplete`` utilizable durante reintentos.

        Los metadatos definitivos de los directorios se aplican únicamente cuando
        todo el árbol está listo para publicarse. Una ejecución anterior puede
        haber dejado permisos restrictivos, por lo que al reanudar se recuperan
        temporalmente lectura, escritura y búsqueda para el propietario.
        """
        current_mode = stat.S_IMODE(os.stat(path, follow_symlinks=False).st_mode)
        work_mode = current_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        if work_mode != current_mode:
            os.chmod(path, work_mode)

    @classmethod
    def _restore_work_directory_permissions(
        cls,
        incomplete_dir: str,
        directories: list[tuple[str, dict]],
    ) -> None:
        """Devuelve el árbol incompleto a un estado reanudable tras fallar la publicación."""
        ordered_paths = [incomplete_dir]
        ordered_paths.extend(
            path
            for path, _item in sorted(directories, key=lambda item: len(item[0]))
            if path != incomplete_dir
        )
        for path in ordered_paths:
            try:
                cls._ensure_work_directory_permissions(path)
            except OSError:
                # El error de publicación es el resultado principal. Si tampoco se
                # pueden reabrir permisos, el siguiente reintento informará del
                # problema al preparar el directorio de trabajo.
                pass

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

