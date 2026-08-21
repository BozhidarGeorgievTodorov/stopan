"""
Reconstrucción de snapshots en disco.

SnapshotRestorer reconstruye la jerarquía de directorios y archivos a partir de
metadata y chunks raw obtenidos mediante ChunkFetchService. La escritura se hace
en un directorio .incomplete y cada archivo se construye en un parcial persistente
verificable por su receta antes de publicarse dentro del árbol de trabajo.
"""

from __future__ import annotations

import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import blake3

from stopan.cli.output import format_bytes, format_duration, format_speed
from stopan.common.fs import atomic_rename_noreplace, fsync_dir
from stopan.metadata.database import MetadataDB
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.models import RestoreRunStats
from stopan.restore.paths import RestorePaths, safe_restore_path, validate_restore_root
from stopan.restore.prefetcher import OrderedBatchChunkPrefetcher
from stopan.errors import StopanStorageError
from stopan.restore.errors import RestoreDataError, RestorePathError


RESTORE_PROGRESS_EVERY_ITEMS = 100
RESTORE_RESUME_SYNC_BYTES = 64 * 1024 * 1024


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


@dataclass(frozen=True, slots=True)
class _VerifiedPrefix:
    next_chunk_order: int
    chunks: int
    bytes: int


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
        contiene una restauración válida. Los reintentos verifican tanto archivos
        completos de ``.incomplete`` como prefijos persistidos por fragmentos.
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

        items_gen = self.db.get_snapshot_items(snapshot_id)
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

            os.makedirs(paths.resume_dir, mode=0o700, exist_ok=True)
            validate_restore_root(paths.resume_dir)
            os.chmod(paths.resume_dir, 0o700)
        except (OSError, RestorePathError) as exc:
            raise StopanStorageError(
                f"No se pudo preparar el estado de restore de {paths.incomplete_dir}: {exc}"
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
        current_resume_path: str | None = None

        try:
            for item in iter_items():
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

                success_file = True
                current_resume_path = paths.resume_file(str(item["path"]))

                try:
                    recipe_id = item.get("recipe_id")
                    if recipe_id is None:
                        raise RestoreDataError(
                            f"Archivo {item['path']} sin receta asociada"
                        )
                    recipe_id = int(recipe_id)
                    expected_chunk_count, expected_size = self.db.get_recipe_restore_summary(
                        recipe_id
                    )
                    item_size = int(item.get("size", -1))
                    if item_size != expected_size:
                        raise RestoreDataError(
                            f"Tamaño inconsistente para {item['path']}: "
                            f"item={item_size} receta={expected_size}"
                        )

                    if self._validate_completed_file(
                        full_path,
                        recipe_id=recipe_id,
                        expected_chunk_count=expected_chunk_count,
                        expected_size=expected_size,
                    ):
                        self._discard_resume_file(current_resume_path)
                        self.apply_item_metadata(full_path, item, is_dir=False)
                        stats.files_reused += 1
                        stats.files_restored += 1
                        stats.chunks_reused += expected_chunk_count
                        stats.bytes_reused += expected_size
                        stats.successful_items += 1
                        current_resume_path = None
                        continue

                    resume_existed = os.path.lexists(current_resume_path)
                    resume_name_synced = resume_existed
                    bytes_since_sync = 0
                    durable_checkpoints = expected_size >= RESTORE_RESUME_SYNC_BYTES

                    with self._open_resume_file(current_resume_path) as handle:
                        try:
                            prefix = self._verify_resume_prefix(
                                handle,
                                recipe_id=recipe_id,
                                expected_chunk_count=expected_chunk_count,
                                expected_size=expected_size,
                            )
                            if prefix.chunks > 0 or prefix.bytes > 0:
                                stats.chunks_reused += prefix.chunks
                                stats.bytes_reused += prefix.bytes
                            if resume_existed and (
                                prefix.chunks > 0 or expected_chunk_count == 0
                            ):
                                stats.files_resumed += 1

                            handle.seek(prefix.bytes)
                            next_order = prefix.next_chunk_order
                            records = self.db.iter_recipe_chunk_entries(
                                recipe_id,
                                start_order=next_order,
                            )
                            for chunk_order, chunk_hash, chunk_size, raw_chunk in (
                                prefetcher.iter_raw_chunk_records(records)
                            ):
                                if chunk_order != next_order:
                                    raise RestoreDataError(
                                        f"Orden de chunk inesperado en {item['path']}: "
                                        f"esperado={next_order} obtenido={chunk_order}"
                                    )
                                if len(raw_chunk) != chunk_size:
                                    raise RestoreDataError(
                                        f"Tamaño de chunk inconsistente en {item['path']}: "
                                        f"hash={chunk_hash[:8]} esperado={chunk_size} "
                                        f"obtenido={len(raw_chunk)}"
                                    )
                                written = handle.write(raw_chunk)
                                if written != len(raw_chunk):
                                    raise RestoreDataError(
                                        f"Escritura incompleta en {item['path']}: "
                                        f"hash={chunk_hash[:8]} esperado={len(raw_chunk)} "
                                        f"escrito={written}"
                                    )
                                stats.bytes_written += written
                                bytes_since_sync += written
                                next_order += 1

                                if (
                                    durable_checkpoints
                                    and bytes_since_sync >= RESTORE_RESUME_SYNC_BYTES
                                ):
                                    self._sync_resume_checkpoint(
                                        handle,
                                        paths.resume_dir,
                                        sync_parent=not resume_name_synced,
                                    )
                                    resume_name_synced = True
                                    bytes_since_sync = 0

                            handle.flush()
                            final_size = int(os.fstat(handle.fileno()).st_size)
                            if next_order != expected_chunk_count:
                                raise RestoreDataError(
                                    f"Receta incompleta para {item['path']}: "
                                    f"esperados={expected_chunk_count} procesados={next_order}"
                                )
                            if final_size != expected_size:
                                raise RestoreDataError(
                                    f"Tamaño final inconsistente para {item['path']}: "
                                    f"esperado={expected_size} obtenido={final_size}"
                                )

                            # En archivos grandes, el último tramo debe quedar tan
                            # durable como los checkpoints anteriores antes de mover
                            # el parcial fuera del espacio de reanudación.
                            if durable_checkpoints:
                                self._sync_resume_checkpoint(
                                    handle,
                                    paths.resume_dir,
                                    sync_parent=not resume_name_synced,
                                )
                                resume_name_synced = True
                        except KeyboardInterrupt:
                            # Ctrl+C es una interrupción ordenada: persiste también
                            # el último tramo, aunque todavía no alcance el umbral
                            # periódico. Ante un corte de energía se conservará, como
                            # mínimo, el último checkpoint que el kernel haya confirmado.
                            if int(os.fstat(handle.fileno()).st_size) > 0:
                                try:
                                    self._sync_resume_checkpoint(
                                        handle,
                                        paths.resume_dir,
                                        sync_parent=not resume_name_synced,
                                    )
                                except OSError as sync_exc:
                                    print(
                                        "   No se pudo sincronizar el último avance "
                                        f"de {item['path']}: {sync_exc}"
                                    )
                            raise

                except Exception as exc:
                    print(f"   Error crítico en archivo {item['path']}: {exc}")
                    success_file = False

                if success_file:
                    try:
                        # La obtención de chunks puede ser larga. Revalida la ruta
                        # inmediatamente antes de publicar para detectar cambios
                        # simbólicos introducidos desde la resolución inicial.
                        full_path = safe_restore_path(paths.incomplete_dir, item["path"])
                        os.replace(current_resume_path, full_path)
                        current_resume_path = None
                        self.apply_item_metadata(full_path, item, is_dir=False)
                        stats.files_restored += 1
                        stats.successful_items += 1
                    except Exception as exc:
                        print(f"   Error finalizando archivo {item['path']}: {exc}")
                        stats.files_failed += 1
                        print(
                            f"   Archivo {item['path']} no restaurado. "
                            "Se conservará su parcial verificable para el próximo intento."
                        )
                else:
                    stats.files_failed += 1
                    print(
                        f"   Archivo {item['path']} no restaurado. "
                        "Se conservará su parcial verificable para el próximo intento."
                    )
                current_resume_path = None

        except KeyboardInterrupt:
            print("\n\nRestauración cancelada (Ctrl+C).")
            print(
                "El progreso verificable por fragmentos se conserva para el próximo intento "
                f"junto a {paths.incomplete_dir}."
            )
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

        if stats.successful_items == stats.processed_items and stats.processed_items > 0:
            try:
                os.rmdir(paths.resume_dir)
            except FileNotFoundError:
                pass
            except OSError as exc:
                message = (
                    "No se pudo cerrar el estado de reanudación antes de publicar el restore: "
                    f"{exc}"
                )
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

    def _validate_completed_file(
        self,
        path: str,
        *,
        recipe_id: int,
        expected_chunk_count: int,
        expected_size: int,
    ) -> bool:
        """Comprueba si un archivo de staging coincide exactamente con su receta."""

        try:
            path_stat = os.lstat(path)
        except FileNotFoundError:
            return False
        except OSError:
            return False

        if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
            return False
        if int(path_stat.st_size) != expected_size:
            return False

        # Un intento anterior puede haber aplicado un modo sin lectura. Dentro
        # del staging se recupera temporalmente lectura para poder verificarlo.
        current_mode = stat.S_IMODE(path_stat.st_mode)
        if not current_mode & stat.S_IRUSR:
            try:
                os.chmod(path, current_mode | stat.S_IRUSR)
            except OSError:
                return False

        try:
            with self._open_regular_file(path, writable=False, create=False) as handle:
                verified_chunks = 0
                verified_bytes = 0
                for chunk_order, chunk_hash, chunk_size in self.db.iter_recipe_chunk_entries(
                    recipe_id
                ):
                    if chunk_order != verified_chunks:
                        raise RestoreDataError(
                            f"Receta no contigua: recipe_id={recipe_id} "
                            f"esperado={verified_chunks} obtenido={chunk_order}"
                        )
                    raw = self._read_exact(handle, chunk_size)
                    if len(raw) != chunk_size:
                        return False
                    if blake3.blake3(raw).hexdigest() != chunk_hash:
                        return False
                    verified_chunks += 1
                    verified_bytes += chunk_size

                if verified_chunks != expected_chunk_count:
                    raise RestoreDataError(
                        f"Número de chunks inconsistente en recipe_id={recipe_id}: "
                        f"esperado={expected_chunk_count} obtenido={verified_chunks}"
                    )
                if verified_bytes != expected_size:
                    raise RestoreDataError(
                        f"Tamaño de receta inconsistente en recipe_id={recipe_id}: "
                        f"esperado={expected_size} obtenido={verified_bytes}"
                    )
                return handle.read(1) == b""
        except (OSError, RestorePathError):
            return False

    def _verify_resume_prefix(
        self,
        handle: BinaryIO,
        *,
        recipe_id: int,
        expected_chunk_count: int,
        expected_size: int,
    ) -> _VerifiedPrefix:
        """Conserva solo el prefijo del parcial demostrable mediante la receta."""

        file_size = int(os.fstat(handle.fileno()).st_size)
        handle.seek(0)
        verified_chunks = 0
        verified_bytes = 0

        for chunk_order, chunk_hash, chunk_size in self.db.iter_recipe_chunk_entries(recipe_id):
            if chunk_order != verified_chunks:
                raise RestoreDataError(
                    f"Receta no contigua: recipe_id={recipe_id} "
                    f"esperado={verified_chunks} obtenido={chunk_order}"
                )
            if verified_bytes + chunk_size > file_size:
                break

            raw = self._read_exact(handle, chunk_size)
            if len(raw) != chunk_size:
                break
            if blake3.blake3(raw).hexdigest() != chunk_hash:
                break

            verified_chunks += 1
            verified_bytes += chunk_size
            if verified_chunks == expected_chunk_count:
                break

        if verified_chunks > expected_chunk_count or verified_bytes > expected_size:
            raise RestoreDataError(
                f"Prefijo fuera de la receta: recipe_id={recipe_id} "
                f"chunks={verified_chunks}/{expected_chunk_count} "
                f"bytes={verified_bytes}/{expected_size}"
            )

        # Cualquier cola no verificada, incluido un chunk escrito a medias tras
        # un corte de energía, se descarta antes de continuar.
        handle.seek(verified_bytes)
        handle.truncate(verified_bytes)

        return _VerifiedPrefix(
            next_chunk_order=verified_chunks,
            chunks=verified_chunks,
            bytes=verified_bytes,
        )

    @staticmethod
    def _read_exact(handle: BinaryIO, size: int) -> bytes:
        """Lee hasta size bytes o EOF sin asumir que read() complete la petición."""

        remaining = int(size)
        parts: list[bytes] = []
        while remaining > 0:
            block = handle.read(remaining)
            if not block:
                break
            parts.append(block)
            remaining -= len(block)
        if not parts:
            return b""
        if len(parts) == 1:
            return parts[0]
        return b"".join(parts)

    @staticmethod
    def _open_regular_file(
        path: str,
        *,
        writable: bool,
        create: bool,
    ) -> BinaryIO:
        """Abre un archivo regular sin seguir symlinks cuando la plataforma lo permite."""

        flags = os.O_RDWR if writable else os.O_RDONLY
        if create:
            flags |= os.O_CREAT
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow:
            flags |= nofollow
        elif os.path.lexists(path):
            existing = os.lstat(path)
            if stat.S_ISLNK(existing.st_mode):
                raise RestorePathError(f"El parcial de restore no puede ser un enlace: {path}")

        fd = os.open(path, flags, 0o600)
        try:
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise RestorePathError(f"El estado de restore no es un archivo regular: {path}")
            if opened.st_nlink != 1:
                raise RestorePathError(
                    f"El estado de restore tiene enlaces adicionales y no es reutilizable: {path}"
                )
            if writable:
                try:
                    os.fchmod(fd, 0o600)
                except AttributeError:
                    os.chmod(path, 0o600)
            return os.fdopen(fd, "r+b" if writable else "rb")
        except Exception:
            os.close(fd)
            raise

    @classmethod
    def _open_resume_file(cls, path: str) -> BinaryIO:
        handle = cls._open_regular_file(path, writable=True, create=True)
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise RestorePathError(
                f"Otro proceso está utilizando el parcial de restore: {path}"
            ) from exc
        except ImportError as exc:
            handle.close()
            raise RestorePathError(
                "La plataforma no dispone de un bloqueo de archivo compatible con "
                "la reanudación segura del restore"
            ) from exc
        except Exception:
            handle.close()
            raise
        return handle

    @staticmethod
    def _sync_resume_checkpoint(
        handle: BinaryIO,
        resume_dir: str,
        *,
        sync_parent: bool,
    ) -> None:
        """Hace durable un checkpoint del parcial sin mantener metadata adicional."""

        handle.flush()
        os.fsync(handle.fileno())
        if sync_parent:
            fsync_dir(Path(resume_dir), strict=True)

    @staticmethod
    def _discard_resume_file(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

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
        suffix = ""
        if stats.bytes_reused:
            suffix = f" reutilizados={stats.bytes_reused}"
        print(
            "   progreso: "
            f"ítems={stats.processed_items} "
            f"archivos={stats.files_restored} "
            f"directorios={stats.directories_created} "
            f"fallidos={stats.files_failed} "
            f"bytes={stats.bytes_written}{suffix}"
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
        if stats.files_reused or stats.files_resumed or stats.chunks_reused:
            print(
                "Reanudación: "
                f"archivos_reutilizados={stats.files_reused} | "
                f"archivos_reanudados={stats.files_resumed} | "
                f"chunks_reutilizados={stats.chunks_reused} | "
                f"bytes_reutilizados={format_bytes(stats.bytes_reused)}"
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

