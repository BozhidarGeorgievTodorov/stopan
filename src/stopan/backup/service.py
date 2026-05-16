"""
Servicio principal de backup.

Coordina el recorrido del árbol de archivos, la creación del snapshot, el
procesamiento concurrente de archivos, la asignación de recipes y la exportación
opcional del grafo de metadata tras cerrar correctamente el snapshot.
"""

from __future__ import annotations

import concurrent.futures
import os
import time
from pathlib import Path

from .identity import resolve_origin_node_id
from .policy import build_backup_fast_path_policy
from .worker import process_file_worker
from stopan.chunking.chunk_index import ChunkIndex
from stopan.metadata.database import MetadataDB
from stopan.metadata.objects.graph.auto_export import (
    MetadataObjectGraphAutoExport,
    export_metadata_object_graph_after_metadata_change,
)
from stopan.scanning.scanner import TreeWalker
from stopan.errors import StopanStorageError, StopanUsageError


_DEFAULT_WORKERS_FALLBACK = 4


def backup_directory(
    source_path: str,
    *,
    num_threads: int,
    fast_local_enabled: bool,
    fast_remote_enabled: bool,
    safe_mode: bool,
    deterministic: bool,
    desired_rf: int,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    max_message_bytes: int,
    local_shard_dir: str,
    db_file: str,
    node_id_file: str,
    metadata_object_graph_auto_export: MetadataObjectGraphAutoExport | None = None,
) -> None:
    """
    Ejecuta un backup completo de source_path.

    Crea un snapshot en metadata, recorre el árbol de archivos, procesa archivos
    en paralelo, registra recipes y cierra el snapshot solo si todos los workers
    terminan correctamente.

    Si metadata_object_graph_auto_export está configurado, exporta el grafo de
    metadata después de confirmar el snapshot.
    """
    root_path = os.path.abspath(source_path)
    if not os.path.isdir(root_path):
        raise StopanUsageError(f"Directorio no encontrado: {root_path}")

    started_at = time.perf_counter()

    origin_node_id = resolve_origin_node_id(
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        node_id_file=node_id_file,
        membership_timeout_s=membership_timeout_s,
        max_message_bytes=max_message_bytes,
    )

    policy = build_backup_fast_path_policy(
        desired_rf=desired_rf,
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        membership_timeout_s=membership_timeout_s,
        max_message_bytes=max_message_bytes,
        fast_local_enabled=fast_local_enabled,
        fast_remote_enabled=fast_remote_enabled,
        safe_mode=safe_mode,
        origin_node_id=origin_node_id,
    )

    workers = _normalize_worker_count(num_threads)
    placement_epoch = policy.placement_epoch

    print(f"Iniciando backup de: {root_path}")
    print(f"Nodo origen: {origin_node_id[:8]}")
    print(f"Workers: {workers}")
    print(f"Copias remotas deseadas: {policy.desired_rf}")

    if placement_epoch and policy.fast_remote_enabled:
        print(f"Fast-path: local + chunks protegidos en remoto ({placement_epoch[:12]})")
    elif policy.fast_local_enabled:
        print("Fast-path: solo chunks locales")
    else:
        print("Fast-path: desactivado")

    db = MetadataDB(db_file)
    snapshot_id: int | None = None
    snapshot_completed = False

    try:
        walker = TreeWalker(root_path, deterministic=deterministic)
        snapshot_id = db.create_snapshot(root_path, origin_node_id=origin_node_id)
        previous_snapshot_id = db.get_prev_snapshot_id(root_path, snapshot_id)
        shared_index = ChunkIndex()

        total_files = 0
        total_size = 0
        total_chunks = 0
        total_processed = 0
        total_written = 0
        total_skipped = 0
        total_skipped_local = 0
        total_skipped_remote = 0

        max_pending_futures = max(1, workers * 3)
        future_map: dict = {}
        failed_files: list[tuple[str, str]] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:

            def collect_completed(*, wait_for_all: bool) -> None:
                nonlocal total_files, total_size
                nonlocal total_chunks, total_processed, total_written
                nonlocal total_skipped, total_skipped_local, total_skipped_remote

                if not future_map:
                    return

                done, _ = concurrent.futures.wait(
                    future_map.keys(),
                    return_when=(
                        concurrent.futures.ALL_COMPLETED
                        if wait_for_all
                        else concurrent.futures.FIRST_COMPLETED
                    ),
                )

                for future in done:
                    item_id, rel_path = future_map.pop(future)

                    try:
                        success, chunks_or_error, file_size, recipe_hash, stats = future.result()
                    except Exception as exc:
                        failed_files.append((rel_path, f"worker falló: {exc}"))
                        continue

                    if not success:
                        failed_files.append((rel_path, str(chunks_or_error)))
                        continue

                    recipe_id = db.get_or_create_recipe(
                        recipe_hash,
                        chunks_or_error,
                        desired_rf=policy.desired_rf,
                    )
                    db.set_item_recipe(item_id, recipe_id)

                    total_files += 1
                    total_size += file_size
                    total_chunks += stats.chunks_total
                    total_processed += stats.processed
                    total_written += stats.written
                    total_skipped += stats.skipped
                    total_skipped_local += stats.skipped_local
                    total_skipped_remote += stats.skipped_remote

            for rel_path, full_path, stat_info, item_type in walker.walk():
                item_id = db.add_item(snapshot_id, rel_path, stat_info, item_type)

                if item_type != "file":
                    continue

                if previous_snapshot_id is not None and not safe_mode:
                    previous_item = db.get_item_by_path(previous_snapshot_id, rel_path)
                    if previous_item is not None:
                        mtime_ns = getattr(
                            stat_info,
                            "st_mtime_ns",
                            int(stat_info.st_mtime * 1_000_000_000),
                        )
                        unchanged = (
                            previous_item["size"] == stat_info.st_size
                            and previous_item["mode"] == stat_info.st_mode
                            and previous_item["uid"] == getattr(stat_info, "st_uid", None)
                            and previous_item["gid"] == getattr(stat_info, "st_gid", None)
                            and previous_item["mtime_ns"] == mtime_ns
                            and previous_item["recipe_id"] is not None
                        )
                        if unchanged:
                            db.set_item_recipe(item_id, previous_item["recipe_id"])
                            db.ensure_recipe_protection(
                                previous_item["recipe_id"],
                                desired_rf=policy.desired_rf,
                            )
                            total_files += 1
                            total_size += stat_info.st_size
                            continue

                future = executor.submit(
                    process_file_worker,
                    full_path,
                    local_shard_dir=local_shard_dir,
                    db_file=db_file,
                    fast_local_enabled=policy.fast_local_enabled,
                    fast_remote_enabled=policy.fast_remote_enabled,
                    safe_mode=safe_mode,
                    shared_index=shared_index,
                    desired_rf=policy.desired_rf,
                    placement_epoch=placement_epoch,
                )
                future_map[future] = (item_id, rel_path)

                if len(future_map) >= max_pending_futures:
                    collect_completed(wait_for_all=False)

            if future_map:
                collect_completed(wait_for_all=True)

        if failed_files:
            preview = "; ".join(f"{path}: {error}" for path, error in failed_files[:5])
            if len(failed_files) > 5:
                preview += f"; ... (+{len(failed_files) - 5} más)"
            raise StopanStorageError(
                f"Backup incompleto: fallaron {len(failed_files)} archivo(s). {preview}"
            )

        db.finish_snapshot(snapshot_id, total_size, total_files)
        db.commit()
        snapshot_completed = True

    except KeyboardInterrupt:
        if snapshot_id is not None:
            try:
                db.fail_snapshot(snapshot_id, "backup interrumpido por el usuario")
                db.commit()
            except Exception:
                db.rollback()
        raise

    except Exception as exc:
        if snapshot_id is not None:
            try:
                db.fail_snapshot(snapshot_id, str(exc))
                db.commit()
            except Exception:
                db.rollback()
        raise

    finally:
        db.close()

    elapsed = time.perf_counter() - started_at
    speed = _format_speed(total_size, elapsed)

    print(f"Backup completado: snapshot {snapshot_id}")
    print(f"Archivos: {total_files}")
    print(f"Tamaño: {total_size} bytes")
    print(f"Tiempo: {elapsed:.2f} segundos")
    print(f"Velocidad: {speed}")
    print(f"Workers: {workers}")
    print(f"Chunks totales: {total_chunks}")
    print(f"Chunks procesados: {total_processed}")
    print(f"Chunks escritos: {total_written}")
    print(
        "Chunks saltados: "
        f"{total_skipped} "
        f"(local: {total_skipped_local}, remoto: {total_skipped_remote})"
    )

    if snapshot_completed and metadata_object_graph_auto_export is not None:
        export_metadata_object_graph_after_metadata_change(
            db_file=db_file,
            settings=metadata_object_graph_auto_export,
            context_label="BACKUP",
        )


def _normalize_worker_count(workers: int) -> int:
    cpu_count = os.cpu_count() or _DEFAULT_WORKERS_FALLBACK
    try:
        requested = int(workers)
    except (TypeError, ValueError):
        requested = _DEFAULT_WORKERS_FALLBACK

    return max(1, min(requested, cpu_count))


def _format_speed(total_size: int, elapsed: float) -> str:
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"
