from __future__ import annotations

import concurrent.futures
import os
import time

from stopan.backup.config import DB_FILE
from stopan.backup.identity import resolve_origin_node_id
from stopan.backup.policy import build_backup_fast_path_policy
from stopan.backup.worker import process_file_worker
from stopan.chunking.chunk_index import ChunkIndex
from stopan.metadata.database import MetadataDB
from stopan.scanning.scanner import TreeWalker

DEFAULT_WORKERS = 4


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
) -> bool:
    if fast_remote_enabled and safe_mode:
        raise ValueError("'--safe' and '--fast-remote' are incompatible.")

    if not os.path.isdir(source_path):
        print(f"Directory not found: {source_path}")
        return False

    start_time = time.perf_counter()
    root_path = os.path.abspath(source_path)
    origin_node_id = resolve_origin_node_id(membership_seed=membership_seed)

    policy = build_backup_fast_path_policy(
        desired_rf=desired_rf,
        fast_local_enabled=fast_local_enabled,
        fast_remote_enabled=fast_remote_enabled,
        safe_mode=safe_mode,
        membership_seed=membership_seed,
        origin_node_id=origin_node_id,
    )

    db = MetadataDB(DB_FILE)
    snapshot_id: int | None = None

    print(f"Starting backup for: {root_path}")
    print(f"Origin node: {origin_node_id[:8]}")
    print(f"Workers: {num_threads}")
    print(f"Replication factor: {policy.desired_rf}")
    if policy.fast_remote_enabled:
        print(f"Fast-path: local + remote protected chunks ({policy.placement_epoch[:12]})")
    elif policy.fast_local_enabled:
        print("Fast-path: local chunks only")
    else:
        print("Fast-path: off")

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
        errors: list[str] = []

        workers = _normalize_worker_count(num_threads)
        max_pending_futures = max(1, workers * 3)
        future_map: dict = {}

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
                        errors.append(f"{rel_path}: worker crashed: {exc}")
                        continue

                    if not success:
                        errors.append(f"{rel_path}: {chunks_or_error}")
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
                    total_processed += stats.chunks_processed
                    total_written += stats.chunks_written
                    total_skipped += stats.chunks_skipped
                    total_skipped_local += stats.chunks_skipped_local
                    total_skipped_remote += stats.chunks_skipped_remote

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
                    fast_local_enabled=policy.fast_local_enabled,
                    fast_remote_enabled=policy.fast_remote_enabled,
                    safe_mode=safe_mode,
                    shared_index=shared_index,
                    desired_rf=policy.desired_rf,
                    current_placement_epoch=policy.placement_epoch,
                )
                future_map[future] = (item_id, rel_path)

                if len(future_map) >= max_pending_futures:
                    collect_completed(wait_for_all=False)

            if future_map:
                collect_completed(wait_for_all=True)

        if errors:
            raise RuntimeError("Could not read all files: " + "; ".join(errors))

        db.finish_snapshot(snapshot_id, total_size, total_files)
        db.commit()

        elapsed = time.perf_counter() - start_time
        speed = _format_speed(total_size, elapsed)

        print(f"Backup completed: Snapshot {snapshot_id}")
        print(f"Files: {total_files}")
        print(f"Size: {total_size} bytes")
        print(f"Time: {elapsed:.2f} seconds")
        print(f"Speed: {speed}")
        print(f"Workers: {workers}")
        print(f"Chunks total: {total_chunks}")
        print(f"Chunks processed: {total_processed}")
        print(f"Chunks written: {total_written}")
        print(
            "Chunks skipped: "
            f"{total_skipped} "
            f"(local: {total_skipped_local}, remote: {total_skipped_remote})"
        )
        return True

    except Exception as exc:
        if snapshot_id is not None:
            try:
                db.fail_snapshot(snapshot_id, str(exc))
                db.commit()
            except Exception:
                db.rollback()
        print(f"Error creating backup: {exc}")
        return False

    finally:
        db.close()


def _normalize_worker_count(workers: int) -> int:
    cpu_count = os.cpu_count() or DEFAULT_WORKERS
    try:
        requested = int(workers)
    except (TypeError, ValueError):
        requested = DEFAULT_WORKERS

    return max(1, min(requested, cpu_count))


def _format_speed(total_size, elapsed):
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"
