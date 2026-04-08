from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import os
import threading
import time
from dataclasses import dataclass

from core.chunk_index import ChunkIndex
from core.chunker import FileChunker
from core.database import MetadataDB
from core.planner import ChunkPlanner
from core.repository import CASRepository
from core.scanner import TreeWalker


AVG_CHUNK_SIZE = 1024 * 1024
MIN_CHUNK_SIZE = 512 * 1024
MAX_CHUNK_SIZE = 8 * 1024 * 1024
DEFAULT_WORKERS = 4
MAX_INDEX_ITEMS = 200_000

LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
DEFAULT_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")

_thread_state = threading.local()


@dataclass
class WorkerStats:
    chunks_total: int = 0
    chunks_processed: int = 0
    chunks_skipped: int = 0
    chunks_skipped_local: int = 0
    chunks_skipped_protected: int = 0
    chunks_written: int = 0


@dataclass(frozen=True)
class BackupPolicy:
    desired_rf: int
    fast_local_enabled: bool
    fast_remote_enabled: bool
    placement_epoch: str | None


def _get_worker_tools():
    if not hasattr(_thread_state, "chunker"):
        _thread_state.chunker = FileChunker(
            avg_chunk_size=AVG_CHUNK_SIZE,
            min_chunk_size=MIN_CHUNK_SIZE,
            max_chunk_size=MAX_CHUNK_SIZE,
        )
        _thread_state.repo = CASRepository(LOCAL_SHARD_DIR)
        _thread_state.db = MetadataDB(DB_FILE, init_schema=False)

    return _thread_state.chunker, _thread_state.repo, _thread_state.db


def _recipe_hash(chunks):
    """Calcula un hash estable para una receta ordenada de chunks."""
    digest = hashlib.sha256()
    for order, chunk_hash, chunk_size in chunks:
        digest.update(order.to_bytes(4, "big", signed=False))
        digest.update(bytes.fromhex(chunk_hash))
        digest.update(chunk_size.to_bytes(8, "big", signed=False))
    return digest.hexdigest()


def _resolve_membership_seed(explicit_seed=None):
    if explicit_seed:
        return explicit_seed.strip()

    seeds = [seed.strip() for seed in os.getenv("SEEDS", "").split(",") if seed.strip()]
    if seeds:
        return seeds[0]

    advertise_addr = os.getenv("ADVERTISE_ADDR", "").strip()
    return advertise_addr or None


def _placement_epoch_for(seed, desired_rf):
    if not seed:
        return None

    from core.cluster_view import ClusterMembershipClient

    self_addr = os.getenv("ADVERTISE_ADDR", "")
    cluster = ClusterMembershipClient(seed, self_addr=self_addr).get_cluster_view()
    if not cluster.members:
        return None

    return cluster.placement_epoch(
        desired_rf=max(int(desired_rf), 1),
        cluster_token=CLUSTER_TOKEN,
    )


def _build_policy(*, desired_rf, fast_enabled, fast_remote_enabled, safe_mode, membership_seed):
    desired_rf = max(int(desired_rf), 1)

    if safe_mode:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=False,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    fast_enabled = bool(fast_enabled or fast_remote_enabled)
    if not fast_enabled:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=False,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    if not fast_remote_enabled:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    seed = _resolve_membership_seed(membership_seed)
    if not seed:
        print("Remote fast-path requested, but no membership seed was found. Using local fast-path only.")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    try:
        placement_epoch = _placement_epoch_for(seed, desired_rf)
    except Exception as exc:
        print(f"Could not read membership view. Using local fast-path only: {exc}")
        placement_epoch = None

    return BackupPolicy(
        desired_rf=desired_rf,
        fast_local_enabled=True,
        fast_remote_enabled=placement_epoch is not None,
        placement_epoch=placement_epoch,
    )


def _process_file(
    full_path,
    *,
    policy,
    safe_mode,
    shared_index,
):
    chunker, repo, db = _get_worker_tools()
    planner = ChunkPlanner(
        repo,
        db,
        index=shared_index,
        fast_path_enabled=policy.fast_local_enabled,
        safe_mode=safe_mode,
        allow_remote_protected_skip=policy.fast_remote_enabled,
        desired_rf=policy.desired_rf,
        current_placement_epoch=policy.placement_epoch,
    )

    chunks = []
    total_size = 0
    stats = WorkerStats()

    with open(full_path, "rb") as handle:
        for order, (chunk_hash, chunk_data) in enumerate(chunker.chunk_stream(handle)):
            chunk_size = len(chunk_data)
            total_size += chunk_size
            stats.chunks_total += 1

            decision = planner.decide(chunk_hash)
            if decision == "process":
                stats.chunks_processed += 1
                if repo.put(chunk_hash, chunk_data):
                    stats.chunks_written += 1
                    shared_index.local_exists.set(chunk_hash, True)
            elif decision == "skip_local":
                stats.chunks_skipped += 1
                stats.chunks_skipped_local += 1
            elif decision == "skip_synced":
                stats.chunks_skipped += 1
                stats.chunks_skipped_protected += 1
            else:
                raise RuntimeError(f"Unknown planner decision: {decision}")

            chunks.append((order, chunk_hash, chunk_size))

    return chunks, total_size, _recipe_hash(chunks), stats


def backup(
    source_path,
    workers=DEFAULT_WORKERS,
    *,
    fast_path_enabled=False,
    fast_remote_enabled=False,
    safe_mode=False,
    deterministic=False,
    desired_rf=DEFAULT_RF,
    membership_seed=None,
):
    """Crea un snapshot de una carpeta."""
    if fast_remote_enabled and safe_mode:
        raise ValueError("'--safe' and '--fast-remote' are incompatible.")

    if not os.path.isdir(source_path):
        print(f"Directory not found: {source_path}")
        return False

    max_workers = _normalize_worker_count(workers)
    policy = _build_policy(
        desired_rf=desired_rf,
        fast_enabled=fast_path_enabled,
        fast_remote_enabled=fast_remote_enabled,
        safe_mode=safe_mode,
        membership_seed=membership_seed,
    )

    db = MetadataDB(DB_FILE)
    shared_index = ChunkIndex(max_items=MAX_INDEX_ITEMS)

    print(f"Starting backup for: {source_path}")
    print(f"Workers: {max_workers}")
    print(f"Replication factor: {policy.desired_rf}")
    if policy.fast_remote_enabled:
        print(f"Fast-path: local + remote protected chunks ({policy.placement_epoch[:12]})")
    elif policy.fast_local_enabled:
        print("Fast-path: local chunks only")
    else:
        print("Fast-path: off")

    start_time = time.perf_counter()
    snapshot_id = None

    try:
        root_path = os.path.abspath(source_path)
        snapshot_id = db.create_snapshot(root_path)
        previous_snapshot_id = db.get_prev_snapshot_id(root_path, snapshot_id)
        walker = TreeWalker(root_path, deterministic=deterministic)

        total_files = 0
        total_size = 0
        pending = {}
        errors = []
        stats_total = WorkerStats()
        max_pending = max_workers * 3

        def handle_finished(done_futures):
            nonlocal total_files, total_size, stats_total

            for future in done_futures:
                item_id, rel_path, file_size = pending.pop(future)

                try:
                    chunks, stored_size, recipe_hash, stats = future.result()
                except Exception as exc:
                    errors.append(f"{rel_path}: {exc}")
                    continue

                recipe_id = db.get_or_create_recipe(
                    recipe_hash,
                    chunks,
                    desired_rf=policy.desired_rf,
                )
                db.set_item_recipe(item_id, recipe_id)

                total_files += 1
                total_size += file_size or stored_size
                stats_total.chunks_total += stats.chunks_total
                stats_total.chunks_processed += stats.chunks_processed
                stats_total.chunks_skipped += stats.chunks_skipped
                stats_total.chunks_skipped_local += stats.chunks_skipped_local
                stats_total.chunks_skipped_protected += stats.chunks_skipped_protected
                stats_total.chunks_written += stats.chunks_written

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for rel_path, full_path, stat_info, item_type in walker.walk():
                item_id = db.add_item(snapshot_id, rel_path, stat_info, item_type)

                if item_type != "file":
                    continue

                if previous_snapshot_id is not None and not safe_mode:
                    previous_item = db.get_item_by_path(previous_snapshot_id, rel_path)
                    if previous_item is not None:
                        mtime_ns = getattr(stat_info, "st_mtime_ns", int(stat_info.st_mtime * 1_000_000_000))
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
                            db.ensure_recipe_protection(previous_item["recipe_id"], desired_rf=policy.desired_rf)
                            total_files += 1
                            total_size += stat_info.st_size
                            continue

                future = executor.submit(
                    _process_file,
                    full_path,
                    policy=policy,
                    safe_mode=safe_mode,
                    shared_index=shared_index,
                )
                pending[future] = (item_id, rel_path, stat_info.st_size)

                if len(pending) >= max_pending:
                    done, _ = concurrent.futures.wait(
                        pending.keys(),
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    handle_finished(done)

            if pending:
                done, _ = concurrent.futures.wait(
                    pending.keys(),
                    return_when=concurrent.futures.ALL_COMPLETED,
                )
                handle_finished(done)

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
        print(f"Chunks total: {stats_total.chunks_total}")
        print(f"Chunks processed: {stats_total.chunks_processed}")
        print(f"Chunks written: {stats_total.chunks_written}")
        print(
            "Chunks skipped: "
            f"{stats_total.chunks_skipped} "
            f"(local: {stats_total.chunks_skipped_local}, protected: {stats_total.chunks_skipped_protected})"
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


def _normalize_worker_count(workers):
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


def parse_args():
    parser = argparse.ArgumentParser(description="Sistema de backup con CAS, SQLite y nodos P2P.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="Crea un snapshot de una carpeta")
    backup_parser.add_argument("source_dir")
    backup_parser.add_argument("workers", nargs="?", type=int, default=DEFAULT_WORKERS)
    backup_parser.add_argument("--fast", action="store_true", help="Salta chunks ya presentes localmente")
    backup_parser.add_argument("--fast-remote", action="store_true", help="Permite saltar chunks con RF ya cubierto")
    backup_parser.add_argument("--safe", action="store_true", help="Procesa todos los chunks sin fast-path")
    backup_parser.add_argument("--deterministic", action="store_true", help="Ordena el recorrido del árbol")
    backup_parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor deseado")
    backup_parser.add_argument("--membership-seed", default=None, help="Nodo seed para obtener la vista de membership")

    return parser.parse_args()


def main():
    args = parse_args()

    if args.command != "backup":
        raise ValueError(f"Unsupported command: {args.command}")

    if args.fast_remote and args.safe:
        raise ValueError("'--safe' and '--fast-remote' are incompatible.")

    ok = backup(
        args.source_dir,
        args.workers,
        fast_path_enabled=args.fast,
        fast_remote_enabled=args.fast_remote,
        safe_mode=args.safe,
        deterministic=args.deterministic,
        desired_rf=args.rf,
        membership_seed=args.membership_seed,
    )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
