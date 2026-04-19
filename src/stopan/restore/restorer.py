from __future__ import annotations

import os

from stopan.metadata.database import MetadataDB
from stopan.restore.fetch import ChunkFetchService
from stopan.restore.paths import RestorePaths, safe_restore_path
from stopan.restore.prefetcher import OrderedBatchChunkPrefetcher


class SnapshotRestorer:
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

    def restore(self, snapshot_id: int) -> None:
        status, error = self.db.get_snapshot_status(snapshot_id)
        if status is None:
            print(f"Snapshot ID {snapshot_id} does not exist.")
            return

        if status != "COMPLETE":
            print(f"Snapshot {snapshot_id} is not restorable (status={status}).")
            if error:
                print(f"Recorded error: {error}")
            return

        snapshot_uuid = self.db.get_snapshot_uuid(snapshot_id)
        if not snapshot_uuid:
            raise RuntimeError("Snapshot has no UUID.")

        paths = RestorePaths.for_snapshot(self.base_output_dir, snapshot_uuid)

        if os.path.exists(paths.final_dir):
            print(f"Snapshot {snapshot_id} is already restored at: {paths.final_dir}")
            return

        os.makedirs(paths.incomplete_dir, exist_ok=True)
        print(f"Restoring snapshot {snapshot_id} into {paths.incomplete_dir}")
        print(
            f"Batch read: target_parallelism={self.batch_target_parallelism} "
            f"window={self.prefetch_window}"
        )

        items_gen = self.db.get_snapshot_items(snapshot_id)
        first_item = next(items_gen, None)
        if first_item is None:
            print(f"Snapshot {snapshot_id} is empty.")
            return

        def iter_items():
            yield first_item
            yield from items_gen

        directories: list[tuple[str, dict]] = []
        current_tmp_path: str | None = None
        processed_items = 0
        successful_items = 0

        try:
            for item in iter_items():
                processed_items += 1

                try:
                    full_path = safe_restore_path(paths.incomplete_dir, item["path"])
                except ValueError as exc:
                    print(f"Skipping unsafe path: {exc}")
                    continue

                if item["item_type"] == "dir":
                    os.makedirs(full_path, exist_ok=True)
                    directories.append((full_path, item))
                    successful_items += 1
                    continue

                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                if os.path.isdir(full_path):
                    print(f"Expected file but found directory at {item['path']}")
                    continue

                print(f"Restoring item {processed_items}: {item['path']}")
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
                            except Exception as exc:
                                print(f"Could not write chunk {chunk_hash[:8]} for {item['path']}: {exc}")
                                success_file = False
                                break

                except Exception as exc:
                    print(f"Could not restore file {item['path']}: {exc}")
                    success_file = False

                if success_file:
                    os.replace(current_tmp_path, full_path)
                    current_tmp_path = None
                    self.apply_item_metadata(full_path, item, is_dir=False)
                    successful_items += 1
                else:
                    if current_tmp_path and os.path.exists(current_tmp_path):
                        os.remove(current_tmp_path)
                    current_tmp_path = None
                    print(f"File {item['path']} was not restored. It can be retried later.")

        except KeyboardInterrupt:
            print("\nRestore interrupted by user.")
            if current_tmp_path and os.path.exists(current_tmp_path):
                os.remove(current_tmp_path)
                print("Temporary file removed.")
            print(f"Partial restore kept at: {paths.incomplete_dir}")
            return

        finally:
            directories.sort(key=lambda item: len(item[0]), reverse=True)
            for directory_path, item in directories:
                self.apply_item_metadata(directory_path, item, is_dir=True)

        if successful_items == processed_items and processed_items > 0:
            try:
                os.replace(paths.incomplete_dir, paths.final_dir)
                print("-" * 40)
                print(f"Restore completed for snapshot {snapshot_id}.")
                print(f"Final directory: {paths.final_dir}")
            except OSError as exc:
                print(f"Could not rename final directory: {exc}")
        else:
            print("-" * 40)
            print(f"Restore incomplete: {successful_items}/{processed_items} items restored.")
            print(f"Work directory: {paths.incomplete_dir}")

    @staticmethod
    def apply_item_metadata(path: str, item: dict, *, is_dir: bool) -> None:
        kind = "directory" if is_dir else "file"
        try:
            os.chmod(path, item["mode"])
            os.utime(path, (item["mtime"], item["mtime"]))
        except OSError:
            print(f"Could not restore metadata for {kind}: {item['path']}")
