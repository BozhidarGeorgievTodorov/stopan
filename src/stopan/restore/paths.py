from __future__ import annotations

import os
from dataclasses import dataclass


def safe_restore_path(base_dir: str, rel_path: str) -> str:
    if rel_path == ".":
        return base_dir

    normalized_rel = os.path.normpath(rel_path)
    full_path = os.path.abspath(os.path.join(base_dir, normalized_rel))
    base_dir_abs = os.path.abspath(base_dir)

    if os.path.commonpath([base_dir_abs, full_path]) != base_dir_abs:
        raise ValueError(f"Path escapes restore directory: {rel_path}")

    return full_path


@dataclass(frozen=True)
class RestorePaths:
    final_dir: str
    incomplete_dir: str

    @staticmethod
    def for_snapshot(base_output_dir: str, snapshot_uuid: str) -> "RestorePaths":
        final_dir = os.path.join(base_output_dir, f"snapshot_{snapshot_uuid}")
        return RestorePaths(final_dir=final_dir, incomplete_dir=final_dir + ".incomplete")
