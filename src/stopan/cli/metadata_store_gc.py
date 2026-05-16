from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime

from stopan.cli.config_utils import add_config_args, choose, load_runtime_config
from stopan.cli.metadata_helpers import format_bytes
from stopan.metadata.packs.distributed_store import MetadataPackStore


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan metadata-store-gc",
        description=(
            "Mantenimiento local del distributed metadata pack store. "
            "Pensado para ejecución manual o systemd timer."
        ),
    )
    add_config_args(parser)
    parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio del distributed metadata pack store. Default: metadata.distributed_pack_store_dir.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help=(
            "Sobrescribe gc.distributed_pack_max_age_days para esta ejecución. "
            "0 desactiva borrado por edad."
        ),
    )
    apply_group = parser.add_mutually_exclusive_group()
    apply_group.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Muestra qué se podaría sin borrar nada. Es el default.",
    )
    apply_group.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Ejecuta el borrado real.",
    )
    return parser.parse_args(argv)


def _format_time(ts: float | None) -> str:
    if ts is None:
        return "disabled"
    return datetime.fromtimestamp(float(ts)).isoformat(timespec="seconds")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)

    root_dir = args.pack_store or cfg.metadata.distributed_pack_store_dir
    max_age_days = int(choose(args.max_age_days, cfg.gc.distributed_pack_max_age_days))

    store = MetadataPackStore(
        root_dir,
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        max_packs_per_owner=int(cfg.metadata.max_distributed_packs_per_owner),
        max_total_bytes_per_owner=int(cfg.metadata.max_distributed_pack_bytes_per_owner),
        max_total_store_bytes=int(cfg.metadata.max_distributed_pack_store_bytes),
        max_age_days=max_age_days,
    )
    result = store.prune_to_limits(dry_run=bool(args.dry_run))

    print("Distributed metadata pack store GC")
    print(f"   pack_store: {result.root_dir}")
    print(f"   dry_run: {result.dry_run}")
    print(f"   max_age_days: {result.max_age_days}")
    print(f"   age_cutoff: {_format_time(result.cutoff_unix)}")
    print("Limits")
    print(f"   max_pack_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes))}")
    print(f"   max_packs_per_owner: {int(cfg.metadata.max_distributed_packs_per_owner)}")
    print(f"   max_bytes_per_owner: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes_per_owner))}")
    print(f"   max_store_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_store_bytes))}")
    print("Scan")
    print(f"   packs_seen: {result.packs_seen}")
    print(f"   owners_seen: {result.owners_seen}")
    print("Prune")
    print(f"   expired_packs: {result.expired_packs}")
    print(f"   quota_packs: {result.quota_packs}")
    print(f"   pruned_packs: {result.pruned_packs}")
    print(f"   pruned_bytes: {format_bytes(result.pruned_bytes)}")

    if result.dry_run:
        print("\nDry-run activo: no se borró nada. Usa --apply para ejecutar el borrado real.")

    return 0
