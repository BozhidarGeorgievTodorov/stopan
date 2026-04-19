from __future__ import annotations

import argparse

from stopan.restore.config import (
    DEFAULT_BATCH_TARGET_PARALLELISM,
    DEFAULT_PREFETCH_WINDOW,
    DEFAULT_RF,
    DEFAULT_SEED,
)
from stopan.restore.service import restore_snapshot


def parse_args():
    parser = argparse.ArgumentParser(
        prog="stopan restore",
        description="Restaura snapshots desde el CAS local y la red P2P bajo demanda.",
    )
    parser.add_argument("snapshot_id", type=int, help="ID del snapshot a restaurar")
    parser.add_argument("out", nargs="?", default="restore_out", help="Directorio base de salida")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Seed de membership para recuperación remota")
    parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor usado para calcular targets HRW")
    parser.add_argument(
        "--prefetch-window",
        type=int,
        default=DEFAULT_PREFETCH_WINDOW,
        help="Número de chunks resueltos por ventana",
    )
    parser.add_argument(
        "--batch-target-parallelism",
        type=int,
        default=DEFAULT_BATCH_TARGET_PARALLELISM,
        help="Máximo de targets remotos consultados en paralelo por ronda HRW",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    restore_snapshot(
        args.snapshot_id,
        seed=args.seed,
        base_output_dir=args.out,
        rf=int(args.rf),
        batch_target_parallelism=int(args.batch_target_parallelism),
        prefetch_window=int(args.prefetch_window),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
