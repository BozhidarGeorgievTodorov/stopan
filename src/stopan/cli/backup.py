from __future__ import annotations

import argparse
import os

from stopan.backup.config import DEFAULT_PROTECTION_RF
from stopan.backup.service import backup_directory


DEFAULT_WORKERS = 4


def parse_args():
    parser = argparse.ArgumentParser(
        prog="stopan backup",
        description="Crea un snapshot local de una carpeta.",
    )
    parser.add_argument("source_path", help="Carpeta origen a respaldar")
    parser.add_argument(
        "workers",
        nargs="?",
        type=int,
        default=DEFAULT_WORKERS,
        help="Número de hilos de trabajo",
    )
    parser.add_argument(
        "--fast",
        dest="fast_local",
        action="store_true",
        help="Salta chunks ya presentes localmente",
    )
    parser.add_argument(
        "--fast-remote",
        action="store_true",
        help="Permite saltar chunks con protección remota suficiente",
    )
    parser.add_argument(
        "--safe",
        dest="safe_mode",
        action="store_true",
        help="Procesa todos los chunks sin fast-path",
    )
    parser.add_argument(
        "--deterministic",
        action="store_true",
        help="Usa recorrido determinista del árbol",
    )
    parser.add_argument(
        "--rf",
        type=int,
        default=DEFAULT_PROTECTION_RF,
        help="Replication factor deseado",
    )
    parser.add_argument(
        "--seed",
        default=None,
        help="Seed de membership para fast-path remoto y resolución del nodo origen",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.fast_remote and args.safe_mode:
        raise ValueError("'--safe' and '--fast-remote' are incompatible.")

    fast_local_enabled = bool(args.fast_local)
    fast_remote_enabled = bool(args.fast_remote)

    if fast_remote_enabled and not fast_local_enabled:
        fast_local_enabled = True
        print("Remote fast-path enabled; local fast-path enabled implicitly.")

    max_workers = os.cpu_count() or DEFAULT_WORKERS
    workers = max(1, min(int(args.workers), max_workers))
    if args.workers > max_workers:
        print(
            f"Requested {args.workers} workers, but this CPU exposes {max_workers}. "
            f"Using {workers}."
        )

    ok = backup_directory(
        args.source_path,
        num_threads=workers,
        fast_local_enabled=fast_local_enabled,
        fast_remote_enabled=fast_remote_enabled,
        safe_mode=bool(args.safe_mode),
        deterministic=bool(args.deterministic),
        desired_rf=int(args.rf),
        membership_seed=args.seed,
    )
    return 0 if ok is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
