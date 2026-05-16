from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config
from stopan.cli.validation import require_int_at_least
from stopan.cli.metadata_auto_export import (
    add_metadata_auto_export_args,
    build_metadata_object_graph_auto_export,
)

_DEFAULT_WORKERS = 4


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan backup",
        allow_abbrev=False,
        description="Crea un snapshot haciendo backup de una carpeta.",
    )
    add_config_args(parser)

    parser.add_argument("source_path", help="Carpeta origen a respaldar")
    parser.add_argument(
        "workers",
        nargs="?",
        type=int,
        default=_DEFAULT_WORKERS,
        help="Número de hilos de trabajo",
    )
    parser.add_argument(
        "--fast",
        dest="fast_local",
        action="store_true",
        help="Activa fast-path local para saltar chunks ya presentes localmente.",
    )
    parser.add_argument(
        "--fast-remote",
        action="store_true",
        help="Permite saltar chunks ya protegidos remotamente si hay evidencia suficiente.",
    )
    parser.add_argument(
        "--safe",
        dest="safe_mode",
        action="store_true",
        help="Modo seguro: ignora fast-path y procesa todo.",
    )
    parser.add_argument(
        "--deterministic",
        action="store_true",
        help="Recorrido determinista del árbol.",
    )
    parser.add_argument(
        "--desired-remote-copies",
        type=int,
        default=None,
        help="Copias remotas deseadas que se guardan en metadata; backup no envía chunks a la red.",
    )
    parser.add_argument(
        "--membership-seed",
        default=None,
        help="Seed de membership para validar el epoch del fast-path remoto.",
    )
    add_metadata_auto_export_args(parser, context="backup")
    args = parser.parse_args(argv)

    require_int_at_least(parser, args.workers, flag="workers", min_value=1)
    require_int_at_least(
        parser,
        args.desired_remote_copies,
        flag="--desired-remote-copies",
        min_value=0,
    )

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)

    fast_local_enabled = bool(args.fast_local)
    fast_remote_enabled = bool(args.fast_remote)

    if args.safe_mode:
        fast_local_enabled = False
        fast_remote_enabled = False
    elif fast_remote_enabled and not fast_local_enabled:
        fast_local_enabled = True
        print("'--fast-remote' activa implícitamente '--fast'.")

    max_workers = os.cpu_count() or _DEFAULT_WORKERS
    workers = max(1, min(int(args.workers), max_workers))
    if args.workers > max_workers:
        print(
            f"Pediste {args.workers} hilos, pero la CPU expone {max_workers}. "
            f"Usando {workers}."
        )

    metadata_object_graph_auto_export = build_metadata_object_graph_auto_export(args, cfg)

    from stopan.backup.service import backup_directory

    result = backup_directory(
        args.source_path,
        num_threads=workers,
        fast_local_enabled=fast_local_enabled,
        fast_remote_enabled=fast_remote_enabled,
        safe_mode=bool(args.safe_mode),
        deterministic=bool(args.deterministic),
        desired_rf=int(choose(args.desired_remote_copies, cfg.protection.remote_copies)),
        membership_seed=args.membership_seed or first_seed(cfg),
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
        membership_timeout_s=cfg.membership.rpc_timeout_s,
        max_message_bytes=cfg.grpc.max_message_bytes,
        local_shard_dir=cfg.node.local_shard_dir,
        db_file=cfg.node.db_file,
        node_id_file=os.path.join(cfg.node.repo_store_dir, "node_id.txt"),
        metadata_object_graph_auto_export=metadata_object_graph_auto_export,
    )
    return 0 if result is not False else 1
