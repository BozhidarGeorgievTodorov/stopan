from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan restore",
        description="Restaura snapshots desde CAS local y red bajo demanda.",
    )
    add_config_args(parser)

    parser.add_argument("snapshot_id", type=int, help="ID del snapshot a restaurar.")
    parser.add_argument("--out", default="restore_out", help="Directorio base de salida.")
    parser.add_argument(
        "--remote-recovery",
        choices=("none", "replication", "ec", "auto"),
        default="replication",
        help=(
            "Modo de recuperación remota: none solo usa stores locales. "
            "replication recupera chunks completos; ec reconstruye desde data packs EC. "
            "auto prueba replication y después EC."
        ),
    )
    parser.add_argument(
        "--rf", 
        type=int, 
        default=None, 
        help="Replication factor HRW para recuperación por chunks completos."
    )
    parser.add_argument(
        "--membership-seed", 
        default=None, 
        help="Seed de membership para unirse a la red y buscar nodos (SWIM)."
    )
    parser.add_argument("--prefetch-window", type=int, default=None, help="Número de chunks a resolver por ventana de batch read.")
    parser.add_argument(
        "--batch-target-parallelism",
        type=int,
        default=None,
        help="Número máximo de targets remotos consultados en paralelo por ronda HRW.",
    )
    args = parser.parse_args(argv)

    if args.remote_recovery in {"none", "ec"}:
        if args.rf is not None:
            parser.error("--rf solo aplica a --remote-recovery replication|auto")

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)

    from stopan.restore.service import restore_snapshot

    uses_replication = args.remote_recovery in {"replication", "auto"}
    uses_network = args.remote_recovery != "none"

    result = restore_snapshot(
        args.snapshot_id,
        membership_seed=(args.membership_seed or first_seed(cfg)) if uses_network else None,
        base_output_dir=args.out,
        rf=int(choose(args.rf, cfg.protection.rf)) if uses_replication else 0,
        batch_target_parallelism=int(choose(args.batch_target_parallelism, cfg.restore.batch_target_parallelism)),
        prefetch_window=int(choose(args.prefetch_window, cfg.restore.prefetch_window)),
        db_file=cfg.node.db_file,
        local_shard_dir=cfg.node.local_shard_dir,
        repo_store_dir=cfg.node.repo_store_dir,
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
        membership_timeout_s=cfg.membership.rpc_timeout_s,
        rpc_timeout_s=cfg.restore.rpc_timeout_s,
        max_message_bytes=cfg.grpc.max_message_bytes,
        max_chunk_size=cfg.storage.max_chunk_size,
        remote_recovery=args.remote_recovery,
    )

    if getattr(result, "interrupted", False):
        return 130
    return 0 if getattr(result, "completed", True) else 2
