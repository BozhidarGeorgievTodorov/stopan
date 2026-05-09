from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config
from stopan.cli.metadata_auto_export import (
    add_metadata_auto_export_args,
    build_metadata_object_graph_auto_export,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan push",
        description="Protección remota de chunks mediante HRW + inventario remoto + streaming.",
    )
    add_config_args(parser)

    parser.add_argument("--membership-seed", default=None, help="Seed de membership para obtener la vista del cluster elegible.")
    parser.add_argument("--rf", type=int, default=None, help="RF deseado para la protección remota.")
    parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución.")
    parser.add_argument("--target-parallelism", type=int, default=None, help="Número de targets procesados en paralelo.")
    parser.add_argument("--probe-batch-hashes", type=int, default=None, help="Hashes por probe de inventario remoto.")
    parser.add_argument("--stream-inflight", type=int, default=None, help="Ventana máxima de chunks en vuelo por stream.")
    parser.add_argument("--probe-timeout-s", type=float, default=None, help="Timeout del RPC ProbeMissingChunks en segundos.")
    parser.add_argument("--stream-timeout-s", type=float, default=None, help="Timeout del RPC ReplicateChunks en segundos.")
    parser.add_argument("--max-message-bytes", type=int, default=None, help="Límite de mensaje gRPC.")
    parser.add_argument("--commit-every", type=int, default=None, help="Persistir progreso cada N resultados.")
    parser.add_argument(
        "--strict-rf",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Exige al menos RF targets remotos elegibles antes de empezar. "
            "Si no hay capacidad remota suficiente, aborta con rc=2 sin marcar chunks."
        ),
    )
    add_metadata_auto_export_args(parser, context="push")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)
    metadata_object_graph_auto_export = build_metadata_object_graph_auto_export(args, cfg)

    from stopan.protection.pusher import push_to_network

    stats = push_to_network(
        membership_seed=args.membership_seed or first_seed(cfg),
        rf=int(choose(args.rf, cfg.protection.rf)),
        limit=args.limit,
        target_parallelism=int(choose(args.target_parallelism, cfg.replication.target_parallelism)),
        probe_batch_hashes=int(choose(args.probe_batch_hashes, cfg.replication.probe_batch_hashes)),
        stream_inflight=int(choose(args.stream_inflight, cfg.replication.stream_inflight)),
        probe_timeout_s=float(choose(args.probe_timeout_s, cfg.replication.probe_timeout_s)),
        stream_timeout_s=float(choose(args.stream_timeout_s, cfg.replication.stream_timeout_s)),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        commit_every=int(choose(args.commit_every, cfg.replication.commit_every)),
        strict_rf=bool(choose(args.strict_rf, cfg.protection.strict_rf)),
        db_file=cfg.node.db_file,
        local_shard_dir=cfg.node.local_shard_dir,
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
        membership_timeout_s=cfg.membership.rpc_timeout_s,
        metadata_object_graph_auto_export=metadata_object_graph_auto_export,
    )

    print("-" * 40)
    print(
        f"Push finalizado. protegidos={stats.protected}/{stats.attempted} | "
        f"fallidos={stats.failed} | stored_remote={stats.stored_remote} | "
        f"already_present_remote={stats.already_present_remote}"
    )

    if getattr(stats, "interrupted", False):
        print(f"Push interrumpido. Progreso persistido hasta attempted={stats.attempted}.")
        return 130

    if getattr(stats, "insufficient_remote_targets", False):
        print(
            "Push no iniciado por strict-rf. "
            f"desired_rf={stats.desired_rf} remote_candidates={stats.remote_candidates}"
        )
        return 2

    degraded = int(getattr(stats, "degraded", 0))
    return 0 if int(stats.failed) == 0 and degraded == 0 else 2
