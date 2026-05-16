from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config
from stopan.cli.metadata_auto_export import (
    add_metadata_auto_export_args,
    build_metadata_object_graph_auto_export,
)
from stopan.config.defaults import DEFAULT_EC_PACK_SIZE_BYTES


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan push",
        description="Protección remota de chunks mediante HRW + inventario remoto + streaming.",
    )
    add_config_args(parser)

    parser.add_argument("--membership-seed", default=None, help="Seed de membership para obtener la vista del cluster elegible.")
    parser.add_argument(
        "--protection-mode",
        choices=("replication", "ec"),
        default="replication",
        help="Modo de protección remota: replication mantiene copias completas por chunk; ec usa data packs con erasure coding.",
    )
    parser.add_argument("--remote-copies", type=int, default=None, help="Copias remotas completas requeridas por chunk en modo replication. La copia local no cuenta.")
    parser.add_argument("--ec-k", type=int, default=None, help="Número de data shards por data pack EC.")
    parser.add_argument("--ec-m", type=int, default=None, help="Número de parity shards por data pack EC.")
    parser.add_argument("--ec-pack-size-bytes", type=int, default=None, help="Tamaño objetivo máximo del payload de cada data pack EC.")
    parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución.")
    parser.add_argument("--target-parallelism", type=int, default=None, help="Número de targets procesados en paralelo.")
    parser.add_argument("--probe-batch-hashes", type=int, default=None, help="Hashes por probe de inventario remoto.")
    parser.add_argument("--stream-inflight", type=int, default=None, help="Ventana máxima de chunks en vuelo por stream.")
    parser.add_argument("--probe-timeout-s", type=float, default=None, help="Timeout del RPC ProbeMissingChunks en segundos.")
    parser.add_argument("--stream-timeout-s", type=float, default=None, help="Timeout del RPC ReplicateChunks en segundos.")
    parser.add_argument("--max-message-bytes", type=int, default=None, help="Límite de mensaje gRPC.")
    parser.add_argument("--commit-every", type=int, default=None, help="Persistir progreso cada N resultados.")
    parser.add_argument(
        "--strict-remote-copies",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Exige suficientes targets remotos elegibles antes de empezar. "
            "Si no hay capacidad remota suficiente, aborta con rc=2 sin marcar chunks."
        ),
    )
    add_metadata_auto_export_args(parser, context="push")
    
    args = parser.parse_args(argv)

    if args.protection_mode == "ec":
        if args.remote_copies is not None:
            parser.error("--remote-copies solo aplica a --protection-mode replication")
        if args.strict_remote_copies is not None:
            parser.error("--strict-remote-copies/--no-strict-remote-copies solo aplica a --protection-mode replication")
        if args.target_parallelism is not None:
            parser.error("--target-parallelism solo aplica a --protection-mode replication")
        if args.probe_batch_hashes is not None:
            parser.error("--probe-batch-hashes solo aplica a --protection-mode replication")
        if args.stream_inflight is not None:
            parser.error("--stream-inflight solo aplica a --protection-mode replication")
        if args.probe_timeout_s is not None:
            parser.error("--probe-timeout-s solo aplica a --protection-mode replication")
            
        if args.ec_k is None:
            parser.error("--ec-k es obligatorio en --protection-mode ec")
        if args.ec_m is None:
            parser.error("--ec-m es obligatorio en --protection-mode ec")
        if args.ec_k < 1:
            parser.error("--ec-k debe ser >= 1")
        if args.ec_m < 0:
            parser.error("--ec-m debe ser >= 0")
        args.ec_pack_size_bytes = DEFAULT_EC_PACK_SIZE_BYTES if args.ec_pack_size_bytes is None else args.ec_pack_size_bytes
        if args.ec_pack_size_bytes < 1:
            parser.error("--ec-pack-size-bytes debe ser >= 1")

    else:
        if args.remote_copies is not None and args.remote_copies < 1:
            parser.error("--remote-copies debe ser >= 1 en --protection-mode replication")
        if args.ec_k is not None:
            parser.error("--ec-k solo aplica a --protection-mode ec")
        if args.ec_m is not None:
            parser.error("--ec-m solo aplica a --protection-mode ec")
        if args.ec_pack_size_bytes is not None:
            parser.error("--ec-pack-size-bytes solo aplica a --protection-mode ec")

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)
    metadata_object_graph_auto_export = build_metadata_object_graph_auto_export(args, cfg)

    if args.protection_mode == "ec":
        from stopan.protection.ec.pusher import push_erasure_data_packs_to_network

        stats = push_erasure_data_packs_to_network(
            membership_seed=args.membership_seed or first_seed(cfg),
            limit=args.limit,
            ec_k=args.ec_k,
            ec_m=args.ec_m,
            ec_pack_size_bytes=args.ec_pack_size_bytes,
            stream_timeout_s=float(choose(args.stream_timeout_s, cfg.replication.stream_timeout_s)),
            max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
            commit_every=int(choose(args.commit_every, cfg.replication.commit_every)),
            db_file=cfg.node.db_file,
            local_shard_dir=cfg.node.local_shard_dir,
            self_addr=cfg.node.advertise_addr,
            cluster_token=cfg.cluster.token,
            membership_timeout_s=cfg.membership.rpc_timeout_s,
            metadata_object_graph_auto_export=metadata_object_graph_auto_export,
        )

        print("-" * 40)
        print(
            f"Push EC finalizado. packs={stats.placed_packs}/{stats.attempted_packs} | "
            f"chunks={stats.placed_chunks}/{stats.packed_chunks} | "
            f"degraded_packs={stats.degraded_packs} | failed_packs={stats.failed_packs} | "
            f"stored_shards={stats.stored_shards} | "
            f"already_present_shards={stats.already_present_shards}"
        )

        if getattr(stats, "interrupted", False):
            print(f"Push EC interrumpido. Progreso persistido hasta packs={stats.attempted_packs}.")
            return 130

        if getattr(stats, "insufficient_remote_targets", False):
            print(
                "Push EC no iniciado. "
                f"required_remote_targets={stats.required_remote_targets} "
                f"remote_candidates={stats.remote_candidates}"
            )
            return 2

        return 0 if int(stats.failed_packs) == 0 and int(stats.degraded_packs) == 0 else 2

    from stopan.protection.pusher import push_to_network

    stats = push_to_network(
        membership_seed=args.membership_seed or first_seed(cfg),
        rf=int(choose(args.remote_copies, cfg.protection.remote_copies)),
        limit=args.limit,
        target_parallelism=int(choose(args.target_parallelism, cfg.replication.target_parallelism)),
        probe_batch_hashes=int(choose(args.probe_batch_hashes, cfg.replication.probe_batch_hashes)),
        stream_inflight=int(choose(args.stream_inflight, cfg.replication.stream_inflight)),
        probe_timeout_s=float(choose(args.probe_timeout_s, cfg.replication.probe_timeout_s)),
        stream_timeout_s=float(choose(args.stream_timeout_s, cfg.replication.stream_timeout_s)),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        commit_every=int(choose(args.commit_every, cfg.replication.commit_every)),
        strict_rf=bool(choose(args.strict_remote_copies, cfg.protection.strict_remote_copies)),
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
            "Push no iniciado por strict-remote-copies. "
            f"remote_copies={stats.desired_rf} remote_candidates={stats.remote_candidates}"
        )
        return 2

    degraded = int(getattr(stats, "degraded", 0))
    return 0 if int(stats.failed) == 0 and degraded == 0 else 2
