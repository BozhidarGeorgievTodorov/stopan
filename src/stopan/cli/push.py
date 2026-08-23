from __future__ import annotations

import argparse
import time
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config
from stopan.cli.output import print_timing_summary
from stopan.cli.metadata_auto_export import (
    add_metadata_auto_export_args,
    build_metadata_object_graph_auto_export,
)
from stopan.cli.validation import (
    CLIUsageError,
    Flag,
    FloatRange,
    IntRange,
    reject_present,
    validate_float_ranges,
    validate_int_ranges,
)
PROTECTION_SCOPE_CHOICES = ("pending", "snapshot", "all-reachable", "all-known-chunks")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan push",
        allow_abbrev=False,
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
    parser.add_argument("--ec-k", type=int, default=None, help="Número de data shards por data pack EC. Default: protection.ec_k.")
    parser.add_argument("--ec-m", type=int, default=None, help="Número de parity shards por data pack EC. Default: protection.ec_m.")
    parser.add_argument(
        "--ec-pack-size-bytes",
        type=int,
        default=None,
        help="Tamaño objetivo del payload de cada data pack EC. Default: protection.ec_pack_size_bytes.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución.")
    parser.add_argument(
        "--scope",
        choices=PROTECTION_SCOPE_CHOICES,
        default="pending",
        help=(
            "Alcance del push. pending conserva el comportamiento actual; "
            "snapshot filtra por --snapshot-id; all-reachable protege chunks alcanzables "
            "desde snapshots completos; all-known-chunks protege todos los chunks conocidos."
        ),
    )
    parser.add_argument("--snapshot-id", type=int, default=None, help="Snapshot completo que acota el push.")
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

    validate_int_ranges(
        parser,
        args,
        (
            IntRange("limit", "--limit", 1),
            IntRange("snapshot_id", "--snapshot-id", 1),
            IntRange("max_message_bytes", "--max-message-bytes", 1),
            IntRange("commit_every", "--commit-every", 1),
        ),
    )
    validate_float_ranges(
        parser,
        args,
        (FloatRange("stream_timeout_s", "--stream-timeout-s", 0.0, inclusive=False),),
    )

    if args.snapshot_id is not None and args.scope not in ("pending", "snapshot"):
        parser.error("--snapshot-id no se puede combinar con --scope distinto de snapshot")
    if args.snapshot_id is not None:
        args.scope = "snapshot"
    if args.scope == "snapshot" and args.snapshot_id is None:
        parser.error("--scope snapshot requiere --snapshot-id")

    if args.protection_mode == "ec":
        reject_present(
            parser,
            args,
            (
                Flag("remote_copies", "--remote-copies"),
                Flag("probe_batch_hashes", "--probe-batch-hashes"),
                Flag("stream_inflight", "--stream-inflight"),
                Flag("probe_timeout_s", "--probe-timeout-s"),
            ),
            "{flag} solo aplica a --protection-mode replication",
        )
        reject_present(
            parser,
            args,
            (Flag("strict_remote_copies", "--strict-remote-copies/--no-strict-remote-copies"),),
            "{flag} solo aplica a --protection-mode replication",
        )
        validate_int_ranges(
            parser,
            args,
            (
                IntRange("ec_k", "--ec-k", 1),
                IntRange("ec_m", "--ec-m", 0),
                IntRange("ec_pack_size_bytes", "--ec-pack-size-bytes", 1),
                IntRange("target_parallelism", "--target-parallelism", 1),
            ),
        )

    else:
        validate_int_ranges(
            parser,
            args,
            (
                IntRange("remote_copies", "--remote-copies", 1),
                IntRange("target_parallelism", "--target-parallelism", 1),
                IntRange("probe_batch_hashes", "--probe-batch-hashes", 1),
                IntRange("stream_inflight", "--stream-inflight", 1),
            ),
        )
        validate_float_ranges(
            parser,
            args,
            (FloatRange("probe_timeout_s", "--probe-timeout-s", 0.0, inclusive=False),),
        )
        reject_present(
            parser,
            args,
            (
                Flag("ec_k", "--ec-k"),
                Flag("ec_m", "--ec-m"),
                Flag("ec_pack_size_bytes", "--ec-pack-size-bytes"),
            ),
            "{flag} solo aplica a --protection-mode ec",
        )

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)
    metadata_object_graph_auto_export = build_metadata_object_graph_auto_export(args, cfg)
    started_at = time.perf_counter()

    if args.protection_mode == "ec":
        from stopan.protection.ec.pusher import push_erasure_data_packs_to_network

        stats = push_erasure_data_packs_to_network(
            membership_seed=args.membership_seed or first_seed(cfg),
            limit=args.limit,
            scope=args.scope,
            snapshot_id=args.snapshot_id,
            ec_k=int(choose(args.ec_k, cfg.protection.ec_k)),
            ec_m=int(choose(args.ec_m, cfg.protection.ec_m)),
            ec_pack_size_bytes=int(choose(args.ec_pack_size_bytes, cfg.protection.ec_pack_size_bytes)),
            target_parallelism=int(
                choose(args.target_parallelism, cfg.protection.ec_target_parallelism)
            ),
            stream_timeout_s=float(choose(args.stream_timeout_s, cfg.replication.stream_timeout_s)),
            max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
            max_shard_size=int(cfg.storage.max_chunk_size),
            commit_every=int(choose(args.commit_every, cfg.replication.commit_every)),
            db_file=cfg.node.catalog_file,
            local_chunk_dir=cfg.storage.local_chunk_dir,
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
        print_timing_summary(processed_bytes=int(getattr(stats, "processed_bytes", 0)), elapsed=time.perf_counter() - started_at)

        if getattr(stats, "interrupted", False):
            print(f"Push EC interrumpido. Progreso persistido hasta packs={stats.attempted_packs}.")
            return 130

        if getattr(stats, "insufficient_remote_targets", False):
            print(
                "Push EC no iniciado: no hay suficientes nodos remotos elegibles para "
                "todos los paquetes seleccionados. "
                f"required_remote_targets={stats.required_remote_targets} "
                f"remote_candidates={stats.remote_candidates}"
            )
            return 2

        return 0 if int(stats.failed_packs) == 0 and int(stats.degraded_packs) == 0 else 2

    remote_copies = int(choose(args.remote_copies, cfg.protection.remote_copies))
    if remote_copies < 1:
        raise CLIUsageError(
            "push replication requiere copias remotas >= 1; "
            "ajusta --remote-copies o protection.remote_copies."
        )

    from stopan.protection.replication.pusher import push_to_network

    stats = push_to_network(
        membership_seed=args.membership_seed or first_seed(cfg),
        rf=remote_copies,
        limit=args.limit,
        scope=args.scope,
        snapshot_id=args.snapshot_id,
        target_parallelism=int(choose(args.target_parallelism, cfg.replication.target_parallelism)),
        probe_batch_hashes=int(choose(args.probe_batch_hashes, cfg.replication.probe_batch_hashes)),
        stream_inflight=int(choose(args.stream_inflight, cfg.replication.stream_inflight)),
        probe_timeout_s=float(choose(args.probe_timeout_s, cfg.replication.probe_timeout_s)),
        stream_timeout_s=float(choose(args.stream_timeout_s, cfg.replication.stream_timeout_s)),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        commit_every=int(choose(args.commit_every, cfg.replication.commit_every)),
        strict_rf=bool(choose(args.strict_remote_copies, cfg.protection.strict_remote_copies)),
        db_file=cfg.node.catalog_file,
        local_chunk_dir=cfg.storage.local_chunk_dir,
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
    print_timing_summary(processed_bytes=int(getattr(stats, "processed_bytes", 0)), elapsed=time.perf_counter() - started_at)

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

