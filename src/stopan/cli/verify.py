from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, choose, first_seed, load_runtime_config
from stopan.cli.validation import FloatRange, IntRange, validate_float_ranges, validate_int_ranges
from stopan.cli.metadata_auto_export import (
    add_metadata_auto_export_args,
    build_metadata_object_graph_auto_export,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan verify",
        allow_abbrev=False,
        description="Audita protección remota de chunks mediante HRW + ProbeMissingChunks.",
    )
    add_config_args(parser)

    parser.add_argument(
        "--protection-mode",
        choices=("replication", "ec"),
        default="replication",
        help="Modo de verificación: replication audita copias remotas por chunk; ec audita shards de data packs.",
    )
    parser.add_argument("--membership-seed", default=None, help="Seed de membership para obtener la vista del cluster elegible.")
    parser.add_argument("--probe-timeout-s", type=float, default=None, help="Timeout del RPC ProbeMissingChunks en segundos.")
    parser.add_argument("--target-parallelism", type=int, default=None, help="Número de targets procesados en paralelo.")
    parser.add_argument("--probe-batch-hashes", type=int, default=None, help="Hashes por probe de inventario remoto.")
    parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a verificar en esta ejecución.")
    parser.add_argument("--reverify-verified", action="store_true", help="Incluye chunks en estado VERIFIED para auditarlos de nuevo.")
    parser.add_argument("--max-message-bytes", type=int, default=None, help="Límite de mensaje gRPC.")
    add_metadata_auto_export_args(parser, context="verify")
    
    args = parser.parse_args(argv)

    validate_float_ranges(
        parser,
        args,
        (FloatRange("probe_timeout_s", "--probe-timeout-s", 0.0, inclusive=False),),
    )
    validate_int_ranges(
        parser,
        args,
        (
            IntRange("target_parallelism", "--target-parallelism", 1),
            IntRange("limit", "--limit", 1),
            IntRange("max_message_bytes", "--max-message-bytes", 1),
            IntRange("probe_batch_hashes", "--probe-batch-hashes", 1),
        ),
    )

    if args.protection_mode == "ec" and args.probe_batch_hashes is not None:
        parser.error("--probe-batch-hashes solo aplica a --protection-mode replication")

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    
    cfg = load_runtime_config(args)
    metadata_object_graph_auto_export = build_metadata_object_graph_auto_export(args, cfg)

    if args.protection_mode == "ec":
        from stopan.protection.ec.verifier import verify_erasure_data_packs

        stats = verify_erasure_data_packs(
            membership_seed=args.membership_seed or first_seed(cfg),
            self_addr=cfg.node.advertise_addr,
            cluster_token=cfg.cluster.token,
            membership_timeout_s=cfg.membership.rpc_timeout_s,
            db_file=cfg.node.db_file,
            include_verified=bool(args.reverify_verified),
            limit=args.limit,
            target_parallelism=int(choose(args.target_parallelism, cfg.verify.target_parallelism)),
            probe_timeout_s=float(choose(args.probe_timeout_s, cfg.verify.probe_timeout_s)),
            max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
            metadata_object_graph_auto_export=metadata_object_graph_auto_export,
        )

        print("-" * 40)
        print(
            f"Verify EC finalizado. verified={stats.verified}/{stats.candidates} | "
            f"degraded={stats.degraded} | failed={stats.failed} | "
            f"rpc_failures={stats.rpc_failures}"
        )
        return 0 if stats.degraded == 0 and stats.failed == 0 else 2

    from stopan.protection.verifier import verify_remote_protection

    stats = verify_remote_protection(
        membership_seed=args.membership_seed or first_seed(cfg),
        db_file=cfg.node.db_file,
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
        membership_timeout_s=cfg.membership.rpc_timeout_s,
        include_verified=bool(args.reverify_verified),
        limit=args.limit,
        target_parallelism=int(choose(args.target_parallelism, cfg.verify.target_parallelism)),
        probe_batch_hashes=int(choose(args.probe_batch_hashes, cfg.verify.probe_batch_hashes)),
        probe_timeout_s=float(choose(args.probe_timeout_s, cfg.verify.probe_timeout_s)),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        metadata_object_graph_auto_export=metadata_object_graph_auto_export,
    )

    print("-" * 40)
    print(
        f"Verify finalizado. verified={stats.verified}/{stats.candidates} | "
        f"degraded={stats.degraded} | rpc_failures={stats.rpc_failures}"
    )
    return 0 if stats.degraded == 0 else 2