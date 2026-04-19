from __future__ import annotations

import argparse

from stopan.protection.verifier import verify_remote_protection
from stopan.protection.verify_config import (
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_PROBE_BATCH_HASHES,
    DEFAULT_PROBE_TIMEOUT_S,
    DEFAULT_TARGET_PARALLELISM,
    DEFAULT_SEED,
)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="stopan verify",
        description="Audita la protección remota de chunks usando HRW y ProbeMissingChunks.",
    )
    parser.add_argument(
        "--seed",
        default=DEFAULT_SEED,
        help="Seed de membership para obtener la vista elegible del cluster",
    )
    parser.add_argument(
        "--probe-timeout-s",
        type=float,
        default=DEFAULT_PROBE_TIMEOUT_S,
        help="Timeout del RPC ProbeMissingChunks en segundos",
    )
    parser.add_argument(
        "--target-parallelism",
        type=int,
        default=DEFAULT_TARGET_PARALLELISM,
        help="Número de targets remotos verificados en paralelo",
    )
    parser.add_argument(
        "--probe-batch-hashes",
        type=int,
        default=DEFAULT_PROBE_BATCH_HASHES,
        help="Hashes por lote de inventario remoto",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite de chunks a verificar en esta ejecución",
    )
    parser.add_argument(
        "--reverify-verified",
        action="store_true",
        help="Incluye chunks que ya están marcados como VERIFIED",
    )
    parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=DEFAULT_MAX_MESSAGE_BYTES,
        help="Límite de tamaño de mensaje gRPC",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    stats = verify_remote_protection(
        seed=args.seed,
        include_verified=bool(args.reverify_verified),
        limit=args.limit,
        target_parallelism=int(args.target_parallelism),
        probe_batch_hashes=int(args.probe_batch_hashes),
        probe_timeout_s=float(args.probe_timeout_s),
        max_message_bytes=int(args.max_message_bytes),
    )

    print("-" * 40)
    print(
        f"Verify finished. verified={stats.verified}/{stats.candidates} | "
        f"degraded={stats.degraded} | rpc_failures={stats.rpc_failures}"
    )

    return 0 if stats.degraded == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
