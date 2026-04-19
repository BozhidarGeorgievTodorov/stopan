from __future__ import annotations

import argparse
import os

from stopan.protection.pusher import push_to_network
from stopan.replication.coordinator import (
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_PROBE_BATCH_HASHES,
    DEFAULT_PROBE_TIMEOUT_S,
    DEFAULT_STREAM_INFLIGHT,
    DEFAULT_STREAM_TIMEOUT_S,
    DEFAULT_TARGET_PARALLELISM,
)


DEFAULT_PROTECTION_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
DEFAULT_COMMIT_EVERY = int(os.getenv("REPLICATION_COMMIT_EVERY", "100"))
DEFAULT_STRICT_RF = os.getenv("REPLICATION_STRICT_RF", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}


def parse_args():
    parser = argparse.ArgumentParser(
        prog="stopan push",
        description="Protege chunks pendientes en la red P2P.",
    )
    parser.add_argument("--seed", default="node1:50051", help="Seed de membership, por ejemplo node1:50051")
    parser.add_argument("--rf", type=int, default=DEFAULT_PROTECTION_RF, help="Replication factor remoto deseado")
    parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución")
    parser.add_argument("--target-parallelism", type=int, default=DEFAULT_TARGET_PARALLELISM, help="Nodos destino procesados en paralelo")
    parser.add_argument("--probe-batch-hashes", type=int, default=DEFAULT_PROBE_BATCH_HASHES, help="Hashes por lote de inventario remoto")
    parser.add_argument("--stream-inflight", type=int, default=DEFAULT_STREAM_INFLIGHT, help="Chunks máximos en vuelo por stream")
    parser.add_argument("--probe-timeout-s", type=float, default=DEFAULT_PROBE_TIMEOUT_S, help="Timeout de ProbeMissingChunks en segundos")
    parser.add_argument("--stream-timeout-s", type=float, default=DEFAULT_STREAM_TIMEOUT_S, help="Timeout de ReplicateChunks en segundos")
    parser.add_argument("--max-message-bytes", type=int, default=DEFAULT_MAX_MESSAGE_BYTES, help="Límite de tamaño de mensaje gRPC")
    parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY, help="Guardar progreso cada N resultados")
    parser.add_argument(
        "--strict-rf",
        dest="strict_rf",
        action="store_true",
        default=DEFAULT_STRICT_RF,
        help="Falla antes de modificar el estado si el cluster no puede cumplir el RF",
    )
    parser.add_argument(
        "--no-strict-rf",
        dest="strict_rf",
        action="store_false",
        help="Permite protección best-effort aunque el cluster no pueda cumplir el RF",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    stats = push_to_network(
        seed=args.seed,
        rf=int(args.rf),
        limit=args.limit,
        target_parallelism=int(args.target_parallelism),
        probe_batch_hashes=int(args.probe_batch_hashes),
        stream_inflight=int(args.stream_inflight),
        probe_timeout_s=float(args.probe_timeout_s),
        stream_timeout_s=float(args.stream_timeout_s),
        max_message_bytes=int(args.max_message_bytes),
        commit_every=int(args.commit_every),
        strict_rf=bool(args.strict_rf),
    )

    print("-" * 40)
    print(
        f"Push finished. protected={stats.protected}/{stats.attempted} | "
        f"degraded={stats.degraded} | failed={stats.failed} | "
        f"stored_remote={stats.stored_remote} | "
        f"already_present_remote={stats.already_present_remote}"
    )

    if stats.interrupted:
        print(f"Push interrupted. Progress persisted up to attempted={stats.attempted}.")
        return 130

    if stats.insufficient_remote_targets:
        print(
            "Push was not started because strict RF cannot be satisfied. "
            f"desired_rf={stats.desired_rf} remote_candidates={stats.remote_candidates}"
        )
        return 2

    return 0 if stats.failed == 0 and stats.degraded == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
