from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass

from core.cluster_view import ClusterMembershipClient
from core.database import MetadataDB
from core.replication import (
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_PROBE_BATCH_HASHES,
    DEFAULT_PROBE_TIMEOUT_S,
    DEFAULT_STREAM_INFLIGHT,
    DEFAULT_STREAM_TIMEOUT_S,
    DEFAULT_TARGET_PARALLELISM,
    StreamingReplicationCoordinator,
)
from core.repository import CASRepository


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_COMMIT_EVERY = int(os.getenv("REPLICATION_COMMIT_EVERY", "100"))
DEFAULT_STRICT_RF = os.getenv("REPLICATION_STRICT_RF", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}


@dataclass(frozen=True)
class PushStats:
    attempted: int = 0
    protected: int = 0
    degraded: int = 0
    failed: int = 0
    stored_remote: int = 0
    already_present_remote: int = 0
    insufficient_remote_targets: bool = False
    desired_rf: int = 0
    remote_candidates: int = 0
    interrupted: bool = False


def resolve_membership_seed(explicit_seed: str | None = None) -> str | None:
    if explicit_seed:
        return explicit_seed.strip()

    seeds = [seed.strip() for seed in os.getenv("SEEDS", "").split(",") if seed.strip()]
    if seeds:
        return seeds[0]

    advertise_addr = os.getenv("ADVERTISE_ADDR", "").strip()
    return advertise_addr or None


def build_cluster_view(seed: str | None):
    resolved_seed = resolve_membership_seed(seed)
    if not resolved_seed:
        raise ValueError("A non-empty membership seed is required.")

    self_addr = os.getenv("ADVERTISE_ADDR", "")
    cluster = ClusterMembershipClient(resolved_seed, self_addr=self_addr).get_cluster_view()

    if not cluster.members:
        raise RuntimeError(f"No eligible members returned by seed {resolved_seed}.")

    if not cluster.self_node_id:
        raise RuntimeError(
            "Could not resolve origin_node_id from membership. "
            "Make sure ADVERTISE_ADDR matches an eligible cluster member."
        )

    return resolved_seed, cluster


def push_to_network(
    seed: str | None,
    *,
    rf: int,
    limit: int | None = None,
    target_parallelism: int = DEFAULT_TARGET_PARALLELISM,
    probe_batch_hashes: int = DEFAULT_PROBE_BATCH_HASHES,
    stream_inflight: int = DEFAULT_STREAM_INFLIGHT,
    probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
    stream_timeout_s: float = DEFAULT_STREAM_TIMEOUT_S,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    commit_every: int = DEFAULT_COMMIT_EVERY,
    strict_rf: bool = DEFAULT_STRICT_RF,
) -> PushStats:
    repo = CASRepository(LOCAL_SHARD_DIR)
    db = MetadataDB(DB_FILE)
    coordinator = None

    desired_rf = max(int(rf), 1)
    protected = 0
    degraded = 0
    failed = 0
    attempted = 0
    stored_remote = 0
    already_present_remote = 0
    pending_chunks: list[str] = []
    start_time = time.perf_counter()
    interrupted = False

    try:
        resolved_seed, cluster = build_cluster_view(seed)
        origin_node_id = cluster.self_node_id
        remote_candidates = len(cluster.candidate_node_ids_excluding({origin_node_id}))

        if strict_rf and remote_candidates < desired_rf:
            print(
                "Not enough remote candidates to satisfy strict RF: "
                f"desired_rf={desired_rf}, remote_candidates={remote_candidates}."
            )
            return PushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidates,
                insufficient_remote_targets=True,
            )

        current_epoch = cluster.placement_epoch_excluding(
            desired_rf=desired_rf,
            cluster_token=CLUSTER_TOKEN,
            excluded_node_ids={origin_node_id},
        )

        db.mark_stale_protection(desired_rf=desired_rf, current_epoch=current_epoch)
        pending_chunks = db.get_pending_protection_chunks(
            desired_rf=desired_rf,
            current_epoch=current_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print("No chunks pending for the current protection policy.")
            return PushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidates,
            )

        coordinator = StreamingReplicationCoordinator(
            repo=repo,
            cluster=cluster,
            rf=desired_rf,
            cluster_token=CLUSTER_TOKEN,
            origin_node_id=origin_node_id,
            probe_timeout_s=probe_timeout_s,
            stream_timeout_s=stream_timeout_s,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            stream_inflight=stream_inflight,
            max_message_bytes=max_message_bytes,
        )

        self_addr = os.getenv("ADVERTISE_ADDR", "")
        print(f"Push stream: {len(pending_chunks)} chunks pending for protection")
        print(f"Membership seed: {resolved_seed}")
        print(f"Eligible members: {[f'{member.node_id[:8]}@{member.address}' for member in cluster.members]}")
        print(f"Origin node: {origin_node_id[:8]}@{self_addr}")
        print(f"Remote candidates: {remote_candidates}")
        print(f"Desired RF: {desired_rf}")
        print(f"Strict RF: {bool(strict_rf)}")
        print(f"placement_epoch={current_epoch[:12]}")
        print(
            f"Pipeline: target_parallelism={target_parallelism} "
            f"probe_batch_hashes={probe_batch_hashes} "
            f"stream_inflight={stream_inflight} "
            f"probe_timeout_s={probe_timeout_s} "
            f"stream_timeout_s={stream_timeout_s}"
        )

        try:
            for outcome in coordinator.replicate_chunks(pending_chunks):
                attempted += 1
                stored_remote += outcome.stored_remote_copies
                already_present_remote += outcome.already_present_remote_copies

                policy_satisfied = outcome.protected_remote_copies >= desired_rf

                if policy_satisfied:
                    db.mark_chunk_placed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                    )
                    protected += 1

                elif outcome.protected_remote_copies > 0:
                    db.mark_chunk_degraded(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                        error=outcome.error or "replication degraded",
                    )
                    degraded += 1
                    print(
                        f"   {outcome.chunk_hash[:8]} degraded: "
                        f"protected={outcome.protected_remote_copies}/{desired_rf} "
                        f"targets={outcome.required_remote_copies} "
                        f"error={outcome.error}"
                    )

                else:
                    db.mark_chunk_failed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                        error=outcome.error or "replication failed",
                    )
                    failed += 1
                    print(
                        f"   {outcome.chunk_hash[:8]} failed: "
                        f"protected={outcome.protected_remote_copies}/{desired_rf} "
                        f"targets={outcome.required_remote_copies} "
                        f"error={outcome.error}"
                    )

                if attempted % max(1, int(commit_every)) == 0:
                    db.commit()
                    print(
                        f"   progress {attempted}/{len(pending_chunks)} | "
                        f"protected={protected} | degraded={degraded} | failed={failed} | "
                        f"stored_remote={stored_remote} | "
                        f"already_present_remote={already_present_remote}"
                    )

        except KeyboardInterrupt:
            interrupted = True
            print("\nPush interrupted by user.")

        return PushStats(
            attempted=attempted,
            protected=protected,
            degraded=degraded,
            failed=failed,
            stored_remote=stored_remote,
            already_present_remote=already_present_remote,
            desired_rf=desired_rf,
            remote_candidates=remote_candidates,
            interrupted=interrupted,
        )

    finally:
        if coordinator is not None:
            coordinator.close()

        db.commit()
        db.close()

        elapsed = time.perf_counter() - start_time
        speed = protected / elapsed if elapsed > 0 else 0.0

        print("-" * 40)
        print(
            f"Push finished in {elapsed:.2f} seconds. "
            f"protected={protected}/{len(pending_chunks)} | degraded={degraded} | "
            f"failed={failed} | attempted={attempted} | "
            f"stored_remote={stored_remote} | already_present_remote={already_present_remote}"
        )
        if protected > 0:
            print(f"Average speed: {speed:.2f} chunks/second")


def parse_args():
    parser = argparse.ArgumentParser(prog="replicator.py")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    push_parser = subparsers.add_parser("push", help="Protege chunks pendientes en la red P2P")
    push_parser.add_argument("--seed", default="node1:50051", help="Seed de membership, por ejemplo node1:50051")
    push_parser.add_argument(
        "--rf",
        type=int,
        default=int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3"))),
        help="Replication factor remoto deseado",
    )
    push_parser.add_argument("--limit", type=int, default=None, help="Límite de chunks a procesar en esta ejecución")
    push_parser.add_argument("--target-parallelism", type=int, default=DEFAULT_TARGET_PARALLELISM, help="Nodos destino procesados en paralelo")
    push_parser.add_argument("--probe-batch-hashes", type=int, default=DEFAULT_PROBE_BATCH_HASHES, help="Hashes por lote de inventario remoto")
    push_parser.add_argument("--stream-inflight", type=int, default=DEFAULT_STREAM_INFLIGHT, help="Chunks máximos en vuelo por stream")
    push_parser.add_argument("--probe-timeout-s", type=float, default=DEFAULT_PROBE_TIMEOUT_S, help="Timeout de ProbeMissingChunks en segundos")
    push_parser.add_argument("--stream-timeout-s", type=float, default=DEFAULT_STREAM_TIMEOUT_S, help="Timeout de ReplicateChunks en segundos")
    push_parser.add_argument("--max-message-bytes", type=int, default=DEFAULT_MAX_MESSAGE_BYTES, help="Límite de tamaño de mensaje gRPC")
    push_parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY, help="Guardar progreso cada N resultados")
    push_parser.add_argument(
        "--strict-rf",
        dest="strict_rf",
        action="store_true",
        default=DEFAULT_STRICT_RF,
        help="Falla antes de modificar el estado si el cluster no puede cumplir el RF",
    )
    push_parser.add_argument(
        "--no-strict-rf",
        dest="strict_rf",
        action="store_false",
        help="Permite protección best-effort aunque el cluster no pueda cumplir el RF",
    )

    return parser.parse_args()

def main() -> int:
    args = parse_args()

    if args.cmd != "push":
        raise ValueError(f"Unsupported command: {args.cmd}")

    stats = push_to_network(
        seed=args.seed,
        rf=args.rf,
        limit=args.limit,
        target_parallelism=args.target_parallelism,
        probe_batch_hashes=args.probe_batch_hashes,
        stream_inflight=args.stream_inflight,
        probe_timeout_s=args.probe_timeout_s,
        stream_timeout_s=args.stream_timeout_s,
        max_message_bytes=args.max_message_bytes,
        commit_every=args.commit_every,
        strict_rf=bool(args.strict_rf),
    )

    if stats.interrupted:
        return 130

    if stats.insufficient_remote_targets or stats.failed > 0 or stats.degraded > 0:
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
