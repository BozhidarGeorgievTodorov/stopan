from __future__ import annotations

import os
from dataclasses import dataclass

from stopan.cas.repository import CASRepository
from stopan.metadata.database import MetadataDB
from stopan.placement.cluster_view import ClusterMembershipClient
from stopan.replication.coordinator import StreamingReplicationCoordinator


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")


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
    *,
    seed: str | None,
    rf: int,
    limit: int | None,
    target_parallelism: int,
    probe_batch_hashes: int,
    stream_inflight: int,
    probe_timeout_s: float,
    stream_timeout_s: float,
    max_message_bytes: int,
    commit_every: int,
    strict_rf: bool,
) -> PushStats:
    desired_rf = max(int(rf), 1)
    commit_every = max(1, int(commit_every))

    repo = CASRepository(LOCAL_SHARD_DIR)
    db = MetadataDB(DB_FILE)
    coordinator: StreamingReplicationCoordinator | None = None

    attempted = 0
    protected = 0
    degraded = 0
    failed = 0
    stored_remote = 0
    already_present_remote = 0
    remote_candidate_count = 0
    interrupted = False

    try:
        resolved_seed, cluster = build_cluster_view(seed)
        origin_node_id = cluster.self_node_id
        remote_candidate_node_ids = cluster.candidate_node_ids_excluding({origin_node_id})
        remote_candidate_count = len(remote_candidate_node_ids)

        if strict_rf and remote_candidate_count < desired_rf:
            print(
                "Not enough remote candidates to satisfy strict RF: "
                f"desired_rf={desired_rf}, remote_candidates={remote_candidate_count}."
            )
            print(f"Remote candidates: {[node_id[:8] for node_id in remote_candidate_node_ids]}")
            return PushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
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
                remote_candidates=remote_candidate_count,
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
        print(f"Remote candidates: {remote_candidate_count}")
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
                    error = (
                        f"partial placement: protected_remote_copies="
                        f"{outcome.protected_remote_copies}/{desired_rf} "
                        f"(planned_targets={outcome.required_remote_copies})"
                    )
                    if outcome.error:
                        error = f"{error}; {outcome.error}"

                    db.mark_chunk_degraded(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=outcome.protected_remote_copies,
                        placement_epoch=current_epoch,
                        error=error,
                    )
                    degraded += 1
                    print(
                        f"   {outcome.chunk_hash[:8]} degraded: "
                        f"protected={outcome.protected_remote_copies}/{desired_rf} "
                        f"planned={outcome.required_remote_copies} "
                        f"error={error}"
                    )

                else:
                    error = outcome.error or "replication failed with zero protected remote copies"
                    db.mark_chunk_failed(
                        outcome.chunk_hash,
                        desired_rf=desired_rf,
                        protected_remote_copies=0,
                        placement_epoch=current_epoch,
                        error=error,
                    )
                    failed += 1
                    print(
                        f"   {outcome.chunk_hash[:8]} failed: "
                        f"protected=0/{desired_rf} "
                        f"planned={outcome.required_remote_copies} "
                        f"error={error}"
                    )

                if attempted % commit_every == 0:
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
            remote_candidates=remote_candidate_count,
            interrupted=interrupted,
        )

    finally:
        if coordinator is not None:
            coordinator.close()
        db.commit()
        db.close()
