from __future__ import annotations

import os
import threading
from collections import defaultdict
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

from stopan.protos import p2p_storage_pb2
from stopan.protos import p2p_storage_pb2_grpc
from stopan.metadata.database import MetadataDB, VerificationCandidate
from stopan.placement.cluster_view import ClusterMembershipClient
from stopan.protection.verify_config import (
    CLUSTER_TOKEN,
    DB_FILE,
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_PROBE_BATCH_HASHES,
    DEFAULT_PROBE_TIMEOUT_S,
    DEFAULT_TARGET_PARALLELISM,
)
from stopan.protection.verify_models import (
    ProbeExecutionResult,
    VerificationAccumulator,
    VerificationOutcome,
    VerificationStats,
)


class ProbeClientPool:
    """
    Reusable gRPC client pool for verifier probes.
    """

    def __init__(self, *, max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES):
        self._lock = threading.Lock()
        self._channels: dict[str, grpc.Channel] = {}
        self._stubs: dict[str, p2p_storage_pb2_grpc.P2PStorageStub] = {}
        self._options = [
            ("grpc.max_send_message_length", int(max_message_bytes)),
            ("grpc.max_receive_message_length", int(max_message_bytes)),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ]

    def get_stub(self, address: str) -> p2p_storage_pb2_grpc.P2PStorageStub:
        with self._lock:
            stub = self._stubs.get(address)
            if stub is not None:
                return stub

            channel = grpc.insecure_channel(address, options=self._options)
            stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
            self._channels[address] = channel
            self._stubs[address] = stub
            return stub

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()


class ChunkProtectionVerifier:
    """
    Canonical remote protection auditor.

    Contract:
      - does not transfer chunk blobs;
      - uses only ProbeMissingChunks;
      - recalculates current HRW placement for each chunk;
      - excludes the snapshot origin node from remote protection;
      - converts PLACED/DEGRADED evidence into VERIFIED or DEGRADED.
    """

    def __init__(
        self,
        *,
        db: MetadataDB,
        cluster,
        cluster_token: str,
        origin_node_id: str,
        probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
        target_parallelism: int = DEFAULT_TARGET_PARALLELISM,
        probe_batch_hashes: int = DEFAULT_PROBE_BATCH_HASHES,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ):
        origin_node_id = str(origin_node_id).strip()
        if not origin_node_id:
            raise ValueError("ChunkProtectionVerifier requires a non-empty origin_node_id.")

        self.db = db
        self.cluster = cluster
        self.cluster_token = cluster_token
        self.origin_node_id = origin_node_id
        self._excluded_node_ids = frozenset({origin_node_id})
        self.probe_timeout_s = float(probe_timeout_s)
        self.target_parallelism = max(1, int(target_parallelism))
        self.probe_batch_hashes = max(1, int(probe_batch_hashes))
        self._client_pool = ProbeClientPool(max_message_bytes=max_message_bytes)
        self._epoch_cache: dict[int, str] = {}

    def close(self) -> None:
        self._client_pool.close()

    def verify(self, candidates: list[VerificationCandidate]) -> VerificationStats:
        if not candidates:
            print("No chunks are pending verification.")
            return VerificationStats()

        remote_candidates = len(
            self.cluster.candidate_node_ids_excluding(self._excluded_node_ids)
        )

        print(f"Verify: {len(candidates)} candidate chunks")
        print(f"Eligible members: {[f'{member.node_id[:8]}@{member.address}' for member in self.cluster.members]}")
        print(f"Origin node: {self.origin_node_id[:8]}")
        print(f"Remote candidates: {remote_candidates}")
        print(
            f"Pipeline: target_parallelism={self.target_parallelism} "
            f"probe_batch_hashes={self.probe_batch_hashes} "
            f"probe_timeout_s={self.probe_timeout_s}"
        )

        outcomes = self._verify_candidates(candidates)
        verified = 0
        degraded = 0
        rpc_failures = 0

        for outcome in outcomes:
            if outcome.success:
                self.db.mark_chunk_verified(
                    outcome.chunk_hash,
                    desired_rf=outcome.desired_rf,
                    protected_remote_copies=outcome.verified_remote_copies,
                    placement_epoch=outcome.placement_epoch,
                )
                verified += 1
                continue

            self.db.mark_chunk_degraded(
                outcome.chunk_hash,
                desired_rf=outcome.desired_rf,
                protected_remote_copies=outcome.verified_remote_copies,
                placement_epoch=outcome.placement_epoch,
                error=outcome.error or "verification failed",
            )
            degraded += 1
            if outcome.error and "RPC" in outcome.error:
                rpc_failures += 1
            print(
                f"   {outcome.chunk_hash[:8]} degraded: "
                f"verified={outcome.verified_remote_copies}/{outcome.required_remote_copies} "
                f"error={outcome.error}"
            )

        return VerificationStats(
            candidates=len(candidates),
            verified=verified,
            degraded=degraded,
            rpc_failures=rpc_failures,
        )

    def _verify_candidates(self, candidates: list[VerificationCandidate]) -> list[VerificationOutcome]:
        accumulators, target_chunks, target_members = self._plan(candidates)

        if target_chunks:
            max_workers = min(self.target_parallelism, len(target_chunks))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="verify-probe") as executor:
                future_map = {
                    executor.submit(
                        self._execute_target_probe,
                        target_members[address],
                        hashes,
                    ): address
                    for address, hashes in target_chunks.items()
                }

                for future in as_completed(future_map):
                    address = future_map[future]
                    planned_hashes = target_chunks[address]
                    member = target_members[address]

                    try:
                        result = future.result()
                    except Exception as exc:
                        result = ProbeExecutionResult(
                            node_id=member.node_id,
                            address=member.address,
                            requested_hashes=tuple(planned_hashes),
                            present_hashes=frozenset(),
                            transport_error=f"target probe crashed: {exc}",
                        )

                    if result.transport_error:
                        message = f"RPC {result.node_id[:8]}@{result.address}: {result.transport_error}"
                        for chunk_hash in result.requested_hashes:
                            accumulators[chunk_hash].errors.append(message)
                        continue

                    for chunk_hash in result.requested_hashes:
                        if chunk_hash in result.present_hashes:
                            accumulators[chunk_hash].verified_remote_copies += 1

        outcomes: list[VerificationOutcome] = []
        for candidate in candidates:
            accumulator = accumulators[candidate.chunk_hash]
            outcomes.append(
                VerificationOutcome(
                    chunk_hash=candidate.chunk_hash,
                    desired_rf=accumulator.desired_rf,
                    placement_epoch=accumulator.placement_epoch,
                    success=accumulator.success,
                    required_remote_copies=accumulator.required_remote_copies,
                    verified_remote_copies=accumulator.verified_remote_copies,
                    error=accumulator.error_summary,
                )
            )

        return outcomes

    def _plan(self, candidates: list[VerificationCandidate]):
        accumulators: dict[str, VerificationAccumulator] = {}
        target_chunks: dict[str, list[str]] = defaultdict(list)
        target_members: dict[str, object] = {}

        for candidate in candidates:
            desired_rf = max(int(candidate.desired_rf), 1)
            placement_epoch = self._placement_epoch(desired_rf)
            remote_targets = self.cluster.hrw_targets_excluding(
                candidate.chunk_hash,
                rf=desired_rf,
                salt=self.cluster_token,
                excluded_node_ids=self._excluded_node_ids,
            )

            accumulators[candidate.chunk_hash] = VerificationAccumulator(
                desired_rf=desired_rf,
                placement_epoch=placement_epoch,
                required_remote_copies=len(remote_targets),
            )

            for member in remote_targets:
                target_chunks[member.address].append(candidate.chunk_hash)
                target_members[member.address] = member

        return accumulators, target_chunks, target_members

    def _placement_epoch(self, desired_rf: int) -> str:
        desired_rf = max(int(desired_rf), 1)
        cached = self._epoch_cache.get(desired_rf)
        if cached is not None:
            return cached

        epoch = self.cluster.placement_epoch_excluding(
            desired_rf=desired_rf,
            cluster_token=self.cluster_token,
            excluded_node_ids=self._excluded_node_ids,
        )
        self._epoch_cache[desired_rf] = epoch
        return epoch

    def _execute_target_probe(self, member, chunk_hashes: list[str]) -> ProbeExecutionResult:
        ordered_hashes = tuple(dict.fromkeys(chunk_hashes))
        present_hashes: set[str] = set()

        try:
            stub = self._client_pool.get_stub(member.address)
            for batch in iter_hash_batches(ordered_hashes, self.probe_batch_hashes):
                requested = set(batch)
                response = stub.ProbeMissingChunks(
                    p2p_storage_pb2.ProbeMissingChunksRequest(chunk_hashes=batch),
                    timeout=self.probe_timeout_s,
                )
                missing = {chunk_hash for chunk_hash in response.missing_hashes if chunk_hash}
                present_hashes.update(requested - missing)

            return ProbeExecutionResult(
                node_id=member.node_id,
                address=member.address,
                requested_hashes=ordered_hashes,
                present_hashes=frozenset(present_hashes),
                transport_error=None,
            )

        except grpc.RpcError as exc:
            return ProbeExecutionResult(
                node_id=member.node_id,
                address=member.address,
                requested_hashes=ordered_hashes,
                present_hashes=frozenset(),
                transport_error=format_rpc_error(exc),
            )
        except Exception as exc:
            return ProbeExecutionResult(
                node_id=member.node_id,
                address=member.address,
                requested_hashes=ordered_hashes,
                present_hashes=frozenset(),
                transport_error=str(exc),
            )


def iter_hash_batches(chunk_hashes: Sequence[str], batch_size: int) -> Iterator[list[str]]:
    batch_size = max(1, int(batch_size))
    current: list[str] = []

    for chunk_hash in chunk_hashes:
        current.append(chunk_hash)
        if len(current) >= batch_size:
            yield current
            current = []

    if current:
        yield current


def format_rpc_error(exc: grpc.RpcError) -> str:
    code = "UNKNOWN"
    details = str(exc)

    try:
        code = exc.code().name
    except Exception:
        pass

    try:
        details = exc.details() or details
    except Exception:
        pass

    return f"{code}: {details}"


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


def verify_remote_protection(
    *,
    seed: str | None,
    include_verified: bool,
    limit: int | None,
    target_parallelism: int,
    probe_batch_hashes: int,
    probe_timeout_s: float,
    max_message_bytes: int,
) -> VerificationStats:
    db = MetadataDB(DB_FILE)
    verifier: ChunkProtectionVerifier | None = None

    try:
        resolved_seed, cluster = build_cluster_view(seed)
        origin_node_id = cluster.self_node_id

        candidates = db.get_verification_candidates(
            include_verified=include_verified,
            limit=limit,
        )

        verifier = ChunkProtectionVerifier(
            db=db,
            cluster=cluster,
            cluster_token=CLUSTER_TOKEN,
            origin_node_id=origin_node_id,
            probe_timeout_s=probe_timeout_s,
            target_parallelism=target_parallelism,
            probe_batch_hashes=probe_batch_hashes,
            max_message_bytes=max_message_bytes,
        )

        print(f"Membership seed: {resolved_seed}")
        if include_verified:
            print("Reverify mode: VERIFIED chunks are included.")

        return verifier.verify(candidates)

    finally:
        if verifier is not None:
            verifier.close()
        db.commit()
        db.close()
