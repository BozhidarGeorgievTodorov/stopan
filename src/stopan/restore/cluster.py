from __future__ import annotations

import os
import threading

from stopan.placement.cluster_view import ClusterMembershipClient


def resolve_membership_seed(explicit_seed: str | None = None) -> str | None:
    if explicit_seed:
        return explicit_seed.strip()

    seeds = [seed.strip() for seed in os.getenv("SEEDS", "").split(",") if seed.strip()]
    if seeds:
        return seeds[0]

    advertise_addr = os.getenv("ADVERTISE_ADDR", "").strip()
    return advertise_addr or None


class LazyClusterResolver:
    """
    Resuelve membership solo cuando aparece un miss local.

    El restore usa origin_node_id del snapshot para consultar los mismos
    targets HRW que fueron usados por replicator/verifier.
    """

    def __init__(self, *, seed: str | None, rf: int, origin_node_id: str):
        origin_node_id = str(origin_node_id).strip()
        if not origin_node_id:
            raise ValueError("LazyClusterResolver requires a non-empty origin_node_id.")

        self.seed = resolve_membership_seed(seed)
        self.rf = max(int(rf), 1)
        self.origin_node_id = origin_node_id
        self._cluster = None
        self._announced = False
        self._lock = threading.Lock()

    def get_cluster(self):
        if self._cluster is not None:
            return self._cluster

        with self._lock:
            if self._cluster is not None:
                return self._cluster

            if not self.seed:
                raise RuntimeError(
                    "A membership seed is required for remote restore. "
                    "Local restore can run without network, but missing chunks need "
                    "--seed or SEEDS/ADVERTISE_ADDR in the environment."
                )

            self_addr = os.getenv("ADVERTISE_ADDR", "")
            cluster = ClusterMembershipClient(self.seed, self_addr=self_addr).get_cluster_view()
            if not cluster.members:
                raise RuntimeError(f"No eligible members returned by seed {self.seed}.")

            self._cluster = cluster
            return cluster

    def announce_once(self) -> None:
        cluster = self.get_cluster()
        with self._lock:
            if self._announced:
                return
            self._announced = True

        self_addr = os.getenv("ADVERTISE_ADDR", "")
        remote_candidates = len(cluster.candidate_node_ids_excluding({self.origin_node_id}))

        print(f"Remote restore enabled. Eligible members: {[f'{m.node_id[:8]}@{m.address}' for m in cluster.members]}")
        if cluster.self_node_id:
            print(f"Self: {cluster.self_node_id[:8]}@{self_addr}")
        print(f"Origin node: {self.origin_node_id[:8]}")
        print(f"Remote candidates: {remote_candidates}")
        print(f"Desired RF: {self.rf}")
