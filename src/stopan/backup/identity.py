from __future__ import annotations

import os
import uuid

from stopan.backup.config import NODE_ID_FILE


def resolve_membership_seed(explicit_seed: str | None = None) -> str | None:
    if explicit_seed:
        return explicit_seed.strip()

    seeds = [seed.strip() for seed in os.getenv("SEEDS", "").split(",") if seed.strip()]
    if seeds:
        return seeds[0]

    advertise_addr = os.getenv("ADVERTISE_ADDR", "").strip()
    return advertise_addr or None


def load_or_create_local_node_id() -> str:
    node_id_file = os.path.abspath(NODE_ID_FILE)
    parent_dir = os.path.dirname(node_id_file)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    if os.path.exists(node_id_file):
        with open(node_id_file, "r", encoding="utf-8") as handle:
            node_id = handle.readline().strip()
            if node_id:
                return node_id

    node_id = uuid.uuid4().hex
    with open(node_id_file, "w", encoding="utf-8") as handle:
        handle.write(node_id + "\n1\n")

    return node_id


def resolve_origin_node_id(*, membership_seed: str | None) -> str:
    seed = resolve_membership_seed(membership_seed)

    if seed:
        try:
            from stopan.placement.cluster_view import ClusterMembershipClient

            self_addr = os.getenv("ADVERTISE_ADDR", "")
            cluster = ClusterMembershipClient(
                seed,
                self_addr=self_addr,
            ).get_cluster_view()

            if cluster.self_node_id:
                return cluster.self_node_id

        except Exception as exc:
            print(f"Could not resolve origin_node_id from membership. Using local identity: {exc}")

    return load_or_create_local_node_id()
