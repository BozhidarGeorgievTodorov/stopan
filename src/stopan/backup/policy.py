from __future__ import annotations

import os

from stopan.backup.config import CLUSTER_TOKEN
from stopan.backup.identity import resolve_membership_seed
from stopan.backup.models import BackupPolicy


def resolve_remote_placement_epoch(
    *,
    desired_rf: int,
    seed: str | None,
    origin_node_id: str,
) -> str | None:
    """
    Resolve the current remote placement_epoch for fast-remote decisions.

    The epoch must exclude origin_node_id so it matches replicator, verifier
    and restore.
    """
    resolved_seed = resolve_membership_seed(seed)
    if not resolved_seed:
        return None

    from stopan.placement.cluster_view import ClusterMembershipClient

    self_addr = os.getenv("ADVERTISE_ADDR", "")
    cluster = ClusterMembershipClient(resolved_seed, self_addr=self_addr).get_cluster_view()

    if not cluster.members:
        return None

    return cluster.placement_epoch_excluding(
        desired_rf=max(int(desired_rf), 1),
        cluster_token=CLUSTER_TOKEN,
        excluded_node_ids={origin_node_id},
    )


def build_backup_fast_path_policy(
    *,
    desired_rf: int,
    fast_local_enabled: bool,
    fast_remote_enabled: bool,
    safe_mode: bool,
    membership_seed: str | None,
    origin_node_id: str,
) -> BackupPolicy:
    desired_rf = max(int(desired_rf), 1)
    fast_local_enabled = bool(fast_local_enabled)
    fast_remote_enabled = bool(fast_remote_enabled)
    safe_mode = bool(safe_mode)
    origin_node_id = str(origin_node_id).strip()

    if not origin_node_id:
        raise ValueError("Backup fast-path policy requires a non-empty origin_node_id.")

    if safe_mode:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=False,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    fast_local_enabled = bool(fast_local_enabled or fast_remote_enabled)

    if not fast_local_enabled:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=False,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    if not fast_remote_enabled:
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    resolved_seed = resolve_membership_seed(membership_seed)
    if not resolved_seed:
        print("Remote fast-path requested, but no membership seed was found. Using local fast-path only.")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    try:
        placement_epoch = resolve_remote_placement_epoch(
            desired_rf=desired_rf,
            seed=resolved_seed,
            origin_node_id=origin_node_id,
        )
    except Exception as exc:
        print(f"Could not read placement_epoch. Using local fast-path only: {exc}")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    if placement_epoch is None:
        print("Membership did not return a usable eligible view. Using local fast-path only.")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    return BackupPolicy(
        desired_rf=desired_rf,
        fast_local_enabled=True,
        fast_remote_enabled=True,
        placement_epoch=placement_epoch,
    )
