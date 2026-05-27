"""
Construcción de la política efectiva de fast-path para backup.

El backup puede saltar chunks ya presentes en el CAS local y, opcionalmente,
chunks que ya tienen protección remota suficiente para el placement actual.
safe_mode desactiva ambos caminos rápidos.
"""

from __future__ import annotations

from stopan.backup.models import BackupPolicy
from stopan.errors import StopanConfigValueError
from stopan.cluster.resolver import try_cluster_view
from stopan.protection.policy import normalize_remote_rf


def resolve_remote_placement_epoch(
    *,
    desired_rf: int,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    max_message_bytes: int,
    origin_node_id: str,
) -> str | None:
    """
    Resuelve el placement_epoch remoto actual para decisiones de fast-remote.

    El epoch excluye origin_node_id porque RF representa exclusivamente copias
    remotas; los chunks locales del nodo origen no cuentan como réplicas P2P.
    """
    origin_node_id = str(origin_node_id or "").strip()
    if not origin_node_id:
        raise StopanConfigValueError("resolve_remote_placement_epoch requiere un origin_node_id.")

    resolved = try_cluster_view(
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        timeout_s=membership_timeout_s,
        max_message_bytes=max_message_bytes,
    )
    if resolved is None:
        return None

    remote_required = normalize_remote_rf(desired_rf, field_name="desired_rf")
    if remote_required <= 0:
        return None

    return resolved.cluster.placement_epoch_excluding(
        desired_rf=remote_required,
        cluster_token=cluster_token,
        excluded_node_ids={origin_node_id},
    )


def build_backup_fast_path_policy(
    *,
    desired_rf: int,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    max_message_bytes: int,
    fast_local_enabled: bool,
    fast_remote_enabled: bool,
    safe_mode: bool,
    origin_node_id: str,
) -> BackupPolicy:
    desired_rf = normalize_remote_rf(desired_rf, field_name="desired_rf")
    fast_local_enabled = bool(fast_local_enabled)
    fast_remote_enabled = bool(fast_remote_enabled)
    safe_mode = bool(safe_mode)
    required_remote_copies = desired_rf
    origin_node_id = str(origin_node_id or "").strip()

    if not origin_node_id:
        raise StopanConfigValueError("La política de fast-path de backup requiere un origin_node_id no vacío.")

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

    if required_remote_copies <= 0:
        print("Copias remotas deseadas: 0. Usando solo fast-path local.")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    if not membership_seed:
        print("Se pidió fast-path remoto, pero no hay membership seed. Usando solo fast-path local.")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    try:
        placement_epoch = resolve_remote_placement_epoch(
            desired_rf=desired_rf,
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            membership_timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            origin_node_id=origin_node_id,
        )
    except Exception as exc:
        print(f"No se pudo leer placement_epoch. Usando solo fast-path local: {exc}")
        return BackupPolicy(
            desired_rf=desired_rf,
            fast_local_enabled=True,
            fast_remote_enabled=False,
            placement_epoch=None,
        )

    if placement_epoch is None:
        print("Membership no devolvió una vista elegible usable. Usando solo fast-path local.")
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
