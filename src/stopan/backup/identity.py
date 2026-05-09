"""
Resolución de identidad del nodo origen para snapshots.

El backup intenta asociar cada snapshot a un origin_node_id estable. Si hay
membership disponible, usa la identidad del nodo dentro del cluster; si no,
recurre a una identidad local persistida en disco.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from stopan.common.fs import atomic_write_bytes
from stopan.placement.cluster_resolver import try_cluster_view


def _canonical_node_id(value: str, *, node_id_file: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized:
        raise ValueError("node_id vacío")

    try:
        parsed = uuid.UUID(hex=normalized)
    except ValueError as exc:
        raise ValueError(f"node_id inválido en {node_id_file}: {value!r}") from exc

    if parsed.hex != normalized:
        raise ValueError(f"node_id no canónico en {node_id_file}: {value!r}")

    return normalized


def _read_persisted_node_id(node_id_file: str) -> str | None:
    if not os.path.exists(node_id_file):
        return None

    with open(node_id_file, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]

    if not lines:
        return None

    return _canonical_node_id(lines[0], node_id_file=node_id_file)


def _write_new_node_identity(node_id_file: str, node_id: str) -> None:
    atomic_write_bytes(
        Path(node_id_file).expanduser().resolve(),
        f"{node_id}\n1\n".encode("utf-8"),
        mode=0o600,
    )


def load_or_create_local_node_id(*, node_id_file: str) -> str:
    """
    Devuelve la identidad estable del nodo origen.

    Usa el mismo formato persistido que NodeIdentityStore:
        <node_id>
        <incarnation>

    Backup solo necesita node_id, pero debe leer correctamente identidades
    creadas por el runtime P2P.
    """
    node_id_file = os.path.abspath(node_id_file)

    existing = _read_persisted_node_id(node_id_file)
    if existing:
        return existing

    node_id = uuid.uuid4().hex
    _write_new_node_identity(node_id_file, node_id)
    return node_id


def resolve_origin_node_id(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    node_id_file: str,
    membership_timeout_s: float,
    max_message_bytes: int,
) -> str:
    """
    Resuelve el origin_node_id del snapshot.

    Preferencia:
      1. membership + self_addr, si está disponible.
      2. identidad local persistida en node_id_file.
    """
    if membership_seed:
        try:
            resolved = try_cluster_view(
                membership_seed=membership_seed,
                self_addr=self_addr,
                cluster_token=cluster_token,
                timeout_s=membership_timeout_s,
                max_message_bytes=max_message_bytes,
            )
            if resolved is not None and resolved.cluster.self_node_id:
                return resolved.cluster.self_node_id

        except Exception as exc:
            print(
                "No se pudo resolver origin_node_id desde membership. "
                f"Se usará la identidad local: {exc}"
            )

    return load_or_create_local_node_id(node_id_file=node_id_file)
