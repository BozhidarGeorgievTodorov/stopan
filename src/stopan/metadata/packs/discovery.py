"""
Descubrimiento de metadata packs distribuidos.

Consulta nodos, valida listados firmados, agrupa copias por pack_hash y, si
existe intención local persistida, calcula estado de presencia. Si la intención
no existe, el estado queda UNKNOWN: discovery observa realidad remota, no inventa
política de protección.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import StrEnum

from stopan.cluster.resolver import require_cluster_view
from stopan.cluster.view import ClusterMember
from stopan.errors import StopanNetworkError
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.packs.remote import (
    MetadataPackSource,
    list_metadata_packs_from_target as _list_packs_from_target,
)


class MetadataPackDiscoveryError(StopanNetworkError, RuntimeError):
    pass


class MetadataPackPresenceState(StrEnum):
    UNKNOWN = "UNKNOWN"
    VERIFIED = "VERIFIED"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class MetadataPackDiscoveryStats:
    sources_seen: int
    unique_packs_seen: int
    list_targets_attempted: int
    list_targets_succeeded: int
    publications_known: int
    list_errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MetadataPackDiscoveryEntry:
    pack_hash: str
    presence_state: MetadataPackPresenceState
    copies_seen: int
    desired_copies: int | None
    desired_copies_source: str
    newest_stored_at_unix: float
    oldest_stored_at_unix: float
    size_bytes: int
    sources: tuple[MetadataPackSource, ...]

    @property
    def valid_copies(self) -> int:
        return int(self.copies_seen)

    @property
    def state(self) -> MetadataPackPresenceState:
        return self.presence_state


@dataclass(frozen=True, slots=True)
class MetadataPackDiscoveryResult:
    owner_id: str
    stats: MetadataPackDiscoveryStats
    entries: tuple[MetadataPackDiscoveryEntry, ...]


def metadata_pack_presence_state(
    *,
    copies_seen: int,
    desired_copies: int | None,
) -> MetadataPackPresenceState:
    copies = max(0, int(copies_seen))
    if desired_copies is None:
        return MetadataPackPresenceState.UNKNOWN

    desired = int(desired_copies)
    if desired < 1:
        raise MetadataPackDiscoveryError("metadata pack presence requiere desired_copies >= 1")
    if copies >= desired:
        return MetadataPackPresenceState.VERIFIED
    if copies > 0:
        return MetadataPackPresenceState.DEGRADED
    return MetadataPackPresenceState.FAILED


def group_metadata_pack_sources(
    sources: list[MetadataPackSource] | tuple[MetadataPackSource, ...],
    *,
    desired_copies_by_hash: Mapping[str, int] | None = None,
    max_candidates: int | None = None,
) -> list[MetadataPackDiscoveryEntry]:
    desired_map = dict(desired_copies_by_hash or {})
    grouped: dict[str, list[MetadataPackSource]] = {}
    for source in sources:
        grouped.setdefault(source.pack_hash, []).append(source)

    entries: list[MetadataPackDiscoveryEntry] = []
    for pack_hash, pack_sources in grouped.items():
        ordered = sorted(
            pack_sources,
            key=lambda item: (item.address, item.node_id, item.pack_hash),
        )
        newest = max(ordered, key=lambda item: (float(item.stored_at_unix), item.address))
        oldest = min(ordered, key=lambda item: (float(item.stored_at_unix), item.address))
        copies_seen = len(ordered)
        desired_copies = desired_map.get(pack_hash)
        entries.append(
            MetadataPackDiscoveryEntry(
                pack_hash=pack_hash,
                presence_state=metadata_pack_presence_state(
                    copies_seen=copies_seen,
                    desired_copies=desired_copies,
                ),
                copies_seen=copies_seen,
                desired_copies=int(desired_copies) if desired_copies is not None else None,
                desired_copies_source="publication_record" if desired_copies is not None else "unknown",
                newest_stored_at_unix=float(newest.stored_at_unix),
                oldest_stored_at_unix=float(oldest.stored_at_unix),
                size_bytes=int(newest.size_bytes),
                sources=tuple(ordered),
            )
        )

    entries.sort(key=lambda item: item.pack_hash)
    if max_candidates is not None and int(max_candidates) > 0:
        entries = entries[: int(max_candidates)]
    return entries


def collect_metadata_pack_sources(
    *,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    target_parallelism: int,
    max_message_bytes: int,
    error_cls: type[Exception] = MetadataPackDiscoveryError,
    query_target: Callable[[ClusterMember], tuple[Sequence[MetadataPackSource], str | None]],
) -> tuple[list[ClusterMember], list[MetadataPackSource], tuple[str, ...]]:
    try:
        resolved = require_cluster_view(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=int(max_message_bytes),
            missing_seed_message="Falta membership seed en la configuración.",
        )
    except Exception as exc:
        raise error_cls(str(exc)) from exc

    targets = [member for member in resolved.cluster.members if str(member.address or "").strip()]
    if not targets:
        raise error_cls("Membership no devolvió miembros elegibles del cluster.")

    parallelism = max(1, int(target_parallelism))
    sources: list[MetadataPackSource] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(parallelism, len(targets))) as executor:
        futures = [executor.submit(query_target, target) for target in targets]
        for future in as_completed(futures):
            records, error = future.result()
            sources.extend(records)
            if error:
                errors.append(error)
    return targets, sources, tuple(errors)


def discover_metadata_packs_from_network(
    *,
    owner_id: str,
    membership_seed: str | None,
    self_addr: str,
    cluster_token: str,
    membership_timeout_s: float,
    rpc_timeout_s: float,
    target_parallelism: int,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
    desired_copies_by_hash: Mapping[str, int] | None = None,
    max_candidates: int | None = None,
) -> MetadataPackDiscoveryResult:
    owner = validate_owner_id(owner_id)
    targets, all_sources, list_errors = collect_metadata_pack_sources(
        membership_seed=membership_seed,
        self_addr=self_addr,
        cluster_token=cluster_token,
        membership_timeout_s=membership_timeout_s,
        target_parallelism=target_parallelism,
        max_message_bytes=max_message_bytes,
        error_cls=MetadataPackDiscoveryError,
        query_target=lambda target: _list_packs_from_target(
            target,
            owner_id=owner,
            cluster_token=cluster_token,
            timeout_s=rpc_timeout_s,
            max_message_bytes=int(max_message_bytes),
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ),
    )

    desired_map = dict(desired_copies_by_hash or {})
    entries = group_metadata_pack_sources(
        all_sources,
        desired_copies_by_hash=desired_map,
        max_candidates=max_candidates,
    )
    return MetadataPackDiscoveryResult(
        owner_id=owner,
        stats=MetadataPackDiscoveryStats(
            sources_seen=len(all_sources),
            unique_packs_seen=len(entries),
            list_targets_attempted=len(targets),
            list_targets_succeeded=len(targets) - len(list_errors),
            publications_known=sum(1 for entry in entries if entry.desired_copies is not None),
            list_errors=list_errors,
        ),
        entries=tuple(entries),
    )
