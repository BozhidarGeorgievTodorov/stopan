"""
Verificación de presencia de metadata packs distribuidos.

El verifier audita presencia remota. No descarga packs ni inspecciona payloads
cifrados: para un pack concreto prioriza los últimos custodios acreditados y
amplía el sondeo solo cuando no bastan para satisfacer el objetivo. El modo
general reutiliza el discovery enriquecido basado en ListMetadataPacks.

La política esperada no se toma de flags operativos: se recibe desde metadata
local persistida. Los custodios recordados son solo una pista para ordenar las
consultas y no forman parte de esa política. Si no existe publicación local para
un pack, el estado queda UNKNOWN y se muestran las copias observadas.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from stopan.cluster.resolver import require_cluster_view
from stopan.errors import StopanNetworkError
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.packs.discovery import (
    MetadataPackDiscoveryEntry,
    MetadataPackPresenceState,
    discover_metadata_packs_from_network,
    group_metadata_pack_sources,
    metadata_pack_presence_state,
)
from stopan.metadata.packs.hashes import validate_pack_hash
from stopan.metadata.packs.remote import (
    MetadataPackSource,
    is_remote_metadata_pack_member,
    probe_metadata_pack_from_target,
)
from stopan.node.lifecycle import current_local_operation_matches


class MetadataPackVerificationError(StopanNetworkError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataPackVerificationStats:
    candidates_checked: int
    verified: int
    degraded: int
    failed: int
    unknown: int
    list_targets_attempted: int
    list_targets_succeeded: int
    sources_seen: int
    publications_known: int
    list_errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MetadataPackVerificationDetails:
    check_kind: str
    presence_state: MetadataPackPresenceState
    desired_copies: int | None
    desired_copies_source: str
    copies_seen: int
    reason: str = ""

    @property
    def state(self) -> MetadataPackPresenceState:
        return self.presence_state


@dataclass(frozen=True, slots=True)
class MetadataPackVerificationResult:
    pack_hash: str
    entry: MetadataPackDiscoveryEntry | None
    details: MetadataPackVerificationDetails

    @property
    def state(self) -> MetadataPackPresenceState:
        return self.details.presence_state

    @property
    def copies_seen(self) -> int:
        return int(self.details.copies_seen)

    @property
    def valid_copies(self) -> int:
        return int(self.details.copies_seen)

    @property
    def desired_copies(self) -> int | None:
        return self.details.desired_copies

    @property
    def newest_stored_at_unix(self) -> float:
        return float(self.entry.newest_stored_at_unix) if self.entry is not None else 0.0

    @property
    def oldest_stored_at_unix(self) -> float:
        return float(self.entry.oldest_stored_at_unix) if self.entry is not None else 0.0

    @property
    def size_bytes(self) -> int:
        return int(self.entry.size_bytes) if self.entry is not None else 0

    @property
    def sources(self) -> tuple[MetadataPackSource, ...]:
        return tuple(self.entry.sources) if self.entry is not None else ()


@dataclass(frozen=True, slots=True)
class MetadataPackVerificationRunResult:
    owner_id: str
    stats: MetadataPackVerificationStats
    results: tuple[MetadataPackVerificationResult, ...]


def _verification_result_from_entry(entry: MetadataPackDiscoveryEntry) -> MetadataPackVerificationResult:
    reason = "" if entry.desired_copies is not None else "no hay publicación local con copias esperadas"
    return MetadataPackVerificationResult(
        pack_hash=entry.pack_hash,
        entry=entry,
        details=MetadataPackVerificationDetails(
            check_kind="presence",
            presence_state=entry.presence_state,
            desired_copies=entry.desired_copies,
            desired_copies_source=entry.desired_copies_source,
            copies_seen=int(entry.copies_seen),
            reason=reason,
        ),
    )


def _missing_pack_result(
    *,
    pack_hash: str,
    desired_copies: int | None,
    desired_copies_source: str,
    reason: str,
) -> MetadataPackVerificationResult:
    return MetadataPackVerificationResult(
        pack_hash=pack_hash,
        entry=None,
        details=MetadataPackVerificationDetails(
            check_kind="presence",
            presence_state=metadata_pack_presence_state(copies_seen=0, desired_copies=desired_copies),
            desired_copies=int(desired_copies) if desired_copies is not None else None,
            desired_copies_source=desired_copies_source,
            copies_seen=0,
            reason=reason,
        ),
    )


def _verification_stats(
    *,
    results: tuple[MetadataPackVerificationResult, ...],
    list_targets_attempted: int,
    list_targets_succeeded: int,
    sources_seen: int,
    publications_known: int,
    list_errors: tuple[str, ...],
) -> MetadataPackVerificationStats:
    return MetadataPackVerificationStats(
        candidates_checked=len(results),
        verified=sum(1 for item in results if item.state == MetadataPackPresenceState.VERIFIED),
        degraded=sum(1 for item in results if item.state == MetadataPackPresenceState.DEGRADED),
        failed=sum(1 for item in results if item.state == MetadataPackPresenceState.FAILED),
        unknown=sum(1 for item in results if item.state == MetadataPackPresenceState.UNKNOWN),
        list_targets_attempted=int(list_targets_attempted),
        list_targets_succeeded=int(list_targets_succeeded),
        sources_seen=int(sources_seen),
        publications_known=int(publications_known),
        list_errors=tuple(list_errors),
    )


def _probe_pack_from_network(
    *,
    owner_id: str,
    pack_hash: str,
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
    desired_copies: int | None,
    preferred_node_ids: Sequence[str] = (),
) -> tuple[MetadataPackVerificationResult, MetadataPackVerificationStats]:
    try:
        resolved = require_cluster_view(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=int(max_message_bytes),
            missing_seed_message="Falta membership seed en la configuración.",
            allow_empty_members=current_local_operation_matches(self_addr),
        )
    except Exception as exc:
        raise MetadataPackVerificationError(str(exc)) from exc

    targets = [
        member
        for member in resolved.cluster.members
        if is_remote_metadata_pack_member(
            member,
            self_addr=self_addr,
            self_node_id=str(getattr(resolved.cluster, "self_node_id", "") or ""),
        )
    ]

    preferred_targets: list[object] = []
    remaining_targets = list(targets)
    if desired_copies is not None and preferred_node_ids:
        by_node_id: dict[str, object] = {}
        for target in targets:
            node_id = str(getattr(target, "node_id", "") or "").strip()
            if node_id and node_id not in by_node_id:
                by_node_id[node_id] = target

        preferred_ids: set[str] = set()
        for raw_node_id in preferred_node_ids:
            node_id = str(raw_node_id or "").strip()
            if not node_id or node_id in preferred_ids:
                continue
            target = by_node_id.get(node_id)
            if target is None:
                continue
            preferred_targets.append(target)
            preferred_ids.add(node_id)

        remaining_targets = [
            target
            for target in targets
            if str(getattr(target, "node_id", "") or "").strip() not in preferred_ids
        ]

    remaining_targets.sort(
        key=lambda target: (
            str(getattr(target, "node_id", "") or ""),
            str(getattr(target, "address", "") or ""),
        )
    )

    parallelism = max(1, int(target_parallelism))
    sources: list[MetadataPackSource] = []
    errors: list[str] = []
    attempted = 0

    def probe_group(group: Sequence[object]) -> None:
        nonlocal attempted
        offset = 0
        while offset < len(group):
            if desired_copies is not None:
                remaining_needed = int(desired_copies) - len(sources)
                if remaining_needed <= 0:
                    return
                batch_size = min(parallelism, remaining_needed, len(group) - offset)
            else:
                batch_size = min(parallelism, len(group) - offset)

            batch = group[offset : offset + batch_size]
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = [
                    executor.submit(
                        probe_metadata_pack_from_target,
                        target,
                        owner_id=owner_id,
                        pack_hash=pack_hash,
                        cluster_token=cluster_token,
                        timeout_s=rpc_timeout_s,
                        max_message_bytes=int(max_message_bytes),
                        grpc_keepalive_time_ms=grpc_keepalive_time_ms,
                        grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
                        grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
                    )
                    for target in batch
                ]
                for future in as_completed(futures):
                    source, error = future.result()
                    if source is not None:
                        sources.append(source)
                    if error:
                        errors.append(error)

            attempted += len(batch)
            offset += len(batch)

    # Con intención local, las pistas se consumen como una fase separada. Solo
    # si no acreditan el objetivo se abre el sondeo al resto de candidatos.
    if desired_copies is not None:
        probe_group(preferred_targets)
        if len(sources) < int(desired_copies):
            probe_group(remaining_targets)
    else:
        # Sin objetivo local no existe una condición de terminación anticipada.
        # La verificación dirigida conserva por ello el sondeo amplio.
        probe_group(remaining_targets)

    desired_map = {pack_hash: int(desired_copies)} if desired_copies is not None else {}
    entries = group_metadata_pack_sources(
        sources,
        desired_copies_by_hash=desired_map,
        max_candidates=None,
    )
    if entries:
        result = _verification_result_from_entry(entries[0])
    else:
        result = _missing_pack_result(
            pack_hash=pack_hash,
            desired_copies=desired_copies,
            desired_copies_source="publication_record" if desired_copies is not None else "unknown",
            reason=(
                "metadata pack no encontrado en ningún nodo consultado"
                if desired_copies is not None
                else "metadata pack no encontrado y no hay publicación local con copias esperadas"
            ),
        )
    stats = _verification_stats(
        results=(result,),
        list_targets_attempted=attempted,
        list_targets_succeeded=attempted - len(errors),
        sources_seen=len(sources),
        publications_known=1 if desired_copies is not None else 0,
        list_errors=tuple(errors),
    )
    return result, stats

def verify_metadata_packs_from_network(
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
    preferred_node_ids_by_hash: Mapping[str, Sequence[str]] | None = None,
    pack_hash: str | None = None,
    verify_all: bool = False,
    max_candidates: int | None = None,
) -> MetadataPackVerificationRunResult:
    owner = validate_owner_id(owner_id)
    if (pack_hash is None and not verify_all) or (pack_hash is not None and verify_all):
        raise MetadataPackVerificationError(
            "la verificación requiere exactamente un selector: pack_hash o verify_all=True"
        )
    requested_hash = validate_pack_hash(pack_hash) if pack_hash is not None else None
    desired_map = dict(desired_copies_by_hash or {})
    preferred_map = dict(preferred_node_ids_by_hash or {})

    if requested_hash is not None:
        result, stats = _probe_pack_from_network(
            owner_id=owner,
            pack_hash=requested_hash,
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            membership_timeout_s=membership_timeout_s,
            rpc_timeout_s=rpc_timeout_s,
            target_parallelism=target_parallelism,
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
            desired_copies=desired_map.get(requested_hash),
            preferred_node_ids=preferred_map.get(requested_hash, ()),
        )
        return MetadataPackVerificationRunResult(owner_id=owner, stats=stats, results=(result,))

    try:
        discovery = discover_metadata_packs_from_network(
            owner_id=owner,
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            membership_timeout_s=membership_timeout_s,
            rpc_timeout_s=rpc_timeout_s,
            target_parallelism=target_parallelism,
            max_message_bytes=int(max_message_bytes),
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
            desired_copies_by_hash=desired_map,
            max_candidates=None,
        )
    except Exception as exc:
        raise MetadataPackVerificationError(str(exc)) from exc

    entries_by_hash = {entry.pack_hash: entry for entry in discovery.entries}

    # Las publicaciones persistidas expresan intención local y no pueden quedar
    # fuera de verify --all por un límite pensado para acotar candidatos
    # descubiertos únicamente en remoto.
    local_hashes = sorted(desired_map)
    remote_only_hashes = sorted(set(entries_by_hash) - set(desired_map))
    if max_candidates is not None and int(max_candidates) > 0:
        remote_only_hashes = remote_only_hashes[: int(max_candidates)]
    candidate_hashes = local_hashes + remote_only_hashes

    results_list: list[MetadataPackVerificationResult] = []
    for candidate_hash in candidate_hashes:
        entry = entries_by_hash.get(candidate_hash)
        if entry is not None:
            results_list.append(_verification_result_from_entry(entry))
            continue

        desired_copies = desired_map.get(candidate_hash)
        results_list.append(
            _missing_pack_result(
                pack_hash=candidate_hash,
                desired_copies=desired_copies,
                desired_copies_source=(
                    "publication_record" if desired_copies is not None else "unknown"
                ),
                reason="metadata pack publicado localmente no encontrado en ningún nodo remoto consultado",
            )
        )

    results = tuple(results_list)
    stats = _verification_stats(
        results=results,
        list_targets_attempted=discovery.stats.list_targets_attempted,
        list_targets_succeeded=discovery.stats.list_targets_succeeded,
        sources_seen=discovery.stats.sources_seen,
        publications_known=sum(1 for item in results if item.desired_copies is not None),
        list_errors=discovery.stats.list_errors,
    )
    return MetadataPackVerificationRunResult(owner_id=owner, stats=stats, results=results)
