"""
Publicación de metadata packs en la red P2P.

Selecciona targets remotos por HRW y transmite packs cifrados y firmados mediante
un flujo de bloques de MetadataPackService.StoreMetadataPack.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from stopan.errors import StopanNetworkError
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.identity.signatures import sign_metadata_pack_hash
from stopan.metadata.packs.hashes import calculate_pack_hash_file, validate_pack_hash
from stopan.metadata.packs.remote import (
    MetadataPackTargetResult,
    select_metadata_pack_targets,
    store_metadata_pack_on_target,
)
from stopan.cluster.resolver import require_cluster_view
from stopan.protection.policy import normalize_remote_rf


class MetadataPackPushError(StopanNetworkError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataPackPushStats:
    desired_rf: int
    remote_candidates: int
    attempted_targets: int
    successful_targets: int
    stored_targets: int
    already_present_targets: int
    failed_targets: int
    insufficient_remote_targets: bool

    @property
    def protected(self) -> bool:
        return self.successful_targets >= self.desired_rf


@dataclass(frozen=True, slots=True)
class MetadataPackPushResult:
    owner_id: str
    pack_hash: str
    pack_path: Path
    pack_size_bytes: int
    stats: MetadataPackPushStats
    target_results: tuple[MetadataPackTargetResult, ...]

    @property
    def protected(self) -> bool:
        return self.stats.protected


def push_metadata_pack_to_network(
    *,
    pack_path: str | Path,
    owner_id: str | None,
    identity_file: str | Path,
    identity_passphrase: str | bytes,
    membership_seed: str | None,
    rf: int,
    strict_rf: bool,
    target_parallelism: int,
    rpc_timeout_s: float,
    membership_timeout_s: float,
    max_message_bytes: int,
    max_pack_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
    self_addr: str,
    cluster_token: str,
) -> MetadataPackPushResult:
    expected_owner = validate_owner_id(owner_id) if owner_id else None
    desired_rf = normalize_remote_rf(rf)
    parallelism = max(1, int(target_parallelism))
    max_message_bytes = int(max_message_bytes)
    max_pack_bytes = max(1, int(max_pack_bytes))

    path = Path(pack_path).expanduser().resolve()
    try:
        pack_size_bytes = int(path.stat().st_size)
    except OSError as exc:
        raise MetadataPackPushError(f"No se pudo inspeccionar el metadata pack {path}: {exc}") from exc
    if not path.is_file():
        raise MetadataPackPushError(f"metadata pack no es un archivo regular: {path}")
    if pack_size_bytes <= 0:
        raise MetadataPackPushError(f"metadata pack vacío: {path}")
    if pack_size_bytes > max_pack_bytes:
        raise MetadataPackPushError(
            f"metadata pack demasiado grande: {pack_size_bytes} bytes > {max_pack_bytes}"
        )

    try:
        pack_hash = validate_pack_hash(calculate_pack_hash_file(path))
    except OSError as exc:
        raise MetadataPackPushError(f"No se pudo leer el metadata pack {path}: {exc}") from exc
    owner, public_key_b64, signature_b64 = sign_metadata_pack_hash(
        identity_file=identity_file,
        passphrase=identity_passphrase,
        pack_hash=pack_hash,
        expected_owner_id=expected_owner,
    )

    if desired_rf == 0:
        return MetadataPackPushResult(
            owner_id=owner,
            pack_hash=pack_hash,
            pack_path=path,
            pack_size_bytes=pack_size_bytes,
            stats=MetadataPackPushStats(
                desired_rf=desired_rf,
                remote_candidates=0,
                attempted_targets=0,
                successful_targets=0,
                stored_targets=0,
                already_present_targets=0,
                failed_targets=0,
                insufficient_remote_targets=False,
            ),
            target_results=(),
        )

    try:
        resolved = require_cluster_view(
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            timeout_s=membership_timeout_s,
            max_message_bytes=max_message_bytes,
            missing_seed_message="Falta membership seed en la configuración.",
        )
    except Exception as exc:
        raise MetadataPackPushError(str(exc)) from exc

    cluster = resolved.cluster
    remote_candidates = [
        member
        for member in cluster.members
        if str(getattr(member, "address", "") or "").strip()
        and str(getattr(member, "address", "") or "").strip() != str(self_addr or "").strip()
        and (
            not str(getattr(cluster, "self_node_id", "") or "").strip()
            or str(getattr(member, "node_id", "") or "").strip()
            != str(getattr(cluster, "self_node_id", "") or "").strip()
        )
    ]
    remote_candidate_count = len(remote_candidates)

    if strict_rf and remote_candidate_count < desired_rf:
        return MetadataPackPushResult(
            owner_id=owner,
            pack_hash=pack_hash,
            pack_path=path,
            pack_size_bytes=pack_size_bytes,
            stats=MetadataPackPushStats(
                desired_rf=desired_rf,
                remote_candidates=remote_candidate_count,
                attempted_targets=0,
                successful_targets=0,
                stored_targets=0,
                already_present_targets=0,
                failed_targets=0,
                insufficient_remote_targets=True,
            ),
            target_results=(),
        )

    targets = select_metadata_pack_targets(
        remote_candidates,
        owner_id=owner,
        pack_hash=pack_hash,
        cluster_token=cluster_token,
        rf=desired_rf,
        self_addr=self_addr,
        self_node_id=str(getattr(cluster, "self_node_id", "") or ""),
    )

    results: list[MetadataPackTargetResult] = []
    if targets:
        with ThreadPoolExecutor(max_workers=min(parallelism, len(targets))) as executor:
            futures = [
                executor.submit(
                    store_metadata_pack_on_target,
                    target,
                    owner_id=owner,
                    pack_hash=pack_hash,
                    pack_path=path,
                    pack_size_bytes=pack_size_bytes,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                    cluster_token=cluster_token,
                    timeout_s=rpc_timeout_s,
                    max_message_bytes=max_message_bytes,
                    grpc_keepalive_time_ms=grpc_keepalive_time_ms,
                    grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
                    grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
                )
                for target in targets
            ]
            for future in as_completed(futures):
                results.append(future.result())

    results.sort(key=lambda item: item.address)
    stored = sum(1 for item in results if item.stored)
    already_present = sum(1 for item in results if item.already_present)
    successful = sum(1 for item in results if item.success)
    failed = len(results) - successful

    return MetadataPackPushResult(
        owner_id=owner,
        pack_hash=pack_hash,
        pack_path=path,
        pack_size_bytes=pack_size_bytes,
        stats=MetadataPackPushStats(
            desired_rf=desired_rf,
            remote_candidates=remote_candidate_count,
            attempted_targets=len(results),
            successful_targets=successful,
            stored_targets=stored,
            already_present_targets=already_present,
            failed_targets=failed,
            insufficient_remote_targets=False,
        ),
        target_results=tuple(results),
    )
