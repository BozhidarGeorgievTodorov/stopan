"""
Publicación de metadata packs en la red P2P.

Selecciona targets remotos por HRW y almacena packs cifrados y firmados mediante
MetadataPackService.StoreMetadataPack.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

import blake3
import grpc

from stopan.errors import StopanNetworkError
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.identity.signatures import sign_metadata_pack_hash
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash
from stopan.placement.cluster_resolver import require_cluster_view
from stopan.protection.policy import normalize_remote_rf
from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc
from stopan.rpc.options import grpc_channel_options


class MetadataPackPushError(StopanNetworkError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataPackTarget:
    node_id: str
    address: str


@dataclass(frozen=True, slots=True)
class MetadataPackTargetResult:
    node_id: str
    address: str
    status: int
    detail: str
    size_bytes: int = 0
    stored_at_unix: float = 0.0
    public_key_b64: str = ""
    signature_b64: str = ""

    @property
    def stored(self) -> bool:
        return self.status == p2p_storage_pb2.METADATA_PACK_STORE_STATUS_STORED

    @property
    def already_present(self) -> bool:
        return self.status == p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ALREADY_PRESENT

    @property
    def success(self) -> bool:
        return self.stored or self.already_present


@dataclass(frozen=True, slots=True)
class MetadataPackPushResult:
    owner_id: str
    pack_hash: str
    pack_path: Path
    pack_size_bytes: int
    desired_rf: int
    remote_candidates: int
    attempted_targets: int
    successful_targets: int
    stored_targets: int
    already_present_targets: int
    failed_targets: int
    insufficient_remote_targets: bool
    target_results: tuple[MetadataPackTargetResult, ...]

    @property
    def protected(self) -> bool:
        return self.successful_targets >= self.desired_rf


def _channel_options(
    *,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> list[tuple[str, int]]:
    return grpc_channel_options(
        max_message_bytes,
        keepalive_time_ms=grpc_keepalive_time_ms,
        keepalive_timeout_ms=grpc_keepalive_timeout_ms,
        keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
    )


def _hrw_score(*, owner_id: str, pack_hash: str, cluster_token: str, node_id: str, address: str) -> int:
    payload = "\x00".join(
        [
            "stopan.metadata-pack.hrw.v1",
            str(cluster_token or ""),
            owner_id,
            pack_hash,
            node_id,
            address,
        ]
    ).encode("utf-8")
    return int.from_bytes(blake3.blake3(payload).digest(), "big", signed=False)


def _select_targets(
    members: Iterable[object],
    *,
    owner_id: str,
    pack_hash: str,
    cluster_token: str,
    rf: int,
    self_addr: str,
    self_node_id: str | None,
) -> list[MetadataPackTarget]:
    candidates: list[MetadataPackTarget] = []
    normalized_self_addr = str(self_addr or "").strip()
    normalized_self_node_id = str(self_node_id or "").strip()

    for member in members:
        node_id = str(getattr(member, "node_id", "") or "").strip()
        address = str(getattr(member, "address", "") or "").strip()
        if not address:
            continue
        if normalized_self_addr and address == normalized_self_addr:
            continue
        if normalized_self_node_id and node_id == normalized_self_node_id:
            continue
        candidates.append(MetadataPackTarget(node_id=node_id, address=address))

    candidates.sort(
        key=lambda target: _hrw_score(
            owner_id=owner_id,
            pack_hash=pack_hash,
            cluster_token=cluster_token,
            node_id=target.node_id,
            address=target.address,
        ),
        reverse=True,
    )
    return candidates[: max(0, int(rf))]


def _store_pack_on_target(
    target: MetadataPackTarget,
    *,
    owner_id: str,
    pack_hash: str,
    pack_data: bytes,
    public_key_b64: str,
    signature_b64: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> MetadataPackTargetResult:
    channel = grpc.insecure_channel(
        target.address,
        options=_channel_options(
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ),
    )
    try:
        stub = p2p_storage_pb2_grpc.MetadataPackServiceStub(channel)
        response = stub.StoreMetadataPack(
            p2p_storage_pb2.StoreMetadataPackRequest(
                cluster_token=str(cluster_token or ""),
                owner_id=owner_id,
                pack_hash=pack_hash,
                pack_data=pack_data,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            ),
            timeout=float(timeout_s),
        )
        return MetadataPackTargetResult(
            node_id=target.node_id,
            address=target.address,
            status=int(response.status),
            detail=str(response.detail or ""),
            size_bytes=int(response.size_bytes),
            stored_at_unix=float(response.stored_at_unix),
            public_key_b64=str(getattr(response, "public_key_b64", "") or ""),
            signature_b64=str(getattr(response, "signature_b64", "") or ""),
        )
    except grpc.RpcError as exc:
        detail = exc.details() or str(exc)
        return MetadataPackTargetResult(
            node_id=target.node_id,
            address=target.address,
            status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
            detail=detail,
        )
    except Exception as exc:
        return MetadataPackTargetResult(
            node_id=target.node_id,
            address=target.address,
            status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
            detail=str(exc),
        )
    finally:
        try:
            channel.close()
        except Exception:
            pass


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

    path = Path(pack_path).expanduser().resolve()
    try:
        pack_data = path.read_bytes()
    except OSError as exc:
        raise MetadataPackPushError(f"No se pudo leer el metadata pack {path}: {exc}") from exc

    if not pack_data:
        raise MetadataPackPushError(f"metadata pack vacío: {path}")
    if len(pack_data) > max_message_bytes:
        raise MetadataPackPushError(
            f"metadata pack demasiado grande para gRPC: {len(pack_data)} bytes > max_message_bytes={max_message_bytes}. "
            "Aumenta grpc.max_message_bytes o usa en el futuro un transporte de metadata packs por streaming/chunks."
        )

    pack_hash = validate_pack_hash(calculate_pack_hash(pack_data))
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
            pack_size_bytes=len(pack_data),
            desired_rf=desired_rf,
            remote_candidates=0,
            attempted_targets=0,
            successful_targets=0,
            stored_targets=0,
            already_present_targets=0,
            failed_targets=0,
            insufficient_remote_targets=False,
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
            pack_size_bytes=len(pack_data),
            desired_rf=desired_rf,
            remote_candidates=remote_candidate_count,
            attempted_targets=0,
            successful_targets=0,
            stored_targets=0,
            already_present_targets=0,
            failed_targets=0,
            insufficient_remote_targets=True,
            target_results=(),
        )

    targets = _select_targets(
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
                    _store_pack_on_target,
                    target,
                    owner_id=owner,
                    pack_hash=pack_hash,
                    pack_data=pack_data,
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
        pack_size_bytes=len(pack_data),
        desired_rf=desired_rf,
        remote_candidates=remote_candidate_count,
        attempted_targets=len(results),
        successful_targets=successful,
        stored_targets=stored,
        already_present_targets=already_present,
        failed_targets=failed,
        insufficient_remote_targets=False,
        target_results=tuple(results),
    )
