"""
Publicación de metadata packs en la red P2P.

Selecciona targets remotos por HRW, consulta primero la presencia exacta del pack
y transmite por StoreMetadataPack únicamente a los custodios que lo necesitan.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from stopan.errors import StopanNetworkError
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.identity.signatures import sign_metadata_pack_hash
from stopan.metadata.packs.crypto import decrypt_pack_payload
from stopan.metadata.packs.hashes import validate_pack_hash
from stopan.metadata.packs.payload import validate_pack_payload
from stopan.metadata.packs.remote import (
    MetadataPackProbeState,
    MetadataPackSource,
    MetadataPackTarget,
    MetadataPackTargetResult,
    is_remote_metadata_pack_member,
    probe_metadata_pack_from_target_detailed,
    select_metadata_pack_targets,
    store_metadata_pack_on_target,
)
from stopan.cluster.resolver import require_cluster_view
from stopan.node.lifecycle import current_local_operation_matches
from stopan.protection.policy import normalize_remote_rf
from stopan.protos import p2p_storage_pb2


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


def _push_metadata_pack_to_target(
    target: MetadataPackTarget,
    *,
    owner_id: str,
    pack_hash: str,
    pack_path: Path,
    pack_size_bytes: int,
    public_key_b64: str,
    signature_b64: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> MetadataPackTargetResult:
    """Asegura la presencia del pack en un target sin retransmitirlo si ya está.

    Un fallo operativo del probe se conserva como fallo del target y no se
    interpreta como ausencia. DATA_LOSS es la excepción deliberada: permite que
    StoreMetadataPack repare un sidecar inválido, mientras que un pack corrupto
    vuelve a ser rechazado por la propia admisión.
    """

    try:
        probe = probe_metadata_pack_from_target_detailed(
            target,
            owner_id=owner_id,
            pack_hash=pack_hash,
            cluster_token=cluster_token,
            timeout_s=timeout_s,
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        )
        if probe.state == MetadataPackProbeState.ERROR:
            return _metadata_pack_target_error(
                target,
                detail=f"probe remoto falló: {probe.detail or 'error desconocido'}",
            )

        if probe.state == MetadataPackProbeState.PRESENT:
            if probe.source is None:
                return _metadata_pack_target_error(
                    target,
                    detail="probe remoto devolvió PRESENT sin evidencia del pack",
                )
            return _metadata_pack_present_result(
                target,
                source=probe.source,
                expected_size_bytes=pack_size_bytes,
                expected_public_key_b64=public_key_b64,
                expected_signature_b64=signature_b64,
            )

        if probe.state not in {
            MetadataPackProbeState.MISSING,
            MetadataPackProbeState.DATA_LOSS,
        }:
            return _metadata_pack_target_error(
                target,
                detail=f"estado inesperado del probe: {probe.state}",
            )

        # DATA_LOSS conserva la capacidad previa de una republicación para
        # reparar un sidecar perdido o inválido. Si el pack está corrupto,
        # StoreMetadataPack volverá a detectarlo y rechazará el target.
        return store_metadata_pack_on_target(
            target,
            owner_id=owner_id,
            pack_hash=pack_hash,
            pack_path=pack_path,
            pack_size_bytes=pack_size_bytes,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
            cluster_token=cluster_token,
            timeout_s=timeout_s,
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        )
    except Exception as exc:
        return _metadata_pack_target_error(
            target,
            detail=f"publicación remota falló: {exc}",
        )


def _metadata_pack_present_result(
    target: MetadataPackTarget,
    *,
    source: MetadataPackSource,
    expected_size_bytes: int,
    expected_public_key_b64: str,
    expected_signature_b64: str,
) -> MetadataPackTargetResult:
    """Convierte un probe PRESENT en el mismo contrato de éxito que Store."""

    if int(source.size_bytes) != int(expected_size_bytes):
        return _metadata_pack_target_error(
            target,
            detail=(
                "el probe devolvió un tamaño distinto: "
                f"esperado={int(expected_size_bytes)} recibido={int(source.size_bytes)}"
            ),
        )
    if (
        source.public_key_b64 != expected_public_key_b64
        or source.signature_b64 != expected_signature_b64
    ):
        return _metadata_pack_target_error(
            target,
            detail="el probe devolvió un sidecar de firma distinto",
        )

    return MetadataPackTargetResult(
        node_id=target.node_id,
        address=target.address,
        status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ALREADY_PRESENT,
        detail="ya presente en target remoto",
        size_bytes=int(source.size_bytes),
        stored_at_unix=float(source.stored_at_unix),
        public_key_b64=source.public_key_b64,
        signature_b64=source.signature_b64,
    )


def _metadata_pack_target_error(
    target: MetadataPackTarget,
    *,
    detail: str,
) -> MetadataPackTargetResult:
    return MetadataPackTargetResult(
        node_id=target.node_id,
        address=target.address,
        status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
        detail=str(detail),
    )


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
        payload, header, _compressed_bytes, _plaintext_bytes = decrypt_pack_payload(
            path,
            identity_file=identity_file,
            passphrase=identity_passphrase,
        )
        validate_pack_payload(payload)
        pack_hash = validate_pack_hash(header.pack_hash)
    except Exception as exc:
        raise MetadataPackPushError(
            f"metadata pack local inválido o no recuperable {path}: {exc}"
        ) from exc

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
            allow_empty_members=current_local_operation_matches(self_addr),
        )
    except Exception as exc:
        raise MetadataPackPushError(str(exc)) from exc

    cluster = resolved.cluster
    remote_candidates = [
        member
        for member in cluster.members
        if is_remote_metadata_pack_member(
            member,
            self_addr=self_addr,
            self_node_id=cluster.self_node_id,
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
                    _push_metadata_pack_to_target,
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
