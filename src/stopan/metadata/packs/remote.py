"""
Cliente remoto mínimo para metadata packs.

Comparte la infraestructura gRPC usada por push y recuperación sin mezclar la
semántica de alto nivel de ambos comandos.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable

import blake3
import grpc

from stopan.errors import StopanNetworkError
from stopan.metadata.identity.signatures import verify_metadata_pack_signature
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash
from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc
from stopan.rpc.channels import temporary_insecure_channel


class MetadataPackRemoteError(StopanNetworkError, RuntimeError):
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
class MetadataPackSource:
    node_id: str
    address: str
    pack_hash: str
    size_bytes: int
    stored_at_unix: float
    public_key_b64: str = ""
    signature_b64: str = ""


def metadata_pack_target_label(source: MetadataPackSource) -> str:
    node = source.node_id[:8] if source.node_id else "desconocido"
    return f"{node}@{source.address}"


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


def select_metadata_pack_targets(
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


def store_metadata_pack_on_target(
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
    try:
        with temporary_insecure_channel(
            target.address,
            max_message_bytes=max_message_bytes,
            keepalive_time_ms=grpc_keepalive_time_ms,
            keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ) as channel:
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

def list_metadata_packs_from_target(
    target: object,
    *,
    owner_id: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> tuple[list[MetadataPackSource], str | None]:
    node_id = str(getattr(target, "node_id", "") or "")
    address = str(getattr(target, "address", "") or "")
    if not address:
        return [], "target sin address"

    try:
        with temporary_insecure_channel(
            address,
            max_message_bytes=max_message_bytes,
            keepalive_time_ms=grpc_keepalive_time_ms,
            keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ) as channel:
            stub = p2p_storage_pb2_grpc.MetadataPackServiceStub(channel)
            response = stub.ListMetadataPacks(
                p2p_storage_pb2.ListMetadataPacksRequest(
                    cluster_token=str(cluster_token or ""),
                    owner_id=owner_id,
                ),
                timeout=float(timeout_s),
            )
        records: list[MetadataPackSource] = []
        for item in response.packs:
            try:
                pack_hash = validate_pack_hash(str(item.pack_hash))
            except (TypeError, ValueError):
                continue
            if str(item.owner_id or "") != owner_id:
                continue
            public_key_b64 = str(getattr(item, "public_key_b64", "") or "")
            signature_b64 = str(getattr(item, "signature_b64", "") or "")
            if not verify_metadata_pack_signature(
                owner_id=owner_id,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
                pack_hash=pack_hash,
            ):
                continue
            records.append(
                MetadataPackSource(
                    node_id=node_id,
                    address=address,
                    pack_hash=pack_hash,
                    size_bytes=int(item.size_bytes),
                    stored_at_unix=float(item.stored_at_unix),
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                )
            )
        return records, None
    except grpc.RpcError as exc:
        return [], f"{node_id[:8] or 'desconocido'}@{address}: {exc.details() or str(exc)}"
    except Exception as exc:
        return [], f"{node_id[:8] or 'desconocido'}@{address}: {exc}"


def probe_metadata_pack_from_target(
    target: object,
    *,
    owner_id: str,
    pack_hash: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> tuple[MetadataPackSource | None, str | None]:
    node_id = str(getattr(target, "node_id", "") or "")
    address = str(getattr(target, "address", "") or "")
    if not address:
        return None, "target sin address"

    try:
        with temporary_insecure_channel(
            address,
            max_message_bytes=max_message_bytes,
            keepalive_time_ms=grpc_keepalive_time_ms,
            keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ) as channel:
            stub = p2p_storage_pb2_grpc.MetadataPackServiceStub(channel)
            response = stub.ProbeMetadataPack(
                p2p_storage_pb2.ProbeMetadataPackRequest(
                    cluster_token=str(cluster_token or ""),
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                ),
                timeout=float(timeout_s),
            )
        status = int(response.status)
        if status == p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_MISSING:
            return None, None
        if status != p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_PRESENT:
            return None, f"{node_id[:8] or 'desconocido'}@{address}: {response.detail or 'probe error'}"

        probed_hash = validate_pack_hash(str(response.pack_hash or pack_hash))
        if probed_hash != pack_hash:
            return None, f"{node_id[:8] or 'desconocido'}@{address}: pack_hash inesperado en probe"
        if str(response.owner_id or "") != owner_id:
            return None, f"{node_id[:8] or 'desconocido'}@{address}: owner_id inesperado en probe"

        public_key_b64 = str(getattr(response, "public_key_b64", "") or "")
        signature_b64 = str(getattr(response, "signature_b64", "") or "")
        if not verify_metadata_pack_signature(
            owner_id=owner_id,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
            pack_hash=pack_hash,
        ):
            return None, f"{node_id[:8] or 'desconocido'}@{address}: firma inválida en probe"

        return (
            MetadataPackSource(
                node_id=node_id,
                address=address,
                pack_hash=pack_hash,
                size_bytes=int(response.size_bytes),
                stored_at_unix=float(response.stored_at_unix),
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            ),
            None,
        )
    except grpc.RpcError as exc:
        return None, f"{node_id[:8] or 'desconocido'}@{address}: {exc.details() or str(exc)}"
    except Exception as exc:
        return None, f"{node_id[:8] or 'desconocido'}@{address}: {exc}"


def retrieve_metadata_pack_from_source(
    source: MetadataPackSource,
    *,
    owner_id: str,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> bytes:
    if source.size_bytes > int(max_message_bytes):
        raise MetadataPackRemoteError(
            f"metadata pack {source.pack_hash} anunciado por {metadata_pack_target_label(source)} supera max_message_bytes: "
            f"{source.size_bytes} > {int(max_message_bytes)}"
        )

    with temporary_insecure_channel(
        source.address,
        max_message_bytes=max_message_bytes,
        keepalive_time_ms=grpc_keepalive_time_ms,
        keepalive_timeout_ms=grpc_keepalive_timeout_ms,
        keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
    ) as channel:
        stub = p2p_storage_pb2_grpc.MetadataPackServiceStub(channel)
        response = stub.RetrieveMetadataPack(
            p2p_storage_pb2.RetrieveMetadataPackRequest(
                cluster_token=str(cluster_token or ""),
                owner_id=owner_id,
                pack_hash=source.pack_hash,
            ),
            timeout=float(timeout_s),
        )
    if int(response.status) != p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_FOUND:
        raise MetadataPackRemoteError(
            f"retrieve {source.pack_hash} desde {metadata_pack_target_label(source)} falló: "
            f"status={int(response.status)} detail={response.detail or ''}"
        )
    data = bytes(response.pack_data)
    calculated = calculate_pack_hash(data)
    if calculated != source.pack_hash:
        raise MetadataPackRemoteError(
            f"pack_hash descargado no coincide desde {metadata_pack_target_label(source)}: "
            f"esperado={source.pack_hash} calculado={calculated}"
        )
    public_key_b64 = str(getattr(response, "public_key_b64", "") or source.public_key_b64 or "")
    signature_b64 = str(getattr(response, "signature_b64", "") or source.signature_b64 or "")
    if not verify_metadata_pack_signature(
        owner_id=owner_id,
        public_key_b64=public_key_b64,
        signature_b64=signature_b64,
        pack_hash=source.pack_hash,
    ):
        raise MetadataPackRemoteError(
            f"firma de metadata pack inválida desde {metadata_pack_target_label(source)}: "
            f"{source.pack_hash}"
        )
    return data
