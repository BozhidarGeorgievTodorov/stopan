"""
Cliente remoto para metadata packs transferidos por bloques.

Comparte la infraestructura gRPC usada por push y recuperación sin mezclar la
semántica de alto nivel de ambos comandos.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum
import os
from pathlib import Path

import blake3
import grpc

from stopan.common.fs import ensure_private_dir, fsync_dir
from stopan.errors import StopanNetworkError
from stopan.metadata.identity.signatures import verify_metadata_pack_signature
from stopan.metadata.packs.hashes import validate_pack_hash
from stopan.metadata.packs.streaming import iter_file_chunks, metadata_pack_stream_chunk_bytes
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


class MetadataPackProbeState(StrEnum):
    PRESENT = "present"
    MISSING = "missing"
    DATA_LOSS = "data_loss"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class MetadataPackProbeOutcome:
    state: MetadataPackProbeState
    source: MetadataPackSource | None = None
    detail: str = ""


def metadata_pack_target_label(source: MetadataPackSource) -> str:
    node = source.node_id[:8] if source.node_id else "desconocido"
    return f"{node}@{source.address}"


def is_remote_metadata_pack_member(
    member: object,
    *,
    self_addr: str,
    self_node_id: str | None,
) -> bool:
    address = str(getattr(member, "address", "") or "").strip()
    if not address:
        return False

    normalized_self_addr = str(self_addr or "").strip()
    if normalized_self_addr and address == normalized_self_addr:
        return False

    node_id = str(getattr(member, "node_id", "") or "").strip()
    normalized_self_node_id = str(self_node_id or "").strip()
    if normalized_self_node_id and node_id == normalized_self_node_id:
        return False
    return True


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
    for member in members:
        if not is_remote_metadata_pack_member(
            member,
            self_addr=self_addr,
            self_node_id=self_node_id,
        ):
            continue
        candidates.append(
            MetadataPackTarget(
                node_id=str(getattr(member, "node_id", "") or "").strip(),
                address=str(getattr(member, "address", "") or "").strip(),
            )
        )

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


def _store_metadata_pack_requests(
    *,
    pack_path: Path,
    chunk_bytes: int,
    cluster_token: str,
    owner_id: str,
    pack_hash: str,
    pack_size_bytes: int,
    public_key_b64: str,
    signature_b64: str,
) -> Iterator[object]:
    yield p2p_storage_pb2.StoreMetadataPackRequest(
        header=p2p_storage_pb2.StoreMetadataPackHeader(
            cluster_token=str(cluster_token or ""),
            owner_id=owner_id,
            pack_hash=pack_hash,
            size_bytes=int(pack_size_bytes),
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
        )
    )
    for block in iter_file_chunks(pack_path, chunk_bytes=chunk_bytes):
        yield p2p_storage_pb2.StoreMetadataPackRequest(chunk_data=block)


def store_metadata_pack_on_target(
    target: MetadataPackTarget,
    *,
    owner_id: str,
    pack_hash: str,
    pack_path: str | Path,
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
    path = Path(pack_path).expanduser().resolve()
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
                _store_metadata_pack_requests(
                    pack_path=path,
                    chunk_bytes=metadata_pack_stream_chunk_bytes(max_message_bytes),
                    cluster_token=cluster_token,
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    pack_size_bytes=pack_size_bytes,
                    public_key_b64=public_key_b64,
                    signature_b64=signature_b64,
                ),
                timeout=float(timeout_s),
            )
        status = int(response.status)
        response_public_key_b64 = str(getattr(response, "public_key_b64", "") or "")
        response_signature_b64 = str(getattr(response, "signature_b64", "") or "")
        if status in {
            p2p_storage_pb2.METADATA_PACK_STORE_STATUS_STORED,
            p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ALREADY_PRESENT,
        }:
            response_owner_id = str(getattr(response, "owner_id", "") or "")
            response_pack_hash = validate_pack_hash(
                str(getattr(response, "pack_hash", "") or "")
            )
            response_size_bytes = int(getattr(response, "size_bytes", 0))
            if response_owner_id != owner_id:
                raise MetadataPackRemoteError(
                    "la respuesta de almacenamiento devolvió un owner_id distinto: "
                    f"esperado={owner_id} recibido={response_owner_id}"
                )
            if response_pack_hash != pack_hash:
                raise MetadataPackRemoteError(
                    "la respuesta de almacenamiento devolvió un pack_hash distinto: "
                    f"esperado={pack_hash} recibido={response_pack_hash}"
                )
            if response_size_bytes != int(pack_size_bytes):
                raise MetadataPackRemoteError(
                    "la respuesta de almacenamiento devolvió un tamaño distinto: "
                    f"esperado={int(pack_size_bytes)} recibido={response_size_bytes}"
                )
            if response_public_key_b64 != public_key_b64 or response_signature_b64 != signature_b64:
                raise MetadataPackRemoteError(
                    "la respuesta de almacenamiento devolvió un sidecar de firma distinto"
                )

        return MetadataPackTargetResult(
            node_id=target.node_id,
            address=target.address,
            status=status,
            detail=str(response.detail or ""),
            size_bytes=int(response.size_bytes),
            stored_at_unix=float(response.stored_at_unix),
            public_key_b64=response_public_key_b64,
            signature_b64=response_signature_b64,
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


def probe_metadata_pack_from_target_detailed(
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
) -> MetadataPackProbeOutcome:
    node_id = str(getattr(target, "node_id", "") or "")
    address = str(getattr(target, "address", "") or "")
    label = f"{node_id[:8] or 'desconocido'}@{address}"
    if not address:
        return MetadataPackProbeOutcome(
            state=MetadataPackProbeState.ERROR,
            detail="target sin address",
        )

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
            return MetadataPackProbeOutcome(state=MetadataPackProbeState.MISSING)
        if status != p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_PRESENT:
            return MetadataPackProbeOutcome(
                state=MetadataPackProbeState.ERROR,
                detail=f"{label}: {response.detail or 'probe error'}",
            )

        probed_hash = validate_pack_hash(str(response.pack_hash or pack_hash))
        if probed_hash != pack_hash:
            return MetadataPackProbeOutcome(
                state=MetadataPackProbeState.ERROR,
                detail=f"{label}: pack_hash inesperado en probe",
            )
        if str(response.owner_id or "") != owner_id:
            return MetadataPackProbeOutcome(
                state=MetadataPackProbeState.ERROR,
                detail=f"{label}: owner_id inesperado en probe",
            )

        public_key_b64 = str(getattr(response, "public_key_b64", "") or "")
        signature_b64 = str(getattr(response, "signature_b64", "") or "")
        if not verify_metadata_pack_signature(
            owner_id=owner_id,
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
            pack_hash=pack_hash,
        ):
            return MetadataPackProbeOutcome(
                state=MetadataPackProbeState.ERROR,
                detail=f"{label}: firma inválida en probe",
            )

        return MetadataPackProbeOutcome(
            state=MetadataPackProbeState.PRESENT,
            source=MetadataPackSource(
                node_id=node_id,
                address=address,
                pack_hash=pack_hash,
                size_bytes=int(response.size_bytes),
                stored_at_unix=float(response.stored_at_unix),
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            ),
        )
    except grpc.RpcError as exc:
        detail = f"{label}: {exc.details() or str(exc)}"
        if exc.code() == getattr(grpc.StatusCode, "DATA_LOSS", None):
            return MetadataPackProbeOutcome(
                state=MetadataPackProbeState.DATA_LOSS,
                detail=detail,
            )
        return MetadataPackProbeOutcome(
            state=MetadataPackProbeState.ERROR,
            detail=detail,
        )
    except Exception as exc:
        return MetadataPackProbeOutcome(
            state=MetadataPackProbeState.ERROR,
            detail=f"{label}: {exc}",
        )


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
    outcome = probe_metadata_pack_from_target_detailed(
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
    if outcome.state == MetadataPackProbeState.PRESENT:
        return outcome.source, None
    if outcome.state == MetadataPackProbeState.MISSING:
        return None, None
    return None, outcome.detail or "probe error"


def retrieve_metadata_pack_from_source(
    source: MetadataPackSource,
    *,
    owner_id: str,
    out_path: str | Path,
    cluster_token: str,
    timeout_s: float,
    max_message_bytes: int,
    max_pack_bytes: int,
    grpc_keepalive_time_ms: int,
    grpc_keepalive_timeout_ms: int,
    grpc_keepalive_permit_without_calls: bool,
) -> Path:
    destination = Path(out_path).expanduser().resolve()
    ensure_private_dir(destination.parent)
    partial = destination.with_name(
        f".{destination.name}.{os.getpid()}.{os.urandom(8).hex()}.part"
    )
    published = False

    try:
        with temporary_insecure_channel(
            source.address,
            max_message_bytes=max_message_bytes,
            keepalive_time_ms=grpc_keepalive_time_ms,
            keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ) as channel:
            stub = p2p_storage_pb2_grpc.MetadataPackServiceStub(channel)
            responses = iter(
                stub.RetrieveMetadataPack(
                    p2p_storage_pb2.RetrieveMetadataPackRequest(
                        cluster_token=str(cluster_token or ""),
                        owner_id=owner_id,
                        pack_hash=source.pack_hash,
                    ),
                    timeout=float(timeout_s),
                )
            )

            try:
                first = next(responses)
            except StopIteration as exc:
                raise MetadataPackRemoteError(
                    f"retrieve {source.pack_hash} desde {metadata_pack_target_label(source)} no devolvió cabecera"
                ) from exc

            if first.WhichOneof("part") != "header":
                raise MetadataPackRemoteError(
                    f"retrieve {source.pack_hash} desde {metadata_pack_target_label(source)} comenzó sin cabecera"
                )
            header = first.header
            if int(header.status) != p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_FOUND:
                raise MetadataPackRemoteError(
                    f"retrieve {source.pack_hash} desde {metadata_pack_target_label(source)} falló: "
                    f"status={int(header.status)} detail={header.detail or ''}"
                )

            received_owner = str(header.owner_id or "")
            received_hash = validate_pack_hash(str(header.pack_hash or ""))
            declared_size = int(header.size_bytes)
            if received_owner != owner_id:
                raise MetadataPackRemoteError(
                    f"owner_id descargado no coincide desde {metadata_pack_target_label(source)}: "
                    f"esperado={owner_id} recibido={received_owner}"
                )
            if received_hash != source.pack_hash:
                raise MetadataPackRemoteError(
                    f"pack_hash anunciado no coincide desde {metadata_pack_target_label(source)}: "
                    f"esperado={source.pack_hash} recibido={received_hash}"
                )
            if declared_size <= 0:
                raise MetadataPackRemoteError(
                    f"metadata pack {source.pack_hash} anunció un tamaño inválido: {declared_size}"
                )
            if declared_size > int(max_pack_bytes):
                raise MetadataPackRemoteError(
                    f"metadata pack {source.pack_hash} supera el límite local: "
                    f"{declared_size} > {int(max_pack_bytes)}"
                )
            if source.size_bytes > 0 and declared_size != int(source.size_bytes):
                raise MetadataPackRemoteError(
                    f"tamaño anunciado no coincide desde {metadata_pack_target_label(source)}: "
                    f"esperado={int(source.size_bytes)} recibido={declared_size}"
                )

            public_key_b64 = str(header.public_key_b64 or source.public_key_b64 or "")
            signature_b64 = str(header.signature_b64 or source.signature_b64 or "")
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

            hasher = blake3.blake3()
            received_size = 0
            try:
                fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except OSError as exc:
                raise MetadataPackRemoteError(
                    f"No se pudo crear el temporal de recuperación {partial}: {exc}"
                ) from exc

            with os.fdopen(fd, "wb") as fh:
                for response in responses:
                    if response.WhichOneof("part") != "chunk_data":
                        raise MetadataPackRemoteError(
                            f"retrieve {source.pack_hash} recibió una parte inesperada"
                        )
                    block = bytes(response.chunk_data)
                    if not block:
                        continue
                    received_size += len(block)
                    if received_size > declared_size:
                        raise MetadataPackRemoteError(
                            f"metadata pack {source.pack_hash} excede el tamaño anunciado: "
                            f"{received_size} > {declared_size}"
                        )
                    hasher.update(block)
                    fh.write(block)
                fh.flush()
                os.fsync(fh.fileno())

        if received_size != declared_size:
            raise MetadataPackRemoteError(
                f"metadata pack {source.pack_hash} incompleto: "
                f"esperado={declared_size} recibido={received_size}"
            )
        calculated = hasher.hexdigest()
        if calculated != source.pack_hash:
            raise MetadataPackRemoteError(
                f"pack_hash descargado no coincide desde {metadata_pack_target_label(source)}: "
                f"esperado={source.pack_hash} calculado={calculated}"
            )

        try:
            os.replace(partial, destination)
        except OSError as exc:
            raise MetadataPackRemoteError(
                f"No se pudo publicar el metadata pack recuperado {destination}: {exc}"
            ) from exc
        published = True
        fsync_dir(destination.parent)
        return destination
    except grpc.RpcError as exc:
        raise MetadataPackRemoteError(
            f"{metadata_pack_target_label(source)}: {exc.details() or str(exc)}"
        ) from exc
    finally:
        if not published:
            try:
                partial.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

