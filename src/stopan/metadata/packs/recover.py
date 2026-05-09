"""
Recuperación de metadata packs desde la red P2P.

Lista packs remotos firmados, descarga candidatos, valida firma/hash, descifra
con la identidad local e importa el estado más reciente recuperable.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import grpc

from stopan.common.fs import atomic_write_bytes, ensure_private_dir
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.identity.signatures import verify_metadata_pack_signature
from stopan.metadata.objects.service import MetadataObjectGraphStoreService
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash
from stopan.metadata.packs.object_pack import MetadataObjectPackService
from stopan.placement.cluster_resolver import require_cluster_view
from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc
from stopan.rpc.options import grpc_channel_options


class MetadataPackRecoverError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataPackSource:
    node_id: str
    address: str
    pack_hash: str
    size_bytes: int
    stored_at_unix: float
    public_key_b64: str = ""
    signature_b64: str = ""


@dataclass(frozen=True, slots=True)
class MetadataPackRecoveryCandidate:
    pack_hash: str
    newest_stored_at_unix: float
    size_bytes: int
    sources: tuple[MetadataPackSource, ...]


@dataclass(frozen=True, slots=True)
class DownloadedMetadataPackCandidate:
    pack_hash: str
    pack_path: Path
    source: MetadataPackSource
    vault_generation: int
    pack_created_at_unix: float
    catalog_hash: str
    state_digest: str
    object_count: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataPackRecoverResult:
    owner_id: str
    object_store_dir: Path
    candidates_seen: int
    unique_packs_seen: int
    list_targets_attempted: int
    list_targets_succeeded: int
    downloads_attempted: int
    recovered_pack_hash: str
    recovered_pack_path: Path
    recovered_from_address: str
    recovered_from_node_id: str
    remote_stored_at_unix: float
    vault_generation: int
    pack_created_at_unix: float
    pack_catalog_hash: str
    pack_state_digest: str
    pack_object_count: int
    pack_snapshot_count: int
    pack_known_chunk_count: int
    pack_protection_record_count: int
    pack_import_result: Any
    db_import_result: Any | None
    list_errors: tuple[str, ...]
    download_errors: tuple[str, ...]


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


def _target_label(source: MetadataPackSource) -> str:
    node = source.node_id[:8] if source.node_id else "desconocido"
    return f"{node}@{source.address}"


def _list_packs_from_target(
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

    channel = grpc.insecure_channel(
        address,
        options=_channel_options(
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ),
    )
    try:
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
    finally:
        try:
            channel.close()
        except Exception:
            pass


def _retrieve_pack_from_source(
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
        raise MetadataPackRecoverError(
            f"metadata pack {source.pack_hash} anunciado por {_target_label(source)} supera max_message_bytes: "
            f"{source.size_bytes} > {int(max_message_bytes)}"
        )

    channel = grpc.insecure_channel(
        source.address,
        options=_channel_options(
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
        ),
    )
    try:
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
            raise MetadataPackRecoverError(
                f"retrieve {source.pack_hash} desde {_target_label(source)} falló: "
                f"status={int(response.status)} detail={response.detail or ''}"
            )
        data = bytes(response.pack_data)
        calculated = calculate_pack_hash(data)
        if calculated != source.pack_hash:
            raise MetadataPackRecoverError(
                f"pack_hash descargado no coincide desde {_target_label(source)}: "
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
            raise MetadataPackRecoverError(
            f"firma de metadata pack inválida desde {_target_label(source)}: "
            f"{source.pack_hash}"
        )
        return data
    finally:
        try:
            channel.close()
        except Exception:
            pass


def _group_candidates(sources: list[MetadataPackSource]) -> list[MetadataPackRecoveryCandidate]:
    grouped: dict[str, list[MetadataPackSource]] = {}
    for source in sources:
        grouped.setdefault(source.pack_hash, []).append(source)

    candidates: list[MetadataPackRecoveryCandidate] = []
    for pack_hash, pack_sources in grouped.items():
        ordered = sorted(
            pack_sources,
            key=lambda item: (float(item.stored_at_unix), int(item.size_bytes), item.address),
            reverse=True,
        )
        newest = ordered[0]
        candidates.append(
            MetadataPackRecoveryCandidate(
                pack_hash=pack_hash,
                newest_stored_at_unix=float(newest.stored_at_unix),
                size_bytes=int(newest.size_bytes),
                sources=tuple(ordered),
            )
        )

    candidates.sort(
        key=lambda item: (float(item.newest_stored_at_unix), int(item.size_bytes), item.pack_hash),
        reverse=True,
    )
    return candidates


def recover_metadata_from_network(
    *,
    owner_id: str,
    identity_file: str | Path,
    object_store_dir: str | Path,
    passphrase: str | bytes,
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
    scrypt_cost: ScryptCost,
    db_file: str | Path,
    import_db: bool = True,
    include_protection: bool = True,
    default_desired_rf: int = 3,
    download_dir: str | Path | None = None,
    pack_out: str | Path | None = None,
    max_candidates: int = 20,
) -> MetadataPackRecoverResult:
    owner = validate_owner_id(owner_id)
    object_store_path = Path(object_store_dir).expanduser().resolve()
    max_message_bytes = int(max_message_bytes)
    parallelism = max(1, int(target_parallelism))

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
        raise MetadataPackRecoverError(str(exc)) from exc

    cluster = resolved.cluster
    targets = [member for member in cluster.members if str(getattr(member, "address", "") or "").strip()]
    if not targets:
        raise MetadataPackRecoverError("Membership no devolvió miembros elegibles del cluster.")

    all_sources: list[MetadataPackSource] = []
    list_errors: list[str] = []
    with ThreadPoolExecutor(max_workers=min(parallelism, len(targets))) as executor:
        futures = [
            executor.submit(
                _list_packs_from_target,
                target,
                owner_id=owner,
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
            records, error = future.result()
            all_sources.extend(records)
            if error:
                list_errors.append(error)

    candidates = _group_candidates(all_sources)
    if max_candidates > 0:
        candidates = candidates[: int(max_candidates)]
    if not candidates:
        detail = "; ".join(list_errors[:5]) if list_errors else "no hay packs remotos"
        raise MetadataPackRecoverError(f"No se encontraron metadata packs para owner_id={owner}. {detail}")

    pack_service = MetadataObjectPackService(scrypt_cost=scrypt_cost)
    graph_service = MetadataObjectGraphStoreService(db_file=str(db_file), scrypt_cost=scrypt_cost)

    if pack_out is not None:
        selected_pack_path_base = Path(pack_out).expanduser().resolve()
        ensure_private_dir(selected_pack_path_base.parent)
    else:
        base = Path(download_dir).expanduser().resolve() if download_dir is not None else object_store_path / "recovered_packs"
        ensure_private_dir(base)
        selected_pack_path_base = base / "recovered.stopanmetapack"

    download_errors: list[str] = []
    downloads_attempted = 0
    valid_downloads: list[DownloadedMetadataPackCandidate] = []

    for candidate in candidates:
        for source in candidate.sources:
            downloads_attempted += 1
            try:
                data = _retrieve_pack_from_source(
                    source,
                    owner_id=owner,
                    cluster_token=cluster_token,
                    timeout_s=rpc_timeout_s,
                    max_message_bytes=max_message_bytes,
                    grpc_keepalive_time_ms=grpc_keepalive_time_ms,
                    grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
                    grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
                )

                if pack_out is None:
                    pack_path = selected_pack_path_base.with_name(f"recovered-{candidate.pack_hash}.stopanmetapack")
                else:
                    pack_path = selected_pack_path_base.with_name(
                        f".{selected_pack_path_base.name}.{candidate.pack_hash}.{source.node_id[:8] or 'node'}.tmp"
                    )

                atomic_write_bytes(pack_path, data, mode=0o600)

                inspection = pack_service.inspect_pack(
                    pack_path,
                    identity_file=identity_file,
                    passphrase=passphrase,
                    decrypt=True,
                )
                if inspection.decrypted is None:
                    raise MetadataPackRecoverError("metadata pack no fue descifrado durante la validación")

                summary = inspection.decrypted
                valid_downloads.append(
                    DownloadedMetadataPackCandidate(
                        pack_hash=candidate.pack_hash,
                        pack_path=pack_path,
                        source=source,
                        vault_generation=int(summary.vault_generation),
                        pack_created_at_unix=float(summary.pack_created_at_unix),
                        catalog_hash=summary.catalog_hash,
                        state_digest=summary.state_digest,
                        object_count=int(summary.object_count),
                        snapshot_count=int(summary.snapshot_count),
                        known_chunk_count=int(summary.known_chunk_count),
                        protection_record_count=int(summary.protection_record_count),
                    )
                )
                break
            except Exception as exc:
                download_errors.append(f"{candidate.pack_hash} desde {_target_label(source)}: {exc}")
                continue

    if valid_downloads:
        best = sorted(
            valid_downloads,
            key=lambda item: (
                int(item.vault_generation),
                float(item.pack_created_at_unix),
                float(item.source.stored_at_unix),
                item.pack_hash,
            ),
            reverse=True,
        )[0]

        if pack_out is not None and best.pack_path != selected_pack_path_base:
            data = best.pack_path.read_bytes()
            atomic_write_bytes(selected_pack_path_base, data, mode=0o600)
            best_path = selected_pack_path_base
        else:
            best_path = best.pack_path

        pack_import_result = pack_service.import_pack(
            best_path,
            identity_file=identity_file,
            object_store_dir=object_store_path,
            passphrase=passphrase,
        )
        db_import_result = None
        if import_db:
            db_import_result = graph_service.import_latest_state(
                object_store_dir=object_store_path,
                passphrase=passphrase,
                include_protection=include_protection,
                default_desired_rf=int(default_desired_rf),
            )

        return MetadataPackRecoverResult(
            owner_id=owner,
            object_store_dir=object_store_path,
            candidates_seen=len(all_sources),
            unique_packs_seen=len(candidates),
            list_targets_attempted=len(targets),
            list_targets_succeeded=len(targets) - len(list_errors),
            downloads_attempted=downloads_attempted,
            recovered_pack_hash=best.pack_hash,
            recovered_pack_path=best_path,
            recovered_from_address=best.source.address,
            recovered_from_node_id=best.source.node_id,
            remote_stored_at_unix=float(best.source.stored_at_unix),
            vault_generation=int(best.vault_generation),
            pack_created_at_unix=float(best.pack_created_at_unix),
            pack_catalog_hash=best.catalog_hash,
            pack_state_digest=best.state_digest,
            pack_object_count=best.object_count,
            pack_snapshot_count=best.snapshot_count,
            pack_known_chunk_count=best.known_chunk_count,
            pack_protection_record_count=best.protection_record_count,
            pack_import_result=pack_import_result,
            db_import_result=db_import_result,
            list_errors=tuple(list_errors),
            download_errors=tuple(download_errors),
        )

    message = "No se pudo descargar e importar ningún metadata pack válido."
    if download_errors:
        message += " Últimos errores: " + "; ".join(download_errors[-5:])
    raise MetadataPackRecoverError(message)
