"""
Recuperación de metadata packs desde la red P2P.

Lista packs remotos firmados, descarga candidatos, valida firma/hash, descifra
con la identidad local e importa el estado más reciente recuperable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stopan.errors import StopanNetworkError, StopanStorageError
from stopan.common.fs import atomic_write_bytes, ensure_private_dir
from stopan.metadata.identity.keys import validate_owner_id
from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.service import MetadataObjectGraphStoreService
from stopan.metadata.packs.object_pack import MetadataObjectPackService
from stopan.metadata.packs.discovery import discover_metadata_packs_from_network
from stopan.metadata.packs.hashes import validate_pack_hash
from stopan.metadata.packs.payload import require_vault_id
from stopan.metadata.packs.remote import (
    MetadataPackSource,
    metadata_pack_target_label as _target_label,
    retrieve_metadata_pack_from_source as _retrieve_pack_from_source,
)


class MetadataPackRecoverError(StopanNetworkError, RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DownloadedMetadataPackCandidate:
    pack_hash: str
    pack_path: Path
    source: MetadataPackSource
    vault_id: str
    vault_generation: int
    pack_created_at_unix: float
    catalog_hash: str
    state_digest: str
    object_count: int
    snapshot_count: int
    known_chunk_count: int
    protection_record_count: int


@dataclass(frozen=True, slots=True)
class MetadataPackRecoverStats:
    candidates_seen: int
    unique_packs_seen: int
    list_targets_attempted: int
    list_targets_succeeded: int
    downloads_attempted: int
    list_errors: tuple[str, ...]
    download_errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MetadataPackRecoverResult:
    owner_id: str
    object_store_dir: Path | None
    stats: MetadataPackRecoverStats
    recovered_pack_hash: str
    vault_id: str
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
    pack_import_result: Any | None
    db_import_result: Any | None



def recover_metadata_from_network(
    *,
    owner_id: str,
    identity_file: str | Path,
    object_store_dir: str | Path | None,
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
    default_desired_rf: int,
    import_db: bool = True,
    include_protection: bool = True,
    download_only: bool = False,
    download_dir: str | Path | None = None,
    pack_out: str | Path | None = None,
    target_hash: str | None = None,
    vault_id: str | None = None,
) -> MetadataPackRecoverResult:
    owner = validate_owner_id(owner_id)
    requested_hash = validate_pack_hash(target_hash) if target_hash is not None else None
    requested_vault_id = require_vault_id("vault_id", vault_id) if vault_id is not None else None
    object_store_path = Path(object_store_dir).expanduser().resolve() if object_store_dir is not None else None
    max_message_bytes = int(max_message_bytes)
    parallelism = max(1, int(target_parallelism))

    try:
        discovery = discover_metadata_packs_from_network(
            owner_id=owner,
            membership_seed=membership_seed,
            self_addr=self_addr,
            cluster_token=cluster_token,
            membership_timeout_s=membership_timeout_s,
            rpc_timeout_s=rpc_timeout_s,
            target_parallelism=parallelism,
            max_message_bytes=max_message_bytes,
            grpc_keepalive_time_ms=grpc_keepalive_time_ms,
            grpc_keepalive_timeout_ms=grpc_keepalive_timeout_ms,
            grpc_keepalive_permit_without_calls=grpc_keepalive_permit_without_calls,
            desired_copies_by_hash=None,
            max_candidates=None,
        )
    except Exception as exc:
        raise MetadataPackRecoverError(str(exc)) from exc

    candidates = list(discovery.entries)
    list_errors = list(discovery.stats.list_errors)
    if requested_hash is not None:
        candidates = [candidate for candidate in candidates if candidate.pack_hash == requested_hash]

    if not candidates:
        detail = "; ".join(list_errors[:5]) if list_errors else "no hay packs remotos"
        if requested_hash is not None:
            raise MetadataPackRecoverError(
                f"No se encontró metadata pack target_hash={requested_hash} para owner_id={owner}. {detail}"
            )
        raise MetadataPackRecoverError(f"No se encontraron metadata packs para owner_id={owner}. {detail}")

    pack_service = MetadataObjectPackService(scrypt_cost=scrypt_cost)
    graph_service = MetadataObjectGraphStoreService(db_file=str(db_file), scrypt_cost=scrypt_cost)

    if pack_out is not None:
        selected_pack_path_base = Path(pack_out).expanduser().resolve()
        ensure_private_dir(selected_pack_path_base.parent)
    else:
        if download_dir is not None:
            base = Path(download_dir).expanduser().resolve()
        elif object_store_path is not None:
            base = object_store_path / "recovered_packs"
        else:
            raise MetadataPackRecoverError(
                "Se requiere --pack-out, --download-dir o --object-store para guardar el pack recuperado."
            )
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
                        vault_id=summary.vault_id,
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
        selectable_downloads = valid_downloads
        if requested_vault_id is not None:
            selectable_downloads = [item for item in valid_downloads if item.vault_id == requested_vault_id]
            if not selectable_downloads:
                raise MetadataPackRecoverError(
                    f"No se encontró ningún metadata pack válido para vault_id={requested_vault_id}"
                )
        elif requested_hash is None:
            vault_ids = sorted({item.vault_id for item in valid_downloads})
            if len(vault_ids) > 1:
                raise MetadataPackRecoverError(
                    "Se encontraron metadata packs de varios vault_id para el mismo owner_id: "
                    + ", ".join(vault_ids)
                    + ". Usa --vault-id o --target-hash para elegir uno."
                )

        best = max(
            selectable_downloads,
            key=lambda item: (
                int(item.vault_generation),
                item.pack_hash,
            ),
        )

        if pack_out is not None and best.pack_path != selected_pack_path_base:
            try:
                data = best.pack_path.read_bytes()
            except OSError as exc:
                raise StopanStorageError(f"No se pudo leer el metadata pack recuperado {best.pack_path}: {exc}") from exc
            atomic_write_bytes(selected_pack_path_base, data, mode=0o600)
            best_path = selected_pack_path_base
        else:
            best_path = best.pack_path

        pack_import_result = None
        db_import_result = None
        if not download_only:
            if object_store_path is None:
                raise MetadataPackRecoverError(
                    "Se requiere --object-store o metadata.object_store_dir para importar el pack recuperado."
                )
            pack_import_result = pack_service.import_pack(
                best_path,
                identity_file=identity_file,
                object_store_dir=object_store_path,
                passphrase=passphrase,
            )
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
            stats=MetadataPackRecoverStats(
                candidates_seen=discovery.stats.sources_seen,
                unique_packs_seen=discovery.stats.unique_packs_seen,
                list_targets_attempted=discovery.stats.list_targets_attempted,
                list_targets_succeeded=discovery.stats.list_targets_succeeded,
                downloads_attempted=downloads_attempted,
                list_errors=tuple(list_errors),
                download_errors=tuple(download_errors),
            ),
            recovered_pack_hash=best.pack_hash,
            vault_id=best.vault_id,
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
        )

    message = "No se pudo descargar e importar ningún metadata pack válido."
    if download_errors:
        message += " Últimos errores: " + "; ".join(download_errors[-5:])
    raise MetadataPackRecoverError(message)
