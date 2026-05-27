"""
Servicio gRPC para metadata packs distribuidos.

Los nodos remotos almacenan packs ya cifrados junto con sidecars firmados. El
servicio valida token de cluster, hash, firma, cuotas y límites de retención.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import grpc

from stopan.metadata.identity import (
    validate_owner_id,
    verify_metadata_pack_signature,
)
from stopan.metadata.packs.distributed_store import (
    MetadataPackCorruptionError,
    MetadataPackQuotaError,
    MetadataPackSignatureError,
    MetadataPackStore,
    MetadataPackStoreError,
)
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash
from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc


def _short(value: str) -> str:
    return str(value)[:32]


class MetadataPackServiceServicer(p2p_storage_pb2_grpc.MetadataPackServiceServicer):
    """
    Servicio gRPC para almacenamiento de metadata packs cifrados.

    Los nodos remotos solo ven metadata de enrutado y sidecars firmados:
    owner_id, pack_hash, size, stored_at_unix, clave pública y firma.
    El payload del pack ya llega cifrado desde la capa de metadata.
    """

    def __init__(
        self,
        pack_store_dir: str | Path,
        *,
        cluster_token: str,
        max_pack_bytes: int,
        max_packs_per_owner: int,
        max_total_bytes_per_owner: int,
        max_total_store_bytes: int,
        max_age_days: int,
    ):
        self._cluster_token = str(cluster_token or "")
        self.store = MetadataPackStore(
            pack_store_dir,
            max_pack_bytes=max_pack_bytes,
            max_packs_per_owner=max_packs_per_owner,
            max_total_bytes_per_owner=max_total_bytes_per_owner,
            max_total_store_bytes=max_total_store_bytes,
            max_age_days=max_age_days,
        )

        prune_result = self.store.prune_to_limits(dry_run=False)
        print(
            "Servicio de metadata packs listo. "
            f"store={Path(pack_store_dir).expanduser().resolve()} "
            f"max_pack_bytes={int(max_pack_bytes)} "
            f"max_packs_per_owner={int(max_packs_per_owner)} "
            f"max_total_bytes_per_owner={int(max_total_bytes_per_owner)} "
            f"max_total_store_bytes={int(max_total_store_bytes)} "
            f"max_age_days={int(max_age_days)}"
        )
        if prune_result.pruned_packs:
            print(
                "Metadata pack store podado al arrancar: "
                f"packs_seen={prune_result.packs_seen} "
                f"owners_seen={prune_result.owners_seen} "
                f"expired_packs={prune_result.expired_packs} "
                f"quota_packs={prune_result.quota_packs} "
                f"pruned_packs={prune_result.pruned_packs} "
                f"pruned_bytes={prune_result.pruned_bytes}"
            )

    def _check_token(self, request: Any, context: grpc.ServicerContext) -> bool:
        """Valida el cluster_token de una request entrante."""

        expected = self._cluster_token
        received = str(getattr(request, "cluster_token", "") or "")
        if received != expected:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details("cluster_token inválido")
            return False
        return True

    def StoreMetadataPack(self, request, context):
        """Valida y almacena un metadata pack remoto."""

        if not self._check_token(request, context):
            return p2p_storage_pb2.StoreMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
                detail="cluster_token inválido",
            )

        try:
            owner_id = validate_owner_id(request.owner_id)
            pack_hash = validate_pack_hash(request.pack_hash)
            data = bytes(request.pack_data)
            public_key_b64 = str(request.public_key_b64 or "")
            signature_b64 = str(request.signature_b64 or "")

            if not data:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                    detail="metadata pack vacío rechazado",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )

            if len(data) > self.store.max_pack_bytes:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_TOO_LARGE,
                    detail=f"metadata pack demasiado grande: {len(data)} bytes > {self.store.max_pack_bytes}",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=len(data),
                )

            calculated = calculate_pack_hash(data)
            if calculated != pack_hash:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_HASH_MISMATCH,
                    detail=f"pack_hash no coincide: esperado={_short(pack_hash)!r} calculado={_short(calculated)!r}",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=len(data),
                )

            if not public_key_b64 or not signature_b64:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_SIGNATURE,
                    detail="public_key_b64 y signature_b64 son obligatorios",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=len(data),
                )

            if not verify_metadata_pack_signature(
                owner_id=owner_id,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
                pack_hash=pack_hash,
            ):
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_SIGNATURE,
                    detail="firma de metadata pack inválida",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=len(data),
                )

            result = self.store.put_pack_bytes(
                owner_id=owner_id,
                pack_hash=pack_hash,
                data=data,
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            )

            try:
                stored_at_unix = float(result.path.stat().st_mtime)
            except OSError:
                stored_at_unix = 0.0

            if result.already_present:
                status = p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ALREADY_PRESENT
                detail = "ya presente"
            else:
                status = p2p_storage_pb2.METADATA_PACK_STORE_STATUS_STORED
                detail = "guardado"
                if result.pruned_packs:
                    detail += f"; pruned_packs={result.pruned_packs} pruned_bytes={result.pruned_bytes}"

            return _store_response(
                status=status,
                detail=detail,
                owner_id=owner_id,
                pack_hash=pack_hash,
                size_bytes=int(result.size_bytes),
                stored_at_unix=stored_at_unix,
                public_key_b64=result.public_key_b64,
                signature_b64=result.signature_b64,
            )

        except (TypeError, ValueError) as exc:
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                detail=str(exc),
            )
        except MetadataPackSignatureError as exc:
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_SIGNATURE,
                detail=str(exc),
            )
        except MetadataPackQuotaError as exc:
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_QUOTA,
                detail=str(exc),
            )
        except MetadataPackCorruptionError as exc:
            context.set_code(grpc.StatusCode.DATA_LOSS)
            context.set_details(str(exc))
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
                detail=str(exc),
            )
        except MetadataPackStoreError as exc:
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
                detail=str(exc),
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
                detail=str(exc),
            )

    def ListMetadataPacks(self, request, context):
        """Lista metadata packs firmados de un owner."""

        if not self._check_token(request, context):
            return p2p_storage_pb2.ListMetadataPacksResponse()

        try:
            owner_id = validate_owner_id(request.owner_id)
            records = self.store.list_packs(owner_id=owner_id)
            return p2p_storage_pb2.ListMetadataPacksResponse(
                packs=[
                    p2p_storage_pb2.MetadataPackRecord(
                        owner_id=record.owner_id,
                        pack_hash=record.pack_hash,
                        size_bytes=int(record.size_bytes),
                        stored_at_unix=float(record.stored_at_unix),
                        public_key_b64=record.public_key_b64,
                        signature_b64=record.signature_b64,
                    )
                    for record in records
                    if record.signed
                ]
            )
        except (TypeError, ValueError) as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return p2p_storage_pb2.ListMetadataPacksResponse()
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.ListMetadataPacksResponse()

    def RetrieveMetadataPack(self, request, context):
        """Recupera un metadata pack concreto junto con su sidecar firmado."""
        
        if not self._check_token(request, context):
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail="cluster_token inválido",
            )

        try:
            owner_id = validate_owner_id(request.owner_id)
            pack_hash = validate_pack_hash(request.pack_hash)

            path = self.store.pack_path(owner_id=owner_id, pack_hash=pack_hash)
            if not path.exists():
                return p2p_storage_pb2.RetrieveMetadataPackResponse(
                    status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_NOT_FOUND,
                    detail="metadata pack no encontrado",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )

            data = self.store.get_pack_bytes(owner_id=owner_id, pack_hash=pack_hash)
            public_key_b64, signature_b64 = self.store.get_signature_record(
                owner_id=owner_id,
                pack_hash=pack_hash,
            )
            if not public_key_b64 or not signature_b64:
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("metadata pack sin sidecar de firma válido")
                return p2p_storage_pb2.RetrieveMetadataPackResponse(
                    status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                    detail="metadata pack sin sidecar de firma válido",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )

            st = path.stat()
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_FOUND,
                detail="ok",
                owner_id=owner_id,
                pack_hash=pack_hash,
                pack_data=data,
                size_bytes=int(st.st_size),
                stored_at_unix=float(st.st_mtime),
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            )

        except (TypeError, ValueError) as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail=str(exc),
            )
        except MetadataPackCorruptionError as exc:
            context.set_code(grpc.StatusCode.DATA_LOSS)
            context.set_details(str(exc))
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail=str(exc),
            )
        except OSError as exc:
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_NOT_FOUND,
                detail=str(exc),
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.RetrieveMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail=str(exc),
            )


    def ProbeMetadataPack(self, request, context):
        """Comprueba presencia exacta de un metadata pack sin descargar el payload."""

        if not self._check_token(request, context):
            return p2p_storage_pb2.ProbeMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_ERROR,
                detail="cluster_token inválido",
            )

        try:
            owner_id = validate_owner_id(request.owner_id)
            pack_hash = validate_pack_hash(request.pack_hash)
            path = self.store.pack_path(owner_id=owner_id, pack_hash=pack_hash)
            if not path.exists():
                return p2p_storage_pb2.ProbeMetadataPackResponse(
                    status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_MISSING,
                    detail="metadata pack no encontrado",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )

            public_key_b64, signature_b64 = self.store.get_signature_record(
                owner_id=owner_id,
                pack_hash=pack_hash,
            )
            if not public_key_b64 or not signature_b64:
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("metadata pack sin sidecar de firma válido")
                return p2p_storage_pb2.ProbeMetadataPackResponse(
                    status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_ERROR,
                    detail="metadata pack sin sidecar de firma válido",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )

            st = path.stat()
            return p2p_storage_pb2.ProbeMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_PRESENT,
                detail="ok",
                owner_id=owner_id,
                pack_hash=pack_hash,
                size_bytes=int(st.st_size),
                stored_at_unix=float(st.st_mtime),
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            )
        except (TypeError, ValueError) as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return p2p_storage_pb2.ProbeMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_ERROR,
                detail=str(exc),
            )
        except OSError as exc:
            return p2p_storage_pb2.ProbeMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_MISSING,
                detail=str(exc),
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return p2p_storage_pb2.ProbeMetadataPackResponse(
                status=p2p_storage_pb2.METADATA_PACK_PROBE_STATUS_ERROR,
                detail=str(exc),
            )


def _store_response(
    *,
    status: int,
    detail: str,
    owner_id: str = "",
    pack_hash: str = "",
    size_bytes: int = 0,
    stored_at_unix: float = 0.0,
    public_key_b64: str = "",
    signature_b64: str = "",
):
    return p2p_storage_pb2.StoreMetadataPackResponse(
        status=status,
        detail=detail,
        owner_id=owner_id,
        pack_hash=pack_hash,
        size_bytes=int(size_bytes),
        stored_at_unix=float(stored_at_unix),
        public_key_b64=public_key_b64,
        signature_b64=signature_b64,
    )
