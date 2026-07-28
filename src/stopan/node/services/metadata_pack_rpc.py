"""
Servicio gRPC para metadata packs distribuidos.

Los nodos remotos reciben y sirven por bloques packs ya cifrados junto con
sidecars firmados. El servicio valida token de cluster, tamaño, hash, firma,
cuotas y límites de retención.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import blake3
import grpc

from stopan.common.fs import ensure_private_dir
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
from stopan.metadata.packs.hashes import calculate_pack_hash_file, validate_pack_hash
from stopan.metadata.packs.streaming import iter_file_chunks, metadata_pack_stream_chunk_bytes
from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc


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
        max_message_bytes: int,
    ):
        self._cluster_token = str(cluster_token or "")
        self._stream_chunk_bytes = metadata_pack_stream_chunk_bytes(max_message_bytes)
        self.store = MetadataPackStore(
            pack_store_dir,
            max_pack_bytes=max_pack_bytes,
            max_packs_per_owner=max_packs_per_owner,
            max_total_bytes_per_owner=max_total_bytes_per_owner,
            max_total_store_bytes=max_total_store_bytes,
            max_age_days=max_age_days,
        )
        self._incoming_dir = self.store.root_dir / ".incoming"
        ensure_private_dir(self._incoming_dir)
        stale_incoming = 0
        for path in self._incoming_dir.iterdir():
            if not path.is_file():
                continue
            try:
                path.unlink()
                stale_incoming += 1
            except OSError:
                pass

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

        if stale_incoming:
            print(f"Temporales de metadata packs eliminados al arrancar: {stale_incoming}")

    def _check_token(self, request: Any, context: grpc.ServicerContext) -> bool:
        """Valida el cluster_token de una request entrante."""

        expected = self._cluster_token
        received = str(getattr(request, "cluster_token", "") or "")
        if received != expected:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details("cluster_token inválido")
            return False
        return True

    def StoreMetadataPack(self, request_iterator, context):
        """Recibe, valida y publica atómicamente un metadata pack por flujo."""

        incoming_path: Path | None = None
        try:
            iterator = iter(request_iterator)
            try:
                first = next(iterator)
            except StopIteration:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                    detail="flujo de metadata pack vacío",
                )

            if first.WhichOneof("part") != "header":
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                    detail="la primera parte del flujo debe ser la cabecera",
                )

            header = first.header
            if not self._check_token(header, context):
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_ERROR,
                    detail="cluster_token inválido",
                )

            owner_id = validate_owner_id(header.owner_id)
            pack_hash = validate_pack_hash(header.pack_hash)
            size_bytes = int(header.size_bytes)
            public_key_b64 = str(header.public_key_b64 or "")
            signature_b64 = str(header.signature_b64 or "")

            if size_bytes <= 0:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                    detail="metadata pack vacío rechazado",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )
            if size_bytes > self.store.max_pack_bytes:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_TOO_LARGE,
                    detail=(
                        f"metadata pack demasiado grande: {size_bytes} bytes > "
                        f"{self.store.max_pack_bytes}"
                    ),
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=size_bytes,
                )
            if not public_key_b64 or not signature_b64:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_SIGNATURE,
                    detail="public_key_b64 y signature_b64 son obligatorios",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=size_bytes,
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
                    size_bytes=size_bytes,
                )

            incoming_path = self._incoming_dir / (
                f".{pack_hash}.{os.getpid()}.{os.urandom(8).hex()}.part"
            )
            try:
                fd = os.open(
                    incoming_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except OSError as exc:
                raise MetadataPackStoreError(
                    f"No se pudo crear el temporal {incoming_path}: {exc}"
                ) from exc

            received = 0
            hasher = blake3.blake3()
            try:
                with os.fdopen(fd, "wb") as fh:
                    for request in iterator:
                        if request.WhichOneof("part") != "chunk_data":
                            return _store_response(
                                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                                detail="el flujo contiene una cabecera o parte inesperada",
                                owner_id=owner_id,
                                pack_hash=pack_hash,
                                size_bytes=received,
                            )
                        block = bytes(request.chunk_data)
                        if not block:
                            continue
                        received += len(block)
                        if received > size_bytes or received > self.store.max_pack_bytes:
                            return _store_response(
                                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_TOO_LARGE,
                                detail=(
                                    "el flujo excede el tamaño anunciado o permitido: "
                                    f"recibido={received} anunciado={size_bytes} "
                                    f"máximo={self.store.max_pack_bytes}"
                                ),
                                owner_id=owner_id,
                                pack_hash=pack_hash,
                                size_bytes=received,
                            )
                        hasher.update(block)
                        fh.write(block)
                    fh.flush()
                    os.fsync(fh.fileno())
            except OSError as exc:
                raise MetadataPackStoreError(
                    f"No se pudo recibir el metadata pack {pack_hash}: {exc}"
                ) from exc

            if received != size_bytes:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_INVALID_ARGUMENT,
                    detail=(
                        "el tamaño recibido no coincide con el anunciado: "
                        f"recibido={received} anunciado={size_bytes}"
                    ),
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=received,
                )

            calculated = hasher.hexdigest()
            if calculated != pack_hash:
                return _store_response(
                    status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_HASH_MISMATCH,
                    detail=(
                        "pack_hash no coincide: "
                        f"esperado={pack_hash} calculado={calculated}"
                    ),
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                    size_bytes=received,
                )

            result = self.store.put_pack_file(
                owner_id=owner_id,
                path=incoming_path,
                expected_pack_hash=pack_hash,
                expected_size_bytes=size_bytes,
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
                    detail += (
                        f", pruned_packs={result.pruned_packs} "
                        f"pruned_bytes={result.pruned_bytes}"
                    )

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
            return _store_response(
                status=p2p_storage_pb2.METADATA_PACK_STORE_STATUS_REJECTED_HASH_MISMATCH,
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
        finally:
            if incoming_path is not None:
                try:
                    incoming_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass

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
        """Transmite un metadata pack concreto junto con su cabecera firmada."""

        if not self._check_token(request, context):
            yield _retrieve_header_response(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail="cluster_token inválido",
            )
            return

        try:
            owner_id = validate_owner_id(request.owner_id)
            pack_hash = validate_pack_hash(request.pack_hash)
            path = self.store.pack_path(owner_id=owner_id, pack_hash=pack_hash)
            if not path.exists():
                yield _retrieve_header_response(
                    status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_NOT_FOUND,
                    detail="metadata pack no encontrado",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )
                return

            public_key_b64, signature_b64 = self.store.get_signature_record(
                owner_id=owner_id,
                pack_hash=pack_hash,
            )
            if not public_key_b64 or not signature_b64:
                yield _retrieve_header_response(
                    status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                    detail="metadata pack sin sidecar de firma válido",
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )
                return

            calculated = calculate_pack_hash_file(path)
            if calculated != pack_hash:
                yield _retrieve_header_response(
                    status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                    detail=(
                        "hash de metadata pack no coincide: "
                        f"esperado={pack_hash} calculado={calculated}"
                    ),
                    owner_id=owner_id,
                    pack_hash=pack_hash,
                )
                return

            st = path.stat()
            yield _retrieve_header_response(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_FOUND,
                detail="ok",
                owner_id=owner_id,
                pack_hash=pack_hash,
                size_bytes=int(st.st_size),
                stored_at_unix=float(st.st_mtime),
                public_key_b64=public_key_b64,
                signature_b64=signature_b64,
            )
            for block in iter_file_chunks(
                path,
                chunk_bytes=self._stream_chunk_bytes,
            ):
                if not context.is_active():
                    return
                yield p2p_storage_pb2.RetrieveMetadataPackResponse(
                    chunk_data=block
                )

        except (TypeError, ValueError) as exc:
            yield _retrieve_header_response(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail=str(exc),
            )
        except MetadataPackCorruptionError as exc:
            yield _retrieve_header_response(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_ERROR,
                detail=str(exc),
            )
        except OSError as exc:
            yield _retrieve_header_response(
                status=p2p_storage_pb2.METADATA_PACK_RETRIEVE_STATUS_NOT_FOUND,
                detail=str(exc),
            )
        except Exception as exc:
            context.abort(grpc.StatusCode.INTERNAL, str(exc))

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


def _retrieve_header_response(
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
    return p2p_storage_pb2.RetrieveMetadataPackResponse(
        header=p2p_storage_pb2.RetrieveMetadataPackHeader(
            status=status,
            detail=detail,
            owner_id=owner_id,
            pack_hash=pack_hash,
            size_bytes=int(size_bytes),
            stored_at_unix=float(stored_at_unix),
            public_key_b64=public_key_b64,
            signature_b64=signature_b64,
        )
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
