from __future__ import annotations

from stopan.metadata.database import (
    ErasureDataPackRecord,
    MetadataDB,
    MetadataDatabaseValueError,
    VerificationCandidate,
)

PROTECTION_SCOPE_CHOICES = ("pending", "snapshot", "all-reachable", "all-known-chunks")
DEFAULT_PROTECTION_SCOPE = "pending"


class ProtectionScopeError(MetadataDatabaseValueError):
    """Scope de protección inválido o no resoluble desde metadata."""


def normalize_protection_scope(scope: str | None, *, snapshot_id: int | None = None) -> str:
    value = str(scope or DEFAULT_PROTECTION_SCOPE).strip()
    if snapshot_id is not None:
        if value not in (DEFAULT_PROTECTION_SCOPE, "snapshot"):
            raise ProtectionScopeError("--snapshot-id no se puede combinar con --scope distinto de snapshot.")
        value = "snapshot"
    if value not in PROTECTION_SCOPE_CHOICES:
        raise ProtectionScopeError(f"Scope de protección no soportado: {value!r}")
    if value == "snapshot" and snapshot_id is None:
        raise ProtectionScopeError("--scope snapshot requiere --snapshot-id.")
    return value


def chunk_hashes_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
) -> tuple[str, ...]:
    if scope == "snapshot":
        if snapshot_id is None:
            raise ProtectionScopeError("--scope snapshot requiere --snapshot-id.")
        status, _error = db.get_snapshot_status(int(snapshot_id))
        if status is None:
            raise ProtectionScopeError(f"Snapshot no existe: {snapshot_id}")
        if status != "COMPLETE":
            raise ProtectionScopeError(f"Snapshot no está completo: {snapshot_id} status={status}")
        return db.snapshot_chunk_hashes(snapshot_id)
    if scope == "all-reachable":
        return db.all_reachable_chunk_hashes()
    if scope == "all-known-chunks":
        return db.all_known_chunk_hashes()
    raise ProtectionScopeError(f"Scope de protección no resoluble como conjunto de chunks: {scope!r}")


def replication_verification_candidates_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
    include_verified: bool,
    limit: int | None,
) -> list[VerificationCandidate]:
    chunk_hashes = chunk_hashes_for_scope(db, scope=scope, snapshot_id=snapshot_id)
    return db.chunk_protection_verification_candidates_for_hashes(
        chunk_hashes,
        include_verified=include_verified,
        limit=limit,
    )


def erasure_verification_candidates_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
    include_verified: bool,
    limit: int | None,
) -> list[ErasureDataPackRecord]:
    if scope == "all-known-chunks":
        pack_hashes = db.all_erasure_pack_hashes()
    else:
        chunk_hashes = chunk_hashes_for_scope(db, scope=scope, snapshot_id=snapshot_id)
        pack_hashes = db.erasure_pack_hashes_for_chunks(chunk_hashes)
    return db.erasure_data_packs_by_hashes(
        pack_hashes,
        include_verified=include_verified,
        limit=limit,
    )


def erasure_pack_by_hash(db: MetadataDB, pack_hash: str) -> ErasureDataPackRecord | None:
    records = db.erasure_data_packs_by_hashes(
        (pack_hash,),
        include_verified=True,
        limit=None,
    )
    return records[0] if records else None


def replication_push_chunks_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
    desired_rf: int,
    current_epoch: str | None,
    limit: int | None,
) -> list[str]:
    chunk_hashes = chunk_hashes_for_scope(db, scope=scope, snapshot_id=snapshot_id)
    return db.pending_protection_chunks_for_hashes(
        chunk_hashes,
        desired_rf=desired_rf,
        current_epoch=current_epoch,
        limit=limit,
    )


def erasure_push_retry_packs_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
    current_epoch: str | None,
    limit: int | None,
) -> list[ErasureDataPackRecord]:
    if scope == "all-known-chunks":
        pack_hashes = db.all_erasure_pack_hashes()
    else:
        chunk_hashes = chunk_hashes_for_scope(db, scope=scope, snapshot_id=snapshot_id)
        pack_hashes = db.erasure_pack_hashes_for_chunks(chunk_hashes)
    return db.pending_erasure_data_packs_by_hashes(
        pack_hashes,
        current_epoch=current_epoch,
        limit=limit,
    )


def erasure_push_new_chunks_for_scope(
    db: MetadataDB,
    *,
    scope: str,
    snapshot_id: int | None,
    limit: int | None,
) -> list[str]:
    chunk_hashes = chunk_hashes_for_scope(db, scope=scope, snapshot_id=snapshot_id)
    return db.erasure_unprotected_chunks_for_hashes(chunk_hashes, limit=limit)


def scoped_replication_verification_candidates(
    db: MetadataDB,
    *,
    scope: str | None,
    snapshot_id: int | None,
    include_verified: bool,
    limit: int | None,
) -> list[VerificationCandidate]:
    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == DEFAULT_PROTECTION_SCOPE:
        return db.get_verification_candidates(include_verified=include_verified, limit=limit)
    return replication_verification_candidates_for_scope(
        db,
        scope=scope_value,
        snapshot_id=snapshot_id,
        include_verified=include_verified,
        limit=limit,
    )


def scoped_erasure_verification_candidates(
    db: MetadataDB,
    *,
    scope: str | None,
    snapshot_id: int | None,
    pack_hash: str | None,
    include_verified: bool,
    limit: int | None,
) -> list[ErasureDataPackRecord]:
    if pack_hash is not None:
        normalized_pack_hash = str(pack_hash).strip().lower()
        if scope not in (None, DEFAULT_PROTECTION_SCOPE):
            raise ProtectionScopeError("--pack-hash no se puede combinar con --scope.")
        if snapshot_id is not None:
            raise ProtectionScopeError("--pack-hash no se puede combinar con --snapshot-id.")
        record = erasure_pack_by_hash(db, normalized_pack_hash)
        return [record] if record is not None else []

    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == DEFAULT_PROTECTION_SCOPE:
        return db.get_erasure_verification_candidates(include_verified=include_verified, limit=limit)
    return erasure_verification_candidates_for_scope(
        db,
        scope=scope_value,
        snapshot_id=snapshot_id,
        include_verified=include_verified,
        limit=limit,
    )


def scoped_replication_push_chunks(
    db: MetadataDB,
    *,
    scope: str | None,
    snapshot_id: int | None,
    desired_rf: int,
    current_epoch: str | None,
    limit: int | None,
) -> list[str]:
    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == DEFAULT_PROTECTION_SCOPE:
        return db.get_pending_protection_chunks(
            desired_rf=desired_rf,
            current_epoch=current_epoch,
            limit=limit,
        )
    return replication_push_chunks_for_scope(
        db,
        scope=scope_value,
        snapshot_id=snapshot_id,
        desired_rf=desired_rf,
        current_epoch=current_epoch,
        limit=limit,
    )


def scoped_erasure_push_retry_packs(
    db: MetadataDB,
    *,
    scope: str | None,
    snapshot_id: int | None,
    current_epoch: str | None,
    limit: int | None,
) -> list[ErasureDataPackRecord]:
    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == DEFAULT_PROTECTION_SCOPE:
        return db.get_pending_erasure_data_packs(current_epoch=current_epoch, limit=limit)
    return erasure_push_retry_packs_for_scope(
        db,
        scope=scope_value,
        snapshot_id=snapshot_id,
        current_epoch=current_epoch,
        limit=limit,
    )


def scoped_erasure_push_new_chunks(
    db: MetadataDB,
    *,
    scope: str | None,
    snapshot_id: int | None,
    limit: int | None,
) -> list[str]:
    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == DEFAULT_PROTECTION_SCOPE:
        return db.get_erasure_unprotected_chunks(limit=limit)
    return erasure_push_new_chunks_for_scope(
        db,
        scope=scope_value,
        snapshot_id=snapshot_id,
        limit=limit,
    )


def describe_protection_scope(scope: str | None, *, snapshot_id: int | None = None) -> str:
    scope_value = normalize_protection_scope(scope, snapshot_id=snapshot_id)
    if scope_value == "snapshot":
        return f"snapshot:{snapshot_id}"
    return scope_value
