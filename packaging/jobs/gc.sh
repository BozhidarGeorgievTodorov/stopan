#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

: "${STOPAN_GC_TARGET:=all}"

set -- "$STOPAN_BIN" gc "$STOPAN_GC_TARGET"

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
fi

apply_mode="${STOPAN_GC_APPLY:-}"

if stopan_job_bool_is_true "$apply_mode"; then
    set -- "$@" --apply
elif [ -z "$apply_mode" ] || stopan_job_bool_is_false "$apply_mode"; then
    set -- "$@" --dry-run
else
    stopan_job_die "STOPAN_GC_APPLY debe ser un valor booleano reconocido"
fi

if [ -n "${STOPAN_CHUNK_STORE:-}" ]; then
    set -- "$@" --chunk-store "$STOPAN_CHUNK_STORE"
fi

if [ -n "${STOPAN_EC_STORE:-}" ]; then
    set -- "$@" --ec-store "$STOPAN_EC_STORE"
fi

if [ -n "${STOPAN_GRACE_HOURS:-}" ]; then
    set -- "$@" --grace-hours "$STOPAN_GRACE_HOURS"
fi

if [ -n "${STOPAN_MAX_AGE_DAYS:-}" ]; then
    set -- "$@" --max-age-days "$STOPAN_MAX_AGE_DAYS"
fi

if [ -n "${STOPAN_METADATA_OBJECT_STORE:-}" ]; then
    set -- "$@" --object-store "$STOPAN_METADATA_OBJECT_STORE"
fi

if [ -n "${STOPAN_METADATA_PASSPHRASE_FILE:-}" ]; then
    set -- "$@" --passphrase-file "$STOPAN_METADATA_PASSPHRASE_FILE"
fi

if [ -n "${STOPAN_METADATA_IDENTITY_FILE:-}" ]; then
    set -- "$@" --identity-file "$STOPAN_METADATA_IDENTITY_FILE"
fi

if [ -n "${STOPAN_OBJECT_GRACE_HOURS:-}" ]; then
    set -- "$@" --object-grace-hours "$STOPAN_OBJECT_GRACE_HOURS"
fi

if [ -n "${STOPAN_PACK_GRACE_HOURS:-}" ]; then
    set -- "$@" --pack-grace-hours "$STOPAN_PACK_GRACE_HOURS"
fi

if [ -n "${STOPAN_METADATA_PACK_DIR:-}" ]; then
    set -- "$@" --pack-dir "$STOPAN_METADATA_PACK_DIR"
fi

if [ -n "${STOPAN_PACK_STORE:-}" ]; then
    set -- "$@" --pack-store "$STOPAN_PACK_STORE"
fi

exec "$@"
