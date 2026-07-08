#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

stopan_job_require STOPAN_BACKUP_SOURCE

set -- "$STOPAN_BIN" backup

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
fi

set -- "$@" "$STOPAN_BACKUP_SOURCE"

if [ -n "${STOPAN_BACKUP_WORKERS:-}" ]; then
    set -- "$@" "$STOPAN_BACKUP_WORKERS"
fi

if stopan_job_bool_is_true "${STOPAN_BACKUP_FAST:-}"; then
    set -- "$@" --fast
fi

if stopan_job_bool_is_true "${STOPAN_BACKUP_FAST_REMOTE:-}"; then
    set -- "$@" --fast-remote
fi

if stopan_job_bool_is_true "${STOPAN_BACKUP_SAFE:-}"; then
    set -- "$@" --safe
fi

if stopan_job_bool_is_true "${STOPAN_BACKUP_DETERMINISTIC:-}"; then
    set -- "$@" --deterministic
fi

if [ -n "${STOPAN_DESIRED_REMOTE_COPIES:-}" ]; then
    set -- "$@" --desired-remote-copies "$STOPAN_DESIRED_REMOTE_COPIES"
fi

if [ -n "${STOPAN_MEMBERSHIP_SEED:-}" ]; then
    set -- "$@" --membership-seed "$STOPAN_MEMBERSHIP_SEED"
fi

if stopan_job_bool_is_true "${STOPAN_METADATA_OBJECT_GRAPH:-}"; then
    set -- "$@" --metadata-object-graph
elif stopan_job_bool_is_false "${STOPAN_METADATA_OBJECT_GRAPH:-}"; then
    set -- "$@" --no-metadata-object-graph
fi

if [ -n "${STOPAN_METADATA_OBJECT_STORE:-}" ]; then
    set -- "$@" --metadata-object-store "$STOPAN_METADATA_OBJECT_STORE"
fi

if [ -n "${STOPAN_METADATA_PASSPHRASE_FILE:-}" ]; then
    set -- "$@" --metadata-passphrase-file "$STOPAN_METADATA_PASSPHRASE_FILE"
fi

if [ -n "${STOPAN_METADATA_IDENTITY_FILE:-}" ]; then
    set -- "$@" --metadata-identity-file "$STOPAN_METADATA_IDENTITY_FILE"
fi

if stopan_job_bool_is_true "${STOPAN_METADATA_OBJECT_PACK:-}"; then
    set -- "$@" --metadata-object-pack
elif stopan_job_bool_is_false "${STOPAN_METADATA_OBJECT_PACK:-}"; then
    set -- "$@" --no-metadata-object-pack
fi

if [ -n "${STOPAN_METADATA_OBJECT_PACK_DIR:-}" ]; then
    set -- "$@" --metadata-object-pack-dir "$STOPAN_METADATA_OBJECT_PACK_DIR"
fi

exec "$@"
