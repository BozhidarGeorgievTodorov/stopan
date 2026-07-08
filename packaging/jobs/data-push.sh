#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

: "${STOPAN_PROTECTION_MODE:=replication}"

set -- "$STOPAN_BIN" push --protection-mode "$STOPAN_PROTECTION_MODE"

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
fi

if [ -n "${STOPAN_MEMBERSHIP_SEED:-}" ]; then
    set -- "$@" --membership-seed "$STOPAN_MEMBERSHIP_SEED"
fi

if [ -n "${STOPAN_REMOTE_COPIES:-}" ]; then
    set -- "$@" --remote-copies "$STOPAN_REMOTE_COPIES"
fi

if [ -n "${STOPAN_EC_K:-}" ]; then
    set -- "$@" --ec-k "$STOPAN_EC_K"
fi

if [ -n "${STOPAN_EC_M:-}" ]; then
    set -- "$@" --ec-m "$STOPAN_EC_M"
fi

if [ -n "${STOPAN_EC_PACK_SIZE_BYTES:-}" ]; then
    set -- "$@" --ec-pack-size-bytes "$STOPAN_EC_PACK_SIZE_BYTES"
fi

if [ -n "${STOPAN_LIMIT:-}" ]; then
    set -- "$@" --limit "$STOPAN_LIMIT"
fi

if [ -n "${STOPAN_SCOPE:-}" ]; then
    set -- "$@" --scope "$STOPAN_SCOPE"
fi

if [ -n "${STOPAN_SNAPSHOT_ID:-}" ]; then
    set -- "$@" --snapshot-id "$STOPAN_SNAPSHOT_ID"
fi

if [ -n "${STOPAN_TARGET_PARALLELISM:-}" ]; then
    set -- "$@" --target-parallelism "$STOPAN_TARGET_PARALLELISM"
fi

if [ -n "${STOPAN_PROBE_BATCH_HASHES:-}" ]; then
    set -- "$@" --probe-batch-hashes "$STOPAN_PROBE_BATCH_HASHES"
fi

if [ -n "${STOPAN_STREAM_INFLIGHT:-}" ]; then
    set -- "$@" --stream-inflight "$STOPAN_STREAM_INFLIGHT"
fi

if [ -n "${STOPAN_PROBE_TIMEOUT_S:-}" ]; then
    set -- "$@" --probe-timeout-s "$STOPAN_PROBE_TIMEOUT_S"
fi

if [ -n "${STOPAN_STREAM_TIMEOUT_S:-}" ]; then
    set -- "$@" --stream-timeout-s "$STOPAN_STREAM_TIMEOUT_S"
fi

if [ -n "${STOPAN_MAX_MESSAGE_BYTES:-}" ]; then
    set -- "$@" --max-message-bytes "$STOPAN_MAX_MESSAGE_BYTES"
fi

if [ -n "${STOPAN_COMMIT_EVERY:-}" ]; then
    set -- "$@" --commit-every "$STOPAN_COMMIT_EVERY"
fi

if stopan_job_bool_is_true "${STOPAN_STRICT_REMOTE_COPIES:-}"; then
    set -- "$@" --strict-remote-copies
elif stopan_job_bool_is_false "${STOPAN_STRICT_REMOTE_COPIES:-}"; then
    set -- "$@" --no-strict-remote-copies
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
