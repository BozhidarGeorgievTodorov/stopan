#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

: "${STOPAN_METADATA_GRAPH_ACTION:=export}"

set -- "$STOPAN_BIN" metadata graph "$STOPAN_METADATA_GRAPH_ACTION"

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
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

if stopan_job_bool_is_true "${STOPAN_METADATA_DECRYPT_LATEST:-}"; then
    set -- "$@" --decrypt-latest
fi

if stopan_job_bool_is_true "${STOPAN_METADATA_NO_PROTECTION:-}"; then
    set -- "$@" --no-protection
fi

if stopan_job_bool_is_true "${STOPAN_METADATA_PACK:-}"; then
    set -- "$@" --pack
fi

if [ -n "${STOPAN_METADATA_PACK_OUT:-}" ]; then
    set -- "$@" --pack-out "$STOPAN_METADATA_PACK_OUT"
fi

if [ -n "${STOPAN_METADATA_PACK_DIR:-}" ]; then
    set -- "$@" --pack-dir "$STOPAN_METADATA_PACK_DIR"
fi

if [ -n "${STOPAN_DEFAULT_DESIRED_REMOTE_COPIES:-}" ]; then
    set -- "$@" --default-desired-remote-copies "$STOPAN_DEFAULT_DESIRED_REMOTE_COPIES"
fi

if [ -n "${STOPAN_SCRYPT_N:-}" ]; then
    set -- "$@" --scrypt-n "$STOPAN_SCRYPT_N"
fi

if [ -n "${STOPAN_SCRYPT_R:-}" ]; then
    set -- "$@" --scrypt-r "$STOPAN_SCRYPT_R"
fi

if [ -n "${STOPAN_SCRYPT_P:-}" ]; then
    set -- "$@" --scrypt-p "$STOPAN_SCRYPT_P"
fi

if [ -n "${STOPAN_METADATA_KEY_LENGTH:-}" ]; then
    set -- "$@" --metadata-key-length "$STOPAN_METADATA_KEY_LENGTH"
fi

exec "$@"
