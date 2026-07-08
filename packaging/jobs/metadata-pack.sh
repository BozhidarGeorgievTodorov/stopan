#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

: "${STOPAN_METADATA_PACK_ACTION:=push}"

set -- "$STOPAN_BIN" metadata pack "$STOPAN_METADATA_PACK_ACTION"

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
fi

case "$STOPAN_METADATA_PACK_ACTION" in
    inspect|import|local-store)
        stopan_job_require STOPAN_METADATA_PACK_PATH
        set -- "$@" "$STOPAN_METADATA_PACK_PATH"
        ;;
esac

if [ -n "${STOPAN_METADATA_OBJECT_STORE:-}" ]; then
    set -- "$@" --object-store "$STOPAN_METADATA_OBJECT_STORE"
fi

if [ -n "${STOPAN_METADATA_PACK_OUT:-}" ]; then
    case "$STOPAN_METADATA_PACK_ACTION" in
        create)
            set -- "$@" --out "$STOPAN_METADATA_PACK_OUT"
            ;;
        push|recover)
            set -- "$@" --pack-out "$STOPAN_METADATA_PACK_OUT"
            ;;
        *)
            stopan_job_die "STOPAN_METADATA_PACK_OUT no aplica a $STOPAN_METADATA_PACK_ACTION"
            ;;
    esac
fi

if [ -n "${STOPAN_METADATA_PACK_DIR:-}" ]; then
    set -- "$@" --pack-dir "$STOPAN_METADATA_PACK_DIR"
fi

if [ -n "${STOPAN_METADATA_PACK_IN:-}" ]; then
    set -- "$@" --pack-in "$STOPAN_METADATA_PACK_IN"
fi

if [ -n "${STOPAN_METADATA_PASSPHRASE_FILE:-}" ]; then
    set -- "$@" --passphrase-file "$STOPAN_METADATA_PASSPHRASE_FILE"
fi

if [ -n "${STOPAN_METADATA_IDENTITY_FILE:-}" ]; then
    set -- "$@" --identity-file "$STOPAN_METADATA_IDENTITY_FILE"
fi

if [ -n "${STOPAN_OWNER_ID:-}" ]; then
    set -- "$@" --owner-id "$STOPAN_OWNER_ID"
fi

if stopan_job_bool_is_true "${STOPAN_DECRYPT:-}"; then
    set -- "$@" --decrypt
fi

if [ -n "${STOPAN_EXPECTED_PACK_HASH:-}" ]; then
    set -- "$@" --expected-pack-hash "$STOPAN_EXPECTED_PACK_HASH"
fi

if [ -n "${STOPAN_PACK_STORE:-}" ]; then
    set -- "$@" --pack-store "$STOPAN_PACK_STORE"
fi

if [ -n "${STOPAN_PACK_HASH:-}" ]; then
    set -- "$@" --pack-hash "$STOPAN_PACK_HASH"
fi

if [ -n "${STOPAN_LOCAL_RETRIEVE_OUT:-}" ]; then
    set -- "$@" --out "$STOPAN_LOCAL_RETRIEVE_OUT"
fi

if [ -n "${STOPAN_MEMBERSHIP_SEED:-}" ]; then
    set -- "$@" --membership-seed "$STOPAN_MEMBERSHIP_SEED"
fi

if [ -n "${STOPAN_PACK_COPIES:-}" ]; then
    set -- "$@" --pack-copies "$STOPAN_PACK_COPIES"
fi

if stopan_job_bool_is_true "${STOPAN_STRICT_PACK_COPIES:-}"; then
    set -- "$@" --strict-pack-copies
elif stopan_job_bool_is_false "${STOPAN_STRICT_PACK_COPIES:-}"; then
    set -- "$@" --no-strict-pack-copies
fi

if [ -n "${STOPAN_TARGET_PARALLELISM:-}" ]; then
    set -- "$@" --target-parallelism "$STOPAN_TARGET_PARALLELISM"
fi

if [ -n "${STOPAN_RPC_TIMEOUT_S:-}" ]; then
    set -- "$@" --rpc-timeout-s "$STOPAN_RPC_TIMEOUT_S"
fi

if [ -n "${STOPAN_MAX_MESSAGE_BYTES:-}" ]; then
    set -- "$@" --max-message-bytes "$STOPAN_MAX_MESSAGE_BYTES"
fi

if [ -n "${STOPAN_MAX_CANDIDATES:-}" ]; then
    set -- "$@" --max-candidates "$STOPAN_MAX_CANDIDATES"
fi

if stopan_job_bool_is_true "${STOPAN_SHOW_SOURCES:-}"; then
    set -- "$@" --show-sources
fi

if stopan_job_bool_is_true "${STOPAN_VERIFY_ALL:-}"; then
    set -- "$@" --all
fi

if [ -n "${STOPAN_DOWNLOAD_DIR:-}" ]; then
    set -- "$@" --download-dir "$STOPAN_DOWNLOAD_DIR"
fi

if [ -n "${STOPAN_DOWNLOAD_PACK_OUT:-}" ]; then
    set -- "$@" --pack-out "$STOPAN_DOWNLOAD_PACK_OUT"
fi

if stopan_job_bool_is_true "${STOPAN_DOWNLOAD_ONLY:-}"; then
    set -- "$@" --download-only
fi

if [ -n "${STOPAN_TARGET_HASH:-}" ]; then
    set -- "$@" --target-hash "$STOPAN_TARGET_HASH"
fi

if [ -n "${STOPAN_VAULT_ID:-}" ]; then
    set -- "$@" --vault-id "$STOPAN_VAULT_ID"
fi

if stopan_job_bool_is_true "${STOPAN_NO_IMPORT_DB:-}"; then
    set -- "$@" --no-import-db
fi

if stopan_job_bool_is_true "${STOPAN_NO_PROTECTION:-}"; then
    set -- "$@" --no-protection
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
