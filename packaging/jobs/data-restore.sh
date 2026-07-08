#!/bin/sh
set -eu

. "${STOPAN_JOB_LIB_DIR:-/usr/lib/stopan/jobs}/common.sh"

stopan_job_require STOPAN_SNAPSHOT_ID

set -- "$STOPAN_BIN" restore

if [ -n "${STOPAN_CONFIG:-}" ]; then
    set -- "$@" --config "$STOPAN_CONFIG"
fi

set -- "$@" "$STOPAN_SNAPSHOT_ID"

if [ -n "${STOPAN_RESTORE_OUT:-}" ]; then
    set -- "$@" --out "$STOPAN_RESTORE_OUT"
fi

if [ -n "${STOPAN_REMOTE_RECOVERY:-}" ]; then
    set -- "$@" --remote-recovery "$STOPAN_REMOTE_RECOVERY"
fi

if [ -n "${STOPAN_REPLICATION_TARGETS:-}" ]; then
    set -- "$@" --replication-targets "$STOPAN_REPLICATION_TARGETS"
fi

if [ -n "${STOPAN_MEMBERSHIP_SEED:-}" ]; then
    set -- "$@" --membership-seed "$STOPAN_MEMBERSHIP_SEED"
fi

if [ -n "${STOPAN_PREFETCH_WINDOW:-}" ]; then
    set -- "$@" --prefetch-window "$STOPAN_PREFETCH_WINDOW"
fi

if [ -n "${STOPAN_BATCH_TARGET_PARALLELISM:-}" ]; then
    set -- "$@" --batch-target-parallelism "$STOPAN_BATCH_TARGET_PARALLELISM"
fi

exec "$@"
