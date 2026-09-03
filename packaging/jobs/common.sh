#!/bin/sh
# Helpers compartidos por los perfiles systemd de Stopan.

: "${STOPAN_BIN:=/usr/bin/stopan}"

# Los jobs escriben en logs persistentes (por ejemplo, journald). El feedback
# interactivo de una sola línea solo tiene sentido en una terminal humana.
export STOPAN_NO_PROGRESS=1

stopan_job_die() {
    echo "stopan job: $*" >&2
    exit 2
}

stopan_job_bool_is_true() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON|si|SI|sí|SÍ) return 0 ;;
        *) return 1 ;;
    esac
}

stopan_job_bool_is_false() {
    case "${1:-}" in
        0|false|FALSE|no|NO|off|OFF) return 0 ;;
        *) return 1 ;;
    esac
}

stopan_job_require() {
    name="$1"
    eval "value=\${$name:-}"
    [ -n "$value" ] || stopan_job_die "la variable $name es obligatoria"
}

