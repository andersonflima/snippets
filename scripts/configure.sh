#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DEV_ENV_SCRIPT="${SCRIPT_DIR}/dev-env.sh"

[ -f "${DEV_ENV_SCRIPT}" ] || {
  printf '[configure] script interno ausente: %s\n' "${DEV_ENV_SCRIPT}" >&2
  exit 1
}

exec sh "${DEV_ENV_SCRIPT}" setup "$@"
