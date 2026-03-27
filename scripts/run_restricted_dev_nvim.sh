#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

. "${SCRIPT_DIR}/activate_restricted_dev_env.sh"

NVIM_BIN="${NVIM_BIN:-nvim}"
exec "${NVIM_BIN}" "$@"
