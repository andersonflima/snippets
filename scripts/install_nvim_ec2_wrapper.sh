#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec sh "${SCRIPT_DIR}/install/install_nvim_ec2_wrapper.sh" "$@"
