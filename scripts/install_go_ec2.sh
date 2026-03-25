#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec "${SCRIPT_DIR}/ec2/go/install_go_ec2.sh" "$@"
