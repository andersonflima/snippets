#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec sh "${SCRIPT_DIR}/ec2/elixir/fetch_mix_hex_cache_from_ec2.sh" "$@"
