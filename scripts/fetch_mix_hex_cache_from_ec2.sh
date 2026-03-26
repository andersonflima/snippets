#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if command -v bash >/dev/null 2>&1; then
  exec bash "${SCRIPT_DIR}/ec2/elixir/fetch_mix_hex_cache_from_ec2.sh" "$@"
fi

printf '[fetch-mix-hex-cache] erro: bash é obrigatório para importar cache do EC2\n' >&2
exit 1
