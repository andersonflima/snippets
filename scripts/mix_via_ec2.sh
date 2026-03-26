#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if command -v bash >/dev/null 2>&1; then
  exec bash "${SCRIPT_DIR}/ec2/elixir/mix_via_ec2.sh" "$@"
fi

printf '[mix-via-ec2] erro: bash é obrigatório para executar o mix via EC2\n' >&2
exit 1
