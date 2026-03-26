#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-wrapper-envs-zsh] erro: bash é obrigatório para configurar o ambiente\n' >&2
  exit 1
}

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

exec "${SCRIPT_DIR}/configure_wrapper_envs.sh" \
  --shell-rc "${HOME}/.zshrc" \
  "$@"
