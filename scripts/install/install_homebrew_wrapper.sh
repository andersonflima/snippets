#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[install-homebrew-wrapper] erro: bash é obrigatório para instalar o wrapper do brew\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[install-homebrew-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_wrapper_binary_path() {
  local candidate_path
  candidate_path="$1"
  [[ "${candidate_path}" == "${INSTALL_DIR}/brew" ]]
}

resolve_real_brew() {
  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF
$(which -a brew 2>/dev/null || true)
EOF

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_homebrew_wrapper.sh [opções]

Opções:
  --install-dir <dir>      Diretório de instalação. Padrão: $HOME/.local/share/homebrew-install-wrapper/bin
  --wrapper-source <file>  Wrapper real do brew.
  --real-brew <path>       Caminho do brew real.
  -h, --help               Mostra esta ajuda.
USAGE
}

INSTALL_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/homebrew_install_wrapper.sh"
REAL_BREW_BIN="${BREW_WRAPPER_REAL_BREW:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --wrapper-source)
      WRAPPER_SOURCE="${2:-}"
      shift 2
      ;;
    --real-brew)
      REAL_BREW_BIN="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parâmetro inválido: $1"
      ;;
  esac
done

[[ -f "${WRAPPER_SOURCE}" ]] || die "wrapper não encontrado: ${WRAPPER_SOURCE}"

if [[ -z "${REAL_BREW_BIN}" ]]; then
  REAL_BREW_BIN="$(resolve_real_brew || true)"
fi

[[ -n "${REAL_BREW_BIN}" ]] || die "não foi possível localizar brew no PATH"
[[ -x "${REAL_BREW_BIN}" ]] || die "brew inválido/não executável: ${REAL_BREW_BIN}"
is_wrapper_binary_path "${REAL_BREW_BIN}" && die "brew real não pode apontar para o wrapper instalado: ${REAL_BREW_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/brew"
chmod 0755 "${INSTALL_DIR}/brew"

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export BREW_WRAPPER_REAL_BREW="${REAL_BREW_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"

2) Para garantir que brew install use os wrappers de curl/git:
export BREW_WRAPPER_CURL_BIN="\$HOME/.local/share/curl-python-wrapper/bin/curl"
export BREW_WRAPPER_GIT_BIN="\$HOME/.local/share/git-zip-wrapper/bin/git"

3) Testes:
brew install jq
brew install --cask wezterm
EOF
