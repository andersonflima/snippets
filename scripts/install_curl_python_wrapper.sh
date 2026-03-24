#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-curl-python-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  install_curl_python_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--real-curl <path>]

Padrões:
  --install-dir: $HOME/.local/share/curl-python-wrapper/bin
  --wrapper-source: scripts/curl_python_wrapper.sh
  --real-curl: primeiro curl encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")" && pwd)/curl_python_wrapper.sh"
REAL_CURL_BIN="${CURL_WRAPPER_REAL_CURL:-}"

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
    --real-curl)
      REAL_CURL_BIN="${2:-}"
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

[[ -n "${INSTALL_DIR}" ]] || die "--install-dir não pode ser vazio"
[[ -n "${WRAPPER_SOURCE}" ]] || die "--wrapper-source não pode ser vazio"
[[ -f "${WRAPPER_SOURCE}" ]] || die "wrapper não encontrado: ${WRAPPER_SOURCE}"

if [[ -z "${REAL_CURL_BIN}" ]]; then
  REAL_CURL_BIN="$(command -v curl || true)"
fi
[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -x "${REAL_CURL_BIN}" ]] || die "curl inválido/não executável: ${REAL_CURL_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/curl"
chmod 0755 "${INSTALL_DIR}/curl"

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export CURL_WRAPPER_REAL_CURL="${REAL_CURL_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"

2) Para LazyVim/Mason (init.lua):
vim.env.CURL_WRAPPER_REAL_CURL = "${REAL_CURL_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH

3) Teste:
curl -fsSL https://github.com/neovim/neovim/archive/HEAD.zip -o /tmp/neovim.zip
EOF

