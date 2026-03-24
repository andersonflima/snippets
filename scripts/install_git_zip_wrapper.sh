#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-git-zip-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  install_git_zip_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--real-git <path>]

Padrões:
  --install-dir: $HOME/.local/share/git-zip-wrapper/bin
  --wrapper-source: scripts/git_zip_clone_wrapper.sh
  --real-git: primeiro git encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")" && pwd)/git_zip_clone_wrapper.sh"
REAL_GIT_BIN="${GIT_ZIP_WRAPPER_REAL_GIT:-}"

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
    --real-git)
      REAL_GIT_BIN="${2:-}"
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

if [[ -z "${REAL_GIT_BIN}" ]]; then
  REAL_GIT_BIN="$(command -v git || true)"
fi
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/git"
chmod 0755 "${INSTALL_DIR}/git"

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"
# opcional: forçar modo estrito (sem fallback para git clone normal)
# export GIT_ZIP_WRAPPER_STRICT=1

2) Para LazyVim/Mason (init.lua):
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "${REAL_GIT_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH
-- opcional: sem fallback para git clone normal
-- vim.env.GIT_ZIP_WRAPPER_STRICT = "1"

3) Teste:
git clone https://github.com/neovim/neovim ~/tmp/neovim-zip-clone
EOF
