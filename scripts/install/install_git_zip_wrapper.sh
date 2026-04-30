#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[install-git-zip-wrapper] erro: bash é obrigatório para instalar o wrapper\n' >&2
  exit 1
}

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
COMMON_HELPER="${SCRIPT_DIR}/lib/common.sh"

# shellcheck disable=SC1090
. "${COMMON_HELPER}"

RESTRICTED_SCRIPT_NAME="install-git-zip-wrapper"

log() {
  restricted_log "$@"
}

die() {
  restricted_die "$@"
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_git_zip_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--js-source-dir <dir>] [--real-git <path>]

Padrões:
  --install-dir: $HOME/.local/share/git-zip-wrapper/bin
  --wrapper-source: scripts/wrappers/git_zip_clone_wrapper.sh
  --js-source-dir: scripts/wrappers/js
  --real-git: primeiro git encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
WRAPPER_SOURCE="$(cd "${SCRIPT_DIR}/.." && pwd)/wrappers/git_zip_clone_wrapper.sh"
JS_SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)/wrappers/js"
JS_SOURCE_DIR_EXPLICIT="0"
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
    --js-source-dir)
      JS_SOURCE_DIR="${2:-}"
      JS_SOURCE_DIR_EXPLICIT="1"
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
if [[ "${JS_SOURCE_DIR_EXPLICIT}" != "1" ]]; then
  JS_SOURCE_DIR="$(cd "$(dirname "${WRAPPER_SOURCE}")" && pwd)/js"
fi
if [[ -n "${JS_SOURCE_DIR}" && ! -d "${JS_SOURCE_DIR}" ]]; then
  die "diretório de JS não encontrado: ${JS_SOURCE_DIR}"
fi

if [[ -z "${REAL_GIT_BIN}" ]]; then
  RESTRICTED_GIT_INSTALL_DIR="${INSTALL_DIR}"
  REAL_GIT_BIN="$(restricted_resolve_real_binary git || true)"
fi
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
RESTRICTED_GIT_INSTALL_DIR="${INSTALL_DIR}"
restricted_is_wrapper_binary_path git "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/git"
chmod 0755 "${INSTALL_DIR}/git"
if [[ -n "${JS_SOURCE_DIR}" ]]; then
  mkdir -p "${INSTALL_DIR}/js"
  cp -R "${JS_SOURCE_DIR}/." "${INSTALL_DIR}/js/"
  chmod 0755 "${INSTALL_DIR}/js/restricted_wrapper_cli.js" 2>/dev/null || true
fi

cat <<EOF2
Instalação concluída.

1) Exporte no shell:
export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"
# padrão para ambiente restrito: Git remoto externo é bloqueado, archive local é obrigatório
export GIT_ZIP_WRAPPER_CLONE_ORDER=local-first
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0
export GIT_ZIP_WRAPPER_USE_JS_ENGINE=1
# padrão para GitHub externo: usar archive .zip
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip
export GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK=1
# proxy do ambiente (preferência: GIT_ZIP_WRAPPER_PROXY > HTTPS_PROXY > ALL_PROXY > HTTP_PROXY)
# export GIT_ZIP_WRAPPER_PROXY=http://proxy.seu-dominio:3128
# opcional: permitir Git remoto real como último fallback
# export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=1
# modo de Git LFS é sempre local
export GIT_ZIP_WRAPPER_LFS_MODE=local
# resolver certificado em ambiente corporativo/proxy:
# export GIT_ZIP_WRAPPER_CURL_CACERT=/etc/pki/ca-trust/source/anchors/corp-ca.pem
# export GIT_ZIP_WRAPPER_CURL_INSECURE=0

2) Para LazyVim/Mason (init.lua):
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "${REAL_GIT_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH
-- padrão para ambiente restrito: Git remoto externo é bloqueado, archive local é obrigatório
vim.env.GIT_ZIP_WRAPPER_CLONE_ORDER = "local-first"
vim.env.GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK = "0"
vim.env.GIT_ZIP_WRAPPER_USE_JS_ENGINE = "1"
-- padrão para GitHub externo: usar archive .zip
vim.env.GIT_ZIP_WRAPPER_ARCHIVE_FORMAT = "zip"
vim.env.GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK = "1"
-- proxy do ambiente
-- vim.env.GIT_ZIP_WRAPPER_PROXY = "http://proxy.seu-dominio:3128"
-- opcional: permitir Git remoto real como último fallback
-- vim.env.GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK = "1"
-- modo de Git LFS é sempre local
vim.env.GIT_ZIP_WRAPPER_LFS_MODE = "local"
-- opcional: informar CA intermediária personalizada
-- vim.env.GIT_ZIP_WRAPPER_CURL_CACERT = "/etc/pki/ca-trust/source/anchors/corp-ca.pem"
-- opcional: aceitar certs inválidos (apenas para ambiente controlado)
-- vim.env.GIT_ZIP_WRAPPER_CURL_INSECURE = "0"

3) Teste:
git clone https://github.com/neovim/neovim ~/tmp/neovim-zip-clone
EOF2
