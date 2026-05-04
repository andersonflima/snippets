#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[install-git-zip-wrapper] erro: bash é obrigatório para instalar o wrapper\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[install-git-zip-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_wrapper_binary_path() {
  local candidate_path
  candidate_path="$1"
  [[ "${candidate_path}" == "${INSTALL_DIR}/git" ]] && return 0
  [[ "${candidate_path}" == "${HOME}/.local/bin/git" ]] && return 0
}

candidate_paths_for_binary() {
  local binary_name
  binary_name="$1"

  case "${binary_name}" in
    git)
      printf '/usr/bin/git\n'
      printf '/usr/local/bin/git\n'
      printf '/opt/homebrew/bin/git\n'
      printf '/bin/git\n'
      printf '/usr/libexec/git-core/git\n'
      ;;
    *)
      :
      ;;
  esac
}

resolve_real_git() {
  local candidate seen=""

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    [[ "${seen}" == *$'\n'"${candidate}"$'\n'* ]] && continue
    seen+="${candidate}"$'\n'
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF2
$(which -a git 2>/dev/null || true)
EOF2

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    [[ "${seen}" == *$'\n'"${candidate}"$'\n'* ]] && continue
    seen+="${candidate}"$'\n'
    [[ -x "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF3
$(candidate_paths_for_binary git)
EOF3

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_git_zip_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--real-git <path>]

Padrões:
  --install-dir: $HOME/.local/share/git-zip-wrapper/bin
  --wrapper-source: scripts/wrappers/git_zip_clone_wrapper.sh
  --real-git: primeiro git encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/git_zip_clone_wrapper.sh"
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
  REAL_GIT_BIN="$(resolve_real_git || true)"
fi
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
is_wrapper_binary_path "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/git"
chmod 0755 "${INSTALL_DIR}/git"

cat <<EOF2
Instalação concluída.

1) Exporte no shell:
export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"
# padrão para ambiente restrito: Git interno funciona normalmente; não-itau usa archive local
export GIT_ZIP_WRAPPER_CLONE_ORDER=local-first
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0
# padrão para GitHub externo: usar archive .zip
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip
export GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK=1
export RESTRICTED_GIT_PLAIN_OWNER_PREFIXES="itau-,itau"
# proxy do ambiente (preferência: GIT_ZIP_WRAPPER_PROXY > HTTPS_PROXY > ALL_PROXY > HTTP_PROXY)
# export GIT_ZIP_WRAPPER_PROXY=http://proxy.seu-dominio:3128
# opcional: reativar fallback remoto real
# export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0
# modo de Git LFS é sempre local
export GIT_ZIP_WRAPPER_LFS_MODE=local
# resolver certificado em ambiente corporativo/proxy:
# export GIT_ZIP_WRAPPER_CURL_CACERT=/etc/pki/ca-trust/source/anchors/corp-ca.pem
# export GIT_ZIP_WRAPPER_CURL_INSECURE=0

2) Para LazyVim/Mason (init.lua):
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "${REAL_GIT_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH
-- padrão para ambiente restrito: Git interno funciona normalmente; não-itau usa archive local
vim.env.GIT_ZIP_WRAPPER_CLONE_ORDER = "local-first"
vim.env.GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK = "0"
-- padrão para GitHub externo: usar archive .zip
vim.env.GIT_ZIP_WRAPPER_ARCHIVE_FORMAT = "zip"
vim.env.GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK = "1"
vim.env.RESTRICTED_GIT_PLAIN_OWNER_PREFIXES = "itau-,itau"
-- proxy do ambiente
-- vim.env.GIT_ZIP_WRAPPER_PROXY = "http://proxy.seu-dominio:3128"
-- opcional: reativar fallback remoto real
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
