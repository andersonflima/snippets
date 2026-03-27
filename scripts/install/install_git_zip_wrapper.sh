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
  [[ "${candidate_path}" == "${INSTALL_DIR}/git" ]]
}

resolve_real_git() {
  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF2
$(which -a git 2>/dev/null || true)
EOF2

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_git_zip_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--ec2-helper-source <file>] [--clone-helper-source <file>] [--fetch-helper-source <file>] [--checkout-helper-source <file>] [--real-git <path>]

Padrões:
  --install-dir: $HOME/.local/share/git-zip-wrapper/bin
  --wrapper-source: scripts/wrappers/git_zip_clone_wrapper.sh
  --ec2-helper-source: scripts/ec2/assets/fetch_url_via_ec2.sh
  --clone-helper-source: scripts/ec2/git/clone_via_ec2.sh
  --fetch-helper-source: scripts/ec2/git/fetch_via_ec2.sh
  --checkout-helper-source: scripts/ec2/git/checkout_via_ec2.sh
  --real-git: primeiro git encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/git_zip_clone_wrapper.sh"
EC2_HELPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/assets/fetch_url_via_ec2.sh"
CLONE_HELPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/git/clone_via_ec2.sh"
FETCH_HELPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/git/fetch_via_ec2.sh"
CHECKOUT_HELPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/git/checkout_via_ec2.sh"
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
    --ec2-helper-source)
      EC2_HELPER_SOURCE="${2:-}"
      shift 2
      ;;
    --clone-helper-source)
      CLONE_HELPER_SOURCE="${2:-}"
      shift 2
      ;;
    --fetch-helper-source)
      FETCH_HELPER_SOURCE="${2:-}"
      shift 2
      ;;
    --checkout-helper-source)
      CHECKOUT_HELPER_SOURCE="${2:-}"
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
[[ -f "${EC2_HELPER_SOURCE}" ]] || die "helper EC2 não encontrado: ${EC2_HELPER_SOURCE}"
[[ -f "${CLONE_HELPER_SOURCE}" ]] || die "helper de clone EC2 não encontrado: ${CLONE_HELPER_SOURCE}"
[[ -f "${FETCH_HELPER_SOURCE}" ]] || die "helper de fetch EC2 não encontrado: ${FETCH_HELPER_SOURCE}"
[[ -f "${CHECKOUT_HELPER_SOURCE}" ]] || die "helper de checkout EC2 não encontrado: ${CHECKOUT_HELPER_SOURCE}"

if [[ -z "${REAL_GIT_BIN}" ]]; then
  REAL_GIT_BIN="$(resolve_real_git || true)"
fi
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
is_wrapper_binary_path "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/git"
cp "${EC2_HELPER_SOURCE}" "${INSTALL_DIR}/fetch-url-via-ec2"
cp "${CLONE_HELPER_SOURCE}" "${INSTALL_DIR}/git-clone-via-ec2"
cp "${FETCH_HELPER_SOURCE}" "${INSTALL_DIR}/git-fetch-via-ec2"
cp "${CHECKOUT_HELPER_SOURCE}" "${INSTALL_DIR}/git-checkout-via-ec2"
chmod 0755 "${INSTALL_DIR}/git"
chmod 0755 "${INSTALL_DIR}/fetch-url-via-ec2"
chmod 0755 "${INSTALL_DIR}/git-clone-via-ec2"
chmod 0755 "${INSTALL_DIR}/git-fetch-via-ec2"
chmod 0755 "${INSTALL_DIR}/git-checkout-via-ec2"

cat <<EOF2
Instalação concluída.

1) Exporte no shell:
export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"
# opcional: forçar modo estrito (sem fallback para git clone normal)
# export GIT_ZIP_WRAPPER_STRICT=1
# padrão do wrapper usa .tar.gz
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=tar.gz
# proxy do ambiente (preferência: GIT_ZIP_WRAPPER_PROXY > HTTPS_PROXY > ALL_PROXY > HTTP_PROXY)
# export GIT_ZIP_WRAPPER_PROXY=http://proxy.seu-dominio:3128
# habilitar .zip somente quando necessário
# export GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK=1
# resolver certificado em ambiente corporativo/proxy:
# export GIT_ZIP_WRAPPER_CURL_CACERT=/etc/pki/ca-trust/source/anchors/corp-ca.pem
# export GIT_ZIP_WRAPPER_CURL_INSECURE=0

2) Para LazyVim/Mason (init.lua):
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "${REAL_GIT_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH
-- opcional: sem fallback para git clone normal
-- vim.env.GIT_ZIP_WRAPPER_STRICT = "1"
-- padrão do wrapper usa .tar.gz
vim.env.GIT_ZIP_WRAPPER_ARCHIVE_FORMAT = "tar.gz"
-- proxy do ambiente
-- vim.env.GIT_ZIP_WRAPPER_PROXY = "http://proxy.seu-dominio:3128"
-- habilitar .zip somente quando necessário
-- vim.env.GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK = "1"
-- opcional: informar CA intermediária personalizada
-- vim.env.GIT_ZIP_WRAPPER_CURL_CACERT = "/etc/pki/ca-trust/source/anchors/corp-ca.pem"
-- opcional: aceitar certs inválidos (apenas para ambiente controlado)
-- vim.env.GIT_ZIP_WRAPPER_CURL_INSECURE = "0"

3) Teste:
git clone https://github.com/neovim/neovim ~/tmp/neovim-zip-clone
EOF2
