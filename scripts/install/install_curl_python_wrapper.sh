#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-curl-python-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_wrapper_binary_path() {
  local candidate_path
  candidate_path="$1"
  [[ "${candidate_path}" == "${INSTALL_DIR}/curl" || "${candidate_path}" == "${INSTALL_DIR}/wget" ]]
}

resolve_real_curl() {
  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF
$(which -a curl 2>/dev/null || true)
EOF

  return 1
}

resolve_real_wget() {
  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF
$(which -a wget 2>/dev/null || true)
EOF

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_curl_python_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--wget-wrapper-source <file>] [--lib-source-dir <dir>] [--ec2-helper-source <file>] [--real-curl <path>] [--real-wget <path>]

Padrões:
  --install-dir: $HOME/.local/share/curl-python-wrapper/bin
  --wrapper-source: scripts/wrappers/curl_python_wrapper.sh
  --wget-wrapper-source: scripts/wrappers/wget_ec2_wrapper.sh
  --lib-source-dir: scripts/wrappers/lib
  --ec2-helper-source: scripts/ec2/assets/fetch_url_via_ec2.sh
  --real-curl: primeiro curl encontrado no PATH
  --real-wget: primeiro wget encontrado no PATH, se existir
USAGE
}

INSTALL_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/curl_python_wrapper.sh"
WGET_WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/wget_ec2_wrapper.sh"
LIB_SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)/wrappers/lib"
EC2_HELPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/assets/fetch_url_via_ec2.sh"
LIB_SOURCE_DIR_EXPLICIT="0"
REAL_CURL_BIN="${CURL_WRAPPER_REAL_CURL:-}"
REAL_WGET_BIN="${WGET_WRAPPER_REAL_WGET:-}"

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
    --wget-wrapper-source)
      WGET_WRAPPER_SOURCE="${2:-}"
      shift 2
      ;;
    --lib-source-dir)
      LIB_SOURCE_DIR="${2:-}"
      LIB_SOURCE_DIR_EXPLICIT="1"
      shift 2
      ;;
    --ec2-helper-source)
      EC2_HELPER_SOURCE="${2:-}"
      shift 2
      ;;
    --real-curl)
      REAL_CURL_BIN="${2:-}"
      shift 2
      ;;
    --real-wget)
      REAL_WGET_BIN="${2:-}"
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
[[ -f "${WGET_WRAPPER_SOURCE}" ]] || die "wrapper wget não encontrado: ${WGET_WRAPPER_SOURCE}"
[[ -f "${EC2_HELPER_SOURCE}" ]] || die "helper EC2 não encontrado: ${EC2_HELPER_SOURCE}"
if [[ "${LIB_SOURCE_DIR_EXPLICIT}" != "1" ]]; then
  LIB_SOURCE_DIR="$(cd "$(dirname "${WRAPPER_SOURCE}")" && pwd)/lib"
fi
if [[ -n "${LIB_SOURCE_DIR}" && ! -d "${LIB_SOURCE_DIR}" ]]; then
  die "diretório de libs não encontrado: ${LIB_SOURCE_DIR}"
fi

if [[ -z "${REAL_CURL_BIN}" ]]; then
  REAL_CURL_BIN="$(resolve_real_curl || true)"
fi
if [[ -z "${REAL_WGET_BIN}" ]]; then
  REAL_WGET_BIN="$(resolve_real_wget || true)"
fi
[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -x "${REAL_CURL_BIN}" ]] || die "curl inválido/não executável: ${REAL_CURL_BIN}"
is_wrapper_binary_path "${REAL_CURL_BIN}" && die "curl real não pode apontar para o wrapper instalado: ${REAL_CURL_BIN}"
if [[ -n "${REAL_WGET_BIN}" ]]; then
  [[ -x "${REAL_WGET_BIN}" ]] || die "wget inválido/não executável: ${REAL_WGET_BIN}"
  is_wrapper_binary_path "${REAL_WGET_BIN}" && die "wget real não pode apontar para o wrapper instalado: ${REAL_WGET_BIN}"
fi

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/curl"
cp "${WGET_WRAPPER_SOURCE}" "${INSTALL_DIR}/wget"
chmod 0755 "${INSTALL_DIR}/curl"
chmod 0755 "${INSTALL_DIR}/wget"
cp "${EC2_HELPER_SOURCE}" "${INSTALL_DIR}/fetch-url-via-ec2"
chmod 0755 "${INSTALL_DIR}/fetch-url-via-ec2"
if [[ -n "${LIB_SOURCE_DIR}" ]]; then
  mkdir -p "${INSTALL_DIR}/lib"
  cp -R "${LIB_SOURCE_DIR}/." "${INSTALL_DIR}/lib/"
fi

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export CURL_WRAPPER_REAL_CURL="${REAL_CURL_BIN}"
export WGET_WRAPPER_REAL_WGET="${REAL_WGET_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"

2) Para LazyVim/Mason (init.lua):
vim.env.CURL_WRAPPER_REAL_CURL = "${REAL_CURL_BIN}"
vim.env.WGET_WRAPPER_REAL_WGET = "${REAL_WGET_BIN}"
vim.env.PATH = "${INSTALL_DIR}:" .. vim.env.PATH
vim.env.CURL_WRAPPER_RELEASE_FALLBACK_REPOS = "elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
vim.env.CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK = "1"
vim.env.CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES = "1"
vim.env.CURL_WRAPPER_RELEASE_CACHE_DIR = vim.fn.expand("~/.cache/curl-python-wrapper/releases")
vim.env.CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS = "omnisharp/omnisharp-roslyn"
vim.env.CURL_WRAPPER_MASON_BUILDERS = "elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"

3) Pré-requisitos de fallback:
- opcional: gh CLI autenticado para assets de release do GitHub (gh auth status)
- para Mason em ambiente corporativo, o wrapper agora tenta automaticamente:
- descobrir assets alternativos da release via API do GitHub
- preferir twin exato do asset pedido quando existir variante equivalente em tarball
- preferir .tar.gz/.tgz/.tar quando o Mason pede .zip
- reempacotar localmente em .zip para preservar o contrato esperado pelo Mason
- usar builders registrados quando não houver asset alternativo equivalente
- builders padrão atuais:
- elixir-lsp/elixir-ls=elixir_ls_release
- omnisharp/omnisharp-roslyn=omnisharp_source_publish
- quando o pacote só publica .zip, o wrapper também tenta o endpoint de assets da API do GitHub
- se a estratégia inteligente falhar, o wrapper ainda tenta `gh release`
- para sobrescrever a lista:
- export CURL_WRAPPER_RELEASE_FALLBACK_REPOS="elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
- para sobrescrever o registro de builders:
- export CURL_WRAPPER_MASON_BUILDERS="elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"
- para sobrescrever os repositórios que devem buildar from scratch:
- export CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS="omnisharp/omnisharp-roslyn"
- para sobrescrever o cache local:
- export CURL_WRAPPER_RELEASE_CACHE_DIR="\$HOME/.cache/curl-python-wrapper/releases"
- para sobrescrever extensões reempacotáveis:
- export CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS="tar.gz,tgz,tar"
- para desabilitar a estratégia inteligente:
- export CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES=0
- para reabilitar fallback direto de release explicitamente:
- export CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK=1
- python3 (usa requests quando disponível; sem requests cai para urllib nativo)
- tar é necessário para estratégias com .tar.gz
- elixir e mix são necessários quando o Mason precisar montar elixir-ls localmente
- padrão do wrapper bloqueia download de .zip; libere se precisar:
- export CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1
- para contornar falhas de certificado em ambientes fechados:
- export CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR=1
- proxy por env (suportado explicitamente pelo wrapper):
- export HTTPS_PROXY=http://proxy.seu-dominio:3128
- export HTTP_PROXY=http://proxy.seu-dominio:3128
- export ALL_PROXY=http://proxy.seu-dominio:3128
- ou sobrescrever explicitamente:
- export CURL_WRAPPER_PROXY=http://proxy.seu-dominio:3128

4) Teste:
curl -fsSL https://github.com/neovim/neovim/archive/refs/heads/master.tar.gz -o /tmp/neovim.tar.gz
EOF
