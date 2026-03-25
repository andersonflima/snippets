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
  scripts/install/install_curl_python_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--real-curl <path>]

Padrões:
  --install-dir: $HOME/.local/share/curl-python-wrapper/bin
  --wrapper-source: scripts/wrappers/curl_python_wrapper.sh
  --real-curl: primeiro curl encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/curl_python_wrapper.sh"
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
vim.env.CURL_WRAPPER_RELEASE_FALLBACK_REPOS = "elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
vim.env.CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES = "1"

3) Pré-requisitos de fallback:
- opcional: gh CLI autenticado para assets de release do GitHub (`gh auth status`)
- para Mason em ambiente corporativo, releases de `elixir-ls`, `lua-language-server` e `omnisharp`
- são tratadas por padrão como restritas
- o wrapper tenta primeiro gerar o artefato localmente por estratégia compatível:
- `omnisharp` e `lua-language-server`: baixa `.tar.gz` equivalente e reempacota em `.zip`
- `elixir-ls`: baixa source tarball do tag, faz build local com `mix elixir_ls.release` e gera `.zip`
- se a estratégia inteligente falhar, o wrapper ainda tenta `gh release`
- para sobrescrever a lista:
- export CURL_WRAPPER_RELEASE_FALLBACK_REPOS="elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
- para desabilitar a estratégia inteligente:
- export CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES=0
- para reabilitar fallback direto de release explicitamente:
- export CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK=1
- python3 (usa requests quando disponível; sem requests cai para urllib nativo)
- `tar` é necessário para estratégias com `.tar.gz`
- `elixir` e `mix` são necessários quando o Mason precisar montar `elixir-ls` localmente
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
