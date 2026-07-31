#!/usr/bin/env bash
set -euo pipefail

# Instala/atualiza a lib do Tesus (@andespindola/brainlink, pacote npm
# PRIVADO) na máquina corporativa, pela mesma rota do update de plugins do
# LazyVim: registry npm oficial com token granular de LEITURA + proxy herdado
# do ~/.npmrc local (a rota que o proxy corporativo permite).
#
# Pré-requisitos (uma vez):
#   - o token granular precisa ter @andespindola/brainlink no escopo (read);
#   - token gravado em ~/.local/share/nvim-docker-toolchain/npm-token
#     (chmod 600) — mesmo arquivo do nvim-plugins-update — ou exportado em
#     NPM_PLUGINS_TOKEN.
#
# Uso:
#   bash config/tesus/install_tesus_from_npm.sh [--prefix <dir>]
#
#   --prefix  Prefixo do npm -g (default: o prefix atual do npm da máquina).
#
# Observação: a dependência opcional nativa (hnswlib-node) pode não compilar
# em host restrito — o Tesus cai no fallback int8 e segue funcional.

log() {
  printf '[tesus-install] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

PACKAGE_NAME="@andespindola/brainlink"
STATE_ROOT="${NVIM_DOCKER_STATE_ROOT:-${HOME}/.local/share/nvim-docker-toolchain}"
TOKEN_FILE="${STATE_ROOT}/npm-token"
NPM_PREFIX=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix) NPM_PREFIX="${2:-}"; shift 2 ;;
    *) die "parametro invalido: $1" ;;
  esac
done

TOKEN="${NPM_PLUGINS_TOKEN:-}"
if [ -z "${TOKEN}" ] && [ -f "${TOKEN_FILE}" ]; then
  TOKEN="$(tr -d '[:space:]' < "${TOKEN_FILE}")"
fi
[ -n "${TOKEN}" ] || die "token ausente: grave-o em ${TOKEN_FILE} (chmod 600) ou exporte NPM_PLUGINS_TOKEN"

command -v npm >/dev/null 2>&1 || die "npm nao encontrado"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

{
  printf 'registry=https://registry.npmjs.org/\n'
  printf '//registry.npmjs.org/:_authToken=%s\n' "${TOKEN}"
  printf 'strict-ssl=false\n'
  if [ -n "${NPM_PREFIX}" ]; then
    printf 'prefix=%s\n' "${NPM_PREFIX}"
  fi
  if [ -f "${HOME}/.npmrc" ]; then
    grep -Ei '^(https-)?proxy[[:space:]]*=' "${HOME}/.npmrc" || true
  fi
} > "${TMP}/npmrc"

log "instalando ${PACKAGE_NAME}@latest via registry npm"
NPM_CONFIG_USERCONFIG="${TMP}/npmrc" npm install -g "${PACKAGE_NAME}@latest" \
  || die "npm install falhou — confira o escopo do token (${PACKAGE_NAME}) e a rota do proxy"

INSTALLED="$(NPM_CONFIG_USERCONFIG="${TMP}/npmrc" npm ls -g "${PACKAGE_NAME}" --depth=0 2>/dev/null | grep -o "${PACKAGE_NAME}@[0-9.]*" | head -1)"
log "instalado: ${INSTALLED:-${PACKAGE_NAME}}"
log "CLI disponível: brainlink --help (MCP: brainlink-mcp)"
