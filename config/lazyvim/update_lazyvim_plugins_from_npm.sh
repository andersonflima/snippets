#!/usr/bin/env bash
set -euo pipefail

# UPDATE dos plugins do LazyVim na máquina corporativa, SEM github e SEM
# imagem nova: baixa o pacote privado @andespindola/nvim-plugins-dist do
# registry npm (rota que passa no proxy corporativo — o npmrc local já tem o
# proxy) e substitui os plugins em ~/.local/share/nvim/lazy.
#
# Uso:
#   bash config/lazyvim/update_lazyvim_plugins_from_npm.sh
#   (ou, com o toolchain instalado: nvim-plugins-update / :PluginsUpdate)
#
# Token de leitura (uma vez): gravar em
#   ~/.local/share/nvim-docker-toolchain/npm-token   (chmod 600)
# ou exportar NPM_PLUGINS_TOKEN no ambiente.

log() {
  printf '[plugins-update] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

PACKAGE_NAME="@andespindola/nvim-plugins-dist"
STATE_ROOT="${NVIM_DOCKER_STATE_ROOT:-${HOME}/.local/share/nvim-docker-toolchain}"
TOKEN_FILE="${STATE_ROOT}/npm-token"
# Honra XDG_DATA_HOME do host: o nvim lê stdpath("data") = $XDG_DATA_HOME/nvim.
LAZY_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/nvim/lazy"

TOKEN="${NPM_PLUGINS_TOKEN:-}"
if [ -z "${TOKEN}" ] && [ -f "${TOKEN_FILE}" ]; then
  TOKEN="$(tr -d '[:space:]' < "${TOKEN_FILE}")"
fi
[ -n "${TOKEN}" ] || die "token ausente: grave-o em ${TOKEN_FILE} (chmod 600) ou exporte NPM_PLUGINS_TOKEN"

command -v npm >/dev/null 2>&1 || die "npm nao encontrado"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# npmrc dedicado: registry oficial + token; proxy herdado do ~/.npmrc do
# usuário (rota corporativa provada) e strict-ssl desligado (TLS interceptado
# pelo proxy). Não toca no ~/.npmrc real.
{
  printf 'registry=https://registry.npmjs.org/\n'
  printf '//registry.npmjs.org/:_authToken=%s\n' "${TOKEN}"
  printf 'strict-ssl=false\n'
  if [ -f "${HOME}/.npmrc" ]; then
    grep -Ei '^(https-)?proxy[[:space:]]*=' "${HOME}/.npmrc" || true
  fi
} > "${TMP}/npmrc"

log "baixando ${PACKAGE_NAME}@latest do registry npm"
NPM_CONFIG_USERCONFIG="${TMP}/npmrc" npm pack "${PACKAGE_NAME}@latest" \
  --pack-destination "${TMP}" >/dev/null \
  || die "npm pack falhou — confira o token (escopo do pacote) e a rota do proxy"

TARBALL="$(ls "${TMP}"/*.tgz 2>/dev/null | head -1)"
[ -n "${TARBALL}" ] || die "tarball nao encontrado apos npm pack"

tar -xzf "${TARBALL}" -C "${TMP}"
[ -d "${TMP}/package/lazy" ] || die "pacote sem diretorio lazy/"

mkdir -p "${LAZY_DIR}"
updated=0
for plugin_path in "${TMP}/package/lazy"/*; do
  plugin_name="$(basename "${plugin_path}")"
  rm -rf "${LAZY_DIR:?}/${plugin_name}"
  cp -a "${plugin_path}" "${LAZY_DIR}/${plugin_name}"
  updated=$((updated + 1))
done

version="$(python3 -c "import json;print(json.load(open('${TMP}/package/package.json'))['version'])" 2>/dev/null || true)"
log "plugins atualizados: ${updated} em ${LAZY_DIR} (versao ${version:-?})"
log "reabra o nvim para carregar as versoes novas"
