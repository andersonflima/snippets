#!/usr/bin/env bash
set -euo pipefail

# Publica o pacote npm PRIVADO com todos os plugins do manifesto — o canal de
# UPDATE dos plugins na máquina corporativa (o registry npm passa pelo proxy
# de lá; github não). Rodar numa máquina com rede aberta.
#
# Uso:
#   bash config/lazyvim/publish_nvim_plugins_npm.sh [--version <semver>] [--dry-run]
#
# - Clona cada plugin do manifesto (depth 1, sem .git no resultado); repos
#   privados (pingu) entram via credencial local do gh — o pacote é PRIVADO
#   (publish --access restricted), então pode conter esse código.
# - Versão default: 0.<YYYYMMDD>.<HHMM> (sempre crescente).
# - Consumo na máquina corporativa: update_lazyvim_plugins_from_npm.sh.

log() {
  printf '[publish-plugins] %s\n' "$*" >&2
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_NAME="@andespindola/nvim-plugins-dist"
VERSION="0.$(date +%Y%m%d).$(date +%H%M | sed 's/^0*//;s/^$/0/')"
DRY_RUN=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) log "parametro invalido: $1"; exit 1 ;;
  esac
done

command -v git >/dev/null 2>&1 || { log "git nao encontrado"; exit 1; }
command -v npm >/dev/null 2>&1 || { log "npm nao encontrado"; exit 1; }

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT
mkdir -p "${STAGE}/lazy"

total=0
ok=0
failed=""
while IFS='|' read -r name repo branch; do
  [ -n "${name}" ] && [ -n "${repo}" ] || continue
  total=$((total + 1))
  clone_args="--depth 1"
  if [ -n "${branch}" ] && [ "${branch}" != "default" ]; then
    clone_args="${clone_args} --branch ${branch}"
  fi
  if git clone ${clone_args} "https://github.com/${repo}" "${STAGE}/lazy/${name}" >/dev/null 2>&1; then
    rm -rf "${STAGE}/lazy/${name}/.git"
    ok=$((ok + 1))
  else
    failed="${failed} ${name}"
  fi
done < <(bash "${SCRIPT_DIR}/setup_lazyvim_mason_from_zip.sh" --print-manifest)

log "plugins empacotados: ${ok}/${total}${failed:+ (falhas:${failed})}"
[ "${ok}" -gt 0 ] || { log "nenhum plugin clonado; abortando"; exit 1; }

cat > "${STAGE}/package.json" <<EOF
{
  "name": "${PACKAGE_NAME}",
  "version": "${VERSION}",
  "description": "Distribuicao privada dos plugins do toolchain LazyVim (uso pessoal)",
  "license": "UNLICENSED",
  "publishConfig": { "access": "restricted" }
}
EOF

if [ "${DRY_RUN}" = "1" ]; then
  log "dry-run: ${PACKAGE_NAME}@${VERSION} pronto em ${STAGE} (sem publish)"
  npm pack --dry-run --prefix "${STAGE}" >/dev/null 2>&1 || true
  exit 0
fi

( cd "${STAGE}" && npm publish --access restricted >/dev/null )
log "publicado: ${PACKAGE_NAME}@${VERSION} (restricted)"

STATUS="$(npm access get status "${PACKAGE_NAME}" 2>/dev/null | tr -d '[:space:]')"
log "visibilidade pos-publish: ${STATUS:-desconhecida}"
case "${STATUS}" in
  *private*) ;;
  *) log "ATENCAO: pacote nao esta private — corrigir com: npm access set status=private ${PACKAGE_NAME}" ;;
esac
