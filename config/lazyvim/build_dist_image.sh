#!/usr/bin/env bash
set -euo pipefail

# Builda e publica a imagem DIST do toolchain (plugins + LSPs já dentro) a
# partir de uma máquina com rede ABERTA. O PC corporativo só faz docker pull
# (via PREBUILT_IMAGE no setup_lazyvim_docker_toolchain.sh) — nenhum download
# de github acontece lá.
#
# Uso:
#   bash config/lazyvim/build_dist_image.sh [--image <ref>] [--platforms <lista>] [--load]
#
#   --image      Default: docker.io/andersonflima/nvim-toolchain:dist
#   --platforms  Default: linux/arm64,linux/amd64 (multi-arch via buildx)
#   --load       Builda só a arch local e carrega no docker local (sem push);
#                sem --load o resultado é enviado com push para o registry.
#   --public     NÃO usa token do gh: repos privados do manifesto (ex.:
#                pingu) ficam de fora e a imagem pode ser pública no
#                registry. Default quando o destino é o Docker Hub público.
#
# Sem --public, repos privados do manifesto entram na imagem quando houver
# token do gh disponível (gh auth token) — a imagem deve ficar PRIVADA no
# registry por causa disso.

log() {
  printf '[build-dist] %s\n' "$*" >&2
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE_REF="docker.io/andersonflima/nvim-toolchain:dist"
PLATFORMS="linux/arm64,linux/amd64"
LOAD=0
PUBLIC=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --image) IMAGE_REF="${2:-}"; shift 2 ;;
    --platforms) PLATFORMS="${2:-}"; shift 2 ;;
    --load) LOAD=1; shift ;;
    --public) PUBLIC=1; shift ;;
    *) log "parametro invalido: $1"; exit 1 ;;
  esac
done

command -v docker >/dev/null 2>&1 || { log "docker nao encontrado"; exit 1; }

CONTEXT_DIR="$(mktemp -d)"
trap 'rm -rf "${CONTEXT_DIR}"' EXIT
cp "${SCRIPT_DIR}/Dockerfile.nvim-toolchain" "${CONTEXT_DIR}/Dockerfile"
cp "${SCRIPT_DIR}/setup_lazyvim_mason_from_zip.sh" "${CONTEXT_DIR}/setup_lazyvim_mason_from_zip.sh"

# Token do gh para bakear repos privados do manifesto (opcional).
GH_TOKEN_FILE=""
if [ "${PUBLIC}" = "1" ]; then
  log "modo --public: repos privados do manifesto ficam de fora (imagem pode ser pública)"
elif command -v gh >/dev/null 2>&1 && gh auth token >/dev/null 2>&1; then
  GH_TOKEN_FILE="${CONTEXT_DIR}/.gh_token"
  gh auth token > "${GH_TOKEN_FILE}"
  chmod 600 "${GH_TOKEN_FILE}"
  log "token do gh disponível: repos privados do manifesto entram na imagem"
else
  log "AVISO: sem token do gh — repos privados (ex.: pingu) ficam de fora"
fi

set -- --build-arg BAKE_PLUGINS=1 -t "${IMAGE_REF}" -f "${CONTEXT_DIR}/Dockerfile"
if [ -n "${GH_TOKEN_FILE}" ]; then
  set -- "$@" --secret "id=gh_token,src=${GH_TOKEN_FILE}"
fi

if [ "${LOAD}" = "1" ]; then
  log "build local (--load, arch nativa) de ${IMAGE_REF}"
  DOCKER_BUILDKIT=1 docker buildx build --load "$@" "${CONTEXT_DIR}"
else
  log "build multi-arch (${PLATFORMS}) + push de ${IMAGE_REF}"
  DOCKER_BUILDKIT=1 docker buildx build --platform "${PLATFORMS}" --push "$@" "${CONTEXT_DIR}"
fi

log "pronto: ${IMAGE_REF}"
log "confirme a visibilidade PRIVADA do repositório no registry (imagem contém código de repos privados quando bakeada com token)"
