#!/usr/bin/env bash
set -euo pipefail

# Sobe o MCP do Tesus em CONTAINER na máquina corporativa, mantendo o vault
# LOCAL da máquina: ~/.brainlink do host é montado no container (mesmo path),
# então índice, notas e config continuam vivendo no filesystem local — o
# container só fornece o runtime (node + @andespindola/brainlink privado).
#
# Uso:
#   bash config/tesus/setup_tesus_mcp_docker.sh
#
# Pré-requisito: token npm de leitura em
#   ~/.local/share/nvim-docker-toolchain/npm-token  (o mesmo dos plugins).
#
# O que faz:
#   1. builda a imagem (npm install do brainlink com o token via secret);
#   2. sobe o container persistente `tesus-mcp` com ~/.brainlink montado;
#   3. gera o wrapper `tesus-mcp-docker` (stdio via docker exec) no PATH do
#      toolchain — é ele que entra na config de MCP dos clientes.
#
# Registro nos clientes MCP (exemplos):
#   claude mcp add tesus -s user -- ~/.local/share/nvim-docker-toolchain/bin/tesus-mcp-docker
#   (ou aponte o comando do server MCP para o wrapper no cliente que usar)

log() {
  printf '[tesus-mcp-docker] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Nome com sufixo -toolchain: "tesus-mcp" seco pode colidir com um container
# de Tesus SaaS local já existente na máquina (e o setup faz rm -f do nome).
IMAGE_NAME="snippets/tesus-mcp-toolchain:latest"
CONTAINER_NAME="${TESUS_MCP_CONTAINER:-tesus-mcp-toolchain}"
STATE_ROOT="${NVIM_DOCKER_STATE_ROOT:-${HOME}/.local/share/nvim-docker-toolchain}"
TOKEN_FILE="${STATE_ROOT}/npm-token"
WRAPPER_DIR="${STATE_ROOT}/bin"
WRAPPER="${WRAPPER_DIR}/tesus-mcp-docker"

command -v docker >/dev/null 2>&1 || die "docker nao encontrado"
[ -f "${TOKEN_FILE}" ] || die "token ausente em ${TOKEN_FILE} (grave o token npm de leitura primeiro)"

CONTEXT_DIR="$(mktemp -d)"
trap 'rm -rf "${CONTEXT_DIR}"' EXIT
cp "${SCRIPT_DIR}/Dockerfile.tesus-mcp" "${CONTEXT_DIR}/Dockerfile"

# Proxy corporativo para o build (mesma derivação do toolchain: ~/.npmrc).
if [ -z "${HTTP_PROXY:-}" ] && [ -f "${HOME}/.npmrc" ]; then
  NPMRC_PROXY=$(grep -Ei '^(https-)?proxy[[:space:]]*=' "${HOME}/.npmrc" | head -1 | cut -d= -f2- | tr -d ' \r' || true)
  if [ -n "${NPMRC_PROXY}" ]; then
    export HTTP_PROXY="${NPMRC_PROXY}" HTTPS_PROXY="${NPMRC_PROXY}" NO_PROXY="${NO_PROXY:-localhost,127.0.0.1}"
    log "proxy derivado do ~/.npmrc para o build: ${NPMRC_PROXY}"
  fi
fi

set -- --secret "id=npm_token,src=${TOKEN_FILE}"
if [ -n "${HTTP_PROXY:-}" ]; then
  set -- "$@" --build-arg "HTTP_PROXY=${HTTP_PROXY}" --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-${HTTP_PROXY}}" --build-arg "NO_PROXY=${NO_PROXY:-localhost,127.0.0.1}"
fi
# TLS interceptado pelo proxy: NPM_INSECURE=1 desliga o strict-ssl do build.
if [ "${NPM_INSECURE:-0}" = "1" ]; then
  set -- "$@" --build-arg "NPM_CONFIG_STRICT_SSL=false"
  log "npm do build com strict-ssl desligado (proxy MITM)"
fi

log "buildando ${IMAGE_NAME}"
DOCKER_BUILDKIT=1 docker build "$@" -t "${IMAGE_NAME}" "${CONTEXT_DIR}" >/dev/null
log "imagem pronta: ${IMAGE_NAME}"

if docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

mkdir -p "${HOME}/.brainlink"
# Vault LOCAL: ~/.brainlink do host montado no MESMO path; HOME apontado para
# o home real, então config/defaultVault resolvem igual dentro e fora.
docker run -d \
  --name "${CONTAINER_NAME}" \
  -v "${HOME}/.brainlink:${HOME}/.brainlink" \
  -e HOME="${HOME}" \
  -e USER="${USER:-user}" \
  -w "${HOME}" \
  "${IMAGE_NAME}" >/dev/null
log "container ${CONTAINER_NAME} no ar (vault local montado de ${HOME}/.brainlink)"

mkdir -p "${WRAPPER_DIR}"
cat > "${WRAPPER}" <<'EOF'
#!/usr/bin/env bash
# MCP do Tesus por stdio via container: cada sessão MCP é um docker exec -i.
set -euo pipefail
container_name="${TESUS_MCP_CONTAINER:-tesus-mcp-toolchain}"
if ! docker inspect "${container_name}" >/dev/null 2>&1; then
  echo "[tesus-mcp-docker] container ${container_name} nao existe; rode setup_tesus_mcp_docker.sh" >&2
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Running}}' "${container_name}")" != "true" ]; then
  docker start "${container_name}" >/dev/null
fi
exec docker exec -i -e HOME="${HOME}" "${container_name}" brainlink-mcp "$@"
EOF
chmod +x "${WRAPPER}"
log "wrapper pronto: ${WRAPPER}"

log "smoke test do CLI dentro do container:"
docker exec -e HOME="${HOME}" "${CONTAINER_NAME}" brainlink --version 2>&1 | head -1 >&2 || true
log "registre no cliente MCP: claude mcp add tesus -s user -- ${WRAPPER}"
