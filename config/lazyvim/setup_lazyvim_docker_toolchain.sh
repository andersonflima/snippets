#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[setup-lazyvim-docker] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash config/lazyvim/setup_lazyvim_docker_toolchain.sh [opcoes]

Opcoes:
  --container-name <nome>   Default: nvim-toolchain
  --image-name <nome>       Default: snippets/nvim-toolchain:latest
  --wrapper-dir <dir>       Default: ~/.local/share/nvim-docker-toolchain/bin
  --state-root <dir>        Default: ~/.local/share/nvim-docker-toolchain
  --config-source-dir <dir> Default: config/nvim do repo snippets
  --host-config-dir <dir>   Default: ~/.config/nvim
  --skip-config             Nao copia a config do Neovim para o host
  --skip-bootstrap          Nao roda sync headless dentro do container
  --mason-package <nome>    Adiciona pacote extra ao bootstrap (repetivel)
  -h, --help                Mostra ajuda

O que o script faz:
  1. builda a imagem Docker do toolchain;
  2. sobe um container persistente com o HOME montado;
  3. prepara um XDG isolado em ~/.local/share/nvim-docker-toolchain/xdg-*;
  4. opcionalmente copia a config versionada do Neovim para ~/.config/nvim;
  5. gera wrappers locais que fazem docker exec nas ferramentas;
  6. roda bootstrap headless do Lazy/LSP/tooling dentro do container.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_CONFIG_SOURCE_DIR="${CONFIG_ROOT}/nvim"

CONTAINER_NAME="nvim-toolchain"
IMAGE_NAME="snippets/nvim-toolchain:latest"
STATE_ROOT="${HOME}/.local/share/nvim-docker-toolchain"
WRAPPER_DIR="${STATE_ROOT}/bin"
CONFIG_SOURCE_DIR="${DEFAULT_CONFIG_SOURCE_DIR}"
HOST_CONFIG_DIR="${HOME}/.config/nvim"
SKIP_CONFIG=0
SKIP_BOOTSTRAP=0
EXTRA_MASON_PACKAGES=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --container-name)
      CONTAINER_NAME="${2:-}"
      shift 2
      ;;
    --image-name)
      IMAGE_NAME="${2:-}"
      shift 2
      ;;
    --wrapper-dir)
      WRAPPER_DIR="${2:-}"
      shift 2
      ;;
    --state-root)
      STATE_ROOT="${2:-}"
      shift 2
      ;;
    --config-source-dir)
      CONFIG_SOURCE_DIR="${2:-}"
      shift 2
      ;;
    --host-config-dir)
      HOST_CONFIG_DIR="${2:-}"
      shift 2
      ;;
    --skip-config)
      SKIP_CONFIG=1
      shift
      ;;
    --skip-bootstrap)
      SKIP_BOOTSTRAP=1
      shift
      ;;
    --mason-package)
      EXTRA_MASON_PACKAGES+=("${2:-}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parametro invalido: $1"
      ;;
  esac
done

command -v docker >/dev/null 2>&1 || die "docker nao encontrado"

[ -n "$CONTAINER_NAME" ] || die "--container-name vazio"
[ -n "$IMAGE_NAME" ] || die "--image-name vazio"
[ -n "$STATE_ROOT" ] || die "--state-root vazio"
[ -n "$WRAPPER_DIR" ] || die "--wrapper-dir vazio"
[ -d "$CONFIG_SOURCE_DIR" ] || die "config source nao encontrado: $CONFIG_SOURCE_DIR"

XDG_ROOT="${STATE_ROOT}/xdg"
XDG_CONFIG_HOME="${XDG_ROOT}/config"
XDG_DATA_HOME="${XDG_ROOT}/data"
XDG_STATE_HOME="${XDG_ROOT}/state"
XDG_CACHE_HOME="${XDG_ROOT}/cache"
CONTAINER_NVIM_CONFIG_DIR="${XDG_CONFIG_HOME}/nvim"
CONTAINER_NVIM_DATA_DIR="${XDG_DATA_HOME}/nvim"
ENV_FILE="${STATE_ROOT}/env.sh"
WRAPPER_DRIVER="${STATE_ROOT}/wrapper-driver.sh"
DOCKER_CONTEXT_DIR="${STATE_ROOT}/docker-context"

mkdir -p \
  "${WRAPPER_DIR}" \
  "${XDG_CONFIG_HOME}" \
  "${XDG_DATA_HOME}" \
  "${XDG_STATE_HOME}" \
  "${XDG_CACHE_HOME}" \
  "${DOCKER_CONTEXT_DIR}"

copy_config_to_host() {
  if [ "$SKIP_CONFIG" = "1" ]; then
    return 0
  fi

  rm -rf "${HOST_CONFIG_DIR}"
  mkdir -p "$(dirname "${HOST_CONFIG_DIR}")"
  cp -R "${CONFIG_SOURCE_DIR}" "${HOST_CONFIG_DIR}"
  log "config do Neovim copiada para ${HOST_CONFIG_DIR}"
}

copy_config_to_container_xdg() {
  rm -rf "${CONTAINER_NVIM_CONFIG_DIR}"
  mkdir -p "$(dirname "${CONTAINER_NVIM_CONFIG_DIR}")"
  cp -R "${CONFIG_SOURCE_DIR}" "${CONTAINER_NVIM_CONFIG_DIR}"
}

build_image() {
  cp "${SCRIPT_DIR}/Dockerfile.nvim-toolchain" "${DOCKER_CONTEXT_DIR}/Dockerfile"
  # ~/.npmrc do host (proxy/registry corporativo) vai como secret de build:
  # o npm do RUN enxerga o arquivo, mas ele nao persiste na imagem final.
  set -- 
  if [ -f "${HOME}/.npmrc" ]; then
    set -- --secret "id=npmrc,src=${HOME}/.npmrc"
    log "usando ~/.npmrc do host como secret de build (proxy/registry)"
  fi
  DOCKER_BUILDKIT=1 docker build "$@" -t "${IMAGE_NAME}" "${DOCKER_CONTEXT_DIR}" >/dev/null
  log "imagem atualizada: ${IMAGE_NAME}"
}

ensure_container_running() {
  if docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null
  fi

  docker run -d \
    --name "${CONTAINER_NAME}" \
    -v "${HOME}:${HOME}" \
    -w "${HOME}" \
    "${IMAGE_NAME}" >/dev/null
}

container_exec() {
  docker exec \
    -i \
    -w "${PWD}" \
    -e HOME="${HOME}" \
    -e USER="${USER:-user}" \
    -e XDG_CONFIG_HOME="${XDG_CONFIG_HOME}" \
    -e XDG_DATA_HOME="${XDG_DATA_HOME}" \
    -e XDG_STATE_HOME="${XDG_STATE_HOME}" \
    -e XDG_CACHE_HOME="${XDG_CACHE_HOME}" \
    "${CONTAINER_NAME}" \
    "$@"
}

write_env_file() {
  cat > "${ENV_FILE}" <<EOF
export NVIM_DOCKER_CONTAINER_NAME="${CONTAINER_NAME}"
export NVIM_DOCKER_IMAGE_NAME="${IMAGE_NAME}"
export NVIM_DOCKER_STATE_ROOT="${STATE_ROOT}"
export NVIM_DOCKER_WRAPPER_BIN="${WRAPPER_DIR}"
export PATH="${WRAPPER_DIR}:\$PATH"
EOF
}

write_wrapper_driver() {
  cat > "${WRAPPER_DRIVER}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

cmd_name="$(basename "$0")"
container_name="${NVIM_DOCKER_CONTAINER_NAME:-nvim-toolchain}"
state_root="${NVIM_DOCKER_STATE_ROOT:-$HOME/.local/share/nvim-docker-toolchain}"
xdg_config_home="${state_root}/xdg/config"
xdg_data_home="${state_root}/xdg/data"
xdg_state_home="${state_root}/xdg/state"
xdg_cache_home="${state_root}/xdg/cache"
mason_bin_dir="${xdg_data_home}/nvim/mason/bin"

if ! command -v docker >/dev/null 2>&1; then
  echo "[nvim-docker-wrapper] docker nao encontrado" >&2
  exit 127
fi

if ! docker inspect "${container_name}" >/dev/null 2>&1; then
  echo "[nvim-docker-wrapper] container ${container_name} nao existe; rode setup_lazyvim_docker_toolchain.sh" >&2
  exit 1
fi

if [ "$(docker inspect -f '{{.State.Running}}' "${container_name}")" != "true" ]; then
  docker start "${container_name}" >/dev/null
fi

exec docker exec \
  -i \
  -w "$PWD" \
  -e HOME="$HOME" \
  -e USER="${USER:-user}" \
  -e XDG_CONFIG_HOME="${xdg_config_home}" \
  -e XDG_DATA_HOME="${xdg_data_home}" \
  -e XDG_STATE_HOME="${xdg_state_home}" \
  -e XDG_CACHE_HOME="${xdg_cache_home}" \
  "${container_name}" \
  bash -lc '
    set -euo pipefail
    cmd_name="$1"
    shift
    mason_bin_dir="$XDG_DATA_HOME/nvim/mason/bin"
    if [ -x "${mason_bin_dir}/${cmd_name}" ]; then
      exec "${mason_bin_dir}/${cmd_name}" "$@"
    fi
    exec "${cmd_name}" "$@"
  ' -- "${cmd_name}" "$@"
EOF
  chmod +x "${WRAPPER_DRIVER}"
}

link_wrappers() {
  local commands=(
    bash-language-server
    black
    elixir-ls
    eslint_d
    gopls
    lua-language-server
    luacheck
    omnisharp
    prettier
    prettierd
    pyright
    pyright-langserver
    ruff
    rust-analyzer
    shellcheck
    shfmt
    stylua
    tailwindcss-language-server
    typescript-language-server
    vscode-css-language-server
    vscode-eslint-language-server
    vscode-html-language-server
    vscode-json-language-server
    yaml-language-server
  )

  ln -sf "${WRAPPER_DRIVER}" "${WRAPPER_DIR}/nvim-docker-exec"
  for command_name in "${commands[@]}"; do
    ln -sf "${WRAPPER_DRIVER}" "${WRAPPER_DIR}/${command_name}"
  done
}

bootstrap_container_toolchain() {
  if [ "$SKIP_BOOTSTRAP" = "1" ]; then
    return 0
  fi

  local mason_packages=(
    lua-language-server
    omnisharp
    elixir-ls
    rust-analyzer
  )
  local package_name
  for package_name in "${EXTRA_MASON_PACKAGES[@]}"; do
    [ -n "${package_name}" ] || continue
    mason_packages+=("${package_name}")
  done

  local package_csv=""
  local index=0
  for package_name in "${mason_packages[@]}"; do
    if [ "$index" -gt 0 ]; then
      package_csv="${package_csv},"
    fi
    package_csv="${package_csv}${package_name}"
    index=$((index + 1))
  done

  container_exec bash -lc "
    set -euo pipefail
    mkdir -p '${CONTAINER_NVIM_CONFIG_DIR}' '${CONTAINER_NVIM_DATA_DIR}' '${XDG_STATE_HOME}' '${XDG_CACHE_HOME}'
    nvim --headless '+Lazy! sync' +qa
    if [ -n '${package_csv}' ]; then
      nvim --headless \"+MasonInstall ${package_csv}\" +qa || true
    fi
  "
}

copy_config_to_host
copy_config_to_container_xdg
build_image
ensure_container_running
write_env_file
write_wrapper_driver
link_wrappers
bootstrap_container_toolchain

log "toolchain pronta"
log "source ${ENV_FILE}"
log "wrappers em ${WRAPPER_DIR}"
