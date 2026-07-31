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
    set -- "$@" --secret "id=npmrc,src=${HOME}/.npmrc"
    log "usando ~/.npmrc do host como secret de build (proxy/registry)"
  fi
  # pip.conf do host (index/proxy corporativo). Ordem: PIP_CONFIG_FILE
  # explicito > XDG > macOS (Application Support) > legado ~/.pip. Sem o
  # arquivo, avisa em vez de silenciar: no proxy corporativo o pip do build
  # quebra com erro de SSL quando builda sem essa config.
  PIP_CONF_SRC=""
  for candidate in \
    "${PIP_CONFIG_FILE:-}" \
    "${XDG_CONFIG_HOME:-${HOME}/.config}/pip/pip.conf" \
    "${HOME}/Library/Application Support/pip/pip.conf" \
    "${HOME}/.pip/pip.conf"; do
    if [ -n "${candidate}" ] && [ -f "${candidate}" ]; then
      PIP_CONF_SRC="${candidate}"
      break
    fi
  done
  if [ -n "${PIP_CONF_SRC}" ]; then
    set -- "$@" --secret "id=pip_conf,src=${PIP_CONF_SRC}"
    log "usando ${PIP_CONF_SRC} como secret de build (pip index/proxy)"
  else
    log "AVISO: nenhum pip.conf encontrado (PIP_CONFIG_FILE, ~/.config/pip, ~/Library/Application Support/pip, ~/.pip) — em rede com proxy o pip do build pode falhar com erro de SSL"
  fi
  # SSL do pip atrás de proxy MITM. PIP_TRUSTED_HOST exportado tem prioridade;
  # sem ele, deriva os hosts do PRÓPRIO pip.conf (index-url/extra-index-url/
  # trusted-host) — cobre índice interno (Artifactory/Nexus) sem configuração
  # manual: o host do índice da empresa entra sozinho na lista.
  if [ -z "${PIP_TRUSTED_HOST:-}" ] && [ -n "${PIP_CONF_SRC}" ]; then
    PIP_TRUSTED_HOST=$(awk -F'=' '
      tolower($1) ~ /^[[:space:]]*(extra-)?index-url[[:space:]]*$/ {
        url=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", url)
        sub(/^https?:\/\//, "", url); sub(/\/.*$/, "", url); sub(/^.*@/, "", url)
        if (url != "") hosts[url]=1
      }
      tolower($1) ~ /^[[:space:]]*trusted-host[[:space:]]*$/ {
        h=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", h)
        if (h != "") hosts[h]=1
      }
      END { sep=""; for (h in hosts) { printf "%s%s", sep, h; sep=" " } }
    ' "${PIP_CONF_SRC}")
    if [ -n "${PIP_TRUSTED_HOST}" ]; then
      log "trusted-host derivado do pip.conf: ${PIP_TRUSTED_HOST}"
    fi
  fi
  if [ -n "${PIP_TRUSTED_HOST:-}" ]; then
    set -- "$@" --build-arg "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}"
    log "pip com trusted-host: ${PIP_TRUSTED_HOST} (verificação TLS desativada só nesses hosts)"
  fi
  # cert= no pip.conf apontando arquivo do HOST não existe dentro do build.
  if [ -n "${PIP_CONF_SRC}" ] && grep -qiE '^[[:space:]]*cert[[:space:]]*=' "${PIP_CONF_SRC}"; then
    log "AVISO: seu pip.conf tem 'cert = ...' com caminho do host — esse arquivo não existe dentro do container e pode causar erro de SSL/arquivo; remova a linha ou conte com o trusted-host"
  fi
  # Proxy para o apt/curl do build: sem HTTP_PROXY exportado, deriva do
  # ~/.npmrc (linhas proxy=/https-proxy=) — npm/pip funcionam via configs
  # próprias, mas o apt vai direto e leva "repository is not signed" quando a
  # rede força passagem pelo proxy.
  if [ -z "${HTTP_PROXY:-}" ] && [ -f "${HOME}/.npmrc" ]; then
    NPMRC_PROXY=$(grep -Ei '^(https-)?proxy[[:space:]]*=' "${HOME}/.npmrc" | head -1 | cut -d= -f2- | tr -d ' \r')
    if [ -n "${NPMRC_PROXY}" ]; then
      export HTTP_PROXY="${NPMRC_PROXY}" HTTPS_PROXY="${NPMRC_PROXY}" NO_PROXY="${NO_PROXY:-localhost,127.0.0.1}"
      log "proxy derivado do ~/.npmrc para o build: ${NPMRC_PROXY}"
    fi
  fi
  if [ -n "${HTTP_PROXY:-}" ]; then
    set -- "$@" --build-arg "HTTP_PROXY=${HTTP_PROXY}" --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-${HTTP_PROXY}}" --build-arg "NO_PROXY=${NO_PROXY:-localhost,127.0.0.1}"
  fi
  # Índice apt corrompido mesmo via proxy: APT_INSECURE=1 libera repo sem
  # assinatura (só rede corporativa).
  if [ "${APT_INSECURE:-0}" = "1" ]; then
    set -- "$@" --build-arg "APT_INSECURE=1"
    log "apt em modo inseguro (repositório sem assinatura aceito) — use só atrás do proxy corporativo"
  fi
  # CA corporativa (conserta TLS de go/curl/npm de forma definitiva):
  # exportar CORP_CA_FILE=/caminho/da/ca.pem antes de rodar o setup.
  if [ -n "${CORP_CA_FILE:-}" ] && [ -f "${CORP_CA_FILE}" ]; then
    set -- "$@" --secret "id=corp_ca,src=${CORP_CA_FILE}"
    log "instalando CA corporativa no trust store da imagem: ${CORP_CA_FILE}"
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
  # Proxy no exec é OPT-IN (CONTAINER_PROXY=1): proxies corporativos têm
  # allowlist por destino — o endpoint do npm (derivado do ~/.npmrc) devolve
  # 403 para github.com, sequestrando a rota do git, que já funciona pela
  # config do próprio ~/.gitconfig do host (montado). GIT_INSECURE=1 segue
  # exportando GIT_SSL_NO_VERIFY. Valores sem espaço → ${VAR:+...} sem aspas
  # é seguro, inclusive no bash 3.2.
  EXEC_HTTP_PROXY=""
  EXEC_HTTPS_PROXY=""
  EXEC_NO_PROXY=""
  if [ "${CONTAINER_PROXY:-0}" = "1" ]; then
    EXEC_HTTP_PROXY="${HTTP_PROXY:-}"
    EXEC_HTTPS_PROXY="${HTTPS_PROXY:-${HTTP_PROXY:-}}"
    EXEC_NO_PROXY="${NO_PROXY:-localhost,127.0.0.1}"
  fi
  # Git DENTRO do container usa o ~/.gitconfig do host (HOME montado) — é
  # nele que vivem proxy/ssl que fazem o git funcionar na rede corporativa.
  # As entradas GIT_CONFIG_* abaixo entram por cima só como rede de
  # segurança: URLs ssh do GitHub viram https (rede bloqueia SSH).
  GIT_INSECURE_OPT=""
  if [ "${GIT_INSECURE:-0}" = "1" ]; then
    GIT_INSECURE_OPT="-e GIT_SSL_NO_VERIFY=1"
  fi
  docker exec \
    -i \
    -w "${PWD}" \
    -e HOME="${HOME}" \
    -e USER="${USER:-user}" \
    -e XDG_CONFIG_HOME="${XDG_CONFIG_HOME}" \
    -e XDG_DATA_HOME="${XDG_DATA_HOME}" \
    -e XDG_STATE_HOME="${XDG_STATE_HOME}" \
    -e XDG_CACHE_HOME="${XDG_CACHE_HOME}" \
    ${EXEC_HTTP_PROXY:+-e HTTP_PROXY=${EXEC_HTTP_PROXY} -e http_proxy=${EXEC_HTTP_PROXY}} \
    ${EXEC_HTTPS_PROXY:+-e HTTPS_PROXY=${EXEC_HTTPS_PROXY} -e https_proxy=${EXEC_HTTPS_PROXY}} \
    ${EXEC_NO_PROXY:+-e NO_PROXY=${EXEC_NO_PROXY} -e no_proxy=${EXEC_NO_PROXY}} \
    ${GIT_INSECURE_OPT} \
    -e GIT_CONFIG_COUNT=2 \
    -e GIT_CONFIG_KEY_0=url.https://github.com/.insteadOf -e GIT_CONFIG_VALUE_0=git@github.com: \
    -e GIT_CONFIG_KEY_1=url.https://github.com/.insteadOf -e GIT_CONFIG_VALUE_1=ssh://git@github.com/ \
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

install_plugins_from_zip() {
  # Rede que bloqueia git/curl para github: PLUGINS_FROM_ZIP=1 usa o fluxo ZIP
  # já existente (Python/urllib com proxy, sem curl) para instalar lazy.nvim +
  # todos os plugins do manifesto direto no XDG que o container enxerga.
  log "instalando lazy.nvim + plugins por ZIP (fluxo setup_lazyvim_mason_from_zip)"
  # Falha parcial não aborta: plugin privado/bloqueado (ex.: repo pessoal sem
  # auth no zip) fica de fora e o restante segue utilizável; instale o faltante
  # manualmente (zip no browser → extrair em ${XDG_DATA_HOME}/nvim/lazy/<nome>).
  if ! bash "${SCRIPT_DIR}/setup_lazyvim_mason_from_zip.sh" \
    --plugins-only \
    --data-dir "${XDG_DATA_HOME}/nvim" \
    ${GITHUB_BASE:+--github-base "${GITHUB_BASE}"}; then
    log "AVISO: alguns plugins falharam no ZIP (veja o log acima) — os demais foram instalados"
  fi
}

ensure_lazy_nvim() {
  # O +Lazy! sync pressupõe o lazy.nvim já presente; o auto-bootstrap da config
  # clonaria do GitHub DENTRO do container (TLS barrado pelo proxy corporativo).
  # Clona pelo HOST — onde o git já funciona — direto no XDG que o container
  # enxerga.
  local lazy_dir="${XDG_DATA_HOME}/nvim/lazy/lazy.nvim"
  if [ -d "${lazy_dir}/.git" ]; then
    return 0
  fi
  log "clonando lazy.nvim (host) em ${lazy_dir}"
  mkdir -p "$(dirname "${lazy_dir}")"
  git clone --filter=blob:none --branch=stable https://github.com/folke/lazy.nvim "${lazy_dir}" \
    || die "clone do lazy.nvim falhou no host; verifique o acesso git a github.com"
}

bootstrap_container_toolchain() {
  if [ "$SKIP_BOOTSTRAP" = "1" ]; then
    return 0
  fi
  if [ "${PLUGINS_FROM_ZIP:-0}" = "1" ]; then
    log "plugins instalados por ZIP: pulando Lazy! sync (sem rede no bootstrap)"
    return 0
  fi

  local mason_packages=(
    lua-language-server
    omnisharp
    elixir-ls
    rust-analyzer
  )
  local package_name
  # Expansão segura de array vazio sob set -u no bash 3.2 do macOS (o
  # "${arr[@]}" puro estoura "unbound variable" quando o array está vazio).
  for package_name in ${EXTRA_MASON_PACKAGES[@]+"${EXTRA_MASON_PACKAGES[@]}"}; do
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
if [ "${PLUGINS_FROM_ZIP:-0}" = "1" ]; then
  install_plugins_from_zip
else
  ensure_lazy_nvim
fi
write_env_file
write_wrapper_driver
link_wrappers
bootstrap_container_toolchain

log "toolchain pronta"
log "source ${ENV_FILE}"
log "wrappers em ${WRAPPER_DIR}"
