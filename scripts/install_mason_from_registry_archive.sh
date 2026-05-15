#!/usr/bin/env bash
set -euo pipefail

readonly REGISTRY_REPO="mason-org/mason-registry"
readonly REGISTRY_BRANCH="main"

DRY_RUN=0
NVIM_BIN="${NVIM_BIN:-nvim}"
NVIM_DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/nvim"
REGISTRY_DIR="${XDG_CACHE_HOME:-${HOME}/.cache}/nvim/mason-registry-main"
INSTALL_TIMEOUT_MS="${INSTALL_TIMEOUT_MS:-1800000}"
MAX_CONCURRENT_INSTALLERS="${MAX_CONCURRENT_INSTALLERS:-4}"
ONLY_PACKAGE=""
TMP_FILES=()

cleanup() {
  local file
  for file in "${TMP_FILES[@]:-}"; do
    [[ -n "${file}" ]] || continue
    rm -f "${file}" 2>/dev/null || true
  done
}

trap cleanup EXIT

log() {
  printf '[mason-registry-archive] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash scripts/install_mason_from_registry_archive.sh [opcoes]

Opcoes:
  --nvim <binario>       Binario do Neovim. Default: nvim
  --data-dir <dir>       Diretorio data do Neovim. Default: ${XDG_DATA_HOME:-$HOME/.local/share}/nvim
  --registry-dir <dir>   Destino local do registry baixado. Default: ${XDG_CACHE_HOME:-$HOME/.cache}/nvim/mason-registry-main
  --only <pacote>        Instala/atualiza apenas um pacote Mason.
  --timeout-ms <ms>      Timeout total do headless install. Default: 1800000
  --list                 Lista os pacotes gerenciados e sai.
  --dry-run              Mostra as acoes sem baixar nem instalar.
  -h, --help             Mostra esta ajuda.

Comportamento:
  - Nao usa git clone.
  - Baixa o mason-registry via archive da branch main no GitHub.
  - Usa registry local file:<registry-dir>, evitando o update remoto padrao.
  - Reinstala pacotes existentes para atualizar conforme o registry local.

Pre-requisitos no computador alvo:
  - Neovim disponivel no PATH.
  - Plugins lazy.nvim e mason.nvim instalados; rode install_lazyvim_archives.sh antes.
  - yq disponivel no PATH, exigido pelo registry local file: do mason.nvim.
  - Gerenciadores necessarios aos pacotes, como npm, python/pip, go, cargo, dotnet e unzip.
USAGE
}

packages_manifest() {
  cat <<'EOF'
bash-language-server
black
codelldb
css-lsp
delve
elixir-ls
eslint_d
eslint-lsp
gofumpt
goimports
golangci-lint
gopls
html-lsp
java-debug-adapter
java-test
jdtls
js-debug-adapter
json-lsp
lua-language-server
luacheck
markdown-toc
markdownlint-cli2
marksman
omnisharp
prettier
pyright
ruff
selene
shellcheck
shfmt
stylua
tailwindcss-language-server
tree-sitter-cli
typescript-language-server
vtsls
yaml-language-server
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nvim)
      NVIM_BIN="${2:-}"
      shift 2
      ;;
    --data-dir)
      NVIM_DATA_DIR="${2:-}"
      shift 2
      ;;
    --registry-dir)
      REGISTRY_DIR="${2:-}"
      shift 2
      ;;
    --only)
      ONLY_PACKAGE="${2:-}"
      shift 2
      ;;
    --timeout-ms)
      INSTALL_TIMEOUT_MS="${2:-}"
      shift 2
      ;;
    --list)
      packages_manifest
      exit 0
      ;;
    --dry-run)
      DRY_RUN=1
      shift
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

[[ -n "${NVIM_BIN}" ]] || die "--nvim nao pode ser vazio"
[[ -n "${NVIM_DATA_DIR}" ]] || die "--data-dir nao pode ser vazio"
[[ -n "${REGISTRY_DIR}" ]] || die "--registry-dir nao pode ser vazio"

download() {
  local url="$1"
  local output="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --connect-timeout 20 "${url}" -o "${output}"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -O "${output}" "${url}"
    return
  fi

  die "curl ou wget e obrigatorio"
}

build_package_list() {
  local matched=0
  local package_name

  while IFS= read -r package_name; do
    [[ -n "${package_name}" ]] || continue
    if [[ -n "${ONLY_PACKAGE}" && "${ONLY_PACKAGE}" != "${package_name}" ]]; then
      continue
    fi
    matched=1
    printf '%s,' "${package_name}"
  done < <(packages_manifest)

  if [[ -n "${ONLY_PACKAGE}" && "${matched}" != "1" ]]; then
    die "pacote nao encontrado no manifesto: ${ONLY_PACKAGE}"
  fi
}

install_registry_archive() {
  local archive_url="https://codeload.github.com/${REGISTRY_REPO}/tar.gz/refs/heads/${REGISTRY_BRANCH}"
  local tmp_dir archive_path extract_dir source_dir backup_dir timestamp

  log "baixando registry ${REGISTRY_REPO}#${REGISTRY_BRANCH}"

  if [[ "${DRY_RUN}" == "1" ]]; then
    printf '[dry-run] download %s -> %s\n' "${archive_url}" "${REGISTRY_DIR}" >&2
    return 0
  fi

  tmp_dir="$(mktemp -d)"
  archive_path="${tmp_dir}/mason-registry.tar.gz"
  extract_dir="${tmp_dir}/extract"
  timestamp="$(date +%Y%m%d_%H%M%S)"
  backup_dir="${REGISTRY_DIR}.archive-backup.${timestamp}"

  mkdir -p "${extract_dir}" "$(dirname "${REGISTRY_DIR}")"
  download "${archive_url}" "${archive_path}"
  tar -xzf "${archive_path}" -C "${extract_dir}"

  source_dir="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "${source_dir}" ]] || die "archive do registry sem diretorio raiz"
  [[ -d "${source_dir}/packages" ]] || die "archive do registry sem packages/"

  if [[ -e "${REGISTRY_DIR}" ]]; then
    mv "${REGISTRY_DIR}" "${backup_dir}"
  fi

  if ! mv "${source_dir}" "${REGISTRY_DIR}"; then
    if [[ -d "${backup_dir}" && ! -e "${REGISTRY_DIR}" ]]; then
      mv "${backup_dir}" "${REGISTRY_DIR}"
    fi
    die "falha ao publicar registry local"
  fi

  rm -rf "${backup_dir}" "${tmp_dir}"
}

run_mason_install() {
  local mason_plugin_dir="${NVIM_DATA_DIR%/}/lazy/mason.nvim"
  local mason_install_root="${NVIM_DATA_DIR%/}/mason"
  local lua_script package_list

  package_list="$(build_package_list)"
  package_list="${package_list%,}"
  [[ -n "${package_list}" ]] || die "lista de pacotes vazia"

  if [[ "${DRY_RUN}" == "1" ]]; then
    printf '[dry-run] %s --headless -u NONE -S <mason install script>\n' "${NVIM_BIN}" >&2
    printf '[dry-run] packages=%s\n' "${package_list}" >&2
    return 0
  fi

  command -v "${NVIM_BIN}" >/dev/null 2>&1 || die "Neovim nao encontrado: ${NVIM_BIN}"
  command -v yq >/dev/null 2>&1 || die "yq nao encontrado no PATH; necessario para registry local file:"
  [[ -d "${mason_plugin_dir}" ]] || die "mason.nvim nao encontrado em ${mason_plugin_dir}; rode install_lazyvim_archives.sh antes"
  [[ -d "${REGISTRY_DIR}/packages" ]] || die "registry local invalido: ${REGISTRY_DIR}"

  lua_script="$(mktemp)"
  TMP_FILES+=("${lua_script}")
  cat > "${lua_script}" <<'LUA'
local mason_plugin_dir = assert(os.getenv("MASON_PLUGIN_DIR"), "MASON_PLUGIN_DIR ausente")
local registry_dir = assert(os.getenv("MASON_REGISTRY_DIR"), "MASON_REGISTRY_DIR ausente")
local install_root = assert(os.getenv("MASON_INSTALL_ROOT"), "MASON_INSTALL_ROOT ausente")
local timeout_ms = tonumber(os.getenv("MASON_INSTALL_TIMEOUT_MS") or "1800000") or 1800000
local max_installers = tonumber(os.getenv("MASON_MAX_CONCURRENT_INSTALLERS") or "4") or 4
local raw_packages = os.getenv("MASON_PACKAGE_LIST") or ""

vim.opt.rtp:prepend(mason_plugin_dir)

local function fail(message)
  io.stderr:write("[mason-registry-archive] erro: " .. message .. "\n")
  vim.cmd("cquit")
end

local packages = {}
for package_name in string.gmatch(raw_packages, "[^,]+") do
  table.insert(packages, package_name)
end

if #packages == 0 then
  fail("lista de pacotes vazia")
end

require("mason").setup({
  install_root_dir = install_root,
  registries = { "file:" .. registry_dir },
  system_registries = {},
  providers = { "mason.providers.client" },
  max_concurrent_installers = max_installers,
  PATH = "prepend",
})

local registry = require("mason-registry")
local refresh_done = false
local refresh_success = false
local refresh_error = nil

registry.refresh(function(success, result)
  refresh_done = true
  refresh_success = success
  if not success then
    refresh_error = tostring(result)
  end
end)

if not vim.wait(600000, function()
  return refresh_done
end, 100) then
  fail("timeout carregando registry local")
end

if not refresh_success then
  fail("falha carregando registry local: " .. tostring(refresh_error))
end

local pending = 0
local failures = {}

local function mark_failure(package_name, message)
  table.insert(failures, package_name .. ": " .. tostring(message))
end

for _, package_name in ipairs(packages) do
  local ok, package = pcall(registry.get_package, package_name)
  if not ok then
    mark_failure(package_name, package)
  else
    pending = pending + 1
    local started, install_error = pcall(function()
      package:install({ force = true }, function(success, result)
        if success then
          io.stdout:write("[mason-registry-archive] instalado: " .. package_name .. "\n")
        else
          mark_failure(package_name, result)
        end
        pending = pending - 1
      end)
    end)

    if not started then
      pending = pending - 1
      mark_failure(package_name, install_error)
    end
  end
end

if pending > 0 and not vim.wait(timeout_ms, function()
  return pending == 0
end, 500) then
  fail("timeout instalando pacotes; pendentes=" .. tostring(pending))
end

if #failures > 0 then
  fail(table.concat(failures, "\n"))
end

vim.cmd("qa")
LUA

  MASON_PLUGIN_DIR="${mason_plugin_dir}" \
    MASON_REGISTRY_DIR="${REGISTRY_DIR}" \
    MASON_INSTALL_ROOT="${mason_install_root}" \
    MASON_INSTALL_TIMEOUT_MS="${INSTALL_TIMEOUT_MS}" \
    MASON_MAX_CONCURRENT_INSTALLERS="${MAX_CONCURRENT_INSTALLERS}" \
    MASON_PACKAGE_LIST="${package_list}" \
    "${NVIM_BIN}" --headless -u NONE -S "${lua_script}"

  rm -f "${lua_script}"
}

command -v tar >/dev/null 2>&1 || die "tar e obrigatorio"

install_registry_archive
run_mason_install

log "pacotes Mason processados em ${NVIM_DATA_DIR%/}/mason"
