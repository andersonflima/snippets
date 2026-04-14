#!/bin/bash
set -eu

log() {
  printf '[bootstrap-lazyvim-mason] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash scripts/install/bootstrap_lazyvim_mason.sh [opções]

Opções:
  --env-file <arquivo>           Env-file do wrapper. Default: $HOME/.config/wrapper-envs.sh
  --nvim <binário>               Binário do neovim. Default: nvim.
  --mason-packages <lista>       Lista de pacotes Mason separados por vírgula ou espaço.
  --bootstrap-timeout <segundos>  Timeout por pacote para espera de instalação. Default: 600.
  --skip-lazy                    Pula bootstrap do LazyVim.
  --skip-mason                   Pula instalação via Mason.
  --bootstrap-strict              Falha em erro; do contrário apenas avisa.
  -h, --help                     Mostra esta ajuda.
USAGE
}

NVIM_BIN="nvim"
ENV_FILE="${HOME}/.config/wrapper-envs.sh"
MASON_PACKAGES=""
BOOTSTRAP_TIMEOUT_SECONDS="600"
SKIP_LAZY="0"
SKIP_MASON="0"
BOOTSTRAP_STRICT="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --nvim)
      NVIM_BIN="${2:-}"
      shift 2
      ;;
    --mason-packages)
      MASON_PACKAGES="${2:-}"
      shift 2
      ;;
    --bootstrap-timeout)
      BOOTSTRAP_TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --skip-lazy)
      SKIP_LAZY="1"
      shift
      ;;
    --skip-mason)
      SKIP_MASON="1"
      shift
      ;;
    --bootstrap-strict)
      BOOTSTRAP_STRICT="1"
      shift
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

[[ -n "${ENV_FILE}" ]] || die "--env-file não pode ser vazio"

if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
fi

case "${BOOTSTRAP_TIMEOUT_SECONDS}" in
  ''|*[!0-9]*)
    die "--bootstrap-timeout deve ser um inteiro positivo"
    ;;
esac

if ! command -v "${NVIM_BIN}" >/dev/null 2>&1; then
  if [[ "${BOOTSTRAP_STRICT}" == "1" ]]; then
    die "neovim não encontrado no PATH: ${NVIM_BIN}"
  fi
  log "neovim não encontrado no PATH, pulando bootstrap do LazyVim/Mason"
  exit 0
fi

if [[ "${SKIP_LAZY}" == "1" && "${SKIP_MASON}" == "1" ]]; then
  log "skip-lazy e skip-mason definidos, não há ação"
  exit 0
fi

BOOTSTRAP_SCRIPT="$(mktemp)"
cleanup_bootstrap_script() {
  rm -f "${BOOTSTRAP_SCRIPT}"
}
trap cleanup_bootstrap_script EXIT

cat <<EOF2 >"${BOOTSTRAP_SCRIPT}"
local skip_lazy = (os.getenv("BOOTSTRAP_SKIP_LAZY") == "1")
local skip_mason = (os.getenv("BOOTSTRAP_SKIP_MASON") == "1")
local strict = (os.getenv("BOOTSTRAP_STRICT") == "1")
local timeout_seconds = tonumber(os.getenv("BOOTSTRAP_TIMEOUT_SECONDS") or "600")
if not timeout_seconds then
  timeout_seconds = 600
end

local packages = {}
local raw_packages = os.getenv("BOOTSTRAP_MASON_PACKAGES") or ""
for token in string.gmatch(raw_packages, "[^,%s]+") do
  if token ~= "" then
    table.insert(packages, token)
  end
end

local function log_line(level, message)
  io.write("[bootstrap-lazyvim-mason] " .. level .. " " .. message .. "\\n")
end

local function command_exists(name)
  return vim.fn.exists(":" .. name) == 2
end

local function wait_for_packages_install()
  if #packages == 0 then
    return true
  end

  local has_registry, registry = pcall(require, "mason-registry")
  if not has_registry then
    log_line("warn:", "mason-registry indisponível; instalação não foi verificada")
    return true
  end

  local uv = vim.loop or vim.uv
  local timeout_ms = timeout_seconds * 1000
  local started = uv.now()

  local function all_installed()
    for _, package_name in ipairs(packages) do
      local ok, package = pcall(registry.get_package, package_name)
      if not ok or not package or not package:is_installed() then
        return false
      end
    end
    return true
  end

  while (uv.now() - started) < timeout_ms do
    if all_installed() then
      return true
    end
    vim.wait(1000)
  end

  return all_installed()
end

local function install_mason_packages()
  for _, package_name in ipairs(packages) do
    local package_cmd = "MasonInstall " .. package_name
    local ok, err = pcall(vim.cmd, package_cmd)
    if not ok then
      log_line("warn:", "falha para " .. package_name .. ": " .. tostring(err))
    end
  end

  if wait_for_packages_install() then
    log_line("info", "pacotes Mason processados")
    return true
  end

  local msg = "timeout aguardando instalação dos pacotes Mason: " .. table.concat(packages, ", ")
  if strict then
    error(msg)
  end
  log_line("warn:", msg)
  return false
end

local function sync_lazy()
  if skip_lazy then
    return true
  end

  if not command_exists("Lazy") then
    log_line("warn:", "comando Lazy não encontrado")
    if strict then
      error("LazyVim não disponível")
    end
    return false
  end

  local ok, err = pcall(vim.cmd, "Lazy! sync")
  if not ok then
    local msg = "falha no Lazy! sync: " .. tostring(err)
    if strict then
      error(msg)
    end
    log_line("warn:", msg)
    return false
  end

  log_line("info", "Lazy! sync executado")
  return true
end

local function sync_mason()
  if skip_mason then
    return true
  end

  if #packages == 0 then
    log_line("info", "nenhum pacote Mason informado")
    return true
  end

  if not command_exists("Mason") then
    local msg = "comando Mason não encontrado"
    if strict then
      error(msg)
    end
    log_line("warn:", msg)
    return false
  end

  return install_mason_packages()
end

local ok, err = pcall(function()
  sync_lazy()
  sync_mason()
end)
if not ok then
  log_line("error:", tostring(err))
  if strict then
    vim.cmd("cq")
  end
end
EOF2

export BOOTSTRAP_SKIP_LAZY="${SKIP_LAZY}"
export BOOTSTRAP_SKIP_MASON="${SKIP_MASON}"
export BOOTSTRAP_STRICT="${BOOTSTRAP_STRICT}"
export BOOTSTRAP_TIMEOUT_SECONDS="${BOOTSTRAP_TIMEOUT_SECONDS}"
export BOOTSTRAP_MASON_PACKAGES="${MASON_PACKAGES}"

set +e
"${NVIM_BIN}" --headless "+luafile ${BOOTSTRAP_SCRIPT}" +qa
exit_code=$?
set -e

if [[ ${exit_code} -ne 0 ]]; then
  if [[ "${BOOTSTRAP_STRICT}" == "1" ]]; then
    die "bootstrap nvim falhou (exit=${exit_code})"
  fi
  log "bootstrap nvim não finalizou com sucesso, mas foi ignorado em modo não estrito"
fi
