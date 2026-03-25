#!/usr/bin/env bash
set -euo pipefail

ELIXIR_REF="v1.17.3"
INSTALL_DIR="/opt/elixir"
PROFILE_FILE="/etc/profile.d/elixir.sh"
FORCE_REMOVE_PACKAGE="0"

log() {
  printf '[fix-elixir-ec2] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<USAGE
Uso:
  $(basename "$0") [--elixir-ref v1.17.3] [--install-dir /opt/elixir] [--force-remove-package]

Objetivo:
  Corrigir ambiente EC2 com Elixir antigo (ex: /usr/bin/elixir 0.12.5),
  instalando uma versão moderna do Elixir via builds.hex.pm e priorizando no PATH.

Opções:
  --elixir-ref             Versão/tag do Elixir. Padrão: ${ELIXIR_REF}
  --install-dir            Diretório de instalação. Padrão: ${INSTALL_DIR}
  --force-remove-package   Tenta remover pacote Elixir do sistema via rpm/dpkg local antes de instalar
  -h, --help               Exibe esta ajuda
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --elixir-ref)
      ELIXIR_REF="${2:-}"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --force-remove-package)
      FORCE_REMOVE_PACKAGE="1"
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

[[ -n "${ELIXIR_REF}" ]] || die "--elixir-ref não pode ser vazio"
[[ -n "${INSTALL_DIR}" ]] || die "--install-dir não pode ser vazio"

SUDO_CMD=()
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo é necessário para esta operação"
  SUDO_CMD=(sudo)
fi

run_with_sudo() {
  "${SUDO_CMD[@]}" "$@"
}

is_ec2_environment() {
  local signal_files=(
    "/sys/hypervisor/uuid"
    "/sys/devices/virtual/dmi/id/product_uuid"
    "/sys/devices/virtual/dmi/id/board_asset_tag"
  )
  local signal_file

  for signal_file in "${signal_files[@]}"; do
    [[ -r "${signal_file}" ]] || continue
    if grep -Eiq '^(ec2|i-[0-9a-f]+)' "${signal_file}"; then
      return 0
    fi
  done

  return 1
}

assert_ec2_environment() {
  if ! is_ec2_environment; then
    die "este script é exclusivo para EC2"
  fi
}

validate_sudo_non_interactive() {
  if [[ ${#SUDO_CMD[@]} -eq 0 ]]; then
    return
  fi

  if ! sudo -n true >/dev/null 2>&1; then
    die "sudo sem modo não interativo. Execute como root ou use: sudo -E bash scripts/fix_elixir_ec2.sh"
  fi
}

detect_distro() {
  [[ -f /etc/os-release ]] || die "arquivo /etc/os-release não encontrado"
  # shellcheck disable=SC1091
  source /etc/os-release
  printf '%s\n' "${ID:-unknown}"
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

ensure_base_tools() {
  local missing=()
  local tool
  for tool in curl unzip; do
    if ! command_exists "${tool}"; then
      missing+=("${tool}")
    fi
  done

  if [[ ${#missing[@]} -eq 0 ]]; then
    return
  fi

  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn)
      if command_exists dnf; then
        run_with_sudo dnf install -y "${missing[@]}"
      elif command_exists yum; then
        run_with_sudo yum install -y "${missing[@]}"
      else
        die "dnf/yum não encontrado para instalar dependências"
      fi
      ;;
    ubuntu|debian)
      run_with_sudo apt-get update
      run_with_sudo apt-get install -y "${missing[@]}"
      ;;
    *)
      die "distribuição não suportada para auto-instalar dependências: ${distro}"
      ;;
  esac
}

ensure_erlang_runtime() {
  if command_exists erl; then
    return
  fi

  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn)
      if command_exists dnf; then
        run_with_sudo dnf install -y erlang || die "falha ao instalar erlang com dnf"
      elif command_exists yum; then
        run_with_sudo yum install -y erlang || die "falha ao instalar erlang com yum"
      else
        die "dnf/yum não encontrado para instalar erlang"
      fi
      ;;
    ubuntu|debian)
      run_with_sudo apt-get update
      run_with_sudo apt-get install -y erlang || die "falha ao instalar erlang com apt"
      ;;
    *)
      die "distribuição não suportada para auto-instalar erlang: ${distro}"
      ;;
  esac
}

remove_system_elixir_if_requested() {
  if [[ "${FORCE_REMOVE_PACKAGE}" != "1" ]]; then
    log "remoção forçada desativada; seguindo sem remover pacote"
    return 0
  fi

  local distro
  distro="$(detect_distro)"
  log "remoção forçada habilitada (modo local, sem resolver repositórios)"

  case "${distro}" in
    amzn)
      if command_exists rpm; then
        if run_with_sudo rpm -q elixir >/dev/null 2>&1; then
          if run_with_sudo rpm -e elixir >/dev/null 2>&1; then
            log "pacote elixir removido via rpm -e"
          else
            log "aviso: não foi possível remover elixir via rpm -e; seguindo sem remover"
          fi
        else
          log "pacote elixir não está instalado via rpm"
        fi
      else
        log "aviso: rpm não encontrado; seguindo sem remover pacote"
      fi
      ;;
    ubuntu|debian)
      if command_exists dpkg; then
        if run_with_sudo dpkg -s elixir >/dev/null 2>&1; then
          if run_with_sudo dpkg -r elixir >/dev/null 2>&1; then
            log "pacote elixir removido via dpkg -r"
          else
            log "aviso: não foi possível remover elixir via dpkg -r; seguindo sem remover"
          fi
        else
          log "pacote elixir não está instalado via dpkg"
        fi
      else
        log "aviso: dpkg não encontrado; seguindo sem remover pacote"
      fi
      ;;
    *)
      log "aviso: distro sem rotina de remoção local (${distro}); seguindo sem remover pacote"
      ;;
  esac
}

read_current_state() {
  local path version_line require_file2
  path="$(command -v elixir || true)"
  version_line="não instalado"
  require_file2="não verificado"

  if [[ -n "${path}" ]]; then
    version_line="$(elixir --version 2>&1 | head -n 1 || true)"
  fi

  printf '%s|%s|%s\n' "${path:-<ausente>}" "${version_line}" "${require_file2}"
}

detect_otp_release() {
  erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().' 2>/dev/null || true
}

download_elixir_archive() {
  local otp_release workdir archive_path otp_url generic_url
  otp_release="$1"
  workdir="$2"
  archive_path="${workdir}/elixir.zip"
  otp_url="https://builds.hex.pm/builds/elixir/${ELIXIR_REF}-otp-${otp_release}.zip"
  generic_url="https://builds.hex.pm/builds/elixir/${ELIXIR_REF}.zip"

  if [[ -n "${otp_release}" ]]; then
    log "tentando baixar build OTP específica: ${otp_url}"
    if curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 "${otp_url}" -o "${archive_path}"; then
      printf '%s\n' "${archive_path}"
      return
    fi
  fi

  log "fallback para build genérica: ${generic_url}"
  curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 "${generic_url}" -o "${archive_path}" || die "falha ao baixar Elixir ${ELIXIR_REF}"
  printf '%s\n' "${archive_path}"
}

install_elixir_archive() {
  local archive_path
  archive_path="$1"

  run_with_sudo rm -rf "${INSTALL_DIR}"
  run_with_sudo mkdir -p "${INSTALL_DIR}"
  run_with_sudo unzip -o "${archive_path}" -d "${INSTALL_DIR}" >/dev/null
}

configure_path() {
  local profile_content
  profile_content="export PATH=${INSTALL_DIR}/bin:\$PATH"
  printf '%s\n' "${profile_content}" | run_with_sudo tee "${PROFILE_FILE}" >/dev/null
  run_with_sudo chmod 0644 "${PROFILE_FILE}"

  run_with_sudo ln -sf "${INSTALL_DIR}/bin/elixir" /usr/local/bin/elixir
  run_with_sudo ln -sf "${INSTALL_DIR}/bin/mix" /usr/local/bin/mix
  run_with_sudo ln -sf "${INSTALL_DIR}/bin/iex" /usr/local/bin/iex
}

validate_installation() {
  export PATH="${INSTALL_DIR}/bin:${PATH}"

  command_exists elixir || die "elixir não encontrado após instalação"
  command_exists mix || die "mix não encontrado após instalação"

  elixir -e 'System.halt(if function_exported?(Code, :require_file, 2), do: 0, else: 1)' || die "Code.require_file/2 não está disponível após instalação"

  if ! mix local.hex --force >/dev/null 2>&1; then
    log "aviso: não foi possível executar mix local.hex --force"
  fi

  if ! mix local.rebar --force >/dev/null 2>&1; then
    log "aviso: não foi possível executar mix local.rebar --force"
  fi
}

main() {
  log "iniciando correção de runtime Elixir"
  assert_ec2_environment
  validate_sudo_non_interactive
  log "etapa: ensure_base_tools"
  ensure_base_tools
  log "etapa: ensure_erlang_runtime"
  ensure_erlang_runtime
  log "etapa: remove_system_elixir_if_requested"
  remove_system_elixir_if_requested

  local before_state before_path before_version before_require_file2
  log "etapa: read_current_state (antes)"
  before_state="$(read_current_state)"
  before_path="${before_state%%|*}"
  before_version="${before_state#*|}"
  before_version="${before_version%%|*}"
  before_require_file2="${before_state##*|}"

  local otp_release
  log "etapa: detect_otp_release"
  otp_release="$(detect_otp_release)"
  [[ -n "${otp_release}" ]] || die "não foi possível detectar OTP release via erl"
  log "OTP detectado: ${otp_release}"

  local workdir archive_path
  workdir="$(mktemp -d -t fix-elixir-XXXXXX)"
  trap 'rm -rf "${workdir}"' EXIT

  log "etapa: download_elixir_archive"
  archive_path="$(download_elixir_archive "${otp_release}" "${workdir}")"
  log "arquivo de instalação: ${archive_path}"
  log "etapa: install_elixir_archive"
  install_elixir_archive "${archive_path}"
  log "etapa: configure_path"
  configure_path
  log "etapa: validate_installation"
  validate_installation

  export PATH="${INSTALL_DIR}/bin:${PATH}"

  local after_state after_path after_version after_require_file2
  log "etapa: read_current_state (depois)"
  after_state="$(read_current_state)"
  after_path="${after_state%%|*}"
  after_version="${after_state#*|}"
  after_version="${after_version%%|*}"
  after_require_file2="${after_state##*|}"

  log "antes: elixir_path=${before_path} version='${before_version}' Code.require_file/2=${before_require_file2}"
  log "depois: elixir_path=${after_path} version='${after_version}' Code.require_file/2=${after_require_file2}"
  log "correção concluída"
  log "para novos shells: source ${PROFILE_FILE}"
}

main "$@"
