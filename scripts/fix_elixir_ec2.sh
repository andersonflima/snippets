#!/usr/bin/env bash
set -euo pipefail

ELIXIR_REF="v1.17.3"
INSTALL_DIR="/opt/elixir"
PROFILE_FILE="/etc/profile.d/elixir.sh"
FORCE_REMOVE_PACKAGE="0"
ELIXIR_REF_EXPLICIT="0"

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
  --elixir-ref             Versão/tag do Elixir. Default: seleção automática por OTP
  --install-dir            Diretório de instalação. Padrão: ${INSTALL_DIR}
  --force-remove-package   Tenta remover pacote Elixir do sistema via rpm/dpkg local antes de instalar
  -h, --help               Exibe esta ajuda
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --elixir-ref)
      ELIXIR_REF="${2:-}"
      ELIXIR_REF_EXPLICIT="1"
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

USE_SUDO="0"
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo é necessário para esta operação"
  USE_SUDO="1"
fi

run_with_sudo() {
  if [[ "${USE_SUDO}" == "1" ]]; then
    sudo "$@"
  else
    "$@"
  fi
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
  if [[ "${USE_SUDO}" != "1" ]]; then
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
  local otp_release otp_major
  otp_release=""
  otp_major=""

  if command_exists erl; then
    otp_release="$(detect_otp_release)"
    otp_major="$(otp_to_integer "${otp_release}" || true)"
    if [[ -n "${otp_major}" && "${otp_major}" -ge 24 ]]; then
      return
    fi
    log "Erlang encontrado, mas OTP atual é ${otp_release:-desconhecido}; tentando atualizar para OTP >= 24"
  else
    log "Erlang não encontrado; instalando runtime OTP >= 24"
  fi

  install_or_upgrade_erlang

  otp_release="$(detect_otp_release)"
  otp_major="$(otp_to_integer "${otp_release}" || true)"
  [[ -n "${otp_major}" && "${otp_major}" -ge 24 ]] || die "Erlang incompatível após atualização (OTP=${otp_release:-desconhecido}). Requer OTP >= 24"
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

otp_to_integer() {
  local otp_release
  otp_release="$1"
  if [[ "${otp_release}" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "${otp_release}"
    return 0
  fi
  return 1
}

install_or_upgrade_erlang() {
  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn)
      if command_exists dnf; then
        run_with_sudo dnf makecache -y || true
        run_with_sudo dnf install -y erlang || die "falha ao instalar/atualizar erlang com dnf"
      elif command_exists yum; then
        run_with_sudo yum makecache -y || true
        if ! run_with_sudo yum install -y erlang; then
          if command_exists amazon-linux-extras; then
            run_with_sudo amazon-linux-extras install -y erlang || true
            run_with_sudo yum install -y erlang || die "falha ao instalar/atualizar erlang com yum"
          else
            die "falha ao instalar/atualizar erlang com yum"
          fi
        fi
      else
        die "dnf/yum não encontrado para instalar/atualizar erlang"
      fi
      ;;
    ubuntu|debian)
      run_with_sudo apt-get update
      run_with_sudo apt-get install -y erlang || die "falha ao instalar/atualizar erlang com apt"
      ;;
    *)
      die "distribuição não suportada para instalar/atualizar erlang: ${distro}"
      ;;
  esac
}

resolve_elixir_ref_for_otp() {
  local otp_major
  otp_major="$1"

  if [[ "${ELIXIR_REF_EXPLICIT}" == "1" ]]; then
    printf '%s\n' "${ELIXIR_REF}"
    return
  fi

  if (( otp_major >= 26 )); then
    printf '%s\n' "v1.17.3"
    return
  fi
  if (( otp_major == 25 )); then
    printf '%s\n' "v1.16.3"
    return
  fi
  if (( otp_major == 24 )); then
    printf '%s\n' "v1.15.8"
    return
  fi
  if (( otp_major == 23 )); then
    printf '%s\n' "v1.14.5"
    return
  fi

  die "OTP ${otp_major} não suportado para instalação automática do Elixir"
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
  local elixir_bin mix_bin
  elixir_bin="${INSTALL_DIR}/bin/elixir"
  mix_bin="${INSTALL_DIR}/bin/mix"

  export PATH="${INSTALL_DIR}/bin:${PATH}"
  hash -r

  [[ -x "${elixir_bin}" ]] || die "elixir não encontrado em ${elixir_bin}"
  [[ -x "${mix_bin}" ]] || die "mix não encontrado em ${mix_bin}"

  local run_output
  run_output="$("${elixir_bin}" -e 'IO.puts("elixir-ok")' 2>&1 || true)"
  if [[ "${run_output}" != *"elixir-ok"* ]]; then
    if [[ -n "${run_output}" ]]; then
      log "detalhe falha do Elixir: ${run_output}"
    fi
    log "erl path atual: $(command -v erl || echo '<ausente>')"
    log "erl version (resumo): $(erl -version 2>&1 | head -n 1 || echo '<indisponível>')"
    die "elixir instalado não executa corretamente"
  fi

  if "${elixir_bin}" -e 'System.halt(if function_exported?(Code, :require_file, 2), do: 0, else: 1)' >/dev/null 2>&1; then
    log "Code.require_file/2 disponível"
  else
    log "aviso: Code.require_file/2 não disponível; prosseguindo"
  fi

  if ! "${mix_bin}" local.hex --force >/dev/null 2>&1; then
    log "aviso: não foi possível executar mix local.hex --force"
  fi

  if ! "${mix_bin}" local.rebar --force >/dev/null 2>&1; then
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
  local otp_major
  otp_major="$(otp_to_integer "${otp_release}" || true)"
  [[ -n "${otp_major}" ]] || die "valor inválido de OTP detectado: ${otp_release}"
  ELIXIR_REF="$(resolve_elixir_ref_for_otp "${otp_major}")"
  log "OTP detectado: ${otp_release} (major=${otp_major})"
  log "Elixir selecionado: ${ELIXIR_REF}"

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
