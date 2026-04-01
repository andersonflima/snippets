#!/usr/bin/env bash
set -euo pipefail

ELIXIR_REF="latest"
INSTALL_DIR="/opt/elixir"
PROFILE_FILE="/etc/profile.d/elixir.sh"
FORCE_REMOVE_PACKAGE="0"
ELIXIR_REF_EXPLICIT="0"
ELIXIR_DEFAULT_FALLBACK_REF="v1.17.3"
ELIXIR_BIN_DIR="${INSTALL_DIR}/bin"

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
  $(basename "$0") [--elixir-ref latest|vX.Y.Z] [--install-dir /opt/elixir] [--force-remove-package]

Objetivo:
  Corrigir ambiente EC2 com Elixir antigo (ex: /usr/bin/elixir 0.12.5),
  instalando por padrão a versão mais recente compatível do Elixir via builds.hex.pm
  (com fallback para package manager) e priorizando no PATH.

Opções:
  --elixir-ref             Versão/tag do Elixir. Padrão: latest (mais recente compatível)
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
    die "sudo sem modo não interativo. Execute como root ou use: sudo -E bash scripts/ec2/elixir/fix_elixir_ec2.sh"
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

install_elixir_from_package_manager() {
  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn)
      if command_exists dnf; then
        run_with_sudo dnf makecache -y || true
        run_with_sudo dnf install -y elixir || die "falha ao instalar elixir com dnf"
      elif command_exists yum; then
        run_with_sudo yum makecache -y || true
        if ! run_with_sudo yum install -y elixir; then
          if command_exists amazon-linux-extras; then
            run_with_sudo amazon-linux-extras install -y elixir || true
            run_with_sudo yum install -y elixir || die "falha ao instalar elixir com yum"
          else
            die "falha ao instalar elixir com yum"
          fi
        fi
      else
        die "dnf/yum não encontrado para instalar elixir"
      fi
      ;;
    ubuntu|debian)
      run_with_sudo apt-get update
      run_with_sudo apt-get install -y elixir || die "falha ao instalar elixir com apt"
      ;;
    *)
      die "distribuição não suportada para instalar elixir via package manager: ${distro}"
      ;;
  esac

  local detected_elixir_bin
  detected_elixir_bin="$(command -v elixir || true)"
  [[ -n "${detected_elixir_bin}" ]] || die "elixir não encontrado após instalação via package manager"
  ELIXIR_BIN_DIR="$(dirname "${detected_elixir_bin}")"
  ELIXIR_REF="package-manager"
}

resolve_latest_elixir_ref_remote() {
  local latest_ref
  latest_ref="$(
    curl -fsSL \
      -H 'Accept: application/vnd.github+json' \
      -H 'User-Agent: fix-elixir-ec2-script' \
      --retry 3 --retry-delay 2 --connect-timeout 20 \
      'https://api.github.com/repos/elixir-lang/elixir/releases/latest' 2>/dev/null \
      | sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]\+\)".*$/\1/p' \
      | head -n 1
  )"

  if [[ "${latest_ref}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s\n' "${latest_ref}"
    return 0
  fi

  return 1
}

resolve_otp_fallback_ref() {
  local otp_major
  otp_major="$1"

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

  printf '%s\n' "${ELIXIR_DEFAULT_FALLBACK_REF}"
}

build_elixir_ref_candidates() {
  local otp_major latest_ref otp_fallback_ref ref
  otp_major="$1"
  latest_ref=""
  otp_fallback_ref=""

  local -a refs=()

  if [[ "${ELIXIR_REF_EXPLICIT}" == "1" && "${ELIXIR_REF}" != "latest" ]]; then
    ref="${ELIXIR_REF}"
    if [[ -n "${ref}" ]]; then
      case " ${refs[*]} " in
        *" ${ref} "*) ;;
        *) refs+=("${ref}") ;;
      esac
    fi
  else
    latest_ref="$(resolve_latest_elixir_ref_remote || true)"
    ref="${latest_ref}"
    if [[ -n "${ref}" ]]; then
      case " ${refs[*]} " in
        *" ${ref} "*) ;;
        *) refs+=("${ref}") ;;
      esac
    fi
    otp_fallback_ref="$(resolve_otp_fallback_ref "${otp_major}")"
    ref="${otp_fallback_ref}"
    if [[ -n "${ref}" ]]; then
      case " ${refs[*]} " in
        *" ${ref} "*) ;;
        *) refs+=("${ref}") ;;
      esac
    fi
    ref="${ELIXIR_DEFAULT_FALLBACK_REF}"
    if [[ -n "${ref}" ]]; then
      case " ${refs[*]} " in
        *" ${ref} "*) ;;
        *) refs+=("${ref}") ;;
      esac
    fi
  fi

  printf '%s\n' "${refs[@]}"
}

download_elixir_archive() {
  local otp_release workdir archive_path candidate_ref
  otp_release="$1"
  workdir="$2"
  shift 2

  local -a candidate_refs=("$@")
  [[ ${#candidate_refs[@]} -gt 0 ]] || die "nenhuma versão candidata de Elixir foi informada"

  archive_path="${workdir}/elixir.zip"

  local -a base_urls=(
    "https://repo.hex.pm/builds/elixir"
    "https://builds.hex.pm/builds/elixir"
  )

  local -a ref_variants=()
  local base_url ref_variant otp_url generic_url normalized_ref

  for candidate_ref in "${candidate_refs[@]}"; do
    ref_variants=()
    normalized_ref="${candidate_ref#v}"

    if [[ -n "${candidate_ref}" ]]; then
      ref_variants+=("${candidate_ref}")
    fi
    if [[ -n "${normalized_ref}" ]]; then
      ref_variants+=("v${normalized_ref}" "${normalized_ref}")
    fi

    local -a unique_ref_variants=()
    for ref_variant in "${ref_variants[@]}"; do
      case " ${unique_ref_variants[*]} " in
        *" ${ref_variant} "*) ;;
        *) unique_ref_variants+=("${ref_variant}") ;;
      esac
    done

    for base_url in "${base_urls[@]}"; do
      for ref_variant in "${unique_ref_variants[@]}"; do
        otp_url="${base_url}/${ref_variant}-otp-${otp_release}.zip"
        generic_url="${base_url}/${ref_variant}.zip"

        if [[ -n "${otp_release}" ]]; then
          log "tentando baixar build OTP específica: ${otp_url}"
          if curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 "${otp_url}" -o "${archive_path}"; then
            ELIXIR_REF="${ref_variant}"
            printf '%s\n' "${archive_path}"
            return
          fi
        fi

        log "fallback para build genérica: ${generic_url}"
        if curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 "${generic_url}" -o "${archive_path}"; then
          ELIXIR_REF="${ref_variant}"
          printf '%s\n' "${archive_path}"
          return
        fi
      done
    done
  done

  log "falha ao baixar Elixir via builds. Candidatos tentados: ${candidate_refs[*]}"
  return 1
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
  profile_content="export PATH=${ELIXIR_BIN_DIR}:\$PATH"
  printf '%s\n' "${profile_content}" | run_with_sudo tee "${PROFILE_FILE}" >/dev/null
  run_with_sudo chmod 0644 "${PROFILE_FILE}"

  [[ -x "${ELIXIR_BIN_DIR}/elixir" ]] && run_with_sudo ln -sf "${ELIXIR_BIN_DIR}/elixir" /usr/local/bin/elixir || true
  [[ -x "${ELIXIR_BIN_DIR}/mix" ]] && run_with_sudo ln -sf "${ELIXIR_BIN_DIR}/mix" /usr/local/bin/mix || true
  [[ -x "${ELIXIR_BIN_DIR}/iex" ]] && run_with_sudo ln -sf "${ELIXIR_BIN_DIR}/iex" /usr/local/bin/iex || true
}

validate_installation() {
  local elixir_bin mix_bin
  elixir_bin="${ELIXIR_BIN_DIR}/elixir"
  mix_bin="${ELIXIR_BIN_DIR}/mix"

  export PATH="${ELIXIR_BIN_DIR}:${PATH}"
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
  log "OTP detectado: ${otp_release} (major=${otp_major})"

  local -a elixir_ref_candidates
  mapfile -t elixir_ref_candidates < <(build_elixir_ref_candidates "${otp_major}")
  [[ ${#elixir_ref_candidates[@]} -gt 0 ]] || die "não foi possível resolver versões candidatas de Elixir"
  log "candidatos de Elixir: ${elixir_ref_candidates[*]}"

  local workdir archive_path
  local used_package_manager_fallback
  used_package_manager_fallback="0"
  workdir="$(mktemp -d -t fix-elixir-XXXXXX)"
  trap 'rm -rf "${workdir}"' EXIT

  log "etapa: download_elixir_archive"
  if archive_path="$(download_elixir_archive "${otp_release}" "${workdir}" "${elixir_ref_candidates[@]}")"; then
    ELIXIR_BIN_DIR="${INSTALL_DIR}/bin"
    log "Elixir selecionado para instalação: ${ELIXIR_REF}"
    log "arquivo de instalação: ${archive_path}"
    log "etapa: install_elixir_archive"
    install_elixir_archive "${archive_path}"
  else
    used_package_manager_fallback="1"
    log "etapa: install_elixir_from_package_manager"
    install_elixir_from_package_manager
    log "fallback package manager aplicado"
  fi

  log "elixir bin dir: ${ELIXIR_BIN_DIR}"
  log "etapa: configure_path"
  configure_path
  log "etapa: validate_installation"
  validate_installation

  export PATH="${ELIXIR_BIN_DIR}:${PATH}"

  local after_state after_path after_version after_require_file2
  log "etapa: read_current_state (depois)"
  after_state="$(read_current_state)"
  after_path="${after_state%%|*}"
  after_version="${after_state#*|}"
  after_version="${after_version%%|*}"
  after_require_file2="${after_state##*|}"

  log "antes: elixir_path=${before_path} version='${before_version}' Code.require_file/2=${before_require_file2}"
  log "depois: elixir_path=${after_path} version='${after_version}' Code.require_file/2=${after_require_file2}"
  log "modo instalação: $([[ \"${used_package_manager_fallback}\" == \"1\" ]] && echo package-manager || echo hex-build)"
  log "correção concluída"
  log "para novos shells: source ${PROFILE_FILE}"
}

main "$@"
