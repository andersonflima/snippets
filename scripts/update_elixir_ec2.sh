#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIX_SCRIPT_PATH="${SCRIPT_DIR}/fix_elixir_ec2.sh"

ELIXIR_REF="${ELIXIR_REF:-v1.17.3}"
INSTALL_DIR="${INSTALL_DIR:-/opt/elixir}"
FORCE_REMOVE_PACKAGE="${FORCE_REMOVE_PACKAGE:-0}"
LOG_FILE="${LOG_FILE:-/tmp/update_elixir_ec2.log}"

log() {
  printf '[update-elixir-ec2] %s\n' "$*"
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<USAGE
Uso:
  $(basename "$0") [--elixir-ref v1.17.3] [--install-dir /opt/elixir] [--force-remove-package] [--log-file /tmp/update_elixir_ec2.log]

Objetivo:
  Executar a correção de runtime Elixir no EC2 com um comando único,
  incluindo validação final de versão e suporte a Code.require_file/2.
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
    --log-file)
      LOG_FILE="${2:-}"
      shift 2
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

[[ -f "${FIX_SCRIPT_PATH}" ]] || die "script base não encontrado: ${FIX_SCRIPT_PATH}"
[[ -n "${ELIXIR_REF}" ]] || die "--elixir-ref não pode ser vazio"
[[ -n "${INSTALL_DIR}" ]] || die "--install-dir não pode ser vazio"
[[ -n "${LOG_FILE}" ]] || die "--log-file não pode ser vazio"

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

build_fix_args() {
  local args=(--elixir-ref "${ELIXIR_REF}" --install-dir "${INSTALL_DIR}")
  if [[ "${FORCE_REMOVE_PACKAGE}" == "1" ]]; then
    args+=(--force-remove-package)
  fi
  printf '%s\n' "${args[@]}"
}

execute_fix() {
  local -a fix_args
  mapfile -t fix_args < <(build_fix_args)

  log "executando ${FIX_SCRIPT_PATH}"
  log "log detalhado em ${LOG_FILE}"

  if [[ "$(id -u)" -eq 0 ]]; then
    bash "${FIX_SCRIPT_PATH}" "${fix_args[@]}" 2>&1 | tee "${LOG_FILE}"
  else
    sudo -E bash "${FIX_SCRIPT_PATH}" "${fix_args[@]}" 2>&1 | tee "${LOG_FILE}"
  fi
}

validate_final_state() {
  export PATH="${INSTALL_DIR}/bin:${PATH}"

  if [[ -f /etc/profile.d/elixir.sh ]]; then
    # shellcheck disable=SC1091
    source /etc/profile.d/elixir.sh || true
    export PATH="${INSTALL_DIR}/bin:${PATH}"
  fi

  hash -r

  command -v elixir >/dev/null 2>&1 || die "elixir não encontrado após correção"

  log "elixir path: $(command -v elixir)"
  log "elixir --version (primeira linha): $(elixir --version 2>&1 | head -n 1)"

  if ! elixir -e 'System.halt(if function_exported?(Code, :require_file, 2), do: 0, else: 1)'; then
    die "Code.require_file/2 não disponível após correção"
  fi

  log "validação concluída"
}

main() {
  assert_ec2_environment
  execute_fix
  validate_final_state
  log "correção aplicada com sucesso"
}

main "$@"
