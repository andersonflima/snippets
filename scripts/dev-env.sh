#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[dev-env] erro: bash é obrigatório para executar este script\n' >&2
  exit 1
}

set -euo pipefail

SCRIPT_PATH="$0"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${SCRIPT_PATH}")" && pwd)"
COMMON_HELPER="${SCRIPT_DIR}/install/lib/common.sh"

# shellcheck disable=SC1090
. "${COMMON_HELPER}"

RESTRICTED_SCRIPT_NAME="dev-env"
SCRIPT_PATH="$(restricted_resolve_script_path "$0")"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${SCRIPT_PATH}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/install/setup_restricted_dev_env.sh"
RESET_SCRIPT="${SCRIPT_DIR}/install/reset_restricted_dev_env.sh"
REINSTALL_SCRIPT="${SCRIPT_DIR}/install/reinstall_wrappers.sh"
VALIDATE_SCRIPT="${SCRIPT_DIR}/install/validate_wrappers.sh"
WRAPPER_ENV_FILE="${HOME}/.config/wrapper-envs.sh"
DEFAULT_SHELL_RC="$(restricted_default_shell_rc)"
STEP=0
START_TS="$(date +%s 2>/dev/null || printf '%s' 0)"
SHOW_SUMMARY=1

usage() {
  cat <<'USAGE'
Uso:
  scripts/dev-env.sh [opções-de-setup]
  scripts/dev-env.sh setup [opções-de-setup]
  scripts/dev-env.sh reset [opções-de-reset]
  scripts/dev-env.sh reinstall-wrappers [opções]
  scripts/dev-env.sh validate

Sem subcomando, executa o fluxo completo: reset limpo + setup + bootstrap.

Subcomandos:
  setup               Executa reset prévio e configuração completa.
  reset               Remove wrappers/envs/estado gerenciado.
  reinstall-wrappers  Reinstala apenas wrappers e regenera envs.
  validate            Valida wrappers instalados.
  help                Mostra esta ajuda.

Para opções detalhadas do setup:
  scripts/dev-env.sh setup --help
USAGE
}

run_step() {
  local description
  description="$1"
  shift

  STEP=$((STEP + 1))
  restricted_log "PASSO ${STEP}: ${description}"
  "$@"
}

is_help_request() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      -h|--help)
        return 0
        ;;
    esac
  done
  return 1
}

should_apply_shell_rc_by_default() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --apply-shell-rc|--no-shell-rc|--shell-rc|--shell-rc=*)
        return 1
        ;;
    esac
    shift
  done
  return 0
}

resolve_shell_rc_target() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --shell-rc)
        if [[ -n "${2:-}" ]]; then
          printf '%s\n' "$2"
        else
          printf '%s\n' "${DEFAULT_SHELL_RC}"
        fi
        return 0
        ;;
      --shell-rc=*)
        printf '%s\n' "${1#--shell-rc=}"
        return 0
        ;;
    esac
    shift
  done

  printf '%s\n' "${DEFAULT_SHELL_RC}"
}

assert_internal_scripts() {
  [[ -f "${SETUP_SCRIPT}" ]] || restricted_die "script interno ausente: ${SETUP_SCRIPT}"
  [[ -f "${RESET_SCRIPT}" ]] || restricted_die "script interno ausente: ${RESET_SCRIPT}"
  [[ -f "${REINSTALL_SCRIPT}" ]] || restricted_die "script interno ausente: ${REINSTALL_SCRIPT}"
  [[ -f "${VALIDATE_SCRIPT}" ]] || restricted_die "script interno ausente: ${VALIDATE_SCRIPT}"
}

load_current_env_if_available() {
  [[ -f "${WRAPPER_ENV_FILE}" ]] || return 0
  restricted_log "aplicando env-file atual na shell ativa"
  # shellcheck disable=SC1090
  . "${WRAPPER_ENV_FILE}"
  rehash 2>/dev/null || true
  hash -r 2>/dev/null || true
}

run_setup_flow() {
  local shell_rc_target

  if is_help_request "$@"; then
    SHOW_SUMMARY=0
    restricted_run_bash_script "${SETUP_SCRIPT}" "$@"
    return $?
  fi

  if should_apply_shell_rc_by_default "$@"; then
    set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
  fi

  shell_rc_target="$(resolve_shell_rc_target "$@")"
  run_step "limpando ambiente legado em memória" restricted_sanitize_wrapper_env
  run_step "removendo wrapper legado do brew" restricted_remove_legacy_brew_wrapper_installation
  run_step "reset inicial com shell rc ${shell_rc_target}" \
    restricted_run_bash_script "${RESET_SCRIPT}" --shell-rc "${shell_rc_target}"
  run_step "limpando ambiente após reset" restricted_sanitize_wrapper_env
  run_step "executando setup completo" restricted_run_bash_script "${SETUP_SCRIPT}" "$@"
  load_current_env_if_available
}

run_reset_flow() {
  if is_help_request "$@"; then
    SHOW_SUMMARY=0
    restricted_run_bash_script "${RESET_SCRIPT}" "$@"
    return $?
  fi

  run_step "limpando ambiente legado em memória" restricted_sanitize_wrapper_env
  run_step "executando reset" restricted_run_bash_script "${RESET_SCRIPT}" "$@"
}

run_reinstall_wrappers_flow() {
  if is_help_request "$@"; then
    SHOW_SUMMARY=0
    restricted_run_bash_script "${REINSTALL_SCRIPT}" "$@"
    return $?
  fi

  run_step "reinstalando wrappers" restricted_run_bash_script "${REINSTALL_SCRIPT}" "$@"
  load_current_env_if_available
}

run_validate_flow() {
  run_step "validando wrappers" restricted_run_bash_script "${VALIDATE_SCRIPT}" "$@"
}

main() {
  local command_name end_ts
  assert_internal_scripts

  command_name="${1:-setup}"
  case "${command_name}" in
    setup|configure)
      shift || true
      run_setup_flow "$@"
      ;;
    reset)
      shift || true
      run_reset_flow "$@"
      ;;
    reinstall-wrappers|repair-wrappers)
      shift || true
      run_reinstall_wrappers_flow "$@"
      ;;
    validate)
      shift || true
      run_validate_flow "$@"
      ;;
    help|-h|--help)
      SHOW_SUMMARY=0
      usage
      ;;
    -*)
      run_setup_flow "$command_name" "$@"
      ;;
    *)
      restricted_die "subcomando inválido: ${command_name}"
      ;;
  esac

  if [[ "${SHOW_SUMMARY}" == "1" ]]; then
    end_ts="$(date +%s 2>/dev/null || printf '%s' 0)"
    restricted_log "finalizado em $((end_ts - START_TS))s"
  fi
}

main "$@"
