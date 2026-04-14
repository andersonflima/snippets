#!/bin/sh
set -eu

VERBOSE=1
if [ "${RESTRICTED_DEV_CONFIGURE_VERBOSE:-}" = "0" ]; then
  VERBOSE=0
fi
STEP=0
START_TS="$(date +%s 2>/dev/null || printf '%s' 0)"

configure_log() {
  configure_ts="$(date +%H:%M:%S 2>/dev/null || printf '%s' "??:??:??")"
  printf '[configure] %s %s\n' "${configure_ts}" "$*" >&2
}

configure_debug() {
  if [ "${VERBOSE}" = "1" ]; then
    configure_log "$*"
  fi
  return 0
}

configure_fail() {
  configure_log "ERRO: $*"
}

run_step() {
  description="$1"
  shift
  STEP=$((STEP + 1))
  configure_log "PASSO ${STEP}: ${description}"
  configure_debug "comando: $*"
  set +e
  "$@"
  script_rc="$?"
  set -e
  if [ "${script_rc}" -ne 0 ]; then
    configure_fail "PASSO ${STEP} falhou (código ${script_rc}): ${description}"
    exit "${script_rc}"
  fi
  configure_debug "PASSO ${STEP} concluído com sucesso"
}

format_args() {
  arg_index=1
  for arg in "$@"; do
    value="$(printf '%s' "${arg}" | sed 's/[\\"]/\\\\&/g' 2>/dev/null || printf '%s' "${arg}")"
    if [ "${arg_index}" -eq 1 ]; then
      printf '"%s"' "${value}"
      arg_index=0
    else
      printf ' "%s"' "${value}"
    fi
  done
}

run_script_with_bash_preference() {
  script_path="$1"
  shift

  configure_debug "local de execução: ${script_path}"
  if [ "${VERBOSE}" = "1" ]; then
    if command -v bash >/dev/null 2>&1; then
      configure_debug "executor: bash $(command -v bash)"
      configure_debug "argumentos: $(format_args "$@")"
    else
      configure_debug "executor: sh (bash ausente)"
      configure_debug "argumentos: $(format_args "$@")"
    fi
  fi

  if command -v bash >/dev/null 2>&1; then
    bash "${script_path}" "$@"
    return $?
  fi

  sh "${script_path}" "$@"
}

resolve_script_path() {
  current_path="$1"

  if ! command -v readlink >/dev/null 2>&1; then
    printf '%s\n' "${current_path}"
    return 0
  fi

  while [ -L "${current_path}" ]; do
    current_dir="$(CDPATH= cd -- "$(dirname -- "${current_path}")" && pwd)"
    link_target="$(readlink "${current_path}")"
    case "${link_target}" in
      /*)
        current_path="${link_target}"
        ;;
      *)
        current_path="${current_dir}/${link_target}"
        ;;
    esac
  done

  printf '%s\n' "${current_path}"
}

SCRIPT_PATH="$(resolve_script_path "$0")"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${SCRIPT_PATH}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/install/setup_restricted_dev_env.sh"
RESET_SCRIPT="${SCRIPT_DIR}/install/reset_restricted_dev_env.sh"
LEGACY_BREW_WRAPPER_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
LEGACY_BREW_WRAPPER_ROOT="${HOME}/.local/share/homebrew-install-wrapper"
CURL_WRAPPER_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
GIT_WRAPPER_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
WRAPPER_ENV_FILE="${HOME}/.config/wrapper-envs.sh"

[ -f "${SETUP_SCRIPT}" ] || {
  printf '[configure] script interno ausente: %s\n' "${SETUP_SCRIPT}" >&2
  printf '[configure] script resolvido: %s\n' "${SCRIPT_PATH}" >&2
  exit 1
}
[ -f "${RESET_SCRIPT}" ] || {
  printf '[configure] script interno ausente: %s\n' "${RESET_SCRIPT}" >&2
  printf '[configure] script resolvido: %s\n' "${SCRIPT_PATH}" >&2
  exit 1
}

resolve_target_shell_name() {
  target_shell="${RESTRICTED_DEV_ENV_TARGET_SHELL:-zsh}"
  target_shell="${target_shell##*/}"

  case "${target_shell}" in
    zsh|bash|fish|sh)
      printf '%s\n' "${target_shell}"
      ;;
    *)
      printf '%s\n' "zsh"
      ;;
  esac
}

resolve_default_shell_rc() {
  shell_name="$(resolve_target_shell_name)"

  case "${shell_name}" in
    fish)
      printf '%s\n' "${HOME}/.config/fish/config.fish"
      ;;
    zsh)
      printf '%s\n' "${HOME}/.zshrc"
      ;;
    bash)
      printf '%s\n' "${HOME}/.bashrc"
      ;;
    *)
      printf '%s\n' "${HOME}/.profile"
      ;;
  esac
}

DEFAULT_SHELL_RC="$(resolve_default_shell_rc)"

sanitize_current_wrapper_env() {
  old_path="${PATH-}"
  new_path=""
  old_ifs="${IFS}"

  unset CURL 2>/dev/null || true
  unset WGET 2>/dev/null || true
  unset GIT 2>/dev/null || true
  unset BREW 2>/dev/null || true

  unset CURL_WRAPPER_REAL_CURL 2>/dev/null || true
  unset WGET_WRAPPER_REAL_WGET 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_REAL_GIT 2>/dev/null || true
  unset BREW_WRAPPER_ENABLED 2>/dev/null || true
  unset BREW_WRAPPER_REAL_BREW 2>/dev/null || true
  unset BREW_WRAPPER_CURL_BIN 2>/dev/null || true
  unset BREW_WRAPPER_GIT_BIN 2>/dev/null || true
  unset BREW_WRAPPER_NO_AUTO_UPDATE 2>/dev/null || true
  unset CURL_WRAPPER_PROXY 2>/dev/null || true
  unset WGET_WRAPPER_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CURL_CACERT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ARCHIVE_FORMAT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CLONE_ORDER 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_LFS_MODE 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS 2>/dev/null || true
  unset CURL_WRAPPER_MASON_SEED_DIR 2>/dev/null || true
  unset CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR 2>/dev/null || true

  IFS=':'
  for path_entry in ${old_path}; do
    [ -n "${path_entry}" ] || continue
    case "${path_entry}" in
      "${LEGACY_BREW_WRAPPER_DIR}"|"${CURL_WRAPPER_DIR}"|"${GIT_WRAPPER_DIR}"|"${HOME}"/.local/share/mix-*-wrapper/bin|"${HOME}"/.local/share/nvim-*-wrapper/bin)
        continue
        ;;
    esac

    if [ -z "${new_path}" ]; then
      new_path="${path_entry}"
    else
      new_path="${new_path}:${path_entry}"
    fi
  done
  IFS="${old_ifs}"

  if [ -n "${new_path}" ]; then
    PATH="${new_path}"
  else
    PATH="/usr/bin:/bin:/usr/sbin:/sbin"
  fi
  export PATH
  configure_debug "PATH normalizado após limpeza de wrappers legados"
  configure_debug "PATH atual (resumo): $(printf '%s\n' "${PATH}" | awk -F: '{ for (i=1; i<=NF; i++) printf "%s ", $i; print "" }' | sed 's/[[:space:]]*$//')"
}

remove_legacy_brew_wrapper_installation() {
  [ -d "${LEGACY_BREW_WRAPPER_ROOT}" ] || return 0
  configure_log "Removendo estrutura de wrapper brew legado: ${LEGACY_BREW_WRAPPER_ROOT}"
  rm -rf "${LEGACY_BREW_WRAPPER_ROOT}"
}

should_apply_shell_rc_by_default() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --apply-shell-rc|--no-shell-rc|--shell-rc)
        return 1
        ;;
    esac
    shift
  done
  return 0
}

is_help_request() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h|--help)
        return 0
        ;;
    esac
    shift
  done
  return 1
}

resolve_shell_rc_target() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --shell-rc)
        if [ "${2:-}" != "" ]; then
          printf '%s' "$2"
        else
          printf '%s' "${DEFAULT_SHELL_RC}"
        fi
        return 0
        ;;
      --shell-rc=*)
        printf '%s' "${1#--shell-rc=}"
        return 0
        ;;
    esac
    shift
  done

  printf '%s' "${DEFAULT_SHELL_RC}"
}

run_full_reset_before_setup() {
  configure_log "Iniciando etapa de limpeza completa antes do setup"
  shell_rc_target="$(resolve_shell_rc_target "$@")"
  configure_debug "shell rc alvo resolvido: ${shell_rc_target}"

  sanitize_current_wrapper_env
  remove_legacy_brew_wrapper_installation
  run_step "reset inicial (sem aplicar estado externo) com target shell rc ${shell_rc_target}" \
    run_script_with_bash_preference "${RESET_SCRIPT}" --shell-rc "${shell_rc_target}"
  sanitize_current_wrapper_env
  remove_legacy_brew_wrapper_installation
}

[ -f "${SETUP_SCRIPT}" ] || {
  configure_fail "script interno ausente: ${SETUP_SCRIPT}"
  printf '[configure] script resolvido: %s\n' "${SCRIPT_PATH}" >&2
  exit 1
}
[ -f "${RESET_SCRIPT}" ] || {
  configure_fail "script interno ausente: ${RESET_SCRIPT}"
  printf '[configure] script resolvido: %s\n' "${SCRIPT_PATH}" >&2
  exit 1
}

configure_log "Início do configure.sh"
configure_debug "shell alvo default: ${DEFAULT_SHELL_RC}"
configure_debug "script path: ${SCRIPT_PATH}"
configure_debug "script dir: ${SCRIPT_DIR}"
configure_debug "setup interno: ${SETUP_SCRIPT}"
configure_debug "reset interno: ${RESET_SCRIPT}"
if [ "${VERBOSE}" = "1" ]; then
  configure_debug "modo detalhado: ligado"
else
  configure_debug "modo detalhado: desligado"
fi

if is_help_request "$@"; then
  configure_log "Help solicitado; delegando para setup_restricted_dev_env.sh"
  run_script_with_bash_preference "${SETUP_SCRIPT}" "$@"
  exit $?
fi

if [ "${1:-}" != "" ] && [ "${1#-}" = "$1" ]; then
  configure_fail "parâmetro posicional não é mais suportado: $1"
  exit 1
fi

if should_apply_shell_rc_by_default "$@"; then
  configure_debug "adicionando padrão --apply-shell-rc e --shell-rc"
  set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
fi

run_step "reset completo de ambiente anterior" run_full_reset_before_setup "$@"
run_step "executando setup principal" run_script_with_bash_preference "${SETUP_SCRIPT}" "$@"

if [ -f "${WRAPPER_ENV_FILE}" ]; then
  configure_log "Aplicando env-file atual na shell ativa para efeito imediato"
  . "${WRAPPER_ENV_FILE}"
  rehash 2>/dev/null || true
  hash -r 2>/dev/null || true
  if [ "${VERBOSE}" = "1" ]; then
    configure_debug "wrappers carregados no ambiente corrente"
    configure_debug "PATH atual (resumo): $(printf '%s\n' "${PATH}" | awk -F: '{ for (i=1; i<=NF; i++) printf "%s ", $i; print "" }' | sed 's/[[:space:]]*$//')"
    configure_debug "CURL_WRAPPER_REAL_CURL=${CURL_WRAPPER_REAL_CURL:-não definido}"
    configure_debug "GIT_ZIP_WRAPPER_REAL_GIT=${GIT_ZIP_WRAPPER_REAL_GIT:-não definido}"
    configure_debug "WGET_WRAPPER_REAL_WGET=${WGET_WRAPPER_REAL_WGET:-não definido}"
  fi
fi

configure_log "configure.sh finalizado"
configure_log "Resumo de execução:"
configure_log " - passos executados: ${STEP}"
configure_log " - arquivo gerado: ${WRAPPER_ENV_FILE}"
configure_log " - shell rc gerenciado: ${DEFAULT_SHELL_RC}"
END_TS="$(date +%s 2>/dev/null || printf '%s' 0)"
configure_log " - duração: $((END_TS - START_TS))s"
