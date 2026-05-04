#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[validate-wrappers] erro: bash é obrigatório para validar os wrappers\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[validate-wrappers] %s\n' "$*" >&2
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/install/validate_wrappers.sh [opções]

Opções:
  --env-file <arquivo>   Env-file dos wrappers. Padrão: $HOME/.config/wrapper-envs.sh
  --current-shell        Valida o shell atual sem carregar o env-file persistido.
  --strict-brew          Exige wrapper do brew ativo.
  -h, --help             Mostra esta ajuda.
USAGE
}

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
CURRENT_SHELL_ONLY="0"
STRICT_BREW="0"
FAILURES=0
WARNINGS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --current-shell)
      CURRENT_SHELL_ONLY="1"
      shift
      ;;
    --strict-brew)
      STRICT_BREW="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "erro: parâmetro inválido: $1"
      exit 1
      ;;
  esac
done

if [[ "${CURRENT_SHELL_ONLY}" == "0" ]]; then
  if [[ ! -f "${ENV_FILE}" ]]; then
    log "erro: env-file não encontrado: ${ENV_FILE}"
    exit 1
  fi

  set +u
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set -u

  rehash 2>/dev/null || true
  hash -r 2>/dev/null || true
fi

ok() {
  printf 'OK   %s\n' "$*"
}

warn() {
  printf 'WARN %s\n' "$*"
  WARNINGS=$((WARNINGS + 1))
}

fail() {
  printf 'FAIL %s\n' "$*"
  FAILURES=$((FAILURES + 1))
}

env_scope_label() {
  if [[ "${CURRENT_SHELL_ONLY}" == "1" ]]; then
    printf '%s\n' "ambiente atual"
    return 0
  fi

  printf '%s\n' "env-file"
}

path_is_under() {
  local value base
  value="$1"
  base="$2"
  case "${value}" in
    "${base}"|"${base}/"*)
      return 0
      ;;
  esac
  return 1
}

resolve_command_target_path() {
  local candidate dir target
  candidate="$1"
  [[ -n "${candidate}" ]] || return 1

  while [[ -L "${candidate}" ]]; do
    target="$(readlink "${candidate}" 2>/dev/null || true)"
    [[ -n "${target}" ]] || break
    if [[ "${target}" == /* ]]; then
      candidate="${target}"
      continue
    fi
    dir="$(dirname "${candidate}")"
    candidate="${dir}/${target}"
  done

  printf '%s\n' "${candidate}"
}

validate_required_wrapper() {
  local name expected_bin expected_dir current_bin resolved_bin
  name="$1"
  expected_bin="$2"
  expected_dir="$3"

  current_bin="$(command -v "${name}" 2>/dev/null || true)"
  if [[ -z "${current_bin}" ]]; then
    fail "${name}: comando não encontrado no PATH"
    return 0
  fi
  resolved_bin="$(resolve_command_target_path "${current_bin}" || true)"
  if [[ "${current_bin}" != "${expected_bin}" ]] &&
    ! path_is_under "${current_bin}" "${expected_dir}" &&
    [[ "${resolved_bin}" != "${expected_bin}" ]] &&
    ! path_is_under "${resolved_bin}" "${expected_dir}"; then
    fail "${name}: wrapper não está ativo no PATH (atual: ${current_bin}, esperado: ${expected_bin})"
    return 0
  fi
  ok "${name}: wrapper ativo (${current_bin})"
}

validate_optional_wrapper() {
  local name expected_bin expected_dir current_bin resolved_bin
  name="$1"
  expected_bin="$2"
  expected_dir="$3"

  current_bin="$(command -v "${name}" 2>/dev/null || true)"
  if [[ -z "${current_bin}" ]]; then
    if [[ "${STRICT_BREW}" == "1" ]]; then
      fail "${name}: comando não encontrado no PATH"
    else
      warn "${name}: comando não encontrado (ignorado em modo não estrito)"
    fi
    return 0
  fi
  resolved_bin="$(resolve_command_target_path "${current_bin}" || true)"
  if [[ "${current_bin}" != "${expected_bin}" ]] &&
    ! path_is_under "${current_bin}" "${expected_dir}" &&
    [[ "${resolved_bin}" != "${expected_bin}" ]] &&
    ! path_is_under "${resolved_bin}" "${expected_dir}"; then
    if [[ "${STRICT_BREW}" == "1" ]]; then
      fail "${name}: wrapper não está ativo no PATH (atual: ${current_bin}, esperado: ${expected_bin})"
    else
      warn "${name}: wrapper não está ativo (atual: ${current_bin})"
    fi
    return 0
  fi
  ok "${name}: wrapper ativo (${current_bin})"
}

brew_wrapper_is_disabled() {
  [[ "${BREW_WRAPPER_ENABLED:-1}" == "0" ]]
}

validate_disabled_brew_wrapper() {
  local current_bin legacy_dir
  legacy_dir="${HOME}/.local/share/homebrew-install-wrapper/bin"
  current_bin="$(command -v brew 2>/dev/null || true)"

  if [[ -z "${current_bin}" ]]; then
    ok "brew: wrapper desabilitado; comando não está presente no PATH"
    return 0
  fi

  if path_is_under "${current_bin}" "${legacy_dir}"; then
    fail "brew: wrapper desabilitado, mas o PATH ainda resolve para ${current_bin}"
    return 0
  fi

  ok "brew: wrapper desabilitado; binário real ativo (${current_bin})"
}

validate_real_binary_env() {
  local var_name value scope_label
  var_name="$1"
  value="${!var_name:-}"
  scope_label="$(env_scope_label)"
  if [[ -z "${value}" ]]; then
    fail "${var_name}: não definido no ${scope_label}"
    return 0
  fi
  if [[ ! -x "${value}" ]]; then
    fail "${var_name}: caminho inválido ou não executável (${value})"
    return 0
  fi
  ok "${var_name}: ${value}"
}

validate_optional_real_binary_env() {
  local var_name value
  var_name="$1"
  value="${!var_name:-}"
  if [[ -z "${value}" ]]; then
    warn "${var_name} não definido (opcional neste host)"
    return 0
  fi
  if [[ ! -x "${value}" ]]; then
    fail "${var_name}: caminho inválido ou não executável (${value})"
    return 0
  fi
  ok "${var_name}: ${value}"
}

validate_curl_wrapper_homebrew_contract() {
  local wrapper_bin resolved_real_curl
  wrapper_bin="${HOME}/.local/share/curl-python-wrapper/bin/curl"

  if [[ ! -x "${wrapper_bin}" ]]; then
    fail "curl: wrapper não encontrado para validar contrato com Homebrew (${wrapper_bin})"
    return 0
  fi

  resolved_real_curl="$("${wrapper_bin}" --homebrew=print-path 2>/dev/null || true)"
  if [[ -z "${resolved_real_curl}" ]]; then
    fail "curl: wrapper não respondeu ao contrato --homebrew=print-path"
    return 0
  fi

  if [[ "${resolved_real_curl}" != "${CURL_WRAPPER_REAL_CURL}" ]]; then
    fail "curl: --homebrew=print-path retornou ${resolved_real_curl}, esperado ${CURL_WRAPPER_REAL_CURL}"
    return 0
  fi

  ok "curl: contrato --homebrew=print-path OK (${resolved_real_curl})"
}

validate_local_policy() {
  local clone_order force_local_downloads lfs_mode archive_format remote_git_fallback
  local allow_zip_download wget_always_use_curl
  clone_order="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
  force_local_downloads="${GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS:-1}"
  lfs_mode="${GIT_ZIP_WRAPPER_LFS_MODE:-local}"
  archive_format="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
  remote_git_fallback="${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"
  allow_zip_download="${CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD:-1}"
  wget_always_use_curl="${WGET_WRAPPER_ALWAYS_USE_CURL:-1}"

  case "${clone_order}" in
    local-first)
      ok "GIT_ZIP_WRAPPER_CLONE_ORDER=local-first"
      ;;
    *)
      fail "GIT_ZIP_WRAPPER_CLONE_ORDER=${clone_order} (esperado: local-first)"
      ;;
  esac

  if [[ "${archive_format}" != "zip" ]]; then
    fail "GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=${archive_format} (esperado: zip)"
  else
    ok "GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip"
  fi

  if [[ "${remote_git_fallback}" != "0" && "${remote_git_fallback}" != "1" ]]; then
    fail "GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=${remote_git_fallback} (esperado: 0|1)"
  else
    ok "GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=${remote_git_fallback}"
  fi

  if [[ "${force_local_downloads}" != "1" ]]; then
    fail "GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=${force_local_downloads} (esperado: 1)"
  else
    ok "GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=1"
  fi

  if [[ "${lfs_mode}" != "local" ]]; then
    fail "GIT_ZIP_WRAPPER_LFS_MODE=${lfs_mode} (esperado: local)"
  else
    ok "GIT_ZIP_WRAPPER_LFS_MODE=local"
  fi

  if [[ "${allow_zip_download}" != "1" ]]; then
    fail "CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=${allow_zip_download} (esperado: 1)"
  else
    ok "CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1"
  fi

  if [[ "${wget_always_use_curl}" != "1" ]]; then
    fail "WGET_WRAPPER_ALWAYS_USE_CURL=${wget_always_use_curl} (esperado: 1)"
  else
    ok "WGET_WRAPPER_ALWAYS_USE_CURL=1"
  fi
}

validate_required_wrapper curl "${HOME}/.local/share/curl-python-wrapper/bin/curl" "${HOME}/.local/share/curl-python-wrapper/bin"
validate_optional_wrapper wget "${HOME}/.local/share/curl-python-wrapper/bin/wget" "${HOME}/.local/share/curl-python-wrapper/bin"
validate_required_wrapper git "${HOME}/.local/share/git-zip-wrapper/bin/git" "${HOME}/.local/share/git-zip-wrapper/bin"
if brew_wrapper_is_disabled; then
  validate_disabled_brew_wrapper
else
  validate_optional_wrapper brew "${HOME}/.local/share/homebrew-install-wrapper/bin/brew" "${HOME}/.local/share/homebrew-install-wrapper/bin"
fi

validate_real_binary_env CURL_WRAPPER_REAL_CURL
validate_curl_wrapper_homebrew_contract
validate_optional_real_binary_env WGET_WRAPPER_REAL_WGET
validate_real_binary_env GIT_ZIP_WRAPPER_REAL_GIT
if brew_wrapper_is_disabled; then
  ok "BREW_WRAPPER_REAL_BREW desabilitado por configuração"
elif [[ -n "${BREW_WRAPPER_REAL_BREW:-}" ]]; then
  validate_real_binary_env BREW_WRAPPER_REAL_BREW
else
  warn "BREW_WRAPPER_REAL_BREW não definido (normal quando brew real não existe no host)"
fi

validate_local_policy

printf '\nResumo: %s falhas, %s avisos\n' "${FAILURES}" "${WARNINGS}"

if [[ "${FAILURES}" -gt 0 ]]; then
  exit 1
fi
