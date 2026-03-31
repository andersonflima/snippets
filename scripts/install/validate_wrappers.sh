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
  sh scripts/validate_wrappers.sh [opções]

Opções:
  --env-file <arquivo>   Env-file dos wrappers. Padrão: $HOME/.config/wrapper-envs.sh
  --strict-brew          Exige wrapper do brew ativo.
  -h, --help             Mostra esta ajuda.
USAGE
}

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
STRICT_BREW="0"
FAILURES=0
WARNINGS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
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

validate_required_wrapper() {
  local name expected_bin expected_dir current_bin
  name="$1"
  expected_bin="$2"
  expected_dir="$3"

  current_bin="$(command -v "${name}" 2>/dev/null || true)"
  if [[ -z "${current_bin}" ]]; then
    fail "${name}: comando não encontrado no PATH"
    return 0
  fi
  if [[ "${current_bin}" != "${expected_bin}" ]] && ! path_is_under "${current_bin}" "${expected_dir}"; then
    fail "${name}: wrapper não está ativo no PATH (atual: ${current_bin}, esperado: ${expected_bin})"
    return 0
  fi
  ok "${name}: wrapper ativo (${current_bin})"
}

validate_optional_wrapper() {
  local name expected_bin expected_dir current_bin
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
  if [[ "${current_bin}" != "${expected_bin}" ]] && ! path_is_under "${current_bin}" "${expected_dir}"; then
    if [[ "${STRICT_BREW}" == "1" ]]; then
      fail "${name}: wrapper não está ativo no PATH (atual: ${current_bin}, esperado: ${expected_bin})"
    else
      warn "${name}: wrapper não está ativo (atual: ${current_bin})"
    fi
    return 0
  fi
  ok "${name}: wrapper ativo (${current_bin})"
}

validate_real_binary_env() {
  local var_name value
  var_name="$1"
  value="${!var_name:-}"
  if [[ -z "${value}" ]]; then
    fail "${var_name}: não definido no env-file"
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

validate_fail_open_policy() {
  local ec2_enabled curl_required wget_required git_required
  ec2_enabled="${WRAPPERS_VIA_EC2_ENABLED:-0}"
  curl_required="${CURL_WRAPPER_EC2_REQUIRED:-0}"
  wget_required="${WGET_WRAPPER_EC2_REQUIRED:-0}"
  git_required="${GIT_ZIP_WRAPPER_EC2_REQUIRED:-0}"

  if [[ "${ec2_enabled}" == "1" ]]; then
    if [[ "${curl_required}" == "1" ]]; then
      warn "CURL_WRAPPER_EC2_REQUIRED=1 (falha remota pode quebrar Mason/brew)"
    else
      ok "CURL_WRAPPER_EC2_REQUIRED=0 (fallback local ativo)"
    fi
    if [[ "${wget_required}" == "1" ]]; then
      warn "WGET_WRAPPER_EC2_REQUIRED=1 (falha remota pode quebrar downloads)"
    else
      ok "WGET_WRAPPER_EC2_REQUIRED=0 (fallback local ativo)"
    fi
    if [[ "${git_required}" == "1" ]]; then
      warn "GIT_ZIP_WRAPPER_EC2_REQUIRED=1 (falha remota pode quebrar clone/fetch)"
    else
      ok "GIT_ZIP_WRAPPER_EC2_REQUIRED=0 (fallback local ativo)"
    fi
  else
    ok "WRAPPERS_VIA_EC2_ENABLED=0 (backend remoto desabilitado)"
  fi
}

validate_required_wrapper curl "${HOME}/.local/share/curl-python-wrapper/bin/curl" "${HOME}/.local/share/curl-python-wrapper/bin"
validate_optional_wrapper wget "${HOME}/.local/share/curl-python-wrapper/bin/wget" "${HOME}/.local/share/curl-python-wrapper/bin"
validate_required_wrapper git "${HOME}/.local/share/git-zip-wrapper/bin/git" "${HOME}/.local/share/git-zip-wrapper/bin"
validate_optional_wrapper brew "${HOME}/.local/share/homebrew-install-wrapper/bin/brew" "${HOME}/.local/share/homebrew-install-wrapper/bin"

validate_real_binary_env CURL_WRAPPER_REAL_CURL
validate_optional_real_binary_env WGET_WRAPPER_REAL_WGET
validate_real_binary_env GIT_ZIP_WRAPPER_REAL_GIT
if [[ -n "${BREW_WRAPPER_REAL_BREW:-}" ]]; then
  validate_real_binary_env BREW_WRAPPER_REAL_BREW
else
  warn "BREW_WRAPPER_REAL_BREW não definido (normal quando brew real não existe no host)"
fi

validate_fail_open_policy

printf '\nResumo: %s falhas, %s avisos\n' "${FAILURES}" "${WARNINGS}"

if [[ "${FAILURES}" -gt 0 ]]; then
  exit 1
fi
