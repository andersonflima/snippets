#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[wget-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

resolve_script_path() {
  local script_path script_dir link_target
  script_path="$1"

  while [[ -L "${script_path}" ]]; do
    script_dir="$(cd "$(dirname "${script_path}")" && pwd)"
    link_target="$(readlink "${script_path}")"
    if [[ "${link_target}" == /* ]]; then
      script_path="${link_target}"
    else
      script_path="${script_dir}/${link_target}"
    fi
  done

  printf '%s\n' "${script_path}"
}

WRAPPER_DIR="$(cd "$(dirname "$(resolve_script_path "${BASH_SOURCE[0]}")")" && pwd)"
WGET_WRAPPER_PROXY="${WGET_WRAPPER_PROXY:-${HTTPS_PROXY:-${https_proxy:-${ALL_PROXY:-${all_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}}}}"
WGET_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR="${WGET_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR:-1}"
WGET_WRAPPER_LAST_COMMAND_STDERR=""

is_proxy_auth_error_log() {
  local output
  output="${1:-}"
  printf '%s\n' "${output}" | grep -Eiq '(^|[[:space:]])407([[:space:]]|$)|proxy[ -]authentication|required|proxy authent(i|y)cation|Proxy-Authenticate|proxy error|Proxy Error'
}

has_explicit_proxy_arg() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      --proxy|--proxy=*|--proxy-user|--proxy-user=*|--proxy-password|--proxy-password=*|--http-proxy|--http-proxy=*|--https-proxy|--https-proxy=*)
        return 0
        ;;
    esac
  done
  return 1
}

run_command_with_stderr_capture() {
  local -a command
  local stderr_file
  local command_status
  local errexit_was_set=0

  if [[ $- == *e* ]]; then
    errexit_was_set=1
  fi

  command=("$@")
  stderr_file="$(mktemp -t wget-wrapper-stderr-XXXXXX)"
  WGET_WRAPPER_LAST_COMMAND_STDERR=""

  set +e
  "${command[@]}" 2>"${stderr_file}"
  command_status=$?
  if (( errexit_was_set == 1 )); then
    set -e
  else
    set +e
  fi

  if [[ -f "${stderr_file}" ]]; then
    WGET_WRAPPER_LAST_COMMAND_STDERR="$(cat "${stderr_file}")"
    rm -f "${stderr_file}"
  fi

  return "${command_status}"
}

canonicalize_binary_path() {
  local path dir base
  path="${1:-}"
  [[ -n "${path}" ]] || return 1

  if command -v realpath >/dev/null 2>&1; then
    realpath "${path}" 2>/dev/null && return 0
  fi

  [[ -e "${path}" ]] || return 1
  dir="$(cd "$(dirname "${path}")" 2>/dev/null && pwd -P)" || return 1
  base="$(basename "${path}")"
  printf '%s/%s\n' "${dir}" "${base}"
}

binary_paths_match() {
  local left right
  left="$(canonicalize_binary_path "${1:-}" 2>/dev/null || true)"
  right="$(canonicalize_binary_path "${2:-}" 2>/dev/null || true)"
  [[ -n "${left}" && -n "${right}" && "${left}" == "${right}" ]]
}

should_skip_real_wget_candidate() {
  local candidate self_path
  candidate="${1:-}"
  self_path="${2:-}"

  [[ -n "${candidate}" ]] || return 0
  [[ -x "${candidate}" ]] || return 0

  if binary_paths_match "${candidate}" "${self_path}"; then
    return 0
  fi

  return 1
}

resolve_real_wget() {
  local self_path
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  if [[ -n "${WGET_WRAPPER_REAL_WGET:-}" ]]; then
    [[ -x "${WGET_WRAPPER_REAL_WGET}" ]] || die "WGET_WRAPPER_REAL_WGET inválido: ${WGET_WRAPPER_REAL_WGET}"
    if should_skip_real_wget_candidate "${WGET_WRAPPER_REAL_WGET}" "${self_path}"; then
      die "WGET_WRAPPER_REAL_WGET não pode apontar para o wrapper instalado: ${WGET_WRAPPER_REAL_WGET}"
    fi
    printf '%s\n' "${WGET_WRAPPER_REAL_WGET}"
    return 0
  fi

  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    should_skip_real_wget_candidate "${candidate}" "${self_path}" && continue
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF2
$(which -a wget 2>/dev/null || true)
EOF2

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  wget [opções-suportadas] <url>

Opções suportadas:
  -O <arquivo>
  --output-document=<arquivo>
  -P <diretório>
  --directory-prefix=<diretório>
  --user-agent=<valor>
  --header=<valor>
  --connect-timeout=<seg>
  --timeout=<seg>
  --tries=<n>
  --no-check-certificate
  -q, --quiet, --no-verbose
  -h, --help
USAGE
}

WGET_URL=""
WGET_OUTPUT=""
WGET_OUTPUT_DIR=""
WGET_CREATE_DIRS="0"
WGET_USER_AGENT=""
WGET_CONNECT_TIMEOUT="20"
WGET_MAX_TIME="300"
WGET_TRIES="2"
WGET_HEADERS=()
WGET_INSECURE="0"
WGET_CAN_HANDLE="1"
WGET_LOG_FILE=""

is_github_release_zip_url() {
  local url
  url="${1%%\?*}"
  [[ "${url}" =~ ^https://github\.com/[^/]+/[^/]+/releases/download/[^/]+/.+\.zip$ ]]
}

is_github_url() {
  local url
  url="${1%%\?*}"
  [[ "${url}" =~ ^https://(github\.com|api\.github\.com|codeload\.github\.com)/.+$ ]]
}

is_mason_registry_api_url() {
  local url
  url="${1%%\?*}"
  [[ "${url}" =~ ^https://api\.mason-registry\.dev/.+$ ]]
}

resolve_curl_wrapper() {
  local candidate
  candidate="${WGET_WRAPPER_CURL_BIN:-${WRAPPER_DIR}/curl}"
  [[ -x "${candidate}" ]] || return 1
  printf '%s\n' "${candidate}"
}

should_delegate_to_curl_wrapper() {
  [[ "${WGET_CAN_HANDLE}" == "1" ]] || return 1
  [[ -n "${WGET_URL}" ]] || return 1

  if is_truthy "${WGET_WRAPPER_ALWAYS_USE_CURL:-0}"; then
    return 0
  fi

  if is_mason_registry_api_url "${WGET_URL}" || is_github_url "${WGET_URL}"; then
    return 0
  fi

  is_truthy "${CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES:-1}" || return 1
  is_github_release_zip_url "${WGET_URL}"
}

download_with_curl_wrapper() {
  local curl_wrapper header
  local -a curl_env
  local -a curl_cmd

  curl_wrapper="$(resolve_curl_wrapper)" || return 1

  curl_cmd=(
    "${curl_wrapper}"
    -fL
    --connect-timeout "${WGET_CONNECT_TIMEOUT}"
    --max-time "${WGET_MAX_TIME}"
    --retry "${WGET_TRIES}"
    --retry-delay "1"
    --retry-all-errors
  )

  if [[ "${WGET_OUTPUT}" != "-" ]]; then
    mkdir -p "$(dirname "${WGET_OUTPUT}")"
    curl_cmd+=(-o "${WGET_OUTPUT}")
  fi

  if [[ -n "${WGET_USER_AGENT}" ]]; then
    curl_cmd+=(-A "${WGET_USER_AGENT}")
  fi

  if [[ -n "${WGET_WRAPPER_PROXY}" ]]; then
    curl_cmd+=(--proxy "${WGET_WRAPPER_PROXY}")
  fi

  if [[ "${WGET_INSECURE}" == "1" ]]; then
    curl_cmd+=(-k)
  fi

  for header in "${WGET_HEADERS[@]+"${WGET_HEADERS[@]}"}"; do
    curl_cmd+=(-H "${header}")
  done

  curl_cmd+=("${WGET_URL}")

  curl_env=()
  if [[ "${WGET_URL}" == *.zip* || "${WGET_OUTPUT}" == *.zip ]]; then
    curl_env+=("CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1")
  fi

  if (( ${#curl_env[@]} > 0 )); then
    env "${curl_env[@]}" "${curl_cmd[@]}"
    return $?
  fi

  "${curl_cmd[@]}"
}

parse_args() {
  local positional=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -O|--output-document)
        [[ $# -ge 2 ]] || return 1
        WGET_OUTPUT="$2"
        shift 2
        ;;
      -o)
        [[ $# -ge 2 ]] || return 1
        WGET_LOG_FILE="$2"
        shift 2
        ;;
      -o*)
        WGET_LOG_FILE="${1#-o}"
        shift
        ;;
      --output-document=*)
        WGET_OUTPUT="${1#--output-document=}"
        shift
        ;;
      -P|--directory-prefix)
        [[ $# -ge 2 ]] || return 1
        WGET_OUTPUT_DIR="$2"
        WGET_CREATE_DIRS="1"
        shift 2
        ;;
      --directory-prefix=*)
        WGET_OUTPUT_DIR="${1#--directory-prefix=}"
        WGET_CREATE_DIRS="1"
        shift
        ;;
      --user-agent)
        [[ $# -ge 2 ]] || return 1
        WGET_USER_AGENT="$2"
        shift 2
        ;;
      --user-agent=*)
        WGET_USER_AGENT="${1#--user-agent=}"
        shift
        ;;
      --header)
        [[ $# -ge 2 ]] || return 1
        WGET_HEADERS+=("$2")
        shift 2
        ;;
      --header=*)
        WGET_HEADERS+=("${1#--header=}")
        shift
        ;;
      --connect-timeout)
        [[ $# -ge 2 ]] || return 1
        WGET_CONNECT_TIMEOUT="$2"
        shift 2
        ;;
      --connect-timeout=*)
        WGET_CONNECT_TIMEOUT="${1#--connect-timeout=}"
        shift
        ;;
      --timeout)
        [[ $# -ge 2 ]] || return 1
        WGET_MAX_TIME="$2"
        shift 2
        ;;
      -T)
        [[ $# -ge 2 ]] || return 1
        WGET_MAX_TIME="$2"
        shift 2
        ;;
      -T*)
        WGET_MAX_TIME="${1#-T}"
        shift
        ;;
      --timeout=*)
        WGET_MAX_TIME="${1#--timeout=}"
        shift
        ;;
      --tries)
        [[ $# -ge 2 ]] || return 1
        WGET_TRIES="$2"
        shift 2
        ;;
      --tries=*)
        WGET_TRIES="${1#--tries=}"
        shift
        ;;
      --retry-connrefused|--no-verbose|-q|--quiet|-nv)
        shift
        ;;
      --no-check-certificate)
        WGET_INSECURE="1"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      --)
        shift
        while [[ $# -gt 0 ]]; do
          positional+=("$1")
          shift
        done
        ;;
      -*)
        WGET_CAN_HANDLE="0"
        return 0
        ;;
      *)
        positional+=("$1")
        shift
        ;;
    esac
  done

  if (( ${#positional[@]} >= 1 )); then
    WGET_URL="${positional[${#positional[@]} - 1]}"
  fi
  [[ -n "${WGET_URL}" ]] || return 1

  if [[ -z "${WGET_OUTPUT}" ]]; then
    local basename_url
    basename_url="${WGET_URL%%\?*}"
    basename_url="${basename_url##*/}"
    [[ -n "${basename_url}" ]] || basename_url="download.bin"
    WGET_OUTPUT="${basename_url}"
  fi

  if [[ -n "${WGET_OUTPUT_DIR}" ]]; then
    WGET_OUTPUT="${WGET_OUTPUT_DIR%/}/${WGET_OUTPUT}"
  fi
}

run_local_wget() {
  local real_wget local_exit_code
  local -a proxy_env=()
  real_wget="$1"
  shift
  proxy_env=(
    "HTTPS_PROXY=${WGET_WRAPPER_PROXY}"
    "HTTP_PROXY=${WGET_WRAPPER_PROXY}"
    "ALL_PROXY=${WGET_WRAPPER_PROXY}"
    "https_proxy=${WGET_WRAPPER_PROXY}"
    "http_proxy=${WGET_WRAPPER_PROXY}"
    "all_proxy=${WGET_WRAPPER_PROXY}"
  )

  if [[ -n "${WGET_WRAPPER_PROXY}" ]]; then
    if run_command_with_stderr_capture env "${proxy_env[@]}" "${real_wget}" "$@"; then
      return 0
    fi
    local_exit_code=$?
    if is_truthy "${WGET_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR}" && \
      ! has_explicit_proxy_arg "$@" && \
      is_proxy_auth_error_log "${WGET_WRAPPER_LAST_COMMAND_STDERR}"; then
      log "wget falhou com erro de autenticação de proxy (407). Tentando novamente sem proxy"
      if run_command_with_stderr_capture "${real_wget}" "$@"; then
        return 0
      fi
      local_exit_code=$?
    fi
    if [[ -n "${WGET_WRAPPER_LAST_COMMAND_STDERR}" ]]; then
      printf '%s\n' "${WGET_WRAPPER_LAST_COMMAND_STDERR}" >&2
    fi
    return "${local_exit_code}"
  fi

  if run_command_with_stderr_capture "${real_wget}" "$@"; then
    return 0
  fi
  local_exit_code=$?
  if [[ -n "${WGET_WRAPPER_LAST_COMMAND_STDERR}" ]]; then
    printf '%s\n' "${WGET_WRAPPER_LAST_COMMAND_STDERR}" >&2
  fi
  return "${local_exit_code}"
}

main() {
  local real_wget
  real_wget="$(resolve_real_wget || true)"

  if [[ $# -eq 0 ]]; then
    if [[ -n "${real_wget}" ]]; then
      exec "${real_wget}"
    fi
    usage
    exit 1
  fi

  parse_args "$@" || die "argumentos wget não suportados para wrapper local"

  if should_delegate_to_curl_wrapper; then
    log "delegando download via curl wrapper: ${WGET_URL}"
    if download_with_curl_wrapper; then
      exit 0
    fi
    log "curl wrapper falhou; seguindo fluxo padrão do wget"
  fi

  [[ -n "${real_wget}" ]] || die "wget real não encontrado para execução local"

  if [[ "${WGET_CAN_HANDLE}" == "1" ]]; then
    if ! run_local_wget "${real_wget}" "$@"; then
      exit $?
    fi
    exit 0
  fi

  run_local_wget "${real_wget}" "$@"
  exit $?
}

main "$@"
