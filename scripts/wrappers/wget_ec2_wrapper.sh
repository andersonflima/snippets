#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[wget-ec2-wrapper] %s\n' "$*" >&2
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

WRAPPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WGET_WRAPPER_USE_EC2="${WGET_WRAPPER_USE_EC2:-${WRAPPERS_VIA_EC2_ENABLED:-0}}"
WGET_WRAPPER_EC2_ALL_URLS="${WGET_WRAPPER_EC2_ALL_URLS:-${WRAPPERS_VIA_EC2_ALL_URLS:-1}}"
WGET_WRAPPER_EC2_HELPER="${WGET_WRAPPER_EC2_HELPER:-${WRAPPER_DIR}/fetch-url-via-ec2}"
WGET_WRAPPER_EC2_REQUIRED="${WGET_WRAPPER_EC2_REQUIRED:-0}"
WGET_WRAPPER_EC2_PROXY="${WGET_WRAPPER_EC2_PROXY:-${WRAPPERS_VIA_EC2_PROXY:-}}"
WGET_WRAPPER_PROXY="${WGET_WRAPPER_PROXY:-${HTTPS_PROXY:-${https_proxy:-${ALL_PROXY:-${all_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}}}}"

resolve_real_wget() {
  if [[ -n "${WGET_WRAPPER_REAL_WGET:-}" ]]; then
    [[ -x "${WGET_WRAPPER_REAL_WGET}" ]] || die "WGET_WRAPPER_REAL_WGET inválido: ${WGET_WRAPPER_REAL_WGET}"
    printf '%s\n' "${WGET_WRAPPER_REAL_WGET}"
    return 0
  fi

  local self_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" != "${self_path}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done <<EOF
$(which -a wget 2>/dev/null || true)
EOF

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  wget [opções-suportadas] <url>

Opções suportadas no modo EC2:
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
WGET_HEADERS=()
WGET_INSECURE="0"
WGET_CAN_HANDLE="1"

parse_args() {
  local positional=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -O|--output-document)
        [[ $# -ge 2 ]] || return 1
        WGET_OUTPUT="$2"
        shift 2
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
      --timeout=*)
        WGET_MAX_TIME="${1#--timeout=}"
        shift
        ;;
      --tries)
        [[ $# -ge 2 ]] || return 1
        shift 2
        ;;
      --tries=*)
        shift
        ;;
      --retry-connrefused|--no-verbose|-q|--quiet)
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

should_use_ec2_backend_for_url() {
  local url url_without_query
  url="$1"

  if ! is_truthy "${WGET_WRAPPER_USE_EC2}"; then
    return 1
  fi
  [[ -n "${url}" ]] || return 1
  if [[ ! -x "${WGET_WRAPPER_EC2_HELPER}" ]]; then
    if is_truthy "${WGET_WRAPPER_EC2_REQUIRED}"; then
      die "helper do backend EC2 não encontrado/executável: ${WGET_WRAPPER_EC2_HELPER}"
    fi
    return 1
  fi

  if is_truthy "${WGET_WRAPPER_EC2_ALL_URLS}"; then
    return 0
  fi

  url_without_query="${url%%\?*}"
  case "${url_without_query}" in
    https://github.com/*|https://codeload.github.com/*|https://api.github.com/*|https://hex.pm/*|https://repo.hex.pm/*|https://builds.hex.pm/*)
      return 0
      ;;
  esac

  return 1
}

download_with_ec2_backend() {
  local -a helper_cmd=("${WGET_WRAPPER_EC2_HELPER}" --url "${WGET_URL}" --output "${WGET_OUTPUT}")
  local header

  if [[ "${WGET_CREATE_DIRS}" == "1" ]]; then
    helper_cmd+=(--create-dirs)
  fi
  if [[ -n "${WGET_USER_AGENT}" ]]; then
    helper_cmd+=(--user-agent "${WGET_USER_AGENT}")
  fi
  if [[ -n "${WGET_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${WGET_WRAPPER_EC2_PROXY}")
  fi
  if [[ "${WGET_INSECURE}" == "1" ]]; then
    helper_cmd+=(--insecure)
  fi
  if [[ -n "${WGET_CONNECT_TIMEOUT}" ]]; then
    helper_cmd+=(--connect-timeout "${WGET_CONNECT_TIMEOUT}")
  fi
  if [[ -n "${WGET_MAX_TIME}" ]]; then
    helper_cmd+=(--max-time "${WGET_MAX_TIME}")
  fi
  for header in "${WGET_HEADERS[@]}"; do
    helper_cmd+=(--header "${header}")
  done

  "${helper_cmd[@]}"
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

  parse_args "$@"

  if [[ "${WGET_CAN_HANDLE}" == "1" ]] && should_use_ec2_backend_for_url "${WGET_URL}"; then
    log "backend selecionado: ec2 (${WGET_URL})"
    if download_with_ec2_backend; then
      exit 0
    fi
    if is_truthy "${WGET_WRAPPER_EC2_REQUIRED}"; then
      die "backend EC2 falhou para ${WGET_URL} e o fallback local está desabilitado"
    fi
    log "backend EC2 falhou; seguindo com wget local"
  fi

  [[ -n "${real_wget}" ]] || die "wget real não encontrado para fallback local"

  if [[ -n "${WGET_WRAPPER_PROXY}" ]]; then
    HTTPS_PROXY="${WGET_WRAPPER_PROXY}" \
    HTTP_PROXY="${WGET_WRAPPER_PROXY}" \
    ALL_PROXY="${WGET_WRAPPER_PROXY}" \
    https_proxy="${WGET_WRAPPER_PROXY}" \
    http_proxy="${WGET_WRAPPER_PROXY}" \
    all_proxy="${WGET_WRAPPER_PROXY}" \
    exec "${real_wget}" "$@"
  fi

  exec "${real_wget}" "$@"
}

main "$@"
