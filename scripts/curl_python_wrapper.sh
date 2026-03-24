#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[curl-python-wrapper] %s\n' "$*" >&2
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

resolve_real_curl() {
  if [[ -n "${CURL_WRAPPER_REAL_CURL:-}" ]]; then
    [[ -x "${CURL_WRAPPER_REAL_CURL}" ]] || die "CURL_WRAPPER_REAL_CURL inválido: ${CURL_WRAPPER_REAL_CURL}"
    printf '%s\n' "${CURL_WRAPPER_REAL_CURL}"
    return
  fi

  local self_path shell_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  shell_path="$(command -v -p curl 2>/dev/null || true)"
  if [[ -n "${shell_path}" && "${shell_path}" != "${self_path}" ]]; then
    printf '%s\n' "${shell_path}"
    return
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" != "${self_path}" ]]; then
      printf '%s\n' "${candidate}"
      return
    fi
  done < <(which -a curl 2>/dev/null || true)

  [[ -x "/usr/bin/curl" ]] || die "não foi possível localizar curl real. Defina CURL_WRAPPER_REAL_CURL."
  printf '%s\n' "/usr/bin/curl"
}

CURL_FALLBACK_URL=""
CURL_FALLBACK_OUTPUT=""
CURL_FALLBACK_USER_AGENT=""
CURL_FALLBACK_CONNECT_TIMEOUT="20"
CURL_FALLBACK_MAX_TIME="300"
CURL_FALLBACK_HEADERS=""
CURL_FALLBACK_ALLOW_REDIRECTS="1"
CURL_FALLBACK_CAN_HANDLE="1"

parse_curl_arguments_for_python_fallback() {
  CURL_FALLBACK_URL=""
  CURL_FALLBACK_OUTPUT=""
  CURL_FALLBACK_USER_AGENT=""
  CURL_FALLBACK_CONNECT_TIMEOUT="20"
  CURL_FALLBACK_MAX_TIME="300"
  CURL_FALLBACK_HEADERS=""
  CURL_FALLBACK_ALLOW_REDIRECTS="1"
  CURL_FALLBACK_CAN_HANDLE="1"

  local args=("$@")
  local index=0
  local positional=()

  while (( index < ${#args[@]} )); do
    local arg="${args[index]}"
    case "${arg}" in
      -o)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_OUTPUT="${args[index + 1]}"
        index=$((index + 2))
        ;;
      -o*)
        CURL_FALLBACK_OUTPUT="${arg#-o}"
        index=$((index + 1))
        ;;
      --output)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_OUTPUT="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --output=*)
        CURL_FALLBACK_OUTPUT="${arg#--output=}"
        index=$((index + 1))
        ;;
      -A|--user-agent)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_USER_AGENT="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --user-agent=*)
        CURL_FALLBACK_USER_AGENT="${arg#--user-agent=}"
        index=$((index + 1))
        ;;
      -H|--header)
        (( index + 1 < ${#args[@]} )) || return 1
        append_header_for_python "${args[index + 1]}"
        index=$((index + 2))
        ;;
      --header=*)
        append_header_for_python "${arg#--header=}"
        index=$((index + 1))
        ;;
      -L|--location)
        CURL_FALLBACK_ALLOW_REDIRECTS="1"
        index=$((index + 1))
        ;;
      --connect-timeout)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_CONNECT_TIMEOUT="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --connect-timeout=*)
        CURL_FALLBACK_CONNECT_TIMEOUT="${arg#--connect-timeout=}"
        index=$((index + 1))
        ;;
      --max-time)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_MAX_TIME="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --max-time=*)
        CURL_FALLBACK_MAX_TIME="${arg#--max-time=}"
        index=$((index + 1))
        ;;
      -f|-s|-S|-k|-4|--http1.1|--retry|--retry-delay|--retry-all-errors|--tlsv1.2)
        if [[ "${arg}" == "--retry" || "${arg}" == "--retry-delay" ]]; then
          (( index + 1 < ${#args[@]} )) || return 1
          index=$((index + 2))
        else
          index=$((index + 1))
        fi
        ;;
      -I|--head|-X|--request|-T|--upload-file|-F|--form|-d|--data|--data-binary|--data-raw|--data-urlencode|--compressed|--proxy|--proxy-user)
        CURL_FALLBACK_CAN_HANDLE="0"
        return 0
        ;;
      --request=*|--data=*|--data-binary=*|--data-raw=*|--data-urlencode=*|--form=*|--proxy=*|--proxy-user=*|--upload-file=*|--cacert=*|--cert=*|--key=*)
        CURL_FALLBACK_CAN_HANDLE="0"
        return 0
        ;;
      --)
        index=$((index + 1))
        while (( index < ${#args[@]} )); do
          positional+=("${args[index]}")
          index=$((index + 1))
        done
        ;;
      -*)
        index=$((index + 1))
        ;;
      *)
        positional+=("${arg}")
        index=$((index + 1))
        ;;
    esac
  done

  if (( ${#positional[@]} >= 1 )); then
    CURL_FALLBACK_URL="${positional[${#positional[@]}-1]}"
  fi
  [[ -n "${CURL_FALLBACK_URL}" ]] || return 1
  return 0
}

append_header_for_python() {
  local header
  header="$1"
  if [[ -z "${CURL_FALLBACK_HEADERS}" ]]; then
    CURL_FALLBACK_HEADERS="${header}"
    return
  fi
  CURL_FALLBACK_HEADERS="${CURL_FALLBACK_HEADERS}"$'\n'"${header}"
}

download_with_python_requests() {
  command -v python3 >/dev/null 2>&1 || return 1

  PYTHON_CURL_WRAPPER_URL="${CURL_FALLBACK_URL}" \
  PYTHON_CURL_WRAPPER_OUTPUT="${CURL_FALLBACK_OUTPUT}" \
  PYTHON_CURL_WRAPPER_USER_AGENT="${CURL_FALLBACK_USER_AGENT}" \
  PYTHON_CURL_WRAPPER_CONNECT_TIMEOUT="${CURL_FALLBACK_CONNECT_TIMEOUT}" \
  PYTHON_CURL_WRAPPER_MAX_TIME="${CURL_FALLBACK_MAX_TIME}" \
  PYTHON_CURL_WRAPPER_HEADERS="${CURL_FALLBACK_HEADERS}" \
  PYTHON_CURL_WRAPPER_ALLOW_REDIRECTS="${CURL_FALLBACK_ALLOW_REDIRECTS}" \
  python3 - <<'PY'
import os
import sys
from typing import Dict

try:
    import requests
except Exception as exc:
    print(f"[curl-python-wrapper] requests indisponível: {exc}", file=sys.stderr)
    raise SystemExit(1)


def parse_headers(raw: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    if not raw:
        return parsed
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key:
            parsed[key] = value
    return parsed


url = os.environ.get("PYTHON_CURL_WRAPPER_URL", "").strip()
output_path = os.environ.get("PYTHON_CURL_WRAPPER_OUTPUT", "").strip()
user_agent = os.environ.get("PYTHON_CURL_WRAPPER_USER_AGENT", "").strip()
connect_timeout = float(os.environ.get("PYTHON_CURL_WRAPPER_CONNECT_TIMEOUT", "20") or "20")
max_time = float(os.environ.get("PYTHON_CURL_WRAPPER_MAX_TIME", "300") or "300")
allow_redirects = os.environ.get("PYTHON_CURL_WRAPPER_ALLOW_REDIRECTS", "1").strip().lower() in {"1", "true", "yes", "on"}

if not url:
    raise SystemExit(1)

headers = parse_headers(os.environ.get("PYTHON_CURL_WRAPPER_HEADERS", ""))
if user_agent and "User-Agent" not in headers:
    headers["User-Agent"] = user_agent
if "Accept" not in headers:
    headers["Accept"] = "*/*"

timeout = (connect_timeout, max_time)
with requests.get(url, stream=True, allow_redirects=allow_redirects, headers=headers, timeout=timeout) as response:
    response.raise_for_status()
    if output_path:
        with open(output_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    handle.write(chunk)
    else:
        out = sys.stdout.buffer
        for chunk in response.iter_content(chunk_size=1024 * 64):
            if chunk:
                out.write(chunk)
PY
}

main() {
  local real_curl
  real_curl="$(resolve_real_curl)"

  if [[ $# -eq 0 ]]; then
    exec "${real_curl}"
  fi

  if "${real_curl}" "$@"; then
    exit 0
  fi
  local curl_exit=$?

  if is_truthy "${CURL_WRAPPER_STRICT:-0}"; then
    exit "${curl_exit}"
  fi

  if ! parse_curl_arguments_for_python_fallback "$@"; then
    exit "${curl_exit}"
  fi
  if [[ "${CURL_FALLBACK_CAN_HANDLE}" != "1" ]]; then
    exit "${curl_exit}"
  fi

  log "curl falhou com exit=${curl_exit}; tentando fallback com python requests para ${CURL_FALLBACK_URL}"
  if download_with_python_requests; then
    log "fallback python requests concluído com sucesso."
    exit 0
  fi

  exit "${curl_exit}"
}

main "$@"
