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

CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD="${CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD:-0}"
CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR="${CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR:-0}"
CURL_WRAPPER_RELEASE_FALLBACK_REPOS="${CURL_WRAPPER_RELEASE_FALLBACK_REPOS:-elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn}"
CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK="${CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK:-0}"
CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES="${CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES:-1}"
CURL_WRAPPER_ACTIVE_PROXY=""
WRAPPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

is_zip_extension() {
  local value
  value="${1%%\?*}"
  value="${value%%#*}"
  [[ "${value}" == *.zip ]]
}

normalize_repo_slug() {
  printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]'
}

github_release_asset_slug() {
  local url
  url="${1%%\?*}"

  if [[ "${url}" =~ ^https://github\.com/([^/]+)/([^/]+)/releases/download/[^/]+/.+$ ]]; then
    printf '%s/%s\n' \
      "$(normalize_repo_slug "${BASH_REMATCH[1]}")" \
      "$(normalize_repo_slug "${BASH_REMATCH[2]}")"
    return 0
  fi

  return 1
}

is_restricted_release_repo_slug() {
  local slug candidate normalized_candidate
  slug="$(normalize_repo_slug "${1:-}")"
  [[ -n "${slug}" ]] || return 1

  IFS=',' read -r -a restricted_repos <<< "${CURL_WRAPPER_RELEASE_FALLBACK_REPOS}"
  for candidate in "${restricted_repos[@]}"; do
    normalized_candidate="$(normalize_repo_slug "${candidate}")"
    [[ -n "${normalized_candidate}" ]] || continue
    if [[ "${normalized_candidate}" == "${slug}" ]]; then
      return 0
    fi
  done

  return 1
}

is_restricted_release_asset_url() {
  local slug
  slug="$(github_release_asset_slug "${1:-}" 2>/dev/null || true)"
  [[ -n "${slug}" ]] || return 1
  is_restricted_release_repo_slug "${slug}"
}

resolve_proxy_config() {
  local proxy
  proxy="${CURL_WRAPPER_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${HTTPS_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${https_proxy:-}"
  [[ -n "${proxy}" ]] || proxy="${ALL_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${all_proxy:-}"
  [[ -n "${proxy}" ]] || proxy="${HTTP_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${http_proxy:-}"
  CURL_WRAPPER_ACTIVE_PROXY="${proxy}"
}

assert_non_zip_download_request() {
  local args_count index arg output_path url_path
  args_count=$#
  local -a args=("$@")
  local -a positional=()
  index=0
  while (( index < args_count )); do
    arg="${args[index]}"
    case "${arg}" in
      -o)
        (( index + 1 < args_count )) || return 1
        output_path="${args[index + 1]}"
        index=$((index + 2))
        ;;
      -o*)
        output_path="${arg#-o}"
        index=$((index + 1))
        ;;
      --output)
        (( index + 1 < args_count )) || return 1
        output_path="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --output=*)
        output_path="${arg#--output=}"
        index=$((index + 1))
        ;;
      --output-dir=*)
        index=$((index + 1))
        ;;
      --output-dir)
        index=$((index + 2))
        ;;
      --)
        index=$((index + 1))
        while (( index < args_count )); do
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

  if (( ${#positional[@]} > 0 )); then
    url_path="${positional[${#positional[@]} - 1]}"
    if is_zip_extension "${url_path}" && ! is_restricted_release_asset_url "${url_path}"; then
      die "download bloqueado para URL .zip: ${url_path}. Defina CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1 para permitir"
    fi
  fi

  if [[ -n "${output_path:-}" ]] && is_zip_extension "${output_path}" && ! is_restricted_release_asset_url "${url_path:-}"; then
    die "download bloqueado para saída .zip: ${output_path}. Defina CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1 para permitir"
  fi
}

normalize_curl_args_for_parser() {
  PARSED_ARGS=()
  local arg payload ch i len

  for arg in "$@"; do
    if [[ "${arg}" == --* ]] || [[ "${arg}" == "-" ]] || [[ "${arg}" != -* ]]; then
      PARSED_ARGS+=("${arg}")
      continue
    fi

    case "${arg}" in
      -o*|-A*|-H*|-d*|-X*|-F*|-T*|-x*)
        PARSED_ARGS+=("${arg}")
        continue
        ;;
    esac

    payload="${arg:1}"
    len="${#payload}"
    if [[ "${arg}" =~ ^-[fsS4kIL]+$ ]]; then
      for (( i = 0; i < len; i++ )); do
        ch="${payload:i:1}"
        PARSED_ARGS+=("-${ch}")
      done
      continue
    fi

    PARSED_ARGS+=("${arg}")
  done
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
CURL_FALLBACK_PROXY=""
CURL_FALLBACK_ALLOW_REDIRECTS="1"
CURL_FALLBACK_CAN_HANDLE="1"
CURL_FALLBACK_REMOTE_NAME="0"
CURL_FALLBACK_OUTPUT_DIR=""
CURL_FALLBACK_CREATE_DIRS="0"
CURL_FALLBACK_INSECURE="0"

parse_curl_arguments_for_python_fallback() {
  CURL_FALLBACK_URL=""
  CURL_FALLBACK_OUTPUT=""
  CURL_FALLBACK_USER_AGENT=""
  CURL_FALLBACK_CONNECT_TIMEOUT="20"
  CURL_FALLBACK_MAX_TIME="300"
  CURL_FALLBACK_HEADERS=""
  CURL_FALLBACK_PROXY=""
  CURL_FALLBACK_ALLOW_REDIRECTS="1"
  CURL_FALLBACK_CAN_HANDLE="1"
  CURL_FALLBACK_REMOTE_NAME="0"
  CURL_FALLBACK_OUTPUT_DIR=""
  CURL_FALLBACK_CREATE_DIRS="0"
  CURL_FALLBACK_INSECURE="0"

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
      -x)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_PROXY="${args[index + 1]}"
        index=$((index + 2))
        ;;
      -x*)
        CURL_FALLBACK_PROXY="${arg#-x}"
        index=$((index + 1))
        ;;
      --proxy)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_PROXY="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --proxy=*)
        CURL_FALLBACK_PROXY="${arg#--proxy=}"
        index=$((index + 1))
        ;;
      -O|--remote-name|--remote-name-all)
        CURL_FALLBACK_REMOTE_NAME="1"
        index=$((index + 1))
        ;;
      --output-dir)
        (( index + 1 < ${#args[@]} )) || return 1
        CURL_FALLBACK_OUTPUT_DIR="${args[index + 1]}"
        index=$((index + 2))
        ;;
      --output-dir=*)
        CURL_FALLBACK_OUTPUT_DIR="${arg#--output-dir=}"
        index=$((index + 1))
        ;;
      --create-dirs)
        CURL_FALLBACK_CREATE_DIRS="1"
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
      -k|--insecure)
        CURL_FALLBACK_INSECURE="1"
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
      -f|-s|-S|-4|--http1.1|--retry|--retry-delay|--retry-all-errors|--tlsv1.2)
        if [[ "${arg}" == "--retry" || "${arg}" == "--retry-delay" ]]; then
          (( index + 1 < ${#args[@]} )) || return 1
          index=$((index + 2))
        else
          index=$((index + 1))
        fi
        ;;
      -I|--head|-X|--request|-T|--upload-file|-F|--form|-d|--data|--data-binary|--data-raw|--data-urlencode|--compressed|--proxy-user)
        CURL_FALLBACK_CAN_HANDLE="0"
        return 0
        ;;
      --request=*|--data=*|--data-binary=*|--data-raw=*|--data-urlencode=*|--form=*|--proxy-user=*|--upload-file=*|--cacert=*|--cert=*|--key=*)
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
        CURL_FALLBACK_CAN_HANDLE="0"
        return 0
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

  if [[ -z "${CURL_FALLBACK_OUTPUT}" && "${CURL_FALLBACK_REMOTE_NAME}" == "1" ]]; then
    local basename_url
    basename_url="${CURL_FALLBACK_URL%%\?*}"
    basename_url="${basename_url##*/}"
    [[ -n "${basename_url}" ]] || basename_url="download.bin"
    CURL_FALLBACK_OUTPUT="${basename_url}"
  fi

  if [[ -n "${CURL_FALLBACK_OUTPUT_DIR}" && -n "${CURL_FALLBACK_OUTPUT}" ]]; then
    CURL_FALLBACK_OUTPUT="${CURL_FALLBACK_OUTPUT_DIR%/}/${CURL_FALLBACK_OUTPUT}"
  fi

  if [[ -z "${CURL_FALLBACK_OUTPUT}" ]]; then
    CURL_FALLBACK_CAN_HANDLE="0"
  fi
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

is_github_release_asset_url() {
  local url
  url="${1%%\?*}"
  [[ "${url}" =~ ^https://github\.com/[^/]+/[^/]+/releases/download/[^/]+/.+ ]]
}

should_skip_direct_release_download() {
  is_restricted_release_asset_url "${1:-}" && ! is_truthy "${CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK}"
}

parse_github_release_asset_url() {
  local url
  url="${1%%\?*}"

  if [[ "${url}" =~ ^https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)$ ]]; then
    GITHUB_RELEASE_OWNER="${BASH_REMATCH[1]}"
    GITHUB_RELEASE_REPO="${BASH_REMATCH[2]}"
    GITHUB_RELEASE_TAG="${BASH_REMATCH[3]}"
    GITHUB_RELEASE_ASSET="${BASH_REMATCH[4]}"
    GITHUB_RELEASE_SLUG="$(normalize_repo_slug "${GITHUB_RELEASE_OWNER}/${GITHUB_RELEASE_REPO}")"
    return 0
  fi

  return 1
}

download_url_with_python_fallback() {
  local url output_path create_dirs
  url="$1"
  output_path="$2"
  create_dirs="${3:-1}"

  (
    CURL_FALLBACK_URL="${url}"
    CURL_FALLBACK_OUTPUT="${output_path}"
    CURL_FALLBACK_USER_AGENT="${CURL_FALLBACK_USER_AGENT:-}"
    CURL_FALLBACK_CONNECT_TIMEOUT="${CURL_FALLBACK_CONNECT_TIMEOUT:-20}"
    CURL_FALLBACK_MAX_TIME="${CURL_FALLBACK_MAX_TIME:-300}"
    CURL_FALLBACK_HEADERS="${CURL_FALLBACK_HEADERS:-}"
    CURL_FALLBACK_PROXY="${CURL_FALLBACK_PROXY:-${CURL_WRAPPER_ACTIVE_PROXY:-}}"
    CURL_FALLBACK_ALLOW_REDIRECTS="1"
    CURL_FALLBACK_CREATE_DIRS="${create_dirs}"
    CURL_FALLBACK_INSECURE="${CURL_FALLBACK_INSECURE:-0}"
    download_with_python_requests
  )
}

download_release_asset_by_name() {
  local owner repo tag asset output_path
  owner="$1"
  repo="$2"
  tag="$3"
  asset="$4"
  output_path="$5"

  if command -v gh >/dev/null 2>&1; then
    mkdir -p "$(dirname "${output_path}")"
    if gh release download "${tag}" -R "${owner}/${repo}" -p "${asset}" -O "${output_path}" --clobber >/dev/null 2>&1; then
      return 0
    fi
  fi

  download_url_with_python_fallback \
    "https://github.com/${owner}/${repo}/releases/download/${tag}/${asset}" \
    "${output_path}"
}

download_source_tarball_for_tag() {
  local owner repo tag output_path
  owner="$1"
  repo="$2"
  tag="$3"
  output_path="$4"

  if command -v gh >/dev/null 2>&1; then
    mkdir -p "$(dirname "${output_path}")"
    if gh api \
      -H "Accept: application/vnd.github+json" \
      "/repos/${owner}/${repo}/tarball/${tag}" > "${output_path}" 2>/dev/null; then
      return 0
    fi
  fi

  download_url_with_python_fallback \
    "https://github.com/${owner}/${repo}/archive/refs/tags/${tag}.tar.gz" \
    "${output_path}"
}

download_with_gh_release() {
  command -v gh >/dev/null 2>&1 || return 1

  local url owner repo tag asset target_dir
  url="${CURL_FALLBACK_URL%%\?*}"
  if [[ "${url}" =~ ^https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)$ ]]; then
    owner="${BASH_REMATCH[1]}"
    repo="${BASH_REMATCH[2]}"
    tag="${BASH_REMATCH[3]}"
    asset="${BASH_REMATCH[4]}"
  else
    return 1
  fi

  if [[ -n "${CURL_FALLBACK_OUTPUT}" ]]; then
    target_dir="$(dirname "${CURL_FALLBACK_OUTPUT}")"
    if [[ "${CURL_FALLBACK_CREATE_DIRS}" == "1" ]]; then
      mkdir -p "${target_dir}"
    fi

    gh release download "${tag}" \
      -R "${owner}/${repo}" \
      -p "${asset}" \
      -O "${CURL_FALLBACK_OUTPUT}" \
      --clobber >/dev/null 2>&1
    return $?
  fi

  gh release download "${tag}" \
    -R "${owner}/${repo}" \
    -p "${asset}" \
    --clobber >/dev/null 2>&1
}

download_with_python_requests() {
  command -v python3 >/dev/null 2>&1 || return 1

  PYTHON_CURL_WRAPPER_URL="${CURL_FALLBACK_URL}" \
  PYTHON_CURL_WRAPPER_OUTPUT="${CURL_FALLBACK_OUTPUT}" \
  PYTHON_CURL_WRAPPER_USER_AGENT="${CURL_FALLBACK_USER_AGENT}" \
  PYTHON_CURL_WRAPPER_CONNECT_TIMEOUT="${CURL_FALLBACK_CONNECT_TIMEOUT}" \
  PYTHON_CURL_WRAPPER_MAX_TIME="${CURL_FALLBACK_MAX_TIME}" \
  PYTHON_CURL_WRAPPER_HEADERS="${CURL_FALLBACK_HEADERS}" \
  PYTHON_CURL_WRAPPER_PROXY="${CURL_FALLBACK_PROXY}" \
  PYTHON_CURL_WRAPPER_ALLOW_REDIRECTS="${CURL_FALLBACK_ALLOW_REDIRECTS}" \
  PYTHON_CURL_WRAPPER_CREATE_DIRS="${CURL_FALLBACK_CREATE_DIRS}" \
  PYTHON_CURL_WRAPPER_INSECURE="${CURL_FALLBACK_INSECURE}" \
  python3 - <<'PY'
import os
import sys
from typing import Dict
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

try:
    import requests
except Exception:
    requests = None


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
create_dirs = os.environ.get("PYTHON_CURL_WRAPPER_CREATE_DIRS", "0").strip().lower() in {"1", "true", "yes", "on"}
insecure = os.environ.get("PYTHON_CURL_WRAPPER_INSECURE", "0").strip().lower() in {"1", "true", "yes", "on"}
raw_proxy = os.environ.get("PYTHON_CURL_WRAPPER_PROXY", "").strip()
parsed_proxy = raw_proxy.strip() if raw_proxy else ""
proxies = {}
if parsed_proxy:
    proxies = {
        "http": parsed_proxy,
        "https": parsed_proxy,
    }

if not url:
    raise SystemExit(1)

headers = parse_headers(os.environ.get("PYTHON_CURL_WRAPPER_HEADERS", ""))
if user_agent and "User-Agent" not in headers:
    headers["User-Agent"] = user_agent
if "Accept" not in headers:
    headers["Accept"] = "*/*"

if output_path and create_dirs:
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

if requests is not None:
    timeout = (connect_timeout, max_time)
    request_kwargs = {
        "stream": True,
        "allow_redirects": allow_redirects,
        "headers": headers,
        "timeout": timeout,
    }
    if insecure:
        request_kwargs["verify"] = False
    if proxies:
        request_kwargs["proxies"] = proxies

    with requests.get(url, **request_kwargs) as response:
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
else:
    import ssl
    context = ssl.create_default_context()
    if insecure:
        context = ssl._create_unverified_context()
    proxy_handler = urllib_request.ProxyHandler(proxies)
    opener = urllib_request.build_opener(proxy_handler, urllib_request.HTTPSHandler(context=context), urllib_request.HTTPHandler())

    req = urllib_request.Request(url, headers=headers, method="GET")
    try:
        with opener.open(req, timeout=max_time) as response:
            if output_path:
                with open(output_path, "wb") as handle:
                    while True:
                        chunk = response.read(1024 * 64)
                        if not chunk:
                            break
                        handle.write(chunk)
            else:
                out = sys.stdout.buffer
                while True:
                    chunk = response.read(1024 * 64)
                    if not chunk:
                        break
                    out.write(chunk)
    except (HTTPError, URLError):
        raise
PY
}

has_explicit_proxy_arg() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      -x|--proxy|-x*|--proxy=*)
        return 0
        ;;
    esac
  done
  return 1
}

source "${WRAPPER_DIR}/lib/mason_release_engine.sh"

main() {
  local real_curl
  real_curl="$(resolve_real_curl)"
  local can_fallback=0
  local parsed_fallback=0
  local -a normalized_args=()
  local -a real_curl_env=()
  local -a real_curl_cmd=("${real_curl}")
  local proxy_from_env
  proxy_from_env=""

  resolve_proxy_config
  local resolved_proxy="${CURL_WRAPPER_ACTIVE_PROXY:-}"
  is_truthy "${CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD}" || assert_non_zip_download_request "$@"
  normalize_curl_args_for_parser "$@"
  normalized_args=("${PARSED_ARGS[@]}")

  if parse_curl_arguments_for_python_fallback "${normalized_args[@]}"; then
    parsed_fallback=1
    if [[ "${CURL_FALLBACK_CAN_HANDLE}" == "1" ]]; then
      can_fallback=1
    fi
  fi

  if [[ $# -eq 0 ]]; then
    exec "${real_curl}"
  fi

  if should_skip_direct_release_download "${CURL_FALLBACK_URL:-}"; then
    log "release corporativamente restrita detectada; pulando curl direto para ${CURL_FALLBACK_URL}"

    if handle_smart_release_asset; then
      exit 0
    fi

    if download_with_gh_release; then
      if [[ -n "${CURL_FALLBACK_OUTPUT}" && ! -f "${CURL_FALLBACK_OUTPUT}" ]]; then
        die "fallback gh não gerou arquivo esperado: ${CURL_FALLBACK_OUTPUT}"
      fi

      log "fallback gh release concluído com sucesso."
      exit 0
    fi

    die "download direto de release bloqueado para ${CURL_FALLBACK_URL}. Garanta gh autenticado ou ajuste CURL_WRAPPER_RELEASE_FALLBACK_REPOS/CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK."
  fi

  if [[ -z "${CURL_FALLBACK_PROXY:-}" ]] && [[ -n "${resolved_proxy}" ]] && ! has_explicit_proxy_arg "$@"; then
    log "proxy ativo para curl: ${resolved_proxy}"
    CURL_FALLBACK_PROXY="${resolved_proxy}"
    real_curl_cmd+=(--proxy "${resolved_proxy}")
    real_curl_env=(
      "HTTPS_PROXY=${resolved_proxy}" "https_proxy=${resolved_proxy}"
      "HTTP_PROXY=${resolved_proxy}" "http_proxy=${resolved_proxy}"
      "ALL_PROXY=${resolved_proxy}" "all_proxy=${resolved_proxy}"
    )
    proxy_from_env="1"
  fi

  local curl_exit
  set +e
  if (( proxy_from_env == 1 )); then
    env "${real_curl_env[@]}" "${real_curl_cmd[@]}" "$@"
  else
    "${real_curl_cmd[@]}" "$@"
  fi
  curl_exit=$?
  set -e
  if [[ "${curl_exit}" -eq 0 ]]; then
    exit 0
  fi

  if is_truthy "${CURL_WRAPPER_STRICT:-0}"; then
    exit "${curl_exit}"
  fi

  if (( parsed_fallback == 0 )) && parse_curl_arguments_for_python_fallback "${normalized_args[@]}"; then
    parsed_fallback=1
  fi
  if (( parsed_fallback == 0 )) || (( can_fallback == 0 )); then
    exit "${curl_exit}"
  fi

  if [[ "${curl_exit}" -eq 60 ]] && is_truthy "${CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR}" && [[ "${CURL_FALLBACK_INSECURE}" != "1" ]]; then
    log "retry em fallback com verificação TLS desativada por política de recuperação de certificado"
    CURL_FALLBACK_INSECURE="1"
  fi

  if [[ "${CURL_FALLBACK_CAN_HANDLE}" != "1" ]]; then
    exit "${curl_exit}"
  fi

  if [[ -n "${CURL_FALLBACK_PROXY}" ]]; then
    log "proxy ativo no fallback python: ${CURL_FALLBACK_PROXY}"
  fi

  if is_github_release_asset_url "${CURL_FALLBACK_URL}"; then
    if is_truthy "${CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES}"; then
      log "curl falhou com exit=${curl_exit}; tentando engine dinâmica de release para ${CURL_FALLBACK_URL}"
      if handle_smart_release_asset; then
        exit 0
      fi
    fi

    log "curl falhou com exit=${curl_exit}; tentando fallback com gh release para ${CURL_FALLBACK_URL}"
    if download_with_gh_release; then
      if [[ -n "${CURL_FALLBACK_OUTPUT}" && ! -f "${CURL_FALLBACK_OUTPUT}" ]]; then
        log "fallback gh não gerou arquivo esperado: ${CURL_FALLBACK_OUTPUT}"
      else
        log "fallback gh release concluído com sucesso."
        exit 0
      fi
    fi
  fi

  log "curl falhou com exit=${curl_exit}; tentando fallback python para ${CURL_FALLBACK_URL}"
  if download_with_python_requests; then
    if [[ -n "${CURL_FALLBACK_OUTPUT}" && ! -f "${CURL_FALLBACK_OUTPUT}" ]]; then
      log "fallback não gerou arquivo esperado: ${CURL_FALLBACK_OUTPUT}"
      exit "${curl_exit}"
    fi
    log "fallback python concluído com sucesso."
    exit 0
  fi

  exit "${curl_exit}"
}

main "$@"
