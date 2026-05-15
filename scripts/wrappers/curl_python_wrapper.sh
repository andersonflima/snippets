#!/usr/bin/env bash
set -euo pipefail

SELF_PATH="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/$(basename -- "$0")"

resolve_real_curl() {
  if [[ -n "${CURL_WRAPPER_REAL_CURL:-}" ]]; then
    printf '%s\n' "${CURL_WRAPPER_REAL_CURL}"
    return
  fi

  local curl_path
  curl_path="$(command -v curl 2>/dev/null || true)"
  if [[ -z "${curl_path}" ]]; then
    printf '%s\n' "curl"
    return
  fi

  if [[ "${curl_path}" == "${SELF_PATH}" ]]; then
    if [[ -n "${CURL_WRAPPER_FALLBACK_REAL_CURL:-}" ]]; then
      printf '%s\n' "${CURL_WRAPPER_FALLBACK_REAL_CURL}"
      return
    fi
    printf '%s\n' "/usr/bin/curl"
    return
  fi

  printf '%s\n' "${curl_path}"
}

REAL_CURL="$(resolve_real_curl)"

extract_url() {
  local token
  for token in "$@"; do
    case "${token}" in
      http://*|https://*)
        printf '%s\n' "${token}"
        return 0
        ;;
    esac
  done
  return 1
}

extract_output_path() {
  local i=1
  while [[ ${i} -le $# ]]; do
    local token="${!i}"
    case "${token}" in
      -o|--output)
        i=$((i + 1))
        if [[ ${i} -le $# ]]; then
          printf '%s\n' "${!i}"
          return 0
        fi
        return 1
        ;;
      --output=*)
        printf '%s\n' "${token#--output=}"
        return 0
        ;;
    esac
    i=$((i + 1))
  done
  return 1
}

replace_url_in_args() {
  local old_url="$1"
  local new_url="$2"
  shift 2

  local replaced=0
  local token
  for token in "$@"; do
    if [[ ${replaced} -eq 0 && "${token}" == "${old_url}" ]]; then
      printf '%s\0' "${new_url}"
      replaced=1
    else
      printf '%s\0' "${token}"
    fi
  done
}

run_with_url() {
  local old_url="$1"
  local new_url="$2"
  shift 2

  local -a updated_args=()
  while IFS= read -r -d '' arg; do
    updated_args+=("${arg}")
  done < <(replace_url_in_args "${old_url}" "${new_url}" "$@")

  "${REAL_CURL}" "${updated_args[@]}"
}

zip_to_targz() {
  local zip_path="$1"
  local tar_path="$2"

  local workdir
  workdir="$(mktemp -d)"

  python3 - "${zip_path}" "${workdir}" <<'PY'
import os
import sys
import zipfile

zip_path, out_dir = sys.argv[1:3]
with zipfile.ZipFile(zip_path) as archive:
    archive.extractall(out_dir)
PY

  tar -czf "${tar_path}" -C "${workdir}" .
  rm -rf "${workdir}"
}

build_fallback_candidates() {
  local url="$1"

  if [[ "${url}" =~ ^https://github\.com/([^/]+/[^/]+)/archive/refs/heads/([^/]+)\.(zip|tar\.gz)$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    local ext="${BASH_REMATCH[3]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.${ext}"
    return
  fi

  if [[ "${url}" =~ ^https://github\.com/([^/]+/[^/]+)/archive/([^/]+)\.tar\.gz$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.zip"
    return
  fi

  if [[ "${url}" =~ ^https://codeload\.github\.com/([^/]+/[^/]+)/zip/refs/heads/([^/]+)$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.zip"
    return
  fi

  if [[ "${url}" =~ ^https://codeload\.github\.com/([^/]+/[^/]+)/tar\.gz/refs/heads/([^/]+)$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.tar.gz"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.zip"
    return
  fi
}

normalize_primary_url() {
  local url="$1"

  if [[ "${url}" =~ ^https://codeload\.github\.com/([^/]+/[^/]+)/zip/refs/heads/([^/]+)$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.zip"
    return
  fi

  if [[ "${url}" =~ ^https://codeload\.github\.com/([^/]+/[^/]+)/tar\.gz/refs/heads/([^/]+)$ ]]; then
    local repo="${BASH_REMATCH[1]}"
    local ref="${BASH_REMATCH[2]}"
    printf '%s\n' "https://github.com/${repo}/archive/${ref}.tar.gz"
    return
  fi

  printf '%s\n' "${url}"
}

python_http_fetch() {
  local url="$1"
  local output_path="$2"

  if ! command -v python3 >/dev/null 2>&1; then
    return 1
  fi

  if [[ -n "${output_path}" ]]; then
    python3 - "${url}" "${output_path}" <<'PY'
import sys
import urllib.request

url, output_path = sys.argv[1:3]
with urllib.request.urlopen(url, timeout=30) as response:
    payload = response.read()
with open(output_path, "wb") as handle:
    handle.write(payload)
PY
    return $?
  fi

  python3 - "${url}" <<'PY'
import sys
import urllib.request

url = sys.argv[1]
with urllib.request.urlopen(url, timeout=30) as response:
    payload = response.read()
sys.stdout.buffer.write(payload)
PY
}

main() {
  local -a original_args=("$@")
  local original_url=""
  original_url="$(extract_url "${original_args[@]}" || true)"
  local request_url="${original_url}"
  if [[ -n "${original_url}" ]]; then
    request_url="$(normalize_primary_url "${original_url}")"
  fi

  if [[ -n "${original_url}" && "${request_url}" != "${original_url}" ]]; then
    local -a rewritten_args=()
    while IFS= read -r -d '' arg; do
      rewritten_args+=("${arg}")
    done < <(replace_url_in_args "${original_url}" "${request_url}" "${original_args[@]}")
    original_args=("${rewritten_args[@]}")
  fi

  local status=0
  if "${REAL_CURL}" "${original_args[@]}"; then
    exit 0
  else
    status=$?
  fi

  original_url="${request_url}"
  if [[ -z "${original_url}" ]]; then
    exit "${status}"
  fi

  local output_path=""
  output_path="$(extract_output_path "${original_args[@]}" || true)"

  local candidate
  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue

    if [[ -n "${output_path}" && "${original_url}" =~ \.tar\.gz$ && "${candidate}" =~ \.zip$ ]]; then
      local tmp_zip
      tmp_zip="$(mktemp)"

      local -a args_with_tmp=()
      while IFS= read -r -d '' arg; do
        args_with_tmp+=("${arg}")
      done < <(replace_url_in_args "${original_url}" "${candidate}" "${original_args[@]}")

      local i
      for ((i = 0; i < ${#args_with_tmp[@]}; i++)); do
        case "${args_with_tmp[$i]}" in
          -o|--output)
            if (( i + 1 < ${#args_with_tmp[@]} )); then
              args_with_tmp[$((i + 1))]="${tmp_zip}"
            fi
            ;;
          --output=*)
            args_with_tmp[$i]="--output=${tmp_zip}"
            ;;
        esac
      done

      if "${REAL_CURL}" "${args_with_tmp[@]}"; then
        zip_to_targz "${tmp_zip}" "${output_path}"
        rm -f "${tmp_zip}"
        exit 0
      fi

      rm -f "${tmp_zip}"
      continue
    fi

    if run_with_url "${original_url}" "${candidate}" "${original_args[@]}"; then
      exit 0
    fi
  done < <(build_fallback_candidates "${original_url}")

  if python_http_fetch "${original_url}" "${output_path}"; then
    exit 0
  fi

  exit "${status}"
}

main "$@"
