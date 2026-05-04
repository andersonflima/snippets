#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[git-zip-wrapper] erro: bash é obrigatório para executar este wrapper\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[git-zip-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

GIT_ZIP_WRAPPER_TMP_DIR=""
ARCHIVE_FORMAT="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
ALLOW_ZIP_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK:-0}"
GIT_ZIP_WRAPPER_CURL_INSECURE="${GIT_ZIP_WRAPPER_CURL_INSECURE:-0}"
GIT_ZIP_WRAPPER_CURL_CACERT="${GIT_ZIP_WRAPPER_CURL_CACERT:-}"
GIT_ZIP_WRAPPER_ACTIVE_PROXY=""
GIT_ZIP_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR="${GIT_ZIP_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR:-1}"
GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR=""
WRAPPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS="1"
GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
GIT_ZIP_WRAPPER_LFS_AUTORUN="${GIT_ZIP_WRAPPER_LFS_AUTORUN:-1}"
GIT_ZIP_WRAPPER_LFS_FORCE="${GIT_ZIP_WRAPPER_LFS_FORCE:-0}"
GIT_ZIP_WRAPPER_LFS_STRICT="${GIT_ZIP_WRAPPER_LFS_STRICT:-0}"
GIT_ZIP_WRAPPER_LFS_RETRY_NO_PROXY="${GIT_ZIP_WRAPPER_LFS_RETRY_NO_PROXY:-1}"
GIT_ZIP_WRAPPER_LFS_MODE="local"
GIT_ZIP_WRAPPER_STRICT="${GIT_ZIP_WRAPPER_STRICT:-0}"
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"
GIT_ZIP_WRAPPER_LAST_DOWNLOAD_ERROR_KIND=""
GIT_GLOBAL_ARGS=()
GIT_SUBCOMMAND=""
GIT_SUBCOMMAND_ARGS=()

normalize_existing_path() {
  local candidate dir base target
  candidate="$1"
  [[ -n "${candidate}" ]] || return 1

  while [[ -L "${candidate}" ]]; do
    dir="$(dirname "${candidate}")"
    dir="$(
      cd "${dir}" >/dev/null 2>&1 &&
        pwd -P
    )" || return 1
    target="$(readlink "${candidate}")" || return 1
    if [[ "${target}" == /* ]]; then
      candidate="${target}"
    else
      candidate="${dir}/${target}"
    fi
  done

  if [[ -d "${candidate}" ]]; then
    (
      cd "${candidate}" >/dev/null 2>&1 &&
        pwd -P
    )
    return 0
  fi

  dir="$(dirname "${candidate}")"
  base="$(basename "${candidate}")"
  [[ -d "${dir}" ]] || return 1
  dir="$(
    cd "${dir}" >/dev/null 2>&1 &&
      pwd -P
  )" || return 1
  printf '%s\n' "${dir}/${base}"
}

paths_refer_to_same_file() {
  local left right
  left="$(normalize_existing_path "${1:-}" 2>/dev/null || true)"
  right="$(normalize_existing_path "${2:-}" 2>/dev/null || true)"
  [[ -n "${left}" && -n "${right}" && "${left}" == "${right}" ]]
}

should_skip_real_git_candidate() {
  local candidate_path self_path
  candidate_path="$1"
  self_path="$2"
  [[ -n "${candidate_path}" ]] || return 0
  paths_refer_to_same_file "${candidate_path}" "${self_path}"
}

resolve_proxy_config() {
  local proxy
  proxy="${GIT_ZIP_WRAPPER_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${HTTPS_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${https_proxy:-}"
  [[ -n "${proxy}" ]] || proxy="${ALL_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${all_proxy:-}"
  [[ -n "${proxy}" ]] || proxy="${HTTP_PROXY:-}"
  [[ -n "${proxy}" ]] || proxy="${http_proxy:-}"
  GIT_ZIP_WRAPPER_ACTIVE_PROXY="${proxy}"
}

cleanup_temp_dir() {
  local dir
  dir="${GIT_ZIP_WRAPPER_TMP_DIR:-}"
  if [[ -n "${dir}" && -d "${dir}" ]]; then
    rm -rf "${dir}"
  fi
}

trap cleanup_temp_dir EXIT

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

is_proxy_auth_error_log() {
  local output
  output="${1:-}"
  printf '%s\n' "${output}" | grep -Eiq '(^|[[:space:]])407([[:space:]]|$)|proxy[ -]authentication|required|proxy authent(i|y)cation|Proxy-Authenticate|proxy error|Proxy Error'
}

is_proxy_or_transport_block_error_log() {
  local output
  output="${1:-}"
  printf '%s\n' "${output}" | grep -Eiq '(^|[[:space:]])407([[:space:]]|$)|(^|[[:space:]])403([[:space:]]|$)|proxy[ -]authentication|required|proxy authent(i|y)cation|Proxy-Authenticate|proxy error|Proxy Error|expected flush after ref listing|The requested URL returned error: 403'
}

is_permanent_download_error_log() {
  local output
  output="${1:-}"
  printf '%s\n' "${output}" | grep -Eiq 'The requested URL returned error: (404|410)|(^|[[:space:]])HTTP[^[:space:]]*[[:space:]]+(404|410)([[:space:]]|$)|(^|[[:space:]])(404|410)([[:space:]]|$)'
}

has_explicit_proxy_arg() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      -x|--proxy|--proxy=*)
        return 0
        ;;
      -x*)
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
  stderr_file="$(mktemp -t git-zip-command-stderr-XXXXXX)"
  GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR=""

  set +e
  "${command[@]}" 2>"${stderr_file}"
  command_status=$?
  if (( errexit_was_set == 1 )); then
    set -e
  else
    set +e
  fi

  if [[ -f "${stderr_file}" ]]; then
    GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR="$(cat "${stderr_file}")"
    rm -f "${stderr_file}"
  fi

  return "${command_status}"
}

extract_requested_ls_remote_target() {
  local arg skip_next
  skip_next=0

  for arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    if [[ "${skip_next}" == "1" ]]; then
      skip_next=0
      continue
    fi

    case "${arg}" in
      --upload-pack)
        skip_next=1
        ;;
      --upload-pack=*|--heads|--tags|--refs|--quiet|-q|--exit-code|--get-url|--symref)
        ;;
      --)
        ;;
      -*)
        ;;
      *)
        printf '%s\n' "${arg}"
        return 0
        ;;
    esac
  done

  return 1
}

ls_remote_target_is_github() {
  local real_git target_url normalized_target_url
  real_git="$1"
  target_url="$(extract_requested_ls_remote_target || true)"
  if [[ -z "${target_url}" ]]; then
    target_url="$(resolve_fetch_origin_url "${real_git}" 2>/dev/null || true)"
  fi
  [[ -n "${target_url}" ]] || return 1

  normalized_target_url="$(normalize_clone_url_for_http_transport "${target_url}" 2>/dev/null || true)"
  [[ -n "${normalized_target_url}" ]] || normalized_target_url="${target_url}"
  extract_github_slug "${normalized_target_url}" >/dev/null 2>&1
}

run_git_command_with_optional_no_proxy_retry() {
  local command_status
  local -a command
  local -a no_proxy_command

  command=("$@")

  if run_command_with_stderr_capture "${command[@]}"; then
    return 0
  else
    command_status=$?
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR}" && \
    [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]] && \
    is_proxy_or_transport_block_error_log "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}"; then
    log "git falhou com erro de acesso remoto via proxy. Tentando novamente sem proxy"
    no_proxy_command=(
      env
      -u HTTPS_PROXY -u https_proxy
      -u HTTP_PROXY -u http_proxy
      -u ALL_PROXY -u all_proxy
    )
    no_proxy_command+=("${command[@]:0:1}" -c http.proxy= -c https.proxy=)
    no_proxy_command+=("${command[@]:1}")

    if run_command_with_stderr_capture "${no_proxy_command[@]}"; then
      return 0
    else
      command_status=$?
    fi
  fi

  if [[ -n "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" ]]; then
    printf '%s\n' "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" >&2
  fi
  return "${command_status}"
}

normalize_archive_format() {
  local requested
  requested="$1"
  requested="$(printf '%s' "${requested}" | tr '[:upper:]' '[:lower:]')"

  case "${requested}" in
    tar.gz|tgz|tar|zip)
      echo "${requested}"
      ;;
    "")
      echo "zip"
      ;;
    *)
      die "formato de arquivo inválido: ${requested}. Valores válidos: tar.gz, tgz, tar, zip"
      ;;
  esac
}

normalize_lfs_mode() {
  local requested
  requested="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"

  case "${requested}" in
    ""|local)
      printf '%s\n' "local"
      ;;
    *)
      log "modo de LFS inválido: ${requested}; forçando local"
      printf '%s\n' "local"
      ;;
  esac
}

normalize_clone_order() {
  local requested
  requested="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"

  case "${requested}" in
    ""|local-first|git-first)
      [[ -n "${requested}" ]] || requested="local-first"
      printf '%s\n' "${requested}"
      ;;
    *)
      log "valor inválido em GIT_ZIP_WRAPPER_CLONE_ORDER=${requested}; forçando local-first"
      printf '%s\n' "local-first"
      ;;
  esac
}

ARCHIVE_FORMAT="$(normalize_archive_format "${ARCHIVE_FORMAT}")"
GIT_ZIP_WRAPPER_LFS_MODE="$(normalize_lfs_mode "${GIT_ZIP_WRAPPER_LFS_MODE}")"
GIT_ZIP_WRAPPER_CLONE_ORDER="$(normalize_clone_order "${GIT_ZIP_WRAPPER_CLONE_ORDER}")"

resolve_real_git() {
  if [[ -n "${GIT_ZIP_WRAPPER_REAL_GIT:-}" ]]; then
    [[ -x "${GIT_ZIP_WRAPPER_REAL_GIT}" ]] || die "GIT_ZIP_WRAPPER_REAL_GIT inválido: ${GIT_ZIP_WRAPPER_REAL_GIT}"
    printf '%s\n' "${GIT_ZIP_WRAPPER_REAL_GIT}"
    return
  fi

  local self_path shell_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  shell_path="$(command -v -p git 2>/dev/null || true)"
  if ! should_skip_real_git_candidate "${shell_path}" "${self_path}"; then
    printf '%s\n' "${shell_path}"
    return
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    should_skip_real_git_candidate "${candidate}" "${self_path}" && continue
    printf '%s\n' "${candidate}"
    return
  done <<EOF2
$(which -a git 2>/dev/null || true)
EOF2

  [[ -x "/usr/bin/git" ]] || die "não foi possível localizar o git real. Defina GIT_ZIP_WRAPPER_REAL_GIT."
  printf '%s\n' "/usr/bin/git"
}

resolve_download_curl() {
  local candidate

  for candidate in "${GIT_ZIP_WRAPPER_CURL_BIN:-}" "${CURL:-}"; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" == */* ]]; then
      [[ -x "${candidate}" ]] || die "curl configurado inválido/não executável: ${candidate}"
      printf '%s\n' "${candidate}"
      return 0
    fi

    candidate="$(command -v "${candidate}" 2>/dev/null || true)"
    [[ -n "${candidate}" ]] || continue
    [[ -x "${candidate}" ]] || continue
    printf '%s\n' "${candidate}"
    return 0
  done

  candidate="$(command -v curl 2>/dev/null || true)"
  if [[ -n "${candidate}" && -x "${candidate}" ]]; then
    printf '%s\n' "${candidate}"
    return 0
  fi

  [[ -x "/usr/bin/curl" ]] || die "não foi possível localizar curl. Defina GIT_ZIP_WRAPPER_CURL_BIN ou CURL."
  printf '%s\n' "/usr/bin/curl"
}

run_download_curl_command() {
  local download_curl="$1"
  shift
  local -a curl_env=()
  local -a base_command
  local -a non_proxy_env=()
  local item
  local command_status

  base_command=("${download_curl}" "$@")

  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    curl_env+=("CURL_FALLBACK_INSECURE=1")
  fi

  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    curl_env+=("CURL_FALLBACK_PROXY=${GIT_ZIP_WRAPPER_ACTIVE_PROXY}")
  fi

  for item in "${curl_env[@]}"; do
    [[ "${item}" == CURL_FALLBACK_PROXY=* ]] && continue
    non_proxy_env+=("${item}")
  done

  if (( ${#curl_env[@]} > 0 )); then
    if run_command_with_stderr_capture env "${curl_env[@]}" "${base_command[@]}"; then
      return 0
    else
      command_status=$?
    fi
  else
    if run_command_with_stderr_capture "${base_command[@]}"; then
      return 0
    else
      command_status=$?
    fi
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR}" && \
    [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]] && \
    ! has_explicit_proxy_arg "${base_command[@]}" && \
    is_proxy_auth_error_log "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}"; then
    log "curl falhou com erro de autenticação de proxy (407). Tentando novamente sem proxy"
    if (( ${#non_proxy_env[@]} > 0 )); then
      if run_command_with_stderr_capture env "${non_proxy_env[@]}" "${base_command[@]}"; then
        return 0
      else
        return $?
      fi
    fi
    if run_command_with_stderr_capture "${base_command[@]}"; then
      return 0
    else
      return $?
    fi
  fi

  if [[ -n "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" ]]; then
    printf '%s\n' "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" >&2
  fi
  return "${command_status}"
}

extract_default_branch_from_github_repo_json() {
  local json_file
  json_file="$1"

  if command -v python3 >/dev/null 2>&1; then
    python3 - "${json_file}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

default_branch = payload.get("default_branch")
if not default_branch:
    raise SystemExit(1)

print(default_branch)
PY
    return $?
  fi

  sed -n 's/.*"default_branch"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${json_file}" | head -n 1
}

resolve_github_default_branch() {
  local slug api_url temp_output download_curl default_branch
  slug="$1"
  [[ -n "${slug}" ]] || return 1

  download_curl="$(resolve_download_curl)"
  api_url="https://api.github.com/repos/${slug}"
  temp_output="$(mktemp -t git-zip-default-branch-XXXXXX.json)"

  if ! run_download_curl_command "${download_curl}" \
    -fsSL \
    --connect-timeout 20 \
    --max-time 60 \
    --retry 2 \
    --retry-delay 1 \
    --retry-all-errors \
    --tlsv1.2 \
    -A "git-zip-wrapper" \
    -H "Accept: application/vnd.github+json" \
    "${api_url}" \
    -o "${temp_output}"; then
    rm -f "${temp_output}"
    return 1
  fi

  default_branch="$(extract_default_branch_from_github_repo_json "${temp_output}" 2>/dev/null || true)"
  rm -f "${temp_output}"
  [[ -n "${default_branch}" ]] || return 1
  printf '%s\n' "${default_branch}"
}

default_destination_from_repo() {
  local repo_url repo_name
  repo_url="$1"
  repo_name="${repo_url%/}"
  repo_name="${repo_name##*/}"
  repo_name="${repo_name%.git}"
  [[ -n "${repo_name}" ]] || die "não foi possível inferir diretório de destino para clone: ${repo_url}"
  printf '%s\n' "${repo_name}"
}

parse_git_invocation() {
  GIT_GLOBAL_ARGS=()
  GIT_SUBCOMMAND=""
  GIT_SUBCOMMAND_ARGS=()

  local arg

  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "${arg}" in
      -C|-c|--git-dir|--work-tree|--namespace|--exec-path|--config-env)
        [[ $# -ge 2 ]] || die "faltou valor para ${arg}"
        GIT_GLOBAL_ARGS+=("$1" "$2")
        shift 2
        ;;
      --git-dir=*|--work-tree=*|--namespace=*|--exec-path=*|--config-env=*)
        GIT_GLOBAL_ARGS+=("$1")
        shift
        ;;
      --bare|--no-pager|--paginate|--literal-pathspecs|--no-literal-pathspecs|--optional-locks|--no-optional-locks)
        GIT_GLOBAL_ARGS+=("$1")
        shift
        ;;
      --)
        shift
        break
        ;;
      -*)
        GIT_GLOBAL_ARGS+=("$1")
        shift
        ;;
      *)
        GIT_SUBCOMMAND="$1"
        shift
        GIT_SUBCOMMAND_ARGS=("$@")
        return 0
        ;;
    esac
  done
}

parse_clone_arguments() {
  CLONE_REPO_URL=""
  CLONE_DESTINATION=""
  CLONE_FORWARD_ARGS=()

  local arg positional_count positional_repo positional_destination
  positional_count=0
  positional_repo=""
  positional_destination=""

  if [[ "${1:-}" == "clone" ]]; then
    shift
  fi

  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "${arg}" in
      -b|--branch|-c|--config|-o|--origin|-u|--upload-pack|-j|--jobs|--depth|--filter|--template|--reference|--reference-if-able|--server-option|--separate-git-dir|--bundle-uri)
        [[ $# -ge 2 ]] || die "faltou valor para ${arg}"
        CLONE_FORWARD_ARGS+=("$1" "$2")
        shift 2
        ;;
      --branch=*|--config=*|--jobs=*|--depth=*|--filter=*|--origin=*|--upload-pack=*|--template=*|--reference=*|--reference-if-able=*|--server-option=*|--separate-git-dir=*|--bundle-uri=*|--recurse-submodules=*)
        CLONE_FORWARD_ARGS+=("$1")
        shift
        ;;
      --single-branch|--no-single-branch|--recurse-submodules|--shallow-submodules|--no-shallow-submodules|--no-tags|--tags|--quiet|--verbose|--progress|--no-checkout|--bare|--mirror|--sparse|--reject-shallow|--dissociate|--local|--shared|--no-local|--hardlinks|--no-hardlinks)
        CLONE_FORWARD_ARGS+=("$1")
        shift
        ;;
      --)
        CLONE_FORWARD_ARGS+=("--")
        shift
        while [[ $# -gt 0 ]]; do
          positional_count=$((positional_count + 1))
          if [[ ${positional_count} -eq 1 ]]; then
            positional_repo="$1"
          elif [[ ${positional_count} -eq 2 ]]; then
            positional_destination="$1"
          else
            die "uso inválido: git clone <repo> [destino]"
          fi
          shift
        done
        ;;
      -*)
        CLONE_FORWARD_ARGS+=("$1")
        shift
        ;;
      *)
        positional_count=$((positional_count + 1))
        if [[ ${positional_count} -eq 1 ]]; then
          positional_repo="${arg}"
        elif [[ ${positional_count} -eq 2 ]]; then
          positional_destination="${arg}"
        else
          die "uso inválido: git clone <repo> [destino]"
        fi
        shift
        ;;
    esac
  done

  [[ ${positional_count} -ge 1 ]] || die "uso inválido: git clone <repo> [destino]"
  CLONE_REPO_URL="${positional_repo}"
  if [[ ${positional_count} -ge 2 ]]; then
    CLONE_DESTINATION="${positional_destination}"
  else
    CLONE_DESTINATION="$(default_destination_from_repo "${CLONE_REPO_URL}")"
  fi
}

normalize_clone_url_for_http_transport() {
  local repo_url host_and_path host path
  repo_url="$1"

  case "${repo_url}" in
    https://*|http://*)
      printf '%s\n' "${repo_url}"
      ;;
    git://*)
      printf 'https://%s\n' "${repo_url#git://}"
      ;;
    ssh://*)
      host_and_path="${repo_url#ssh://}"
      host="${host_and_path%%/*}"
      path="${host_and_path#*/}"
      host="${host#*@}"
      [[ -n "${host}" && -n "${path}" && "${path}" != "${host_and_path}" ]] || return 1
      printf 'https://%s/%s\n' "${host}" "${path}"
      ;;
    *@*:* )
      host_and_path="${repo_url#*@}"
      host="${host_and_path%%:*}"
      path="${host_and_path#*:}"
      [[ -n "${host}" && -n "${path}" && "${path}" != "${host_and_path}" ]] || return 1
      printf 'https://%s/%s\n' "${host}" "${path}"
      ;;
    *)
      return 1
      ;;
  esac
}

extract_github_slug() {
  local repo_url slug
  repo_url="$1"
  slug=""
  case "${repo_url}" in
    git@github.com:*)
      slug="${repo_url#git@github.com:}"
      ;;
    ssh://git@github.com/*)
      slug="${repo_url#ssh://git@github.com/}"
      ;;
    https://github.com/*)
      slug="${repo_url#https://github.com/}"
      ;;
    http://github.com/*)
      slug="${repo_url#http://github.com/}"
      ;;
    git://github.com/*)
      slug="${repo_url#git://github.com/}"
      ;;
  esac
  slug="${slug%.git}"
  slug="${slug#/}"
  [[ "${slug}" == */* ]] || return 1
  printf '%s\n' "${slug}"
}

extract_repo_source_owner() {
  local repo_url normalized_repo_url host_and_path path owner
  repo_url="$1"
  normalized_repo_url="$(normalize_clone_url_for_http_transport "${repo_url}" 2>/dev/null || true)"
  [[ -n "${normalized_repo_url}" ]] || normalized_repo_url="${repo_url}"

  case "${normalized_repo_url}" in
    https://*)
      host_and_path="${normalized_repo_url#https://}"
      ;;
    http://*)
      host_and_path="${normalized_repo_url#http://}"
      ;;
    *)
      return 1
      ;;
  esac

  path="${host_and_path#*/}"
  [[ -n "${path}" && "${path}" != "${host_and_path}" ]] || return 1
  owner="${path%%/*}"
  owner="${owner#/}"
  [[ -n "${owner}" && "${owner}" != "${path}" ]] || return 1
  printf '%s\n' "${owner}"
}

repo_source_requires_plain_git() {
  local repo_url owner normalized_owner raw_prefixes normalized_prefixes prefix normalized_prefix
  repo_url="$1"
  owner="$(extract_repo_source_owner "${repo_url}" 2>/dev/null || true)"
  [[ -n "${owner}" ]] || return 1
  normalized_owner="$(printf '%s' "${owner}" | tr '[:upper:]' '[:lower:]')"
  raw_prefixes="${RESTRICTED_GIT_PLAIN_OWNER_PREFIXES:-itau-,itau}"
  IFS=',' read -r -a normalized_prefixes <<<"${raw_prefixes}"

  for prefix in "${normalized_prefixes[@]-}"; do
    normalized_prefix="$(printf '%s' "${prefix}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
    [[ -n "${normalized_prefix}" ]] || continue
    case "${normalized_owner}" in
      "${normalized_prefix}"*)
        return 0
        ;;
    esac
  done

  return 1
}

log_plain_git_source() {
  local context
  context="$1"
  log "source interno configurado detectado no ${context}; usando git comum"
}

resolve_archive_ref_type() {
  local ref
  ref="$(canonicalize_archive_ref "$1")"

  if [[ -z "${ref}" || "${ref}" == "HEAD" ]]; then
    printf '%s\n' "head"
    return 0
  fi

  if looks_like_git_commit_sha "${ref}"; then
    printf '%s\n' "commit"
    return 0
  fi

  case "${ref}" in
    refs/tags/*|tags/*)
      printf '%s\n' "tag"
      return 0
      ;;
    refs/remotes/origin/*|remotes/origin/*|refs/heads/*|heads/*|origin/*)
      printf '%s\n' "branch"
      return 0
      ;;
    *)
      printf '%s\n' "branch"
      return 0
      ;;
  esac
}

normalize_archive_ref_name() {
  local ref ref_type normalized_ref
  ref="$(canonicalize_archive_ref "$1")"
  ref_type="$(resolve_archive_ref_type "${ref}")"
  normalized_ref="${ref}"

  case "${ref_type}" in
    head)
      normalized_ref=""
      ;;
    commit)
      ;;
    tag)
      case "${normalized_ref}" in
        refs/tags/*)
          normalized_ref="${normalized_ref#refs/tags/}"
          ;;
        tags/*)
          normalized_ref="${normalized_ref#tags/}"
          ;;
      esac
      ;;
    branch)
      case "${normalized_ref}" in
        refs/remotes/origin/*)
          normalized_ref="${normalized_ref#refs/remotes/origin/}"
          ;;
        remotes/origin/*)
          normalized_ref="${normalized_ref#remotes/origin/}"
          ;;
        refs/heads/*)
          normalized_ref="${normalized_ref#refs/heads/}"
          ;;
        heads/*)
          normalized_ref="${normalized_ref#heads/}"
          ;;
        origin/*)
          normalized_ref="${normalized_ref#origin/}"
          ;;
      esac
      ;;
  esac

  printf '%s\n' "${normalized_ref}"
}

normalize_archive_branch_name() {
  local ref ref_type normalized_ref
  ref="$1"
  ref_type="$(resolve_archive_ref_type "${ref}")"
  [[ "${ref_type}" == "branch" ]] || return 1

  normalized_ref="$(normalize_archive_ref_name "${ref}")"
  [[ -n "${normalized_ref}" ]] || return 1
  printf '%s\n' "${normalized_ref}"
}

download_github_archive() {
  local slug branch archive_path ref_type normalized_ref
  slug="$1"
  branch="$2"
  archive_path="$3"
  ref_type="$(resolve_archive_ref_type "${branch}")"
  normalized_ref="$(normalize_archive_ref_name "${branch}")"
  local encoded_ref

  encoded_ref="$(url_encode_github_archive_ref "${normalized_ref}")"

  if [[ "${ARCHIVE_FORMAT}" == "zip" ]]; then
    case "${ref_type}" in
      commit)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
        ;;
      tag)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
        ;;
      branch)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
        ;;
      *)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/main.zip" \
          "https://github.com/${slug}/archive/master.zip" \
          "https://github.com/${slug}/archive/HEAD.zip" && return 0
        ;;
    esac
  else
    case "${ref_type}" in
      commit)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.tar.gz" && return 0
        ;;
      tag)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.tar.gz" && return 0
        ;;
      branch)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/${encoded_ref}.tar.gz" && return 0
        ;;
      *)
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/main.tar.gz" \
          "https://github.com/${slug}/archive/master.tar.gz" \
          "https://github.com/${slug}/archive/HEAD.tar.gz" && return 0
        ;;
    esac

    if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
      case "${ref_type}" in
        commit)
          try_download_candidate_urls "${archive_path}" \
            "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
          ;;
        tag)
          try_download_candidate_urls "${archive_path}" \
            "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
          ;;
        branch)
          try_download_candidate_urls "${archive_path}" \
            "https://github.com/${slug}/archive/${encoded_ref}.zip" && return 0
          ;;
        *)
          try_download_candidate_urls "${archive_path}" \
            "https://github.com/${slug}/archive/main.zip" \
            "https://github.com/${slug}/archive/master.zip" \
            "https://github.com/${slug}/archive/HEAD.zip" && return 0
          ;;
      esac
    fi
  fi

  return 1
}

url_encode_github_archive_ref() {
  local ref
  local encoded

  ref="${1:-}"
  [[ -n "${ref}" ]] || {
    printf '%s\n' ""
    return 0
  }

  if command -v python3 >/dev/null 2>&1; then
    encoded="$(python3 - "${ref}" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe=""))
PY
)" || encoded=""
  else
    encoded="${ref//%/%25}"
    encoded="${encoded//\//%2F}"
    encoded="${encoded// /%20}"
    encoded="${encoded//\"/%22}"
    encoded="${encoded//#/%23}"
    encoded="${encoded//&/%26}"
    encoded="${encoded/=/%3D}"
    encoded="${encoded/?/%3F}"
    encoded="${encoded/\$/%24}"
  fi

  printf '%s\n' "${encoded}"
}

try_download_candidate_urls() {
  local archive_path url
  archive_path="$1"
  shift

  for url in "$@"; do
    if download_url_with_retries "${url}" "${archive_path}"; then
      printf '%s\n' "${url}"
      return 0
    fi
  done
  return 1
}

assert_supported_archive_format() {
  local archive_path="$1"
  case "${archive_path}" in
    *.zip)
      if [[ "${ARCHIVE_FORMAT}" == "zip" ]] || is_truthy "${ALLOW_ZIP_FALLBACK}"; then
        return 0
      fi
      die "formato .zip não permitido para este wrapper (GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK=1 para habilitar)"
      ;;
    *.tar.gz|*.tgz|*.tar)
      return 0
      ;;
    *)
      die "formato de arquivo não suportado: ${archive_path}"
      ;;
  esac
}

archive_path_for_temp_dir() {
  local temp_dir
  temp_dir="$1"

  case "${ARCHIVE_FORMAT}" in
    tar.gz)
      printf '%s\n' "${temp_dir}/repo.tar.gz"
      ;;
    tgz)
      printf '%s\n' "${temp_dir}/repo.tgz"
      ;;
    tar)
      printf '%s\n' "${temp_dir}/repo.tar"
      ;;
    zip)
      printf '%s\n' "${temp_dir}/repo.zip"
      ;;
    *)
      printf '%s\n' "${temp_dir}/repo.zip"
      ;;
  esac
}

download_url_with_retries() {
  local url archive_path
  url="$1"
  archive_path="$2"

  local mode_name attempt mode_label user_agent
  user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    log "download usando proxy: ${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  log "backend selecionado: local (${url})"

  for mode_name in default http1 ipv4 ipv4_http1; do
    mode_label="$(curl_mode_label "${mode_name}")"
    for attempt in 1 2 3; do
      if run_curl_download "${mode_name}" "${url}" "${archive_path}" "${user_agent}"; then
        return 0
      fi
      if [[ "${GIT_ZIP_WRAPPER_LAST_DOWNLOAD_ERROR_KIND}" == "permanent" ]]; then
        log "download retornou erro permanente; pulando novas tentativas para: ${url}"
        return 1
      fi
      log "download falhou (tentativa ${attempt}/3, modo ${mode_label}): ${url}"
      sleep 2
    done
  done

  return 1
}

resolve_fetch_git_dir() {
  local real_git git_dir
  real_git="$1"

  git_dir="$("${real_git}" "${GIT_GLOBAL_ARGS[@]+"${GIT_GLOBAL_ARGS[@]}"}" rev-parse --absolute-git-dir 2>/dev/null || true)"
  [[ -n "${git_dir}" ]] || return 1
  [[ -d "${git_dir}" ]] || return 1
  printf '%s\n' "${git_dir}"
}

path_looks_like_mix_install_git_dir() {
  local git_dir normalized_git_dir
  git_dir="$1"
  normalized_git_dir="${git_dir%/}"

  case "${normalized_git_dir}" in
    */.cache/mix/installs/*/.git|*/.mix/installs/*/.git|*/Library/Caches/mix/installs/*/.git)
      return 0
      ;;
  esac

  return 1
}

resolve_fetch_worktree_dir() {
  local real_git worktree_dir
  real_git="$1"

  worktree_dir="$("${real_git}" "${GIT_GLOBAL_ARGS[@]+"${GIT_GLOBAL_ARGS[@]}"}" rev-parse --show-toplevel 2>/dev/null || true)"
  [[ -n "${worktree_dir}" ]] || return 1
  [[ -d "${worktree_dir}" ]] || return 1
  printf '%s\n' "${worktree_dir}"
}

resolve_fetch_origin_url() {
  local real_git origin_url
  real_git="$1"

  origin_url="$("${real_git}" "${GIT_GLOBAL_ARGS[@]+"${GIT_GLOBAL_ARGS[@]}"}" config --get remote.origin.url 2>/dev/null || true)"
  [[ -n "${origin_url}" ]] || return 1
  printf '%s\n' "${origin_url}"
}

current_repo_origin_requires_plain_git() {
  local real_git origin_url
  real_git="$1"
  origin_url="$(resolve_fetch_origin_url "${real_git}" 2>/dev/null || true)"
  [[ -n "${origin_url}" ]] || return 1
  repo_source_requires_plain_git "${origin_url}"
}

current_repo_origin_is_github() {
  local real_git origin_url normalized_origin_url
  real_git="$1"
  origin_url="$(resolve_fetch_origin_url "${real_git}" 2>/dev/null || true)"
  [[ -n "${origin_url}" ]] || return 1
  normalized_origin_url="$(normalize_clone_url_for_http_transport "${origin_url}" 2>/dev/null || true)"
  [[ -n "${normalized_origin_url}" ]] || normalized_origin_url="${origin_url}"
  extract_github_slug "${normalized_origin_url}" >/dev/null 2>&1
}

canonicalize_archive_ref() {
  local ref
  ref="$1"

  [[ -n "${ref}" ]] || {
    printf '%s\n' ""
    return 0
  }

  case "${ref}" in
    +*)
      ref="${ref#+}"
      ;;
  esac

  case "${ref}" in
    *:*)
      ref="${ref%%:*}"
      ;;
  esac

  printf '%s\n' "${ref}"
}

is_downloadable_archive_ref() {
  local ref
  ref="${1:-}"
  [[ -n "${ref}" ]] || return 1

  case "${ref}" in
    *'*'*|*'?'*|*'['*|*']'*)
      return 1
      ;;
  esac

  return 0
}

extract_requested_fetch_ref() {
  local arg positional_ref positional_count skip_next
  positional_ref=""
  positional_count=0
  skip_next=0

  for arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    if [[ "${skip_next}" == "1" ]]; then
      skip_next=0
      continue
    fi

    case "${arg}" in
      --depth|--deepen|--shallow-since|--shallow-exclude|--negotiation-tip|--recurse-submodules-default|--refmap|--server-option|--upload-pack|--jobs|-j|--filter)
        skip_next=1
        ;;
      --depth=*|--deepen=*|--shallow-since=*|--shallow-exclude=*|--negotiation-tip=*|--recurse-submodules-default=*|--refmap=*|--server-option=*|--upload-pack=*|--jobs=*|--filter=*)
        ;;
      -*)
        ;;
      *)
        positional_count=$((positional_count + 1))
        if [[ "${positional_count}" -ge 2 ]]; then
          canonicalize_archive_ref "${arg}"
          return 0
        fi
        positional_ref="${arg}"
        ;;
    esac
  done

  if [[ -n "${positional_ref}" && "${positional_ref}" != "origin" ]]; then
    canonicalize_archive_ref "${positional_ref}"
    return 0
  fi

  return 1
}

extract_requested_checkout_ref() {
  local arg
  for arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    case "${arg}" in
      -*)
        ;;
      *)
        printf '%s\n' "${arg}"
        return 0
        ;;
    esac
  done
  return 1
}

checkout_target_is_available_locally() {
  local real_git worktree_dir target_ref
  real_git="$1"
  worktree_dir="$2"
  target_ref="$3"

  [[ -n "${target_ref}" ]] || return 1

  case "${target_ref}" in
    FETCH_HEAD|HEAD)
      "${real_git}" -C "${worktree_dir}" rev-parse --verify "${target_ref}" >/dev/null 2>&1
      return $?
      ;;
  esac

  "${real_git}" -C "${worktree_dir}" rev-parse --verify "${target_ref}^{commit}" >/dev/null 2>&1
}

run_local_checkout_with_suppressed_failure() {
  local real_git worktree_dir stderr_log checkout_exit_code
  real_git="$1"
  worktree_dir="$2"
  shift 2

  stderr_log="$(mktemp -t git-zip-checkout-stderr-XXXXXX)"
  set +e
  "${real_git}" "$@" 2>"${stderr_log}"
  checkout_exit_code=$?
  set -e

  if [[ "${checkout_exit_code}" -eq 0 ]]; then
    rm -f "${stderr_log}"
    return 0
  fi

  cat "${stderr_log}" >&2
  rm -f "${stderr_log}"
  return "${checkout_exit_code}"
}

looks_like_git_commit_sha() {
  local value
  value="$1"
  [[ "${value}" =~ ^[0-9a-fA-F]{7,40}$ ]]
}

resolve_current_local_branch() {
  local real_git worktree_dir current_branch
  real_git="$1"
  worktree_dir="$2"

  current_branch="$("${real_git}" -C "${worktree_dir}" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
  [[ -n "${current_branch}" ]] || return 1
  printf '%s\n' "${current_branch}"
}

resolve_remote_origin_head_branch() {
  local real_git worktree_dir remote_head_ref
  real_git="$1"
  worktree_dir="$2"

  remote_head_ref="$("${real_git}" -C "${worktree_dir}" symbolic-ref --quiet refs/remotes/origin/HEAD 2>/dev/null || true)"
  case "${remote_head_ref}" in
    refs/remotes/origin/*)
      printf '%s\n' "${remote_head_ref#refs/remotes/origin/}"
      return 0
      ;;
  esac

  return 1
}

overlay_directory_tree() {
  local source_dir destination_dir
  source_dir="$1"
  destination_dir="$2"
  cp -a "${source_dir}/." "${destination_dir}/"
}

build_archive_snapshot_repository() {
  local real_git repo_url target_ref temp_dir archive_path extracted_repo_dir source_url normalized_origin_url slug snapshot_branch snapshot_commit
  real_git="$1"
  repo_url="$2"
  target_ref="$3"

  normalized_origin_url="$(normalize_clone_url_for_http_transport "${repo_url}" 2>/dev/null || true)"
  [[ -n "${normalized_origin_url}" ]] || normalized_origin_url="${repo_url}"
  slug="$(extract_github_slug "${normalized_origin_url}" || true)"
  [[ -n "${slug}" ]] || return 1

  temp_dir="$(mktemp -d -t git-zip-archive-repo-XXXXXX)"
  archive_path="$(archive_path_for_temp_dir "${temp_dir}")"
  assert_supported_archive_format "${archive_path}"
  source_url="$(download_github_archive "${slug}" "${target_ref}" "${archive_path}" || true)"
  if [[ -z "${source_url}" ]]; then
    rm -rf "${temp_dir}"
    return 1
  fi

  extracted_repo_dir="${temp_dir}/repo"
  mkdir -p "${extracted_repo_dir}"
  extract_archive_to_destination "${archive_path}" "${extracted_repo_dir}"

  "${real_git}" init --quiet "${extracted_repo_dir}"
  create_archive_snapshot_commit "${real_git}" "${extracted_repo_dir}" "${repo_url}" "${target_ref}"
  snapshot_branch="$(sanitize_archive_clone_branch_name "${target_ref}")"
  snapshot_commit="$("${real_git}" -C "${extracted_repo_dir}" rev-parse HEAD)"

  printf '%s\t%s\t%s\t%s\n' "${temp_dir}" "${snapshot_branch}" "${snapshot_commit}" "${source_url}"
}

import_archive_snapshot_ref_into_repo() {
  local real_git destination source_repo_dir source_branch destination_ref
  real_git="$1"
  destination="$2"
  source_repo_dir="$3"
  source_branch="$4"
  destination_ref="$5"

  "${real_git}" -C "${destination}" fetch --quiet "${source_repo_dir}" "+refs/heads/${source_branch}:${destination_ref}"
}

fallback_fetch_repo_with_archive() {
  local real_git git_dir worktree_dir origin_url target_ref archive_info archive_dir snapshot_branch snapshot_commit source_url current_branch destination_ref normalized_branch_ref
  real_git="$1"
  git_dir="$2"

  worktree_dir="$(resolve_fetch_worktree_dir "${real_git}" || true)"
  origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
  [[ -n "${worktree_dir}" && -n "${origin_url}" ]] || return 1

  target_ref="$(extract_requested_fetch_ref || true)"
  if ! is_downloadable_archive_ref "${target_ref}"; then
    target_ref=""
  fi
  if [[ -z "${target_ref}" ]]; then
    current_branch="$(resolve_current_local_branch "${real_git}" "${worktree_dir}" || true)"
    if [[ -n "${current_branch}" ]]; then
      target_ref="${current_branch}"
    else
      target_ref="$(resolve_remote_origin_head_branch "${real_git}" "${worktree_dir}" || true)"
    fi
  fi
  if ! is_downloadable_archive_ref "${target_ref}"; then
    target_ref="HEAD"
  fi
  normalized_branch_ref="$(normalize_archive_branch_name "${target_ref}" || true)"

  archive_info="$(build_archive_snapshot_repository "${real_git}" "${origin_url}" "${target_ref}" || true)"
  [[ -n "${archive_info}" ]] || return 1

  archive_dir="${archive_info%%$'\t'*}"
  archive_info="${archive_info#*$'\t'}"
  snapshot_branch="${archive_info%%$'\t'*}"
  archive_info="${archive_info#*$'\t'}"
  snapshot_commit="${archive_info%%$'\t'*}"
  source_url="${archive_info#*$'\t'}"

  if [[ -n "${normalized_branch_ref}" ]]; then
    destination_ref="refs/remotes/origin/${normalized_branch_ref}"
  else
    destination_ref="refs/archive-fallback/fetch/${snapshot_branch}"
  fi

  if import_archive_snapshot_ref_into_repo "${real_git}" "${worktree_dir}" "${archive_dir}/repo" "${snapshot_branch}" "${destination_ref}"; then
    if [[ -n "${normalized_branch_ref}" ]]; then
      "${real_git}" -C "${worktree_dir}" symbolic-ref refs/remotes/origin/HEAD "refs/remotes/origin/${normalized_branch_ref}" >/dev/null 2>&1 || true
    fi
    rm -rf "${archive_dir}"
    log "fallback por archive local concluído para fetch: ${worktree_dir} (${target_ref} -> ${snapshot_commit}, source: ${source_url})"
    return 0
  fi

  rm -rf "${archive_dir}"
  return 1
}

fallback_checkout_repo_with_archive() {
  local real_git worktree_dir origin_url target_ref archive_info archive_dir snapshot_branch snapshot_commit source_url import_ref replaced_target
  local -a checkout_args
  real_git="$1"

  worktree_dir="$(resolve_fetch_worktree_dir "${real_git}" || true)"
  origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
  target_ref="$(extract_requested_checkout_ref || true)"
  [[ -n "${worktree_dir}" && -n "${origin_url}" && -n "${target_ref}" ]] || return 1

  archive_info="$(build_archive_snapshot_repository "${real_git}" "${origin_url}" "${target_ref}" || true)"
  [[ -n "${archive_info}" ]] || return 1

  archive_dir="${archive_info%%$'\t'*}"
  archive_info="${archive_info#*$'\t'}"
  snapshot_branch="${archive_info%%$'\t'*}"
  archive_info="${archive_info#*$'\t'}"
  snapshot_commit="${archive_info%%$'\t'*}"
  source_url="${archive_info#*$'\t'}"

  import_ref="refs/archive-fallback/${snapshot_branch}"
  import_archive_snapshot_ref_into_repo "${real_git}" "${worktree_dir}" "${archive_dir}/repo" "${snapshot_branch}" "${import_ref}" || {
    rm -rf "${archive_dir}"
    return 1
  }

  snapshot_commit="$("${real_git}" -C "${worktree_dir}" rev-parse "${import_ref}")"
  checkout_args=()
  replaced_target="0"
  for arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    if [[ "${replaced_target}" == "0" && "${arg}" == "${target_ref}" ]]; then
      checkout_args+=("${snapshot_commit}")
      replaced_target="1"
      continue
    fi
    checkout_args+=("${arg}")
  done

  if [[ "${replaced_target}" == "0" ]]; then
    rm -rf "${archive_dir}"
    return 1
  fi

  if "${real_git}" -C "${worktree_dir}" checkout "${checkout_args[@]}"; then
    rm -rf "${archive_dir}"
    log "fallback por archive local concluído para checkout: ${worktree_dir} (${target_ref} -> ${snapshot_commit}, source: ${source_url})"
    return 0
  fi

  rm -rf "${archive_dir}"
  return 1
}

replace_mix_install_repo_with_archive() {
  local real_git git_dir worktree_dir origin_url ref normalized_origin_url slug temp_dir archive_path extracted_repo_dir source_url
  real_git="$1"
  git_dir="$2"

  path_looks_like_mix_install_git_dir "${git_dir}" || return 1
  worktree_dir="$(resolve_fetch_worktree_dir "${real_git}" || true)"
  origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
  [[ -n "${worktree_dir}" && -n "${origin_url}" ]] || return 1

  normalized_origin_url="$(normalize_clone_url_for_http_transport "${origin_url}" 2>/dev/null || true)"
  [[ -n "${normalized_origin_url}" ]] || normalized_origin_url="${origin_url}"
  slug="$(extract_github_slug "${normalized_origin_url}" || true)"
  [[ -n "${slug}" ]] || return 1

  ref="$(extract_requested_fetch_ref || true)"
  temp_dir="$(mktemp -d -t git-zip-fetch-archive-XXXXXX)"
  archive_path="$(archive_path_for_temp_dir "${temp_dir}")"
  assert_supported_archive_format "${archive_path}"

  source_url="$(download_github_archive "${slug}" "${ref}" "${archive_path}" || true)"
  if [[ -z "${source_url}" ]]; then
    rm -rf "${temp_dir}"
    return 1
  fi

  extracted_repo_dir="${temp_dir}/repo"
  mkdir -p "${extracted_repo_dir}"
  extract_archive_to_destination "${archive_path}" "${extracted_repo_dir}"
  rm -rf "${worktree_dir}"
  mkdir -p "${worktree_dir}"
  cp -a "${extracted_repo_dir}/." "${worktree_dir}/"
  rm -rf "${temp_dir}"
  log "fallback por archive local concluído para cache do Mix.install: ${worktree_dir} (source: ${source_url})"
}

curl_mode_label() {
  case "$1" in
    default) printf '%s\n' "default" ;;
    http1) printf '%s\n' "--http1.1" ;;
    ipv4) printf '%s\n' "-4" ;;
    ipv4_http1) printf '%s\n' "-4 --http1.1" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

run_curl_download() {
  local mode_name url archive_path user_agent download_curl
  local -a curl_env=()
  local command_status
  mode_name="$1"
  url="$2"
  archive_path="$3"
  user_agent="$4"
  download_curl="$(resolve_download_curl)"
  GIT_ZIP_WRAPPER_LAST_DOWNLOAD_ERROR_KIND=""

  set -- "${download_curl}" -fsSL \
    --connect-timeout 20 \
    --max-time 300 \
    --retry 3 \
    --retry-delay 2 \
    --retry-all-errors \
    --tlsv1.2

  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    set -- "$@" --insecure
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_CURL_CACERT}" ]]; then
    set -- "$@" --cacert "${GIT_ZIP_WRAPPER_CURL_CACERT}"
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    set -- "$@" --proxy "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  case "${mode_name}" in
    http1)
      set -- "$@" --http1.1
      ;;
    ipv4)
      set -- "$@" -4
      ;;
    ipv4_http1)
      set -- "$@" -4 --http1.1
      ;;
  esac

  set -- "$@" \
    -A "${user_agent}" \
    -H "Accept: application/octet-stream,*/*" \
    "${url}" \
    -o "${archive_path}"

  curl_env+=(
    "CURL_FALLBACK_URL=${url}"
    "CURL_FALLBACK_OUTPUT=${archive_path}"
    "CURL_FALLBACK_USER_AGENT=${user_agent}"
    "CURL_FALLBACK_CONNECT_TIMEOUT=20"
    "CURL_FALLBACK_MAX_TIME=300"
    "CURL_FALLBACK_HEADERS=Accept: application/octet-stream,*/*"
  )

  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    curl_env+=("CURL_FALLBACK_PROXY=${GIT_ZIP_WRAPPER_ACTIVE_PROXY}")
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    curl_env+=("CURL_FALLBACK_INSECURE=1")
  fi

  case "${archive_path}" in
    *.zip)
      curl_env+=("CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1")
      ;;
  esac

  if run_command_with_stderr_capture env "${curl_env[@]}" "$@"; then
    return 0
  else
    command_status=$?
  fi

  if is_permanent_download_error_log "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}"; then
    GIT_ZIP_WRAPPER_LAST_DOWNLOAD_ERROR_KIND="permanent"
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" ]]; then
    printf '%s\n' "${GIT_ZIP_WRAPPER_LAST_COMMAND_STDERR}" >&2
  fi
  return "${command_status}"
}

validate_clone_destination() {
  local destination
  destination="$1"
  if [[ -e "${destination}" && ! -d "${destination}" ]]; then
    die "destino existe e não é diretório: ${destination}"
  fi
  if [[ -d "${destination}" ]] && [[ -n "$(ls -A "${destination}" 2>/dev/null)" ]]; then
    die "destino já existe e não está vazio: ${destination}"
  fi
  mkdir -p "${destination}"
}

reset_clone_destination_after_failed_local_clone() {
  local destination
  destination="$1"

  mkdir -p "${destination}"
  find "${destination}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
}

extract_archive_to_destination() {
  local archive_path destination temp_extract top_dir
  archive_path="$1"
  destination="$2"

  temp_extract="$(mktemp -d -t git-zip-extract-XXXXXX)"

  case "${archive_path}" in
    *.zip)
      unzip -q "${archive_path}" -d "${temp_extract}"
      ;;
    *.tar.gz|*.tgz)
      tar -xzf "${archive_path}" -C "${temp_extract}"
      ;;
    *.tar)
      tar -xf "${archive_path}" -C "${temp_extract}"
      ;;
    *)
      die "tipo de arquivo não suportado para extração: ${archive_path}"
      ;;
  esac

  top_dir="$(find "${temp_extract}" -mindepth 1 -maxdepth 1 -type d | head -n 1 || true)"
  [[ -n "${top_dir}" ]] || die "não foi possível localizar conteúdo extraído do arquivo de origem"
  cp -a "${top_dir}/." "${destination}/"
  rm -rf "${temp_extract}"
}

sanitize_archive_clone_branch_name() {
  local candidate
  candidate="$(canonicalize_archive_ref "$1")"
  if [[ -z "${candidate}" ]]; then
    printf '%s\n' "archive-snapshot"
    return 0
  fi

  candidate="${candidate//[^[:alnum:]._-]/-}"
  candidate="${candidate#-}"
  candidate="${candidate%-}"
  if looks_like_git_commit_sha "${candidate}"; then
    candidate="archive-snapshot-${candidate}"
  fi
  [[ -n "${candidate}" ]] || candidate="archive-snapshot"
  printf '%s\n' "${candidate}"
}

infer_branch_from_archive_source_url() {
  local source_url branch
  source_url="$1"
  branch=""

  case "${source_url}" in
    *"/archive/refs/heads/"*)
      branch="${source_url##*/archive/refs/heads/}"
      branch="${branch%.tar.gz}"
      branch="${branch%.tgz}"
      branch="${branch%.tar}"
      branch="${branch%.zip}"
      ;;
    *"/tar.gz/refs/heads/"*)
      branch="${source_url##*/tar.gz/refs/heads/}"
      ;;
    *"/zip/refs/heads/"*)
      branch="${source_url##*/zip/refs/heads/}"
      ;;
  esac

  [[ -n "${branch}" ]] || return 1
  printf '%s\n' "${branch}"
}

resolve_archive_clone_target_ref() {
  local requested_ref source_url inferred_ref
  requested_ref="$1"
  source_url="$2"

  if [[ -n "${requested_ref}" ]]; then
    printf '%s\n' "${requested_ref}"
    return 0
  fi

  inferred_ref="$(infer_branch_from_archive_source_url "${source_url}" 2>/dev/null || true)"
  [[ -n "${inferred_ref}" ]] || return 1
  printf '%s\n' "${inferred_ref}"
}

resolve_remote_default_branch() {
  local real_git destination ls_remote_output line ref_prefix ref_suffix
  real_git="$1"
  destination="$2"

  ls_remote_output="$("${real_git}" -C "${destination}" ls-remote --symref origin HEAD 2>/dev/null || true)"
  [[ -n "${ls_remote_output}" ]] || return 1

  ref_prefix="ref: refs/heads/"
  ref_suffix="$(printf '\tHEAD')"
  while IFS= read -r line; do
    case "${line}" in
      "${ref_prefix}"*"${ref_suffix}")
        line="${line#${ref_prefix}}"
        printf '%s\n' "${line%${ref_suffix}}"
        return 0
        ;;
    esac
  done <<EOF2
${ls_remote_output}
EOF2

  return 1
}

fetch_archive_clone_target() {
  local real_git destination target_ref normalized_target_ref target_ref_type fetch_depth fetch_exit_code clone_filter
  local -a fetch_args
  real_git="$1"
  destination="$2"
  target_ref="$3"
  target_ref_type="$(resolve_archive_ref_type "${target_ref}")"
  normalized_target_ref="$(normalize_archive_ref_name "${target_ref}")"
  fetch_depth="$4"
  clone_filter="$(first_forward_value_for_option --filter || true)"
  fetch_args=("${real_git}" -C "${destination}" fetch --quiet)

  if [[ -n "${fetch_depth}" ]]; then
    fetch_args+=(--depth "${fetch_depth}")
  fi
  if [[ -n "${clone_filter}" ]]; then
    fetch_args+=(--filter "${clone_filter}")
  fi

  set +e
  if [[ "${target_ref_type}" == "branch" && -n "${normalized_target_ref}" ]]; then
    "${fetch_args[@]}" origin "+refs/heads/${normalized_target_ref}:refs/remotes/origin/${normalized_target_ref}"
    fetch_exit_code=$?
    if [[ "${fetch_exit_code}" -eq 0 ]]; then
      set -e
      return 0
    fi

    "${fetch_args[@]}" origin "+refs/tags/${normalized_target_ref}:refs/tags/${normalized_target_ref}"
    fetch_exit_code=$?
    set -e
    return "${fetch_exit_code}"
  fi

  if [[ "${target_ref_type}" == "tag" && -n "${normalized_target_ref}" ]]; then
    "${fetch_args[@]}" origin "+refs/tags/${normalized_target_ref}:refs/tags/${normalized_target_ref}"
    fetch_exit_code=$?
    set -e
    return "${fetch_exit_code}"
  fi

  "${fetch_args[@]}" origin HEAD
  fetch_exit_code=$?
  set -e
  return "${fetch_exit_code}"
}

checkout_archive_clone_target() {
  local real_git destination target_ref normalized_target_ref target_ref_type
  real_git="$1"
  destination="$2"
  target_ref="$3"
  target_ref_type="$(resolve_archive_ref_type "${target_ref}")"
  normalized_target_ref="$(normalize_archive_ref_name "${target_ref}")"

  if [[ "${target_ref_type}" == "branch" && -n "${normalized_target_ref}" ]] &&
    "${real_git}" -C "${destination}" rev-parse --verify "refs/remotes/origin/${normalized_target_ref}" >/dev/null 2>&1; then
    if "${real_git}" -C "${destination}" checkout --quiet -B "${normalized_target_ref}" "refs/remotes/origin/${normalized_target_ref}"; then
      return 0
    fi
  fi

  if [[ "${target_ref_type}" == "tag" && -n "${normalized_target_ref}" ]] &&
    "${real_git}" -C "${destination}" rev-parse --verify "refs/tags/${normalized_target_ref}" >/dev/null 2>&1; then
    if "${real_git}" -C "${destination}" checkout --quiet --detach "refs/tags/${normalized_target_ref}"; then
      return 0
    fi
  fi

  if "${real_git}" -C "${destination}" rev-parse --verify FETCH_HEAD >/dev/null 2>&1; then
    if [[ "${target_ref_type}" == "branch" && -n "${normalized_target_ref}" ]]; then
      if "${real_git}" -C "${destination}" checkout --quiet -B "${normalized_target_ref}" FETCH_HEAD; then
        return 0
      fi
    else
      if "${real_git}" -C "${destination}" checkout --quiet --detach FETCH_HEAD; then
        return 0
      fi
    fi
  fi

  return 1
}

clone_forward_flag_enabled() {
  local enabled_flag disabled_flag current state
  enabled_flag="$1"
  disabled_flag="${2:-}"
  state=""

  for current in "${CLONE_FORWARD_ARGS[@]+"${CLONE_FORWARD_ARGS[@]}"}"; do
    if [[ "${current}" == "${enabled_flag}" ]]; then
      state="enabled"
      continue
    fi

    if [[ -n "${disabled_flag}" && "${current}" == "${disabled_flag}" ]]; then
      state="disabled"
    fi
  done

  [[ "${state}" == "enabled" ]]
}

archive_clone_has_remote_branch_ref() {
  local real_git destination branch_name
  real_git="$1"
  destination="$2"
  branch_name="$(normalize_archive_branch_name "${3:-}" || true)"

  [[ -n "${branch_name}" ]] || return 1
  "${real_git}" -C "${destination}" rev-parse --verify "refs/remotes/origin/${branch_name}" >/dev/null 2>&1
}

archive_clone_snapshot_commit() {
  local real_git destination
  real_git="$1"
  destination="$2"

  "${real_git}" -C "${destination}" rev-parse --verify HEAD 2>/dev/null || return 1
}

materialize_archive_clone_snapshot_branch_ref() {
  local real_git destination branch_name snapshot_commit
  real_git="$1"
  destination="$2"
  branch_name="$(normalize_archive_branch_name "${3:-}" || true)"

  [[ -n "${branch_name}" ]] || return 1
  archive_clone_has_remote_branch_ref "${real_git}" "${destination}" "${branch_name}" && return 0

  snapshot_commit="$(archive_clone_snapshot_commit "${real_git}" "${destination}" || true)"
  [[ -n "${snapshot_commit}" ]] || return 1
  "${real_git}" -C "${destination}" update-ref "refs/remotes/origin/${branch_name}" "${snapshot_commit}"
}

configure_archive_clone_fetch_refspec() {
  local real_git destination branch_name
  real_git="$1"
  destination="$2"
  branch_name="$(normalize_archive_branch_name "${3:-}" || true)"

  [[ -n "${branch_name}" ]] || return 1
  archive_clone_has_remote_branch_ref "${real_git}" "${destination}" "${branch_name}" || return 1
  clone_forward_flag_enabled --single-branch --no-single-branch || return 0

  "${real_git}" -C "${destination}" config --unset-all remote.origin.fetch >/dev/null 2>&1 || true
  "${real_git}" -C "${destination}" config remote.origin.fetch "+refs/heads/${branch_name}:refs/remotes/origin/${branch_name}"
}

configure_archive_clone_origin_head() {
  local real_git destination preferred_branch fallback_branch branch_name
  real_git="$1"
  destination="$2"
  preferred_branch="$(normalize_archive_branch_name "${3:-}" || true)"
  fallback_branch="$(normalize_archive_branch_name "${4:-}" || true)"

  branch_name="${preferred_branch}"
  if ! archive_clone_has_remote_branch_ref "${real_git}" "${destination}" "${branch_name}"; then
    branch_name="${fallback_branch}"
  fi

  archive_clone_has_remote_branch_ref "${real_git}" "${destination}" "${branch_name}" || return 1
  "${real_git}" -C "${destination}" symbolic-ref refs/remotes/origin/HEAD "refs/remotes/origin/${branch_name}"
}

configure_archive_clone_partial_clone() {
  local real_git destination
  real_git="$1"
  destination="$2"

  "${real_git}" -C "${destination}" config --unset-all remote.origin.promisor >/dev/null 2>&1 || true
  "${real_git}" -C "${destination}" config --unset-all remote.origin.partialclonefilter >/dev/null 2>&1 || true
}

create_archive_snapshot_commit() {
  local real_git destination repo_url target_ref branch_name commit_message
  real_git="$1"
  destination="$2"
  repo_url="$3"
  target_ref="$4"

  branch_name="$(sanitize_archive_clone_branch_name "${target_ref}")"
  "${real_git}" -C "${destination}" symbolic-ref HEAD "refs/heads/${branch_name}" >/dev/null 2>&1 || true
  "${real_git}" -C "${destination}" add -A
  if "${real_git}" -C "${destination}" diff --cached --quiet --ignore-submodules --exit-code >/dev/null 2>&1; then
    return 0
  fi

  commit_message="Archive snapshot of ${repo_url}"
  if [[ -n "${target_ref}" ]]; then
    commit_message="${commit_message} (${target_ref})"
  fi

  "${real_git}" \
    -C "${destination}" \
    -c user.name=git-zip-wrapper \
    -c user.email=git-zip-wrapper@local \
    commit --quiet -m "${commit_message}"
}

bootstrap_archive_clone_repository() {
  local real_git repo_url destination requested_ref source_url target_ref remote_default_branch fetch_depth
  real_git="$1"
  repo_url="$2"
  destination="$3"
  requested_ref="$4"
  source_url="$5"

  [[ -d "${destination}" ]] || return 1
  [[ ! -d "${destination}/.git" ]] || return 0

  "${real_git}" init --quiet "${destination}"
  "${real_git}" -C "${destination}" remote remove origin >/dev/null 2>&1 || true
  "${real_git}" -C "${destination}" remote add origin "${repo_url}"

  target_ref="$(resolve_archive_clone_target_ref "${requested_ref}" "${source_url}" || true)"
  remote_default_branch="$(normalize_archive_branch_name "${target_ref}" || true)"
  if [[ -z "${target_ref}" ]]; then
    target_ref="${remote_default_branch}"
  fi

  # Commit the extracted archive first so a later checkout can safely replace
  # tracked files instead of aborting on an untracked working tree.
  create_archive_snapshot_commit "${real_git}" "${destination}" "${repo_url}" "${target_ref}"
  materialize_archive_clone_snapshot_branch_ref "${real_git}" "${destination}" "${target_ref}" || true
  configure_archive_clone_fetch_refspec "${real_git}" "${destination}" "${target_ref}" || true
  configure_archive_clone_partial_clone "${real_git}" "${destination}" || true

  configure_archive_clone_origin_head "${real_git}" "${destination}" "${remote_default_branch}" "${target_ref}" || true

  if checkout_archive_clone_target "${real_git}" "${destination}" "${target_ref}"; then
    log "metadados Git materializados após clone por archive: ${destination}"
    return 0
  fi

  log "clone por archive materializado como snapshot Git local: ${destination}"
}

has_lfs_attributes() {
  local destination
  destination="$1"
  [[ -f "${destination}/.gitattributes" ]] || return 1
  grep -Eq '(^|[[:space:]])filter=lfs($|[[:space:]])' "${destination}/.gitattributes"
}

git_lfs_error_looks_like_signed_url_failure() {
  local log_file
  log_file="$1"
  grep -Eq 'SignatureDoesNotMatch|AuthorizationQueryParametersError|RequestTimeTooSkewed|Request has expired' "${log_file}"
}

git_lfs_error_looks_like_proxy_auth_error() {
  local log_file
  log_file="$1"
  grep -Eiq '(^|[[:space:]])407([[:space:]]|$)|proxy-Authenticate|proxy error|authentication required|Proxy-Authenticate' "${log_file}"
}

run_git_lfs_pull_with_optional_no_proxy_retry() {
  local destination initial_log retry_log
  destination="$1"
  initial_log="$(mktemp -t git-zip-lfs-pull-XXXXXX)"

  if "${real_git}" -C "${destination}" lfs pull >"${initial_log}" 2>&1; then
    rm -f "${initial_log}"
    return 0
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_LFS_RETRY_NO_PROXY}" &&
    [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]] &&
    (git_lfs_error_looks_like_signed_url_failure "${initial_log}" || \
     git_lfs_error_looks_like_proxy_auth_error "${initial_log}"); then
    log "git lfs pull falhou com erro de acesso remoto/proxy; tentando novamente sem proxy"
    retry_log="$(mktemp -t git-zip-lfs-pull-noproxy-XXXXXX)"
    if env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
      "${real_git}" \
        -c http.proxy= \
        -c https.proxy= \
        -C "${destination}" \
        lfs pull >"${retry_log}" 2>&1; then
      rm -f "${initial_log}" "${retry_log}"
      return 0
    fi
    cat "${initial_log}" >&2
    cat "${retry_log}" >&2
    rm -f "${initial_log}" "${retry_log}"
    return 1
  fi

  cat "${initial_log}" >&2
  rm -f "${initial_log}"
  return 1
}

run_git_lfs_post_clone() {
  local destination
  destination="$1"

  if ! is_truthy "${GIT_ZIP_WRAPPER_LFS_AUTORUN}"; then
    return 0
  fi

  if ! is_truthy "${GIT_ZIP_WRAPPER_LFS_FORCE}" && ! has_lfs_attributes "${destination}"; then
    return 0
  fi

  if ! "${real_git}" -C "${destination}" lfs env >/dev/null 2>&1; then
    log "git lfs indisponível em ${destination}; pulando auto pull"
    return 0
  fi

  if ! "${real_git}" -C "${destination}" lfs install --local >/dev/null 2>&1; then
    if is_truthy "${GIT_ZIP_WRAPPER_LFS_STRICT}"; then
      die "falha ao executar git lfs install em ${destination}"
    fi
    log "falha no git lfs install em ${destination}; seguindo sem LFS"
    return 0
  fi

  if ! run_git_lfs_pull_with_optional_no_proxy_retry "${destination}"; then
    if is_truthy "${GIT_ZIP_WRAPPER_LFS_STRICT}"; then
      die "falha ao executar git lfs pull em ${destination}"
    fi
    log "falha no git lfs pull em ${destination}; seguindo sem LFS"
  fi
}

first_forward_value_for_option() {
  local option_prefix option_name index current next_value
  option_name="$1"
  option_prefix="${option_name}="

  for ((index = 0; index < ${#CLONE_FORWARD_ARGS[@]}; index++)); do
    current="${CLONE_FORWARD_ARGS[index]}"
    if [[ "${current}" == "${option_name}" ]]; then
      if (( index + 1 < ${#CLONE_FORWARD_ARGS[@]} )); then
        next_value="${CLONE_FORWARD_ARGS[index + 1]}"
        printf '%s\n' "${next_value}"
        return 0
      fi
      return 1
    fi
    if [[ "${current}" == ${option_prefix}* ]]; then
      printf '%s\n' "${current#${option_prefix}}"
      return 0
    fi
  done

  return 1
}

first_clone_branch_value() {
  local branch_value
  branch_value="$(first_forward_value_for_option --branch || true)"
  if [[ -n "${branch_value}" ]]; then
    printf '%s\n' "${branch_value}"
    return 0
  fi

  branch_value="$(first_forward_value_for_option -b || true)"
  [[ -n "${branch_value}" ]] || return 1
  printf '%s\n' "${branch_value}"
}

clone_with_real_git() {
  local real_git
  real_git="$1"
  shift

  log "tentando git clone real"
  run_git_command_with_optional_no_proxy_retry "${real_git}" "$@"
}

remote_git_fallback_enabled() {
  is_truthy "${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK}"
}

resolve_clone_archive_refs() {
  local slug requested_branch default_branch
  slug="$1"
  requested_branch="${2:-}"

  if [[ -n "${requested_branch}" ]]; then
    printf '%s\n' "${requested_branch}"
    return 0
  fi

  if default_branch="$(resolve_github_default_branch "${slug}" 2>/dev/null)"; then
    if [[ -n "${default_branch}" ]]; then
      printf '%s\n' "${default_branch}"
      return 0
    fi
  fi

  printf '%s\n' "main"
  printf '%s\n' "master"
  printf '%s\n' "HEAD"
}

clone_global_args_are_archive_safe() {
  local index current
  index=0

  while (( index < ${#GIT_GLOBAL_ARGS[@]} )); do
    current="${GIT_GLOBAL_ARGS[index]}"
    case "${current}" in
      -c|--config-env)
        index=$((index + 2))
        ;;
      --config-env=*|--no-pager|--literal-pathspecs|--no-literal-pathspecs|--optional-locks|--no-optional-locks)
        index=$((index + 1))
        ;;
      *)
        return 1
        ;;
    esac
  done

  return 0
}

main() {
  local real_git
  real_git="$(resolve_real_git)"

  if [[ $# -eq 0 ]]; then
    exec "${real_git}"
  fi

  parse_git_invocation "$@"

  case "${GIT_SUBCOMMAND}" in
    ls-remote)
      resolve_proxy_config
      if current_repo_origin_requires_plain_git "${real_git}" && [[ -z "$(extract_requested_ls_remote_target || true)" ]]; then
        log_plain_git_source "ls-remote"
        exec "${real_git}" "$@"
      fi

      if ls_remote_target_is_github "${real_git}"; then
        run_git_command_with_optional_no_proxy_retry "${real_git}" "$@"
        return $?
      fi

      exec "${real_git}" "$@"
      ;;
    fetch)
      resolve_proxy_config
      local fetch_git_dir fetch_exit_code fetch_origin_url
      if current_repo_origin_requires_plain_git "${real_git}"; then
        log_plain_git_source "fetch"
        exec "${real_git}" "$@"
      fi
      fetch_git_dir="$(resolve_fetch_git_dir "${real_git}" || true)"
      fetch_origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
      if current_repo_origin_is_github "${real_git}"; then
        if [[ -n "${fetch_git_dir}" ]] && fallback_fetch_repo_with_archive "${real_git}" "${fetch_git_dir}"; then
          return 0
        fi
        if [[ -n "${fetch_git_dir}" ]] && replace_mix_install_repo_with_archive "${real_git}" "${fetch_git_dir}"; then
          return 0
        fi
        if remote_git_fallback_enabled && run_git_command_with_optional_no_proxy_retry "${real_git}" "$@"; then
          return 0
        fi
        die "falha ao atualizar refs via archive para ${fetch_origin_url:-origin}; fetch remoto via git está desabilitado para repositórios GitHub"
      fi
      set +e
      "${real_git}" "$@"
      fetch_exit_code=$?
      set -e
      if [[ "${fetch_exit_code}" -eq 0 ]]; then
        return 0
      fi
      if [[ -n "${fetch_git_dir}" ]] && fallback_fetch_repo_with_archive "${real_git}" "${fetch_git_dir}"; then
        return 0
      fi
      if [[ -n "${fetch_git_dir}" ]] && replace_mix_install_repo_with_archive "${real_git}" "${fetch_git_dir}"; then
        return 0
      fi
      exit "${fetch_exit_code}"
      ;;
    checkout)
      resolve_proxy_config
      local checkout_git_dir checkout_exit_code checkout_origin_url checkout_target_ref
      if current_repo_origin_requires_plain_git "${real_git}"; then
        log_plain_git_source "checkout"
        exec "${real_git}" "$@"
      fi
      checkout_git_dir="$(resolve_fetch_git_dir "${real_git}" || true)"
      checkout_origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
      checkout_target_ref="$(extract_requested_checkout_ref || true)"
      if current_repo_origin_is_github "${real_git}"; then
        if [[ -n "${checkout_git_dir}" ]] && [[ -n "${checkout_target_ref}" ]] &&
          checkout_target_is_available_locally "${real_git}" "${checkout_git_dir%/.git}" "${checkout_target_ref}"; then
          if run_local_checkout_with_suppressed_failure "${real_git}" "${checkout_git_dir%/.git}" "$@"; then
            return 0
          fi
        fi
        if fallback_checkout_repo_with_archive "${real_git}"; then
          return 0
        fi
        if remote_git_fallback_enabled && run_git_command_with_optional_no_proxy_retry "${real_git}" "$@"; then
          return 0
        fi
        die "falha ao fazer checkout via archive para ${checkout_origin_url:-origin}; checkout remoto via git está desabilitado para repositórios GitHub"
      fi
      set +e
      "${real_git}" "$@"
      checkout_exit_code=$?
      set -e
      if [[ "${checkout_exit_code}" -eq 0 ]]; then
        return 0
      fi
      if fallback_checkout_repo_with_archive "${real_git}"; then
        return 0
      fi
      exit "${checkout_exit_code}"
      ;;
    clone)
      if [[ ${#GIT_GLOBAL_ARGS[@]} -gt 0 ]] && ! clone_global_args_are_archive_safe; then
        exec "${real_git}" "$@"
      fi
      ;;
    *)
      exec "${real_git}" "$@"
      ;;
  esac

  command -v tar >/dev/null 2>&1 || die "tar não encontrado"
  command -v mktemp >/dev/null 2>&1 || die "mktemp não encontrado"
  if [[ "${ARCHIVE_FORMAT}" == "zip" ]] || is_truthy "${ALLOW_ZIP_FALLBACK}"; then
    command -v unzip >/dev/null 2>&1 || die "unzip não encontrado"
  fi

  parse_clone_arguments "clone" "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"
  local repo_url destination branch archive_ref archive_refs candidate_archive_ref
  repo_url="${CLONE_REPO_URL}"
  destination="${CLONE_DESTINATION}"
  branch="$(first_clone_branch_value || true)"
  if repo_source_requires_plain_git "${repo_url}"; then
    log_plain_git_source "clone"
    exec "${real_git}" "$@"
  fi
  resolve_proxy_config
  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    log "proxy ativo para wrapper git clone: ${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  validate_clone_destination "${destination}"
  GIT_ZIP_WRAPPER_TMP_DIR="$(mktemp -d -t git-zip-clone-XXXXXX)"

  local slug archive_path source_url local_git_clone_exit_code
  if ! slug="$(extract_github_slug "${repo_url}")"; then
    exec "${real_git}" "$@"
  fi

  archive_refs="$(resolve_clone_archive_refs "${slug}" "${branch}")"

  if [[ "${GIT_ZIP_WRAPPER_CLONE_ORDER}" == "git-first" ]]; then
    if clone_with_real_git "${real_git}" "$@"; then
      return 0
    fi
  fi

  if resolve_download_curl >/dev/null 2>&1; then
    archive_path="$(archive_path_for_temp_dir "${GIT_ZIP_WRAPPER_TMP_DIR}")"
    assert_supported_archive_format "${archive_path}"

    GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=1
    source_url=""
    while IFS= read -r candidate_archive_ref; do
      [[ -n "${candidate_archive_ref}" ]] || continue
      archive_ref="${candidate_archive_ref}"
      source_url="$(download_github_archive "${slug}" "${archive_ref}" "${archive_path}" || true)"
      [[ -n "${source_url}" ]] && break
    done <<EOF2
${archive_refs}
EOF2
    GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=0
    if [[ -n "${source_url}" ]]; then
      extract_archive_to_destination "${archive_path}" "${destination}"
      bootstrap_archive_clone_repository "${real_git}" "${repo_url}" "${destination}" "${archive_ref}" "${source_url}"
      run_git_lfs_post_clone "${destination}"
      log "clone(${ARCHIVE_FORMAT}) concluído: ${repo_url} -> ${destination} (source: ${source_url})"
      return 0
    fi
    if remote_git_fallback_enabled && clone_with_real_git "${real_git}" "$@"; then
      return 0
    fi
    die "falha ao baixar arquivo para ${repo_url} (branch/tag: ${archive_ref:-HEAD}); clone remoto via git está desabilitado para repositórios GitHub"
  else
    if remote_git_fallback_enabled && clone_with_real_git "${real_git}" "$@"; then
      return 0
    fi
    die "curl não encontrado para clone local por arquivo: ${repo_url}"
  fi
}

main "$@"
