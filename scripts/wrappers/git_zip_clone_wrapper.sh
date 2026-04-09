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
ARCHIVE_FORMAT="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-tar.gz}"
ALLOW_ZIP_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK:-0}"
GIT_ZIP_WRAPPER_CURL_INSECURE="${GIT_ZIP_WRAPPER_CURL_INSECURE:-0}"
GIT_ZIP_WRAPPER_CURL_CACERT="${GIT_ZIP_WRAPPER_CURL_CACERT:-}"
GIT_ZIP_WRAPPER_ACTIVE_PROXY=""
WRAPPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_ZIP_WRAPPER_USE_EC2="${GIT_ZIP_WRAPPER_USE_EC2:-${WRAPPERS_VIA_EC2_ENABLED:-0}}"
GIT_ZIP_WRAPPER_EC2_ALL_URLS="${GIT_ZIP_WRAPPER_EC2_ALL_URLS:-${WRAPPERS_VIA_EC2_ALL_URLS:-1}}"
GIT_ZIP_WRAPPER_EC2_FETCH_HELPER="${GIT_ZIP_WRAPPER_EC2_HELPER:-${WRAPPER_DIR}/fetch-url-via-ec2}"
GIT_ZIP_WRAPPER_EC2_CLONE_HELPER="${GIT_ZIP_WRAPPER_EC2_CLONE_HELPER:-${WRAPPER_DIR}/git-clone-via-ec2}"
GIT_ZIP_WRAPPER_EC2_GIT_FETCH_HELPER="${GIT_ZIP_WRAPPER_EC2_GIT_FETCH_HELPER:-${WRAPPER_DIR}/git-fetch-via-ec2}"
GIT_ZIP_WRAPPER_EC2_GIT_CHECKOUT_HELPER="${GIT_ZIP_WRAPPER_EC2_GIT_CHECKOUT_HELPER:-${WRAPPER_DIR}/git-checkout-via-ec2}"
GIT_ZIP_WRAPPER_EC2_REQUIRED="${GIT_ZIP_WRAPPER_EC2_REQUIRED:-0}"
GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS="${GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS:-0}"
GIT_ZIP_WRAPPER_EC2_PROXY="${GIT_ZIP_WRAPPER_EC2_PROXY:-${WRAPPERS_VIA_EC2_PROXY:-}}"
GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
GIT_ZIP_WRAPPER_LFS_AUTORUN="${GIT_ZIP_WRAPPER_LFS_AUTORUN:-1}"
GIT_ZIP_WRAPPER_LFS_FORCE="${GIT_ZIP_WRAPPER_LFS_FORCE:-0}"
GIT_ZIP_WRAPPER_LFS_STRICT="${GIT_ZIP_WRAPPER_LFS_STRICT:-0}"
GIT_ZIP_WRAPPER_LFS_RETRY_NO_PROXY="${GIT_ZIP_WRAPPER_LFS_RETRY_NO_PROXY:-1}"
GIT_ZIP_WRAPPER_LFS_MODE="${GIT_ZIP_WRAPPER_LFS_MODE:-local}"
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

normalize_archive_format() {
  local requested
  requested="$1"
  requested="$(printf '%s' "${requested}" | tr '[:upper:]' '[:lower:]')"

  case "${requested}" in
    tar.gz|tgz|tar|zip)
      echo "${requested}"
      ;;
    "")
      echo "tar.gz"
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
    ec2|remote)
      printf '%s\n' "ec2"
      ;;
    *)
      die "modo de LFS inválido: ${requested}. Valores válidos: local, ec2"
      ;;
  esac
}

normalize_clone_order() {
  local requested
  requested="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"

  case "${requested}" in
    ""|local-first)
      printf '%s\n' "local-first"
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
  local repo_url owner normalized_owner
  repo_url="$1"
  owner="$(extract_repo_source_owner "${repo_url}" 2>/dev/null || true)"
  [[ -n "${owner}" ]] || return 1
  normalized_owner="$(printf '%s' "${owner}" | tr '[:upper:]' '[:lower:]')"

  case "${normalized_owner}" in
    itau-*)
      return 0
      ;;
  esac

  return 1
}

download_github_archive() {
  local slug branch archive_path
  slug="$1"
  branch="$2"
  archive_path="$3"

  if [[ "${ARCHIVE_FORMAT}" == "zip" ]]; then
    if [[ -n "${branch}" ]]; then
      try_download_candidate_urls "${archive_path}" \
        "https://github.com/${slug}/archive/refs/heads/${branch}.zip" \
        "https://github.com/${slug}/archive/refs/tags/${branch}.zip" \
        "https://codeload.github.com/${slug}/zip/refs/heads/${branch}" \
        "https://codeload.github.com/${slug}/zip/refs/tags/${branch}" \
        "https://codeload.github.com/${slug}/zip/${branch}" && return 0
    else
      try_download_candidate_urls "${archive_path}" \
        "https://github.com/${slug}/archive/HEAD.zip" \
        "https://codeload.github.com/${slug}/zip/HEAD" && return 0
    fi
  else
    if [[ -n "${branch}" ]]; then
      try_download_candidate_urls "${archive_path}" \
        "https://github.com/${slug}/archive/refs/heads/${branch}.tar.gz" \
        "https://github.com/${slug}/archive/refs/tags/${branch}.tar.gz" \
        "https://codeload.github.com/${slug}/tar.gz/refs/heads/${branch}" \
        "https://codeload.github.com/${slug}/tar.gz/refs/tags/${branch}" \
        "https://codeload.github.com/${slug}/tar.gz/${branch}" && return 0
    else
      try_download_candidate_urls "${archive_path}" \
        "https://github.com/${slug}/archive/HEAD.tar.gz" \
        "https://codeload.github.com/${slug}/tar.gz/HEAD" \
        "https://github.com/${slug}/archive/refs/heads/main.tar.gz" \
        "https://codeload.github.com/${slug}/tar.gz/refs/heads/main" \
        "https://github.com/${slug}/archive/refs/heads/master.tar.gz" \
        "https://codeload.github.com/${slug}/tar.gz/refs/heads/master" && return 0
    fi

    if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
      if [[ -n "${branch}" ]]; then
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/refs/heads/${branch}.zip" \
          "https://github.com/${slug}/archive/refs/tags/${branch}.zip" \
          "https://codeload.github.com/${slug}/zip/refs/heads/${branch}" \
          "https://codeload.github.com/${slug}/zip/refs/tags/${branch}" \
          "https://codeload.github.com/${slug}/zip/${branch}" && return 0
      else
        try_download_candidate_urls "${archive_path}" \
          "https://github.com/${slug}/archive/HEAD.zip" \
          "https://codeload.github.com/${slug}/zip/HEAD" \
          "https://github.com/${slug}/archive/refs/heads/main.zip" \
          "https://codeload.github.com/${slug}/zip/refs/heads/main" \
          "https://github.com/${slug}/archive/refs/heads/master.zip" \
          "https://codeload.github.com/${slug}/zip/refs/heads/master" && return 0
      fi
    fi
  fi

  return 1
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
      if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
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

download_url_with_retries() {
  local url archive_path
  url="$1"
  archive_path="$2"

  local mode_name attempt mode_label user_agent
  user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    log "download usando proxy: ${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  if should_use_ec2_backend_for_git_url "${url}" "${archive_path}"; then
    log "backend selecionado: ec2 (${url})"
    if download_with_ec2_backend "${url}" "${archive_path}" "${user_agent}"; then
      return 0
    fi
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "backend EC2 falhou para ${url} e o fallback local está desabilitado"
    fi
    log "backend EC2 falhou; seguindo com tentativas locais"
  else
    log "backend selecionado: local (${url})"
  fi

  for mode_name in default http1 ipv4 ipv4_http1; do
    mode_label="$(curl_mode_label "${mode_name}")"
    for attempt in 1 2 3; do
      if run_curl_download "${mode_name}" "${url}" "${archive_path}" "${user_agent}"; then
        return 0
      fi
      log "download falhou (tentativa ${attempt}/3, modo ${mode_label}): ${url}"
      sleep 2
    done
  done
  return 1
}

should_use_ec2_backend_for_git_url() {
  local url archive_path url_without_query
  url="$1"
  archive_path="$2"

  if is_truthy "${GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS}"; then
    return 1
  fi
  if ! is_truthy "${GIT_ZIP_WRAPPER_USE_EC2}"; then
    return 1
  fi
  [[ -n "${url}" && -n "${archive_path}" ]] || return 1
  if repo_source_requires_plain_git "${url}"; then
    return 1
  fi
  if [[ ! -x "${GIT_ZIP_WRAPPER_EC2_FETCH_HELPER}" ]]; then
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "helper do backend EC2 não encontrado/executável: ${GIT_ZIP_WRAPPER_EC2_FETCH_HELPER}"
    fi
    return 1
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_EC2_ALL_URLS}"; then
    return 0
  fi

  url_without_query="${url%%\?*}"
  case "${url_without_query}" in
    https://github.com/*|https://codeload.github.com/*|https://api.github.com/*)
      return 0
      ;;
  esac

  return 1
}

should_use_ec2_backend_for_clone_url() {
  local url
  url="$1"

  if ! is_truthy "${GIT_ZIP_WRAPPER_USE_EC2}"; then
    return 1
  fi
  [[ -n "${url}" ]] || return 1
  if repo_source_requires_plain_git "${url}"; then
    return 1
  fi
  if [[ ! -x "${GIT_ZIP_WRAPPER_EC2_CLONE_HELPER}" ]]; then
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "helper de clone do backend EC2 não encontrado/executável: ${GIT_ZIP_WRAPPER_EC2_CLONE_HELPER}"
    fi
    return 1
  fi

  if is_truthy "${GIT_ZIP_WRAPPER_EC2_ALL_URLS}"; then
    return 0
  fi

  case "${url}" in
    https://*|http://*)
      return 0
      ;;
  esac

  return 1
}

should_use_ec2_backend_for_fetch() {
  if ! is_truthy "${GIT_ZIP_WRAPPER_USE_EC2}"; then
    return 1
  fi

  if [[ ! -x "${GIT_ZIP_WRAPPER_EC2_GIT_FETCH_HELPER}" ]]; then
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "helper de fetch do backend EC2 não encontrado/executável: ${GIT_ZIP_WRAPPER_EC2_GIT_FETCH_HELPER}"
    fi
    return 1
  fi

  return 0
}

should_use_ec2_backend_for_checkout() {
  if ! is_truthy "${GIT_ZIP_WRAPPER_USE_EC2}"; then
    return 1
  fi

  if [[ ! -x "${GIT_ZIP_WRAPPER_EC2_GIT_CHECKOUT_HELPER}" ]]; then
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "helper de checkout do backend EC2 não encontrado/executável: ${GIT_ZIP_WRAPPER_EC2_GIT_CHECKOUT_HELPER}"
    fi
    return 1
  fi

  return 0
}

download_with_ec2_backend() {
  local url archive_path user_agent
  url="$1"
  archive_path="$2"
  user_agent="$3"

  local -a helper_cmd=("${GIT_ZIP_WRAPPER_EC2_FETCH_HELPER}" --url "${url}" --output "${archive_path}" --create-dirs)

  if [[ -n "${user_agent}" ]]; then
    helper_cmd+=(--user-agent "${user_agent}")
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}"
}

clone_with_ec2_backend() {
  local repo_url archive_path
  repo_url="$1"
  archive_path="$2"

  local -a helper_cmd=("${GIT_ZIP_WRAPPER_EC2_CLONE_HELPER}" --repo-url "${repo_url}" --output "${archive_path}" --create-dirs)
  local clone_arg

  for clone_arg in "${CLONE_FORWARD_ARGS[@]+"${CLONE_FORWARD_ARGS[@]}"}"; do
    helper_cmd+=(--git-arg "${clone_arg}")
  done
  if [[ "${GIT_ZIP_WRAPPER_LFS_MODE}" == "ec2" ]]; then
    helper_cmd+=(--materialize-lfs)
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}"
}

resolve_fetch_git_dir() {
  local real_git git_dir
  real_git="$1"

  git_dir="$("${real_git}" "${GIT_GLOBAL_ARGS[@]+"${GIT_GLOBAL_ARGS[@]}"}" rev-parse --absolute-git-dir 2>/dev/null || true)"
  [[ -n "${git_dir}" ]] || return 1
  [[ -d "${git_dir}" ]] || return 1
  printf '%s\n' "${git_dir}"
}

fetch_with_ec2_backend() {
  local git_dir fetch_arg
  git_dir="$1"

  local -a helper_cmd=("${GIT_ZIP_WRAPPER_EC2_GIT_FETCH_HELPER}" --git-dir "${git_dir}")

  for fetch_arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    helper_cmd+=(--git-arg "${fetch_arg}")
  done
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}"
}

checkout_with_ec2_backend() {
  local repo_url archive_path checkout_arg
  repo_url="$1"
  archive_path="$2"

  local -a helper_cmd=("${GIT_ZIP_WRAPPER_EC2_GIT_CHECKOUT_HELPER}" --repo-url "${repo_url}" --output "${archive_path}" --create-dirs)

  for checkout_arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    helper_cmd+=(--git-arg "${checkout_arg}")
  done
  if [[ "${GIT_ZIP_WRAPPER_LFS_MODE}" == "ec2" ]]; then
    helper_cmd+=(--materialize-lfs)
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}"
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

replace_mix_install_repo_with_checkout() {
  local real_git git_dir worktree_dir origin_url archive_path temp_dir extracted_repo_dir normalized_origin_url
  real_git="$1"
  git_dir="$2"

  path_looks_like_mix_install_git_dir "${git_dir}" || return 1
  worktree_dir="$(resolve_fetch_worktree_dir "${real_git}" || true)"
  origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
  [[ -n "${worktree_dir}" && -n "${origin_url}" ]] || return 1

  normalized_origin_url="$(normalize_clone_url_for_http_transport "${origin_url}" 2>/dev/null || true)"
  [[ -n "${normalized_origin_url}" ]] || normalized_origin_url="${origin_url}"

  temp_dir="$(mktemp -d -t git-zip-checkout-clone-XXXXXX)"
  archive_path="${temp_dir}/repo-checkout.tar.gz"

  checkout_with_ec2_backend "${normalized_origin_url}" "${archive_path}" || {
    rm -rf "${temp_dir}"
    return 1
  }

  extracted_repo_dir="${temp_dir}/repo"
  mkdir -p "${extracted_repo_dir}"
  tar -xzf "${archive_path}" -C "${extracted_repo_dir}" --strip-components=1
  rm -rf "${worktree_dir}"
  mkdir -p "$(dirname "${worktree_dir}")"
  cp -a "${extracted_repo_dir}" "${worktree_dir}"
  rm -rf "${temp_dir}"
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

extract_requested_fetch_ref() {
  local arg
  for arg in "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"; do
    case "${arg}" in
      -*)
        ;;
      origin)
        ;;
      *)
        printf '%s\n' "${arg}"
        return 0
        ;;
    esac
  done
  return 1
}

overlay_directory_tree() {
  local source_dir destination_dir
  source_dir="$1"
  destination_dir="$2"
  cp -a "${source_dir}/." "${destination_dir}/"
}

replace_mix_install_repo_with_clone() {
  local real_git git_dir worktree_dir origin_url ref archive_path temp_dir extracted_repo_dir
  real_git="$1"
  git_dir="$2"

  path_looks_like_mix_install_git_dir "${git_dir}" || return 1
  worktree_dir="$(resolve_fetch_worktree_dir "${real_git}" || true)"
  origin_url="$(resolve_fetch_origin_url "${real_git}" || true)"
  [[ -n "${worktree_dir}" && -n "${origin_url}" ]] || return 1

  temp_dir="$(mktemp -d -t git-zip-fetch-clone-XXXXXX)"
  archive_path="${temp_dir}/repo-clone.tar.gz"

  local -a helper_cmd=("${GIT_ZIP_WRAPPER_EC2_CLONE_HELPER}" --repo-url "${origin_url}" --output "${archive_path}" --create-dirs)
  ref="$(extract_requested_fetch_ref || true)"
  if [[ -n "${ref}" ]]; then
    helper_cmd+=(--git-arg --branch --git-arg "${ref}" --git-arg --single-branch)
  fi
  if [[ "${GIT_ZIP_WRAPPER_LFS_MODE}" == "ec2" ]]; then
    helper_cmd+=(--materialize-lfs)
  fi
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}" || {
    rm -rf "${temp_dir}"
    return 1
  }

  extracted_repo_dir="${temp_dir}/repo"
  mkdir -p "${extracted_repo_dir}"
  tar -xzf "${archive_path}" -C "${extracted_repo_dir}" --strip-components=1
  overlay_directory_tree "${extracted_repo_dir}" "${worktree_dir}"
  rm -rf "${temp_dir}"
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
  archive_path="${temp_dir}/repo.tar.gz"

  source_url="$(download_github_archive "${slug}" "${ref}" "${archive_path}" || true)"
  if [[ -z "${source_url}" ]]; then
    rm -rf "${temp_dir}"
    return 1
  fi

  extracted_repo_dir="${temp_dir}/repo"
  mkdir -p "${extracted_repo_dir}"
  tar -xzf "${archive_path}" -C "${extracted_repo_dir}" --strip-components=1
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
  local mode_name url archive_path user_agent
  mode_name="$1"
  url="$2"
  archive_path="$3"
  user_agent="$4"

  set -- curl -fsSL \
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

  "$@"
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
  candidate="$1"
  if [[ -z "${candidate}" ]]; then
    printf '%s\n' "archive-snapshot"
    return 0
  fi

  candidate="${candidate//[^[:alnum:]._-]/-}"
  candidate="${candidate#-}"
  candidate="${candidate%-}"
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
  local real_git destination target_ref fetch_depth fetch_exit_code
  real_git="$1"
  destination="$2"
  target_ref="$3"
  fetch_depth="$4"

  set +e
  if [[ -n "${target_ref}" ]]; then
    "${real_git}" -C "${destination}" fetch --quiet --depth "${fetch_depth}" origin \
      "+refs/heads/${target_ref}:refs/remotes/origin/${target_ref}"
    fetch_exit_code=$?
    if [[ "${fetch_exit_code}" -eq 0 ]]; then
      set -e
      return 0
    fi

    "${real_git}" -C "${destination}" fetch --quiet --depth "${fetch_depth}" origin \
      "+refs/tags/${target_ref}:refs/tags/${target_ref}"
    fetch_exit_code=$?
    set -e
    return "${fetch_exit_code}"
  fi

  "${real_git}" -C "${destination}" fetch --quiet --depth "${fetch_depth}" origin HEAD
  fetch_exit_code=$?
  set -e
  return "${fetch_exit_code}"
}

checkout_archive_clone_target() {
  local real_git destination target_ref
  real_git="$1"
  destination="$2"
  target_ref="$3"

  if [[ -n "${target_ref}" ]] &&
    "${real_git}" -C "${destination}" rev-parse --verify "refs/remotes/origin/${target_ref}" >/dev/null 2>&1; then
    "${real_git}" -C "${destination}" checkout --quiet -B "${target_ref}" "refs/remotes/origin/${target_ref}"
    return 0
  fi

  if [[ -n "${target_ref}" ]] &&
    "${real_git}" -C "${destination}" rev-parse --verify "refs/tags/${target_ref}" >/dev/null 2>&1; then
    "${real_git}" -C "${destination}" checkout --quiet --detach "refs/tags/${target_ref}"
    return 0
  fi

  if "${real_git}" -C "${destination}" rev-parse --verify FETCH_HEAD >/dev/null 2>&1; then
    if [[ -n "${target_ref}" ]]; then
      "${real_git}" -C "${destination}" checkout --quiet -B "${target_ref}" FETCH_HEAD
    else
      "${real_git}" -C "${destination}" checkout --quiet --detach FETCH_HEAD
    fi
    return 0
  fi

  return 1
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
  local real_git repo_url destination requested_ref source_url target_ref fetch_depth
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
  if [[ -z "${target_ref}" ]]; then
    target_ref="$(resolve_remote_default_branch "${real_git}" "${destination}" || true)"
  fi

  fetch_depth="$(first_forward_value_for_option --depth || true)"
  [[ -n "${fetch_depth}" ]] || fetch_depth="1"

  if fetch_archive_clone_target "${real_git}" "${destination}" "${target_ref}" "${fetch_depth}"; then
    if checkout_archive_clone_target "${real_git}" "${destination}" "${target_ref}"; then
      log "metadados Git materializados após clone por archive: ${destination}"
      return 0
    fi
  fi

  create_archive_snapshot_commit "${real_git}" "${destination}" "${repo_url}" "${target_ref}"
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
    git_lfs_error_looks_like_signed_url_failure "${initial_log}"; then
    log "git lfs pull falhou com URL assinada; tentando novamente sem proxy"
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

main() {
  local real_git
  real_git="$(resolve_real_git)"

  if [[ $# -eq 0 ]]; then
    exec "${real_git}"
  fi

  parse_git_invocation "$@"

  case "${GIT_SUBCOMMAND}" in
    fetch)
      resolve_proxy_config
      local fetch_git_dir fetch_exit_code
      if current_repo_origin_requires_plain_git "${real_git}"; then
        log "source itau-* detectado no origin; usando git comum para fetch"
        exec "${real_git}" "$@"
      fi
      if should_use_ec2_backend_for_fetch; then
        fetch_git_dir="$(resolve_fetch_git_dir "${real_git}" || true)"
        if [[ -n "${fetch_git_dir}" ]]; then
          log "backend selecionado: ec2 git-fetch (${fetch_git_dir})"
          if fetch_with_ec2_backend "${fetch_git_dir}"; then
            log "fetch remoto concluído: ${fetch_git_dir}"
            return 0
          fi
          if replace_mix_install_repo_with_clone "${real_git}" "${fetch_git_dir}"; then
            log "fallback por clone remoto concluído para cache do Mix.install: ${fetch_git_dir}"
            return 0
          fi
          if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
            die "backend EC2 falhou para git fetch em ${fetch_git_dir} e o fallback local está desabilitado"
          fi
          log "backend EC2 do git fetch falhou; seguindo com fallback local"
        elif is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
          die "não foi possível resolver o diretório git para o fetch remoto"
        fi
      fi
      fetch_git_dir="$(resolve_fetch_git_dir "${real_git}" || true)"
      set +e
      "${real_git}" "$@"
      fetch_exit_code=$?
      set -e
      if [[ "${fetch_exit_code}" -eq 0 ]]; then
        return 0
      fi
      if [[ -n "${fetch_git_dir}" ]] && replace_mix_install_repo_with_archive "${real_git}" "${fetch_git_dir}"; then
        return 0
      fi
      exit "${fetch_exit_code}"
      ;;
    checkout)
      resolve_proxy_config
      local checkout_git_dir
      if current_repo_origin_requires_plain_git "${real_git}"; then
        log "source itau-* detectado no origin; usando git comum para checkout"
        exec "${real_git}" "$@"
      fi
      if should_use_ec2_backend_for_checkout; then
        checkout_git_dir="$(resolve_fetch_git_dir "${real_git}" || true)"
        if [[ -n "${checkout_git_dir}" ]] && replace_mix_install_repo_with_checkout "${real_git}" "${checkout_git_dir}"; then
          log "fallback por checkout remoto concluído para cache do Mix.install: ${checkout_git_dir}"
          return 0
        fi
      fi
      exec "${real_git}" "$@"
      ;;
    clone)
      if [[ ${#GIT_GLOBAL_ARGS[@]} -gt 0 ]]; then
        exec "${real_git}" "$@"
      fi
      ;;
    *)
      exec "${real_git}" "$@"
      ;;
  esac

  command -v tar >/dev/null 2>&1 || die "tar não encontrado"
  command -v mktemp >/dev/null 2>&1 || die "mktemp não encontrado"
  if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
    command -v unzip >/dev/null 2>&1 || die "unzip não encontrado"
  fi

  parse_clone_arguments "clone" "${GIT_SUBCOMMAND_ARGS[@]+"${GIT_SUBCOMMAND_ARGS[@]}"}"
  local repo_url destination branch normalized_repo_url
  repo_url="${CLONE_REPO_URL}"
  destination="${CLONE_DESTINATION}"
  branch="$(first_forward_value_for_option --branch || true)"
  if repo_source_requires_plain_git "${repo_url}"; then
    log "source itau-* detectado no clone; usando git comum sem backend EC2"
    exec "${real_git}" "$@"
  fi
  resolve_proxy_config
  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    log "proxy ativo para wrapper git clone: ${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  validate_clone_destination "${destination}"
  GIT_ZIP_WRAPPER_TMP_DIR="$(mktemp -d -t git-zip-clone-XXXXXX)"

  if normalized_repo_url="$(normalize_clone_url_for_http_transport "${repo_url}" 2>/dev/null)"; then
    :
  else
    normalized_repo_url=""
  fi

  local slug archive_path source_url local_git_clone_exit_code
  if ! slug="$(extract_github_slug "${repo_url}")"; then
    exec "${real_git}" "$@"
  fi

  if command -v curl >/dev/null 2>&1; then
    case "${ARCHIVE_FORMAT}" in
      tar.gz)
        archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo.tar.gz"
        ;;
      tgz)
        archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo.tgz"
        ;;
      tar)
        archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo.tar"
        ;;
      zip)
        archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo.zip"
        ;;
      *)
        archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo.tar.gz"
        ;;
    esac

    assert_supported_archive_format "${archive_path}"

    GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=1
    source_url="$(download_github_archive "${slug}" "${branch}" "${archive_path}" || true)"
    GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=0
    if [[ -n "${source_url}" ]]; then
      extract_archive_to_destination "${archive_path}" "${destination}"
      bootstrap_archive_clone_repository "${real_git}" "${repo_url}" "${destination}" "${branch}" "${source_url}"
      run_git_lfs_post_clone "${destination}"
      log "clone(${ARCHIVE_FORMAT}) concluído: ${repo_url} -> ${destination} (source: ${source_url})"
      return 0
    fi
    if is_truthy "${GIT_ZIP_WRAPPER_STRICT:-0}"; then
      die "falha ao baixar arquivo para ${repo_url} (branch/tag: ${branch:-HEAD})"
    fi
    log "falha ao baixar arquivo para ${repo_url}; tentando git clone normal antes do backend EC2."
  else
    log "curl não encontrado; pulando clone local por arquivo para ${repo_url}"
  fi

  set +e
  "${real_git}" "$@"
  local_git_clone_exit_code=$?
  set -e
  if [[ "${local_git_clone_exit_code}" -eq 0 ]]; then
    return 0
  fi

  if [[ -n "${normalized_repo_url}" ]] && should_use_ec2_backend_for_clone_url "${normalized_repo_url}"; then
    local clone_archive_path
    clone_archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo-clone.tar.gz"
    log "git clone local falhou; tentando backend EC2 para ${repo_url}"
    reset_clone_destination_after_failed_local_clone "${destination}"
    log "backend selecionado: ec2 git-clone (${normalized_repo_url})"
    if clone_with_ec2_backend "${normalized_repo_url}" "${clone_archive_path}"; then
      extract_archive_to_destination "${clone_archive_path}" "${destination}"
      if [[ "${GIT_ZIP_WRAPPER_LFS_MODE}" == "ec2" ]]; then
        log "repositório extraído com Git LFS materializado no EC2"
      else
        run_git_lfs_post_clone "${destination}"
      fi
      log "clone remoto(http) concluído: ${repo_url} -> ${destination} (source: ${normalized_repo_url})"
      return 0
    fi
    if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
      die "git clone local falhou e o backend EC2 falhou para git clone ${repo_url}"
    fi
    log "backend EC2 do git clone falhou; mantendo erro do git clone local"
  elif is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
    die "git clone local falhou e o backend EC2 não está disponível para ${repo_url}"
  fi

  exit "${local_git_clone_exit_code}"
}

main "$@"
