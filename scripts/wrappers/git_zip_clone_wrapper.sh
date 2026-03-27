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
GIT_ZIP_WRAPPER_EC2_REQUIRED="${GIT_ZIP_WRAPPER_EC2_REQUIRED:-${WRAPPERS_VIA_EC2_ENABLED:-0}}"
GIT_ZIP_WRAPPER_EC2_PROXY="${GIT_ZIP_WRAPPER_EC2_PROXY:-${WRAPPERS_VIA_EC2_PROXY:-}}"

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

ARCHIVE_FORMAT="$(normalize_archive_format "${ARCHIVE_FORMAT}")"

resolve_real_git() {
  if [[ -n "${GIT_ZIP_WRAPPER_REAL_GIT:-}" ]]; then
    [[ -x "${GIT_ZIP_WRAPPER_REAL_GIT}" ]] || die "GIT_ZIP_WRAPPER_REAL_GIT inválido: ${GIT_ZIP_WRAPPER_REAL_GIT}"
    printf '%s\n' "${GIT_ZIP_WRAPPER_REAL_GIT}"
    return
  fi

  local self_path shell_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  shell_path="$(command -v -p git 2>/dev/null || true)"
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

  if ! is_truthy "${GIT_ZIP_WRAPPER_USE_EC2}"; then
    return 1
  fi
  [[ -n "${url}" && -n "${archive_path}" ]] || return 1
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

  for clone_arg in "${CLONE_FORWARD_ARGS[@]}"; do
    helper_cmd+=(--git-arg "${clone_arg}")
  done
  if [[ -n "${GIT_ZIP_WRAPPER_EC2_PROXY}" ]]; then
    helper_cmd+=(--proxy "${GIT_ZIP_WRAPPER_EC2_PROXY}")
  fi
  if is_truthy "${GIT_ZIP_WRAPPER_CURL_INSECURE}"; then
    helper_cmd+=(--insecure)
  fi

  "${helper_cmd[@]}"
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

  if [[ "$1" != "clone" ]]; then
    exec "${real_git}" "$@"
  fi

  command -v tar >/dev/null 2>&1 || die "tar não encontrado"
  command -v mktemp >/dev/null 2>&1 || die "mktemp não encontrado"
  if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
    command -v unzip >/dev/null 2>&1 || die "unzip não encontrado"
  fi

  parse_clone_arguments "$@"
  local repo_url destination branch normalized_repo_url
  repo_url="${CLONE_REPO_URL}"
  destination="${CLONE_DESTINATION}"
  branch="$(first_forward_value_for_option --branch || true)"
  resolve_proxy_config
  if [[ -n "${GIT_ZIP_WRAPPER_ACTIVE_PROXY}" ]]; then
    log "proxy ativo para wrapper git clone: ${GIT_ZIP_WRAPPER_ACTIVE_PROXY}"
  fi

  validate_clone_destination "${destination}"
  GIT_ZIP_WRAPPER_TMP_DIR="$(mktemp -d -t git-zip-clone-XXXXXX)"

  if normalized_repo_url="$(normalize_clone_url_for_http_transport "${repo_url}" 2>/dev/null)"; then
    local clone_archive_path
    clone_archive_path="${GIT_ZIP_WRAPPER_TMP_DIR}/repo-clone.tar.gz"
    if should_use_ec2_backend_for_clone_url "${normalized_repo_url}"; then
      log "backend selecionado: ec2 git-clone (${normalized_repo_url})"
      if clone_with_ec2_backend "${normalized_repo_url}" "${clone_archive_path}"; then
        extract_archive_to_destination "${clone_archive_path}" "${destination}"
        log "clone remoto(http) concluído: ${repo_url} -> ${destination} (source: ${normalized_repo_url})"
        return 0
      fi
      if is_truthy "${GIT_ZIP_WRAPPER_EC2_REQUIRED}"; then
        die "backend EC2 falhou para git clone ${repo_url} e o fallback local está desabilitado"
      fi
      log "backend EC2 do git clone falhou; seguindo com fallback local"
    fi
  fi

  local slug
  if ! slug="$(extract_github_slug "${repo_url}")"; then
    log "host não suportado para clone por arquivo (${repo_url}); fallback para git clone normal."
    exec "${real_git}" "$@"
  fi

  command -v curl >/dev/null 2>&1 || die "curl não encontrado"
  local archive_path source_url
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

  if ! source_url="$(download_github_archive "${slug}" "${branch}" "${archive_path}")"; then
    if is_truthy "${GIT_ZIP_WRAPPER_STRICT:-0}"; then
      die "falha ao baixar arquivo para ${repo_url} (branch/tag: ${branch:-HEAD})"
    fi
    log "falha ao baixar arquivo para ${repo_url}; fallback para git clone normal."
    exec "${real_git}" "$@"
  fi

  extract_archive_to_destination "${archive_path}" "${destination}"
  log "clone(${ARCHIVE_FORMAT}) concluído: ${repo_url} -> ${destination} (source: ${source_url})"
}

main "$@"
