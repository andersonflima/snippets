#!/usr/bin/env bash
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
  requested="${requested,,}"

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
  done < <(which -a git 2>/dev/null || true)

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
  CLONE_BRANCH=""
  CLONE_DESTINATION=""

  local args=("$@")
  local index=0
  local positional=()

  while (( index < ${#args[@]} )); do
    local arg="${args[index]}"
    case "${arg}" in
      -b|--branch)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        CLONE_BRANCH="${args[index + 1]}"
        index=$((index + 2))
        ;;
      -c|--config)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        index=$((index + 2))
        ;;
      -c*)
        index=$((index + 1))
        ;;
      -o|--origin|-u|--upload-pack)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        index=$((index + 2))
        ;;
      -j|--jobs)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        index=$((index + 2))
        ;;
      --branch=*)
        CLONE_BRANCH="${arg#--branch=}"
        index=$((index + 1))
        ;;
      --config=*|--jobs=*)
        index=$((index + 1))
        ;;
      --depth|--filter|--origin|--config|--upload-pack|--template|--reference|--reference-if-able|--server-option|--separate-git-dir)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        index=$((index + 2))
        ;;
      --depth=*|--filter=*|--origin=*|--config=*|--upload-pack=*|--template=*|--reference=*|--reference-if-able=*|--server-option=*|--separate-git-dir=*)
        index=$((index + 1))
        ;;
      --single-branch|--no-single-branch|--recurse-submodules|--shallow-submodules|--no-shallow-submodules|--no-tags|--tags|--quiet|--verbose|--progress|--no-checkout)
        index=$((index + 1))
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

  (( ${#positional[@]} >= 1 )) || die "uso inválido: git clone <repo> [destino]"
  CLONE_REPO_URL="${positional[0]}"
  if (( ${#positional[@]} >= 2 )); then
    CLONE_DESTINATION="${positional[1]}"
  else
    CLONE_DESTINATION="$(default_destination_from_repo "${CLONE_REPO_URL}")"
  fi
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

  local candidate_urls=()

  if [[ "${ARCHIVE_FORMAT}" == "zip" ]]; then
    if [[ -n "${branch}" ]]; then
      candidate_urls+=("https://github.com/${slug}/archive/refs/heads/${branch}.zip")
      candidate_urls+=("https://github.com/${slug}/archive/refs/tags/${branch}.zip")
      candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/heads/${branch}")
      candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/tags/${branch}")
      candidate_urls+=("https://codeload.github.com/${slug}/zip/${branch}")
    else
      candidate_urls+=("https://github.com/${slug}/archive/HEAD.zip")
      candidate_urls+=("https://codeload.github.com/${slug}/zip/HEAD")
    fi
  else
    if [[ -n "${branch}" ]]; then
      candidate_urls+=("https://github.com/${slug}/archive/refs/heads/${branch}.tar.gz")
      candidate_urls+=("https://github.com/${slug}/archive/refs/tags/${branch}.tar.gz")
      candidate_urls+=("https://codeload.github.com/${slug}/tar.gz/refs/heads/${branch}")
      candidate_urls+=("https://codeload.github.com/${slug}/tar.gz/refs/tags/${branch}")
      candidate_urls+=("https://codeload.github.com/${slug}/tar.gz/${branch}")
    else
      candidate_urls+=("https://github.com/${slug}/archive/HEAD.tar.gz")
      candidate_urls+=("https://codeload.github.com/${slug}/tar.gz/HEAD")
    fi

    if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
      if [[ -n "${branch}" ]]; then
        candidate_urls+=("https://github.com/${slug}/archive/refs/heads/${branch}.zip")
        candidate_urls+=("https://github.com/${slug}/archive/refs/tags/${branch}.zip")
        candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/heads/${branch}")
        candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/tags/${branch}")
        candidate_urls+=("https://codeload.github.com/${slug}/zip/${branch}")
      else
        candidate_urls+=("https://github.com/${slug}/archive/HEAD.zip")
        candidate_urls+=("https://codeload.github.com/${slug}/zip/HEAD")
      fi
    fi
  fi

  local url
  for url in "${candidate_urls[@]}"; do
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

  local mode attempt mode_label
  local -a curl_modes=("" "--http1.1" "-4" "-4 --http1.1")
  local user_agent
  user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
  for mode in "${curl_modes[@]}"; do
    mode_label="default"
    if [[ -n "${mode}" ]]; then
      mode_label="${mode}"
    fi
    for attempt in 1 2 3; do
      if curl -fsSL \
        --connect-timeout 20 \
        --max-time 300 \
        --retry 3 \
        --retry-delay 2 \
        --retry-all-errors \
        --tlsv1.2 \
        -A "${user_agent}" \
        -H "Accept: application/octet-stream,*/*" \
        ${mode} \
        "${url}" \
        -o "${archive_path}"; then
        return 0
      fi
      log "download falhou (tentativa ${attempt}/3, modo ${mode_label}): ${url}"
      sleep 2
    done
  done
  return 1
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

main() {
  local real_git
  real_git="$(resolve_real_git)"

  if [[ $# -eq 0 ]]; then
    exec "${real_git}"
  fi

  if [[ "$1" != "clone" ]]; then
    exec "${real_git}" "$@"
  fi

  command -v curl >/dev/null 2>&1 || die "curl não encontrado"
  command -v tar >/dev/null 2>&1 || die "tar não encontrado"
  command -v mktemp >/dev/null 2>&1 || die "mktemp não encontrado"
  if is_truthy "${ALLOW_ZIP_FALLBACK}"; then
    command -v unzip >/dev/null 2>&1 || die "unzip não encontrado"
  fi

  parse_clone_arguments "${@:2}"
  local repo_url branch destination
  repo_url="${CLONE_REPO_URL}"
  branch="${CLONE_BRANCH}"
  destination="${CLONE_DESTINATION}"

  local slug
  if ! slug="$(extract_github_slug "${repo_url}")"; then
    log "host não suportado para clone por zip (${repo_url}); fallback para git clone normal."
    exec "${real_git}" "$@"
  fi

  validate_clone_destination "${destination}"

  local archive_path source_url
  GIT_ZIP_WRAPPER_TMP_DIR="$(mktemp -d -t git-zip-clone-XXXXXX)"
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
