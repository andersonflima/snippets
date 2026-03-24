#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[git-zip-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

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

CLONE_REPO_URL=""
CLONE_BRANCH=""
CLONE_DESTINATION=""

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
      --branch=*)
        CLONE_BRANCH="${arg#--branch=}"
        index=$((index + 1))
        ;;
      --depth|--filter|--origin|--config|--upload-pack|--template|--reference|--reference-if-able|--server-option|--separate-git-dir)
        (( index + 1 < ${#args[@]} )) || die "faltou valor para ${arg}"
        index=$((index + 2))
        ;;
      --depth=*|--filter=*|--origin=*|--config=*|--upload-pack=*|--template=*|--reference=*|--reference-if-able=*|--server-option=*|--separate-git-dir=*)
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
  if [[ -n "${branch}" ]]; then
    candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/heads/${branch}")
    candidate_urls+=("https://codeload.github.com/${slug}/zip/refs/tags/${branch}")
    candidate_urls+=("https://codeload.github.com/${slug}/zip/${branch}")
  else
    candidate_urls+=("https://codeload.github.com/${slug}/zip/HEAD")
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

download_url_with_retries() {
  local url archive_path
  url="$1"
  archive_path="$2"

  local mode attempt mode_label
  for mode in "" "--http1.1"; do
    mode_label="default"
    if [[ -n "${mode}" ]]; then
      mode_label="${mode}"
    fi
    for attempt in 1 2 3; do
      if curl -fsSL --connect-timeout 20 --max-time 300 ${mode} "${url}" -o "${archive_path}"; then
        return 0
      fi
      log "download falhou (tentativa ${attempt}/3, modo ${mode_label}): ${url}"
      sleep 2
    done
  done
  return 1
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
  unzip -q "${archive_path}" -d "${temp_extract}"
  top_dir="$(find "${temp_extract}" -mindepth 1 -maxdepth 1 -type d | head -n 1 || true)"
  [[ -n "${top_dir}" ]] || die "não foi possível localizar conteúdo extraído do arquivo zip"
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
  command -v unzip >/dev/null 2>&1 || die "unzip não encontrado"
  command -v mktemp >/dev/null 2>&1 || die "mktemp não encontrado"

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

  local temp_dir archive_path source_url
  temp_dir="$(mktemp -d -t git-zip-clone-XXXXXX)"
  archive_path="${temp_dir}/repo.zip"
  trap 'rm -rf "${temp_dir}"' EXIT

  if ! source_url="$(download_github_archive "${slug}" "${branch}" "${archive_path}")"; then
    if is_truthy "${GIT_ZIP_WRAPPER_STRICT:-0}"; then
      die "falha ao baixar zip para ${repo_url} (branch/tag: ${branch:-HEAD})"
    fi
    log "falha ao baixar zip para ${repo_url}; fallback para git clone normal."
    exec "${real_git}" "$@"
  fi

  extract_archive_to_destination "${archive_path}" "${destination}"
  log "clone(zip) concluído: ${repo_url} -> ${destination} (source: ${source_url})"
}

main "$@"
