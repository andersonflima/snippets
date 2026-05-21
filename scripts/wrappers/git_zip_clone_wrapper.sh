#!/usr/bin/env bash
set -euo pipefail

SELF_PATH="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/$(basename -- "$0")"

resolve_real_git() {
  if [[ -n "${GIT_ZIP_WRAPPER_REAL_GIT:-}" ]]; then
    printf '%s\n' "${GIT_ZIP_WRAPPER_REAL_GIT}"
    return
  fi

  local git_path
  git_path="$(command -v git 2>/dev/null || true)"
  if [[ -z "${git_path}" ]]; then
    printf '%s\n' "git"
    return
  fi

  if [[ "${git_path}" == "${SELF_PATH}" ]]; then
    printf '%s\n' "/usr/bin/git"
    return
  fi

  printf '%s\n' "${git_path}"
}

REAL_GIT="$(resolve_real_git)"
CURL_BIN="${CURL:-curl}"
HTTP_CLIENT="${GIT_ZIP_WRAPPER_HTTP_CLIENT:-curl}"
STRICT_MODE="${GIT_ZIP_WRAPPER_STRICT:-0}"
CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
ARCHIVE_FORMAT="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
ALLOW_REMOTE_GIT_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"

log() {
  if [[ "${STRICT_MODE}" == "1" ]]; then
    printf '[git-zip-wrapper] %s\n' "$*" >&2
  fi
}

exec_real_git() {
  exec "${REAL_GIT}" "$@"
}

first_git_command() {
  local idx=1
  while [[ ${idx} -le $# ]]; do
    local token="${!idx}"
    case "${token}" in
      -c|--config|--exec-path|--git-dir|--work-tree|--namespace|--super-prefix|--config-env)
        idx=$((idx + 2))
        ;;
      --exec-path=*|--git-dir=*|--work-tree=*|--namespace=*|--super-prefix=*|--config-env=*)
        idx=$((idx + 1))
        ;;
      --*|-*)
        idx=$((idx + 1))
        ;;
      *)
        printf '%s\n' "${token}"
        return 0
        ;;
    esac
  done
  return 1
}

parse_clone_target() {
  local repo=""
  local dest=""
  local branch="main"
  local idx=1
  local -a positionals=()

  while [[ ${idx} -le $# ]]; do
    local token="${!idx}"
    case "${token}" in
      -c|--config)
        idx=$((idx + 2))
        ;;
      --branch|-b)
        idx=$((idx + 1))
        if [[ ${idx} -le $# ]]; then
          branch="${!idx}"
        fi
        idx=$((idx + 1))
        ;;
      --branch=*)
        branch="${token#--branch=}"
        idx=$((idx + 1))
        ;;
      --origin|--reference|--reference-if-able|--dissociate|--separate-git-dir|--depth|--shallow-since|--shallow-exclude|--filter|--upload-pack|--template)
        idx=$((idx + 2))
        ;;
      --origin=*|--reference=*|--reference-if-able=*|--separate-git-dir=*|--depth=*|--shallow-since=*|--shallow-exclude=*|--filter=*|--upload-pack=*|--template=*)
        idx=$((idx + 1))
        ;;
      --single-branch|--no-single-branch|--recurse-submodules|--no-recurse-submodules|--bare|--mirror|--local|--no-local|--quiet|--verbose|--progress|--no-progress)
        idx=$((idx + 1))
        ;;
      --)
        idx=$((idx + 1))
        while [[ ${idx} -le $# ]]; do
          positionals+=("${!idx}")
          idx=$((idx + 1))
        done
        ;;
      -*)
        idx=$((idx + 1))
        ;;
      *)
        if [[ "${token}" == "clone" && ${#positionals[@]} -eq 0 ]]; then
          idx=$((idx + 1))
          continue
        fi
        positionals+=("${token}")
        idx=$((idx + 1))
        ;;
    esac
  done

  if [[ ${#positionals[@]} -ge 1 ]]; then
    repo="${positionals[0]}"
  fi
  if [[ ${#positionals[@]} -ge 2 ]]; then
    dest="${positionals[1]}"
  fi

  if [[ -z "${repo}" ]]; then
    return 1
  fi

  if [[ -z "${dest}" ]]; then
    local repo_basename="${repo##*/}"
    repo_basename="${repo_basename%.git}"
    dest="${repo_basename}"
  fi

  printf '%s\n%s\n%s\n' "${repo}" "${dest}" "${branch}"
}

extract_github_repo() {
  local repo_url="$1"

  if [[ "${repo_url}" =~ ^https://github\.com/([^/]+/[^/]+)(\.git)?$ ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi

  if [[ "${repo_url}" =~ ^git@github\.com:([^/]+/[^/]+)(\.git)?$ ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi

  return 1
}

extract_archive_to_destination() {
  local archive_path="$1"
  local destination="$2"

  local extract_dir
  extract_dir="$(mktemp -d)"

  if command -v unzip >/dev/null 2>&1; then
    unzip -q "${archive_path}" -d "${extract_dir}"
  else
    python3 - "${archive_path}" "${extract_dir}" <<'PY'
import sys
import zipfile

archive_path, out_dir = sys.argv[1:3]
with zipfile.ZipFile(archive_path) as archive:
    archive.extractall(out_dir)
PY
  fi

  local source_dir
  source_dir="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | sed -n '1p')"
  if [[ -z "${source_dir}" ]]; then
    rm -rf "${extract_dir}"
    return 1
  fi

  rm -rf "${destination}"
  mkdir -p "${destination}"

  shopt -s dotglob nullglob
  local item
  for item in "${source_dir}"/*; do
    mv "${item}" "${destination}/"
  done
  shopt -u dotglob nullglob

  rm -rf "${extract_dir}"
  return 0
}

init_local_git_repo() {
  local destination="$1"
  local branch="$2"
  local repo_url="$3"

  "${REAL_GIT}" -C "${destination}" init >/dev/null 2>&1
  "${REAL_GIT}" -C "${destination}" checkout -B "${branch}" >/dev/null 2>&1 || true
  "${REAL_GIT}" -C "${destination}" remote remove origin >/dev/null 2>&1 || true
  "${REAL_GIT}" -C "${destination}" remote add origin "${repo_url}" >/dev/null 2>&1

  # Snapshot commit local para manter HEAD/branch válidos para ferramentas que esperam repo Git.
  "${REAL_GIT}" -C "${destination}" add -A >/dev/null 2>&1
  GIT_AUTHOR_NAME='archive-wrapper' \
  GIT_AUTHOR_EMAIL='archive-wrapper@local.invalid' \
  GIT_COMMITTER_NAME='archive-wrapper' \
  GIT_COMMITTER_EMAIL='archive-wrapper@local.invalid' \
    "${REAL_GIT}" -C "${destination}" commit -m "archive snapshot" >/dev/null 2>&1 || true
}

archive_clone() {
  local repo_url="$1"
  local destination="$2"
  local branch="$3"

  local repo
  repo="$(extract_github_repo "${repo_url}" || true)"
  if [[ -z "${repo}" ]]; then
    log "repositorio nao suportado para archive: ${repo_url}"
    return 1
  fi

  local archive_url="https://github.com/${repo}/archive/${branch}.zip"
  local archive_tmp_dir
  archive_tmp_dir="$(mktemp -d)"
  local archive_path="${archive_tmp_dir}/archive.zip"

  if ! download_archive "${archive_url}" "${archive_path}"; then
    rm -rf "${archive_tmp_dir}"
    return 1
  fi

  if ! extract_archive_to_destination "${archive_path}" "${destination}"; then
    rm -rf "${archive_tmp_dir}"
    return 1
  fi

  rm -rf "${archive_tmp_dir}"
  init_local_git_repo "${destination}" "${branch}" "${repo_url}"
  return 0
}

download_archive() {
  local url="$1"
  local output_path="$2"

  if [[ "${HTTP_CLIENT}" == "python" ]]; then
    python3 - "${url}" "${output_path}" <<'PY'
import sys
import urllib.request

url, output = sys.argv[1:3]
with urllib.request.urlopen(url, timeout=60) as response:
    payload = response.read()
with open(output, "wb") as handle:
    handle.write(payload)
PY
    return $?
  fi

  "${CURL_BIN}" -fsSL "${url}" --output "${output_path}"
}

run_clone_with_strategy() {
  local repo_url="$1"
  local destination="$2"
  local branch="$3"
  shift 3
  local -a original_args=("$@")

  case "${CLONE_ORDER}" in
    git-first)
      if "${REAL_GIT}" "${original_args[@]}"; then
        return 0
      fi
      ;;
    local-first)
      ;;
    *)
      log "clone order desconhecido (${CLONE_ORDER}), usando local-first"
      ;;
  esac

  if archive_clone "${repo_url}" "${destination}" "${branch}"; then
    return 0
  fi

  if [[ "${ALLOW_REMOTE_GIT_FALLBACK}" == "1" ]]; then
    "${REAL_GIT}" "${original_args[@]}"
    return $?
  fi

  return 1
}

main() {
  local -a args=("$@")

  local command
  command="$(first_git_command "${args[@]}" || true)"
  if [[ "${command}" != "clone" ]]; then
    exec_real_git "$@"
  fi

  local parsed
  parsed="$(parse_clone_target "${args[@]}" || true)"
  if [[ -z "${parsed}" ]]; then
    exec_real_git "$@"
  fi

  local repo_url
  local destination
  local branch
  repo_url="$(printf '%s\n' "${parsed}" | sed -n '1p')"
  destination="$(printf '%s\n' "${parsed}" | sed -n '2p')"
  branch="$(printf '%s\n' "${parsed}" | sed -n '3p')"

  run_clone_with_strategy "${repo_url}" "${destination}" "${branch}" "$@"
}

main "$@"
