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
STRICT_MODE="${GIT_ZIP_WRAPPER_STRICT:-0}"
CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
ARCHIVE_FORMAT="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-tar.gz}"
ALLOW_REMOTE_GIT_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"

log() {
  if [[ "${STRICT_MODE}" == "1" ]]; then
    printf '[git-zip-wrapper] %s\n' "$*" >&2
  fi
}

exec_real_git() {
  exec "${REAL_GIT}" "$@"
}

parse_command() {
  local -n _args_ref=$1
  local _idx=0

  while (( _idx < ${#_args_ref[@]} )); do
    local token="${_args_ref[$_idx]}"
    case "${token}" in
      -c|--config)
        _idx=$((_idx + 2))
        ;;
      --exec-path|--git-dir|--work-tree|--namespace|--super-prefix|--config-env)
        _idx=$((_idx + 2))
        ;;
      --exec-path=*|--git-dir=*|--work-tree=*|--namespace=*|--super-prefix=*|--config-env=*)
        _idx=$((_idx + 1))
        ;;
      --*)
        _idx=$((_idx + 1))
        ;;
      -*)
        _idx=$((_idx + 1))
        ;;
      *)
        printf '%s\n' "${token}"
        return 0
        ;;
    esac
  done

  return 1
}

parse_clone_args() {
  local -n _args_ref=$1
  local -n _repo_ref=$2
  local -n _dest_ref=$3
  local -n _branch_ref=$4

  _repo_ref=""
  _dest_ref=""
  _branch_ref="main"

  local _idx=0
  local -a _positionals=()
  while (( _idx < ${#_args_ref[@]} )); do
    local token="${_args_ref[$_idx]}"
    case "${token}" in
      -c|--config)
        _idx=$((_idx + 2))
        continue
        ;;
      --branch|-b)
        _idx=$((_idx + 1))
        if (( _idx < ${#_args_ref[@]} )); then
          _branch_ref="${_args_ref[$_idx]}"
        fi
        _idx=$((_idx + 1))
        continue
        ;;
      --branch=*)
        _branch_ref="${token#--branch=}"
        _idx=$((_idx + 1))
        continue
        ;;
      --origin|--reference|--reference-if-able|--dissociate|--separate-git-dir|--depth|--shallow-since|--shallow-exclude|--filter|--upload-pack|--template)
        _idx=$((_idx + 2))
        continue
        ;;
      --origin=*|--reference=*|--reference-if-able=*|--separate-git-dir=*|--depth=*|--shallow-since=*|--shallow-exclude=*|--filter=*|--upload-pack=*|--template=*)
        _idx=$((_idx + 1))
        continue
        ;;
      --single-branch|--no-single-branch|--recurse-submodules|--no-recurse-submodules|--bare|--mirror|--local|--no-local|--quiet|--verbose|--progress|--no-progress)
        _idx=$((_idx + 1))
        continue
        ;;
      --)
        _idx=$((_idx + 1))
        while (( _idx < ${#_args_ref[@]} )); do
          _positionals+=("${_args_ref[$_idx]}")
          _idx=$((_idx + 1))
        done
        break
        ;;
      -*)
        _idx=$((_idx + 1))
        continue
        ;;
      *)
        if [[ "${token}" == "clone" && ${#_positionals[@]} -eq 0 ]]; then
          _idx=$((_idx + 1))
          continue
        fi
        _positionals+=("${token}")
        _idx=$((_idx + 1))
        ;;
    esac
  done

  if (( ${#_positionals[@]} >= 1 )); then
    _repo_ref="${_positionals[0]}"
  fi

  if (( ${#_positionals[@]} >= 2 )); then
    _dest_ref="${_positionals[1]}"
  fi

  if [[ -z "${_repo_ref}" ]]; then
    return 1
  fi

  if [[ -z "${_dest_ref}" ]]; then
    local repo_basename="${_repo_ref##*/}"
    repo_basename="${repo_basename%.git}"
    _dest_ref="${repo_basename}"
  fi

  return 0
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

  case "${archive_path}" in
    *.zip)
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
      ;;
    *)
      tar -xzf "${archive_path}" -C "${extract_dir}"
      ;;
  esac

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

  "${REAL_GIT}" -C "${destination}" init -b "${branch}" >/dev/null 2>&1 || {
    "${REAL_GIT}" -C "${destination}" init >/dev/null 2>&1
    "${REAL_GIT}" -C "${destination}" checkout -B "${branch}" >/dev/null 2>&1 || true
  }
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

  local archive_ext="tar.gz"
  if [[ "${ARCHIVE_FORMAT}" == "zip" ]]; then
    archive_ext="zip"
  fi

  local archive_url="https://github.com/${repo}/archive/${branch}.${archive_ext}"
  local archive_path
  archive_path="$(mktemp "${TMPDIR:-/tmp}/git-zip-wrapper-archive.XXXXXX.${archive_ext}")"

  if ! "${CURL_BIN}" -fsSL "${archive_url}" --output "${archive_path}"; then
    rm -f "${archive_path}"
    return 1
  fi

  if ! extract_archive_to_destination "${archive_path}" "${destination}"; then
    rm -f "${archive_path}"
    return 1
  fi

  rm -f "${archive_path}"
  init_local_git_repo "${destination}" "${branch}"
  return 0
}

run_clone_with_strategy() {
  local -a original_args=("$@")

  local repo_url=""
  local destination=""
  local branch="main"
  if ! parse_clone_args original_args repo_url destination branch; then
    if [[ "${STRICT_MODE}" == "1" ]]; then
      log "falha parse clone; encaminhando para git real"
    fi
    exec_real_git "${original_args[@]}"
  fi

  if [[ "${CLONE_ORDER}" == "git-first" ]]; then
    if "${REAL_GIT}" "${original_args[@]}"; then
      return 0
    fi

    if archive_clone "${repo_url}" "${destination}" "${branch}"; then
      return 0
    fi

    if [[ "${ALLOW_REMOTE_GIT_FALLBACK}" == "1" ]]; then
      "${REAL_GIT}" "${original_args[@]}"
      return $?
    fi

    return 1
  fi

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

  local command=""
  command="$(parse_command args || true)"
  if [[ "${command}" != "clone" ]]; then
    exec_real_git "$@"
  fi

  run_clone_with_strategy "$@"
}

main "$@"
