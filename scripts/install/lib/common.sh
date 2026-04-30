#!/usr/bin/env bash

restricted_log() {
  printf '[%s] %s\n' "${RESTRICTED_SCRIPT_NAME:-restricted-dev-env}" "$*" >&2
}

restricted_die() {
  restricted_log "erro: $*"
  exit 1
}

restricted_is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

restricted_resolve_script_path() {
  local current_path current_dir link_target
  current_path="$1"

  if ! command -v readlink >/dev/null 2>&1; then
    printf '%s\n' "${current_path}"
    return 0
  fi

  while [[ -L "${current_path}" ]]; do
    current_dir="$(CDPATH= cd -- "$(dirname -- "${current_path}")" && pwd)"
    link_target="$(readlink "${current_path}")"
    case "${link_target}" in
      /*)
        current_path="${link_target}"
        ;;
      *)
        current_path="${current_dir}/${link_target}"
        ;;
    esac
  done

  printf '%s\n' "${current_path}"
}

restricted_candidate_paths_for_binary() {
  local binary_name
  binary_name="$1"

  case "${binary_name}" in
    curl)
      printf '/usr/bin/curl\n'
      printf '/usr/local/bin/curl\n'
      printf '/opt/homebrew/bin/curl\n'
      printf '/bin/curl\n'
      printf '/sbin/curl\n'
      ;;
    wget)
      printf '/usr/bin/wget\n'
      printf '/usr/local/bin/wget\n'
      printf '/opt/homebrew/bin/wget\n'
      printf '/bin/wget\n'
      printf '/sbin/wget\n'
      ;;
    git)
      printf '/usr/bin/git\n'
      printf '/usr/local/bin/git\n'
      printf '/opt/homebrew/bin/git\n'
      printf '/bin/git\n'
      printf '/usr/libexec/git-core/git\n'
      ;;
    mix)
      printf '/usr/bin/mix\n'
      printf '/usr/local/bin/mix\n'
      printf '/opt/homebrew/bin/mix\n'
      printf '/bin/mix\n'
      ;;
    *)
      :
      ;;
  esac
}

restricted_is_wrapper_binary_path() {
  local binary_name candidate_path wrapper_path
  binary_name="$1"
  candidate_path="$2"

  case "${binary_name}" in
    curl)
      wrapper_path="${RESTRICTED_CURL_INSTALL_DIR:-${HOME}/.local/share/curl-python-wrapper/bin}/curl"
      [[ "${candidate_path}" == "${RESTRICTED_WRAPPER_SHIM_DIR:-${HOME}/.local/bin}/curl" ]] && return 0
      ;;
    wget)
      wrapper_path="${RESTRICTED_CURL_INSTALL_DIR:-${HOME}/.local/share/curl-python-wrapper/bin}/wget"
      [[ "${candidate_path}" == "${RESTRICTED_WRAPPER_SHIM_DIR:-${HOME}/.local/bin}/wget" ]] && return 0
      ;;
    git)
      wrapper_path="${RESTRICTED_GIT_INSTALL_DIR:-${HOME}/.local/share/git-zip-wrapper/bin}/git"
      [[ "${candidate_path}" == "${RESTRICTED_WRAPPER_SHIM_DIR:-${HOME}/.local/bin}/git" ]] && return 0
      ;;
    mix)
      case "${candidate_path}" in
        "${HOME}"/.local/share/mix-*-wrapper/bin/mix)
          return 0
          ;;
      esac
      return 1
      ;;
    brew)
      wrapper_path="${RESTRICTED_BREW_INSTALL_DIR:-${HOME}/.local/share/homebrew-install-wrapper/bin}/brew"
      ;;
    *)
      return 1
      ;;
  esac

  [[ "${candidate_path}" == "${wrapper_path}" ]]
}

restricted_resolve_real_binary() {
  local binary_name candidate seen
  binary_name="$1"
  seen=""

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    restricted_is_wrapper_binary_path "${binary_name}" "${candidate}" && continue
    [[ "${seen}" == *$'\n'"${candidate}"$'\n'* ]] && continue
    seen+="${candidate}"$'\n'
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF2
$(which -a "${binary_name}" 2>/dev/null || true)
EOF2

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    [[ "${seen}" == *$'\n'"${candidate}"$'\n'* ]] && continue
    seen+="${candidate}"$'\n'
    [[ -x "${candidate}" ]] || continue
    restricted_is_wrapper_binary_path "${binary_name}" "${candidate}" && continue
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF3
$(restricted_candidate_paths_for_binary "${binary_name}")
EOF3

  return 1
}

restricted_target_shell_name() {
  local target_shell
  target_shell="${RESTRICTED_DEV_ENV_TARGET_SHELL:-zsh}"
  target_shell="${target_shell##*/}"

  case "${target_shell}" in
    zsh|bash|fish|sh)
      printf '%s\n' "${target_shell}"
      ;;
    *)
      printf '%s\n' "zsh"
      ;;
  esac
}

restricted_default_shell_rc() {
  local shell_name
  shell_name="$(restricted_target_shell_name)"

  case "${shell_name}" in
    fish)
      printf '%s\n' "${HOME}/.config/fish/config.fish"
      ;;
    zsh)
      printf '%s\n' "${HOME}/.zshrc"
      ;;
    bash)
      printf '%s\n' "${HOME}/.bashrc"
      ;;
    *)
      printf '%s\n' "${HOME}/.profile"
      ;;
  esac
}

restricted_sanitize_wrapper_env() {
  local old_path new_path old_ifs path_entry
  old_path="${PATH-}"
  new_path=""
  old_ifs="${IFS}"

  unset CURL 2>/dev/null || true
  unset WGET 2>/dev/null || true
  unset GIT 2>/dev/null || true
  unset BREW 2>/dev/null || true

  unset CURL_WRAPPER_REAL_CURL 2>/dev/null || true
  unset WGET_WRAPPER_REAL_WGET 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_REAL_GIT 2>/dev/null || true
  unset BREW_WRAPPER_ENABLED 2>/dev/null || true
  unset BREW_WRAPPER_REAL_BREW 2>/dev/null || true
  unset BREW_WRAPPER_CURL_BIN 2>/dev/null || true
  unset BREW_WRAPPER_GIT_BIN 2>/dev/null || true
  unset BREW_WRAPPER_NO_AUTO_UPDATE 2>/dev/null || true
  unset CURL_WRAPPER_PROXY 2>/dev/null || true
  unset WGET_WRAPPER_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CURL_CACERT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ARCHIVE_FORMAT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CLONE_ORDER 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_LFS_MODE 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS 2>/dev/null || true
  unset CURL_WRAPPER_MASON_SEED_DIR 2>/dev/null || true
  unset CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR 2>/dev/null || true

  IFS=':'
  for path_entry in ${old_path}; do
    [[ -n "${path_entry}" ]] || continue
    case "${path_entry}" in
      "${RESTRICTED_BREW_INSTALL_DIR:-${HOME}/.local/share/homebrew-install-wrapper/bin}"|\
      "${RESTRICTED_CURL_INSTALL_DIR:-${HOME}/.local/share/curl-python-wrapper/bin}"|\
      "${RESTRICTED_GIT_INSTALL_DIR:-${HOME}/.local/share/git-zip-wrapper/bin}"|\
      "${HOME}"/.local/share/mix-*-wrapper/bin|\
      "${HOME}"/.local/share/nvim-*-wrapper/bin)
        continue
        ;;
    esac

    if [[ -z "${new_path}" ]]; then
      new_path="${path_entry}"
    else
      new_path="${new_path}:${path_entry}"
    fi
  done
  IFS="${old_ifs}"

  if [[ -n "${new_path}" ]]; then
    PATH="${new_path}"
  else
    PATH="/usr/bin:/bin:/usr/sbin:/sbin"
  fi
  export PATH
}

restricted_remove_legacy_brew_wrapper_installation() {
  local brew_wrapper_root
  brew_wrapper_root="${RESTRICTED_BREW_WRAPPER_ROOT:-${HOME}/.local/share/homebrew-install-wrapper}"
  [[ -d "${brew_wrapper_root}" ]] || return 0
  rm -rf "${brew_wrapper_root}"
  restricted_log "wrapper legado do brew removido: ${brew_wrapper_root}"
}

restricted_run_bash_script() {
  local script_path
  script_path="$1"
  shift

  if command -v bash >/dev/null 2>&1; then
    bash "${script_path}" "$@"
    return $?
  fi

  sh "${script_path}" "$@"
}
