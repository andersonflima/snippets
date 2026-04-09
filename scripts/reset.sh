#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
RESET_SCRIPT="${SCRIPT_DIR}/install/reset_restricted_dev_env.sh"
LEGACY_BREW_WRAPPER_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
LEGACY_BREW_WRAPPER_ROOT="${HOME}/.local/share/homebrew-install-wrapper"

sanitize_legacy_wrapper_env() {
  old_path="${PATH-}"
  new_path=""
  old_ifs="${IFS}"

  unset BREW 2>/dev/null || true
  unset BREW_WRAPPER_ENABLED 2>/dev/null || true
  unset BREW_WRAPPER_REAL_BREW 2>/dev/null || true
  unset BREW_WRAPPER_CURL_BIN 2>/dev/null || true
  unset BREW_WRAPPER_GIT_BIN 2>/dev/null || true
  unset BREW_WRAPPER_NO_AUTO_UPDATE 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CLONE_ORDER 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS 2>/dev/null || true

  IFS=':'
  for path_entry in ${old_path}; do
    [ -n "${path_entry}" ] || continue
    case "${path_entry}" in
      "${LEGACY_BREW_WRAPPER_DIR}"|"${HOME}"/.local/share/mix-*-wrapper/bin|"${HOME}"/.local/share/nvim-*-wrapper/bin)
        continue
        ;;
    esac

    if [ -z "${new_path}" ]; then
      new_path="${path_entry}"
    else
      new_path="${new_path}:${path_entry}"
    fi
  done
  IFS="${old_ifs}"

  if [ -n "${new_path}" ]; then
    PATH="${new_path}"
  else
    PATH="/usr/bin:/bin:/usr/sbin:/sbin"
  fi
  export PATH
}

should_keep_install_dirs() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --keep-install-dirs)
        return 0
        ;;
    esac
    shift
  done
  return 1
}

remove_legacy_brew_wrapper_installation() {
  [ -d "${LEGACY_BREW_WRAPPER_ROOT}" ] || return 0
  rm -rf "${LEGACY_BREW_WRAPPER_ROOT}"
}

sanitize_legacy_wrapper_env
if ! should_keep_install_dirs "$@"; then
  remove_legacy_brew_wrapper_installation
fi

if command -v bash >/dev/null 2>&1; then
  exec bash "${RESET_SCRIPT}" "$@"
fi

exec sh "${RESET_SCRIPT}" "$@"
