#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/install/setup_restricted_dev_env.sh"
RESET_SCRIPT="${SCRIPT_DIR}/install/reset_restricted_dev_env.sh"
LEGACY_BREW_WRAPPER_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
LEGACY_BREW_WRAPPER_ROOT="${HOME}/.local/share/homebrew-install-wrapper"
CURL_WRAPPER_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
GIT_WRAPPER_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
MIX_WRAPPER_DIR="${HOME}/.local/share/mix-ec2-wrapper/bin"
NVIM_WRAPPER_DIR="${HOME}/.local/share/nvim-ec2-wrapper/bin"

resolve_default_shell_rc() {
  active_shell="${SHELL-}"
  shell_name="${active_shell##*/}"

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
      if [ -f "${HOME}/.zshrc" ]; then
        printf '%s\n' "${HOME}/.zshrc"
      elif [ -f "${HOME}/.bashrc" ]; then
        printf '%s\n' "${HOME}/.bashrc"
      else
        printf '%s\n' "${HOME}/.profile"
      fi
      ;;
  esac
}

DEFAULT_SHELL_RC="$(resolve_default_shell_rc)"

run_script_with_bash_preference() {
  script_path="$1"
  shift

  if command -v bash >/dev/null 2>&1; then
    bash "${script_path}" "$@"
    return $?
  fi

  sh "${script_path}" "$@"
}

sanitize_current_wrapper_env() {
  old_path="${PATH-}"
  new_path=""
  old_ifs="${IFS}"

  unset CURL 2>/dev/null || true
  unset WGET 2>/dev/null || true
  unset GIT 2>/dev/null || true
  unset MIX 2>/dev/null || true

  unset CURL_WRAPPER_REAL_CURL 2>/dev/null || true
  unset WGET_WRAPPER_REAL_WGET 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_REAL_GIT 2>/dev/null || true
  unset MIX_WRAPPER_REAL_MIX 2>/dev/null || true

  unset WRAPPERS_VIA_EC2_ENABLED 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_ALL_URLS 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_INSTANCE_NAME 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_AWS_PROFILE 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_AWS_REGION 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_S3_BUCKET 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_S3_PREFIX 2>/dev/null || true
  unset WRAPPERS_VIA_EC2_PROXY 2>/dev/null || true

  unset CURL_WRAPPER_USE_EC2 2>/dev/null || true
  unset CURL_WRAPPER_EC2_ALL_URLS 2>/dev/null || true
  unset CURL_WRAPPER_EC2_REQUIRED 2>/dev/null || true
  unset CURL_WRAPPER_EC2_PROXY 2>/dev/null || true
  unset CURL_WRAPPER_PROXY 2>/dev/null || true
  unset CURL_WRAPPER_CACERT 2>/dev/null || true
  unset CURL_WRAPPER_MASON_SEED_DIR 2>/dev/null || true
  unset CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR 2>/dev/null || true

  unset WGET_WRAPPER_USE_EC2 2>/dev/null || true
  unset WGET_WRAPPER_EC2_ALL_URLS 2>/dev/null || true
  unset WGET_WRAPPER_EC2_REQUIRED 2>/dev/null || true
  unset WGET_WRAPPER_EC2_PROXY 2>/dev/null || true
  unset WGET_WRAPPER_PROXY 2>/dev/null || true

  unset GIT_ZIP_WRAPPER_USE_EC2 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_EC2_ALL_URLS 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_EC2_REQUIRED 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_EC2_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_PROXY 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CURL_CACERT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ARCHIVE_FORMAT 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_CLONE_ORDER 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_LFS_MODE 2>/dev/null || true
  unset GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS 2>/dev/null || true

  unset MIX_VIA_EC2_INSTANCE_NAME 2>/dev/null || true
  unset MIX_VIA_EC2_AWS_PROFILE 2>/dev/null || true
  unset MIX_VIA_EC2_AWS_REGION 2>/dev/null || true
  unset MIX_VIA_EC2_S3_BUCKET 2>/dev/null || true
  unset MIX_VIA_EC2_S3_PREFIX 2>/dev/null || true
  unset MIX_VIA_EC2_SSH_IDENTITY 2>/dev/null || true
  unset MIX_VIA_EC2_PROXY 2>/dev/null || true
  unset MIX_VIA_EC2_CA_CERT 2>/dev/null || true
  unset MIX_VIA_EC2_HEX_UNSAFE_HTTPS 2>/dev/null || true
  unset MIX_WRAPPER_REMOTE_COMMANDS 2>/dev/null || true

  unset BREW 2>/dev/null || true
  unset BREW_WRAPPER_ENABLED 2>/dev/null || true
  unset BREW_WRAPPER_REAL_BREW 2>/dev/null || true
  unset BREW_WRAPPER_CURL_BIN 2>/dev/null || true
  unset BREW_WRAPPER_GIT_BIN 2>/dev/null || true
  unset BREW_WRAPPER_CURL_EC2_REQUIRED 2>/dev/null || true
  unset BREW_WRAPPER_GIT_EC2_REQUIRED 2>/dev/null || true
  unset BREW_WRAPPER_NO_AUTO_UPDATE 2>/dev/null || true

  IFS=':'
  for path_entry in ${old_path}; do
    [ -n "${path_entry}" ] || continue
    case "${path_entry}" in
      "${LEGACY_BREW_WRAPPER_DIR}"|\
      "${CURL_WRAPPER_DIR}"|\
      "${GIT_WRAPPER_DIR}"|\
      "${MIX_WRAPPER_DIR}"|\
      "${NVIM_WRAPPER_DIR}")
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

remove_legacy_brew_wrapper_installation() {
  [ -d "${LEGACY_BREW_WRAPPER_ROOT}" ] || return 0
  rm -rf "${LEGACY_BREW_WRAPPER_ROOT}"
}

should_apply_shell_rc_by_default() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --apply-shell-rc|--no-shell-rc|--shell-rc)
        return 1
        ;;
    esac
    shift
  done
  return 0
}

is_help_request() {
  case "${1:-}" in
    -h|--help)
      return 0
      ;;
  esac
  return 1
}

resolve_shell_rc_target() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --shell-rc)
        if [ "${2:-}" != "" ]; then
          printf '%s' "$2"
        else
          printf '%s' "${DEFAULT_SHELL_RC}"
        fi
        return 0
        ;;
      --shell-rc=*)
        printf '%s' "${1#--shell-rc=}"
        return 0
        ;;
    esac
    shift
  done

  printf '%s' "${DEFAULT_SHELL_RC}"
}

run_full_reset_before_setup() {
  shell_rc_target="$(resolve_shell_rc_target "$@")"

  sanitize_current_wrapper_env
  remove_legacy_brew_wrapper_installation
  run_script_with_bash_preference "${RESET_SCRIPT}" --shell-rc "${shell_rc_target}"
  sanitize_current_wrapper_env
  remove_legacy_brew_wrapper_installation
}

if is_help_request "$@"; then
  run_script_with_bash_preference "${SETUP_SCRIPT}" "$@"
  exit $?
fi

if [ "${1:-}" != "" ] && [ "${1#-}" = "$1" ]; then
  printf '[configure] argumento legado ignorado: %s (fluxo agora é local-only, sem bucket/EC2)\n' "$1" >&2
  shift
fi

if should_apply_shell_rc_by_default "$@"; then
  set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
fi

run_full_reset_before_setup "$@"
run_script_with_bash_preference "${SETUP_SCRIPT}" "$@"
