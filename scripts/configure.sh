#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/install/setup_restricted_dev_env.sh"
DEFAULT_SHELL_RC="${HOME}/.zshrc"
WRAPPER_ENV_FILE="${HOME}/.config/wrapper-envs.sh"
MIX_ENV_FILE="${HOME}/.config/mix-via-ec2-envs.sh"
LEGACY_BREW_WRAPPER_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
LEGACY_BREW_WRAPPER_ROOT="${HOME}/.local/share/homebrew-install-wrapper"

sanitize_legacy_brew_wrapper_env() {
  old_path="${PATH-}"
  new_path=""
  old_ifs="${IFS}"

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
    [ "${path_entry}" = "${LEGACY_BREW_WRAPPER_DIR}" ] && continue

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

extract_bucket_from_env_file() {
  env_file="$1"

  [ -f "${env_file}" ] || return 1

  (
    set +u
    # shellcheck disable=SC1090
    . "${env_file}" >/dev/null 2>&1 || exit 1

    if [ -n "${WRAPPERS_VIA_EC2_S3_BUCKET:-}" ]; then
      printf '%s' "${WRAPPERS_VIA_EC2_S3_BUCKET}"
      exit 0
    fi

    if [ -n "${MIX_VIA_EC2_S3_BUCKET:-}" ]; then
      printf '%s' "${MIX_VIA_EC2_S3_BUCKET}"
      exit 0
    fi

    exit 1
  )
}

resolve_default_bucket() {
  bucket=""

  bucket="$(extract_bucket_from_env_file "${WRAPPER_ENV_FILE}" 2>/dev/null || true)"
  if [ -n "${bucket}" ]; then
    printf '%s' "${bucket}"
    return 0
  fi

  bucket="$(extract_bucket_from_env_file "${MIX_ENV_FILE}" 2>/dev/null || true)"
  if [ -n "${bucket}" ]; then
    printf '%s' "${bucket}"
    return 0
  fi

  return 1
}

if [ "${1:-}" = "" ]; then
  resolved_bucket="$(resolve_default_bucket || true)"
  if [ -n "${resolved_bucket}" ]; then
    set -- "${resolved_bucket}"
  else
    printf 'Uso: sh scripts/configure.sh <bucket> [opções extras do setup]\n' >&2
    printf 'Padrão do entrypoint público: persiste automaticamente no %s\n' "${DEFAULT_SHELL_RC}" >&2
    printf 'Dica: após a primeira configuração, você pode executar sem bucket.\n' >&2
    exit 1
  fi
fi

if [ "${1#-}" != "$1" ]; then
  if should_apply_shell_rc_by_default "$@"; then
    set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
  fi
  sanitize_legacy_brew_wrapper_env
  remove_legacy_brew_wrapper_installation
  exec sh "${SETUP_SCRIPT}" "$@"
fi

S3_BUCKET="$1"
shift

set -- --s3-bucket "${S3_BUCKET}" "$@"

if should_apply_shell_rc_by_default "$@"; then
  set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
fi

sanitize_legacy_brew_wrapper_env
remove_legacy_brew_wrapper_installation
exec sh "${SETUP_SCRIPT}" "$@"
