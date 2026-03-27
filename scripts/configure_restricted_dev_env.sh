#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/install/setup_restricted_dev_env.sh"
DEFAULT_SHELL_RC="${HOME}/.zshrc"

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

if [ "${1:-}" = "" ]; then
  printf 'Uso: sh scripts/configure_restricted_dev_env.sh <bucket> [opções extras do setup]\n' >&2
  printf 'Padrão do entrypoint público: persiste automaticamente no %s\n' "${DEFAULT_SHELL_RC}" >&2
  exit 1
fi

if [ "${1#-}" != "$1" ]; then
  if should_apply_shell_rc_by_default "$@"; then
    set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
  fi
  exec sh "${SETUP_SCRIPT}" "$@"
fi

S3_BUCKET="$1"
shift

set -- --s3-bucket "${S3_BUCKET}" "$@"

if should_apply_shell_rc_by_default "$@"; then
  set -- --apply-shell-rc --shell-rc "${DEFAULT_SHELL_RC}" "$@"
fi

exec sh "${SETUP_SCRIPT}" "$@"
