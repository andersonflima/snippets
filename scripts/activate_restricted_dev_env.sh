#!/bin/sh
set -eu

[ -f "${HOME}/.config/mix-via-ec2-envs.sh" ] || {
  printf '[activate-restricted-dev-env] erro: arquivo não encontrado: %s\n' "${HOME}/.config/mix-via-ec2-envs.sh" >&2
  return 1 2>/dev/null || exit 1
}

[ -f "${HOME}/.config/wrapper-envs.sh" ] || {
  printf '[activate-restricted-dev-env] erro: arquivo não encontrado: %s\n' "${HOME}/.config/wrapper-envs.sh" >&2
  return 1 2>/dev/null || exit 1
}

. "${HOME}/.config/mix-via-ec2-envs.sh"
. "${HOME}/.config/wrapper-envs.sh"
rehash 2>/dev/null || true
hash -r 2>/dev/null || true

printf '[activate-restricted-dev-env] ambiente carregado na sessão atual\n' >&2
