#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

if [ -f "${HOME}/.config/mix-via-ec2-envs.sh" ]; then
  . "${HOME}/.config/mix-via-ec2-envs.sh"
fi

if [ -f "${HOME}/.config/wrapper-envs.sh" ]; then
  . "${HOME}/.config/wrapper-envs.sh"
fi

rehash 2>/dev/null || true
hash -r 2>/dev/null || true

printf 'mix=%s\n' "$(command -v mix 2>/dev/null || printf 'não encontrado')"
printf 'curl=%s\n' "$(command -v curl 2>/dev/null || printf 'não encontrado')"
printf 'wget=%s\n' "$(command -v wget 2>/dev/null || printf 'não encontrado')"
printf 'git=%s\n' "$(command -v git 2>/dev/null || printf 'não encontrado')"
printf 'nvim=%s\n' "$(command -v nvim 2>/dev/null || printf 'não encontrado')"
printf 'env MIX=%s\n' "${MIX:-}"
printf 'env CURL=%s\n' "${CURL:-}"
printf 'env WGET=%s\n' "${WGET:-}"
printf 'env GIT=%s\n' "${GIT:-}"
printf 'env WRAPPERS_VIA_EC2_ENABLED=%s\n' "${WRAPPERS_VIA_EC2_ENABLED:-}"
printf 'env WRAPPERS_VIA_EC2_INSTANCE_NAME=%s\n' "${WRAPPERS_VIA_EC2_INSTANCE_NAME:-}"
printf 'env WRAPPERS_VIA_EC2_AWS_REGION=%s\n' "${WRAPPERS_VIA_EC2_AWS_REGION:-}"
printf 'env WRAPPERS_VIA_EC2_S3_BUCKET=%s\n' "${WRAPPERS_VIA_EC2_S3_BUCKET:-}"

if command -v nvim >/dev/null 2>&1; then
  printf 'nvim exepath curl/git/mix:\n'
  nvim --headless -u NORC -i NONE \
    +'lua print(vim.fn.exepath("curl"))' \
    +'lua print(vim.fn.exepath("wget"))' \
    +'lua print(vim.fn.exepath("git"))' \
    +'lua print(vim.fn.exepath("mix"))' \
    +qa
fi
