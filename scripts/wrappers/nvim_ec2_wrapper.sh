#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[nvim-ec2-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

WRAPPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

resolve_real_nvim() {
  if [[ -n "${NVIM_WRAPPER_REAL_NVIM:-}" ]]; then
    [[ -x "${NVIM_WRAPPER_REAL_NVIM}" ]] || die "NVIM_WRAPPER_REAL_NVIM inválido: ${NVIM_WRAPPER_REAL_NVIM}"
    printf '%s\n' "${NVIM_WRAPPER_REAL_NVIM}"
    return
  fi

  local self_path shell_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  shell_path="$(command -v -p nvim 2>/dev/null || true)"
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
  done <<EOF
$(which -a nvim 2>/dev/null || true)
EOF

  die "não foi possível localizar o nvim real. Defina NVIM_WRAPPER_REAL_NVIM."
}

source_env_file_if_exists() {
  local env_file
  env_file="$1"
  [[ -f "${env_file}" ]] || return 0
  # shellcheck disable=SC1090
  . "${env_file}"
}

source_env_file_if_exists "${HOME}/.config/mix-via-ec2-envs.sh"
source_env_file_if_exists "${HOME}/.config/wrapper-envs.sh"

rehash 2>/dev/null || true
hash -r 2>/dev/null || true

exec "$(resolve_real_nvim)" "$@"
