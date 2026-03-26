#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[mix-ec2-wrapper] erro: bash é obrigatório para executar o wrapper do mix\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[mix-ec2-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

WRAPPER_DIR="$(cd "$(dirname "$0")" && pwd)"
MIX_VIA_EC2_ENTRYPOINT="${MIX_VIA_EC2_ENTRYPOINT:-${WRAPPER_DIR}/mix-via-ec2}"
MIX_WRAPPER_REMOTE_COMMANDS="${MIX_WRAPPER_REMOTE_COMMANDS:-deps.get,deps.compile,deps.update,deps.unlock,local.hex,local.rebar,archive.install,archive.build,phx.new,hex.info}"
MIX_WRAPPER_FORCE_REMOTE="${MIX_WRAPPER_FORCE_REMOTE:-0}"
MIX_WRAPPER_DISABLE_REMOTE="${MIX_WRAPPER_DISABLE_REMOTE:-0}"

resolve_real_mix() {
  if [[ -n "${MIX_WRAPPER_REAL_MIX:-}" ]]; then
    [[ -x "${MIX_WRAPPER_REAL_MIX}" ]] || die "MIX_WRAPPER_REAL_MIX inválido: ${MIX_WRAPPER_REAL_MIX}"
    printf '%s\n' "${MIX_WRAPPER_REAL_MIX}"
    return
  fi

  local self_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" != "${self_path}" ]]; then
      printf '%s\n' "${candidate}"
      return
    fi
  done < <(which -a mix 2>/dev/null || true)

  [[ -x "/usr/local/bin/mix" ]] && printf '%s\n' "/usr/local/bin/mix" && return
  die "não foi possível localizar o mix real. Defina MIX_WRAPPER_REAL_MIX."
}

should_route_to_remote() {
  local joined_commands first_arg

  if is_truthy "${MIX_WRAPPER_DISABLE_REMOTE}"; then
    return 1
  fi

  if is_truthy "${MIX_WRAPPER_FORCE_REMOTE}"; then
    return 0
  fi

  (( $# > 0 )) || return 1
  first_arg="$1"

  case "${first_arg}" in
    -h|--help|help|--version|-v)
      return 1
      ;;
  esac

  joined_commands=",${MIX_WRAPPER_REMOTE_COMMANDS},"
  [[ "${joined_commands}" == *",${first_arg},"* ]]
}

REAL_MIX_BIN="$(resolve_real_mix)"

if should_route_to_remote "$@"; then
  [[ -x "${MIX_VIA_EC2_ENTRYPOINT}" ]] || die "entrypoint mix-via-ec2 não encontrado/executável: ${MIX_VIA_EC2_ENTRYPOINT}"
  log "delegando para o EC2: mix $*"
  exec "${MIX_VIA_EC2_ENTRYPOINT}" -- "$@"
fi

exec "${REAL_MIX_BIN}" "$@"
