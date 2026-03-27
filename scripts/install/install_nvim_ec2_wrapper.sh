#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-nvim-ec2-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_wrapper_binary_path() {
  local candidate_path
  candidate_path="$1"
  [[ "${candidate_path}" == "${INSTALL_DIR}/nvim" ]]
}

resolve_real_nvim() {
  local candidate

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF
$(which -a nvim 2>/dev/null || true)
EOF

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_nvim_ec2_wrapper.sh [--install-dir <dir>] [--wrapper-source <file>] [--real-nvim <path>]

Padrões:
  --install-dir: $HOME/.local/share/nvim-ec2-wrapper/bin
  --wrapper-source: scripts/wrappers/nvim_ec2_wrapper.sh
  --real-nvim: primeiro nvim encontrado no PATH
USAGE
}

INSTALL_DIR="${HOME}/.local/share/nvim-ec2-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/nvim_ec2_wrapper.sh"
REAL_NVIM_BIN="${NVIM_WRAPPER_REAL_NVIM:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --wrapper-source)
      WRAPPER_SOURCE="${2:-}"
      shift 2
      ;;
    --real-nvim)
      REAL_NVIM_BIN="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parâmetro inválido: $1"
      ;;
  esac
done

[[ -n "${INSTALL_DIR}" ]] || die "--install-dir não pode ser vazio"
[[ -n "${WRAPPER_SOURCE}" ]] || die "--wrapper-source não pode ser vazio"
[[ -f "${WRAPPER_SOURCE}" ]] || die "wrapper não encontrado: ${WRAPPER_SOURCE}"

if [[ -z "${REAL_NVIM_BIN}" ]]; then
  REAL_NVIM_BIN="$(resolve_real_nvim || true)"
fi
[[ -n "${REAL_NVIM_BIN}" ]] || die "não foi possível localizar nvim no PATH"
[[ -x "${REAL_NVIM_BIN}" ]] || die "nvim inválido/não executável: ${REAL_NVIM_BIN}"
is_wrapper_binary_path "${REAL_NVIM_BIN}" && die "nvim real não pode apontar para o wrapper instalado: ${REAL_NVIM_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/nvim"
chmod 0755 "${INSTALL_DIR}/nvim"

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export NVIM_WRAPPER_REAL_NVIM="${REAL_NVIM_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"

2) Para usar com o ambiente restrito:
. "$HOME/.config/mix-via-ec2-envs.sh"
. "$HOME/.config/wrapper-envs.sh"
nvim
EOF
