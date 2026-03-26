#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[install-mix-ec2-wrapper] erro: bash é obrigatório para instalar o wrapper do mix\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[install-mix-ec2-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/install_mix_ec2_wrapper.sh [opções]

Opções:
  --install-dir <dir>        Diretório de instalação. Padrão: $HOME/.local/share/mix-ec2-wrapper/bin
  --wrapper-source <file>    Wrapper real do mix.
  --entrypoint-source <file> Entrypoint do mix-via-ec2.
  --real-mix <path>          Caminho do mix real.
  -h, --help                 Mostra esta ajuda.
USAGE
}

INSTALL_DIR="${HOME}/.local/share/mix-ec2-wrapper/bin"
WRAPPER_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/wrappers/mix_ec2_wrapper.sh"
ENTRYPOINT_SOURCE="$(cd "$(dirname "$0")/.." && pwd)/ec2/elixir/mix_via_ec2.sh"
REAL_MIX_BIN="${MIX_WRAPPER_REAL_MIX:-}"

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
    --entrypoint-source)
      ENTRYPOINT_SOURCE="${2:-}"
      shift 2
      ;;
    --real-mix)
      REAL_MIX_BIN="${2:-}"
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

[[ -f "${WRAPPER_SOURCE}" ]] || die "wrapper não encontrado: ${WRAPPER_SOURCE}"
[[ -f "${ENTRYPOINT_SOURCE}" ]] || die "entrypoint não encontrado: ${ENTRYPOINT_SOURCE}"

if [[ -z "${REAL_MIX_BIN}" ]]; then
  REAL_MIX_BIN="$(command -v mix || true)"
fi

[[ -n "${REAL_MIX_BIN}" ]] || die "não foi possível localizar mix no PATH"
[[ -x "${REAL_MIX_BIN}" ]] || die "mix inválido/não executável: ${REAL_MIX_BIN}"

mkdir -p "${INSTALL_DIR}"
cp "${WRAPPER_SOURCE}" "${INSTALL_DIR}/mix"
cp "${ENTRYPOINT_SOURCE}" "${INSTALL_DIR}/mix-via-ec2"
chmod 0755 "${INSTALL_DIR}/mix" "${INSTALL_DIR}/mix-via-ec2"

cat <<EOF
Instalação concluída.

1) Exporte no shell:
export MIX_WRAPPER_REAL_MIX="${REAL_MIX_BIN}"
export PATH="${INSTALL_DIR}:\$PATH"

2) Defina o backend remoto:
export MIX_VIA_EC2_INSTANCE_NAME="Dander"
export MIX_VIA_EC2_AWS_REGION="sa-east-1"
export MIX_VIA_EC2_SSH_IDENTITY="\$HOME/.ssh/dander.pem"

3) Comandos delegados por padrão:
export MIX_WRAPPER_REMOTE_COMMANDS="deps.get,deps.compile,deps.update,deps.unlock,local.hex,local.rebar,archive.install,archive.build,phx.new,hex.info"

4) Teste:
mix deps.get
EOF
