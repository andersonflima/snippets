#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[update-elixir-only-ec2] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<USAGE
Uso:
  $0 [--remove-old]

Objetivo:
  Atualiza o Elixir no EC2 usando somente gerenciador de pacotes (dnf/yum/apt), sem download por curl.

Opções:
  --remove-old  Remove instalações antigas de elixir/erlang antes da instalação.
  -h, --help    Exibe esta mensagem.
USAGE
}

REMOVE_OLD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remove-old)
      REMOVE_OLD=1
      shift
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

if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo não encontrado"
  SUDO=(sudo)
else
  SUDO=()
fi

run() {
  "${SUDO[@]}" "$@"
}

if [[ "${#SUDO[@]}" -eq 0 ]]; then
  sudo_check="Sem sudo"
else
  sudo_check="Com sudo"
fi
log "modo: ${sudo_check}"

if ! [[ -r /sys/hypervisor/uuid && -s /sys/hypervisor/uuid ]]; then
  log "aviso: sem garantia de que é EC2; prosseguindo mesmo assim"
fi

if ! [[ -f /etc/os-release ]]; then
  die "/etc/os-release não encontrado"
fi

# shellcheck disable=SC1091
source /etc/os-release
DISTRIB_ID="${ID:-unknown}"

if [[ "${REMOVE_OLD}" == "1" ]]; then
  log "removendo versões antigas de Elixir/Erlang via gerenciador"
  if [[ "${DISTRIB_ID}" == "amzn" || "${DISTRIB_ID}" == "amzn2" ]]; then
    if command -v dnf >/dev/null 2>&1; then
      run dnf remove -y elixir erlang || true
    elif command -v yum >/dev/null 2>&1; then
      run yum remove -y elixir erlang || true
    fi
  elif [[ "${DISTRIB_ID}" == "ubuntu" || "${DISTRIB_ID}" == "debian" ]]; then
    run apt-get remove -y "elixir" "erlang" || true
  else
    log "remove-old não suportado para ${DISTRIB_ID} (seguindo para instalação)"
  fi
fi

log "distribuição detectada: ${DISTRIB_ID}"

case "${DISTRIB_ID}" in
  amzn)
    if command -v dnf >/dev/null 2>&1; then
      run dnf makecache -y || true
      run dnf install -y erlang elixir pigz awscli2 || run dnf install -y erlang elixir pigz awscli
    elif command -v yum >/dev/null 2>&1; then
      run yum makecache -y || true
      if ! run yum install -y erlang elixir pigz awscli2; then
        if command -v amazon-linux-extras >/dev/null 2>&1; then
          run amazon-linux-extras install -y epel || true
        fi
        run yum install -y erlang elixir pigz awscli || run yum install -y awscli2
      fi
    else
      die "dnf/yum não encontrado no sistema"
    fi
    ;;
  ubuntu|debian)
    run apt-get update
    run DEBIAN_FRONTEND=noninteractive apt-get install -y erlang elixir pigz awscli
    ;;
  *)
    die "distribuição não suportada: ${DISTRIB_ID}"
    ;;
esac

hash -r

log "validando instalação"
if command -v elixir >/dev/null 2>&1; then
  log "elixir path: $(command -v elixir)"
  elixir --version | sed -n '1,2p'
else
  die "elixir não foi encontrado no PATH após instalação"
fi

if command -v erl >/dev/null 2>&1; then
  log "otp_release: $(erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt()' 2>/dev/null)"
fi

which -a elixir || true
log "concluído"
