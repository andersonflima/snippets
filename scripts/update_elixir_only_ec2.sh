#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIX_SCRIPT_PATH="${SCRIPT_DIR}/fix_elixir_ec2.sh"

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
  $0 [--remove-old] [--min-elixir-version x.y.z] [--force-latest-build]

Objetivo:
  Atualiza o Elixir no EC2 para versão >= mínima configurada,
  preferindo gerenciador de pacotes e, se necessário, fallback para
  instalação por build atual (via script de correção existente).

Opções:
  --remove-old         Remove instalações antigas de elixir/erlang antes da instalação.
  --min-elixir-version Define versão mínima aceitável (padrão: 1.14.0).
  --force-latest-build Ignora pacote e força o fluxo de build/correção.
  -h, --help           Exibe esta mensagem.
USAGE
}

REMOVE_OLD=0
FORCE_LATEST_BUILD=0
MIN_ELIXIR_VERSION="${MIN_ELIXIR_VERSION:-1.14.0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remove-old)
      REMOVE_OLD=1
      shift
      ;;
    --force-latest-build)
      FORCE_LATEST_BUILD=1
      shift
      ;;
    --min-elixir-version)
      MIN_ELIXIR_VERSION="${2:-}"
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

if [[ -z "${MIN_ELIXIR_VERSION}" ]]; then
  die "--min-elixir-version não pode ser vazio"
fi

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

[[ -f /etc/os-release ]] || die "/etc/os-release não encontrado"
# shellcheck disable=SC1091
source /etc/os-release
DISTRIB_ID="${ID:-unknown}"

version_at_least() {
  local current="$1"
  local required="$2"

  awk -v c="${current}" -v r="${required}" '
    BEGIN {
      split(c, ca, ".")
      split(r, ra, ".")
      for (i = 1; i <= 3; i++) {
        cv = (i in ca) ? ca[i] + 0 : 0
        rv = (i in ra) ? ra[i] + 0 : 0
        if (cv > rv) exit 0
        if (cv < rv) exit 1
      }
      exit 0
    }
  '
}

read_elixir_version() {
  elixir --version 2>/dev/null | awk '/^Elixir / {print $2}' | head -n 1 || true
}

is_minimum_elixir_version() {
  local current
  current="$(read_elixir_version || true)"

  if [[ -z "${current}" ]]; then
    return 1
  fi

  version_at_least "${current}" "${MIN_ELIXIR_VERSION}"
}

ensure_minimum_version() {
  local current
  if command -v elixir >/dev/null 2>&1 && is_minimum_elixir_version; then
    current="$(read_elixir_version)"
    log "elixir já atende versão mínima: ${current}"
    return 0
  fi

  current="$(read_elixir_version || echo desconhecida)"
  log "elixir atual ${current} abaixo de ${MIN_ELIXIR_VERSION} ou não detectado"
  return 1
}

install_awscli_with_fallback() {
  local pm="$1"

  case "${pm}" in
    dnf)
      if run dnf install -y awscli2; then
        log "awscli2 instalado"
        return 0
      fi
      log "aviso: awscli2 indisponível no dnf (tentando awscli)"
      if run dnf install -y awscli; then
        log "awscli instalado"
        return 0
      fi
      log "aviso: awscli indisponível no dnf"
      return 1
      ;;
    yum)
      if run yum install -y awscli2; then
        log "awscli2 instalado"
        return 0
      fi
      log "aviso: awscli2 indisponível no yum"
      if command -v amazon-linux-extras >/dev/null 2>&1; then
        run amazon-linux-extras install -y epel || true
      fi
      if run yum install -y awscli; then
        log "awscli instalado"
        return 0
      fi
      log "aviso: awscli indisponível no yum"
      return 1
      ;;
  esac

  return 1
}

install_via_fix_script() {
  if [[ ! -f "${FIX_SCRIPT_PATH}" ]]; then
    die "script de fallback não encontrado: ${FIX_SCRIPT_PATH}"
  fi

  if [[ "${#SUDO[@]}" -eq 0 ]]; then
    bash "${FIX_SCRIPT_PATH}" --elixir-ref latest --install-dir /opt/elixir --force-remove-package
  else
    run bash "${FIX_SCRIPT_PATH}" --elixir-ref latest --install-dir /opt/elixir --force-remove-package
  fi

  if is_minimum_elixir_version; then
    return 0
  fi

  return 1
}

if [[ "${REMOVE_OLD}" == "1" ]]; then
  log "removendo versões antigas de Elixir/Erlang via gerenciador"
  if [[ "${DISTRIB_ID}" == "amzn" || "${DISTRIB_ID}" == "amzn2" ]]; then
    if command -v dnf >/dev/null 2>&1; then
      run dnf remove -y elixir erlang || true
    elif command -v yum >/dev/null 2>&1; then
      run yum remove -y elixir erlang || true
    fi
  elif [[ "${DISTRIB_ID}" == "ubuntu" || "${DISTRIB_ID}" == "debian" ]]; then
    run apt-get remove -y elixir erlang || true
  else
    log "remove-old não suportado para ${DISTRIB_ID} (seguindo para instalação)"
  fi
fi

log "distribuição detectada: ${DISTRIB_ID}"

case "${DISTRIB_ID}" in
  amzn)
    if command -v dnf >/dev/null 2>&1; then
      run dnf makecache -y || true
      if ! run dnf install -y erlang elixir pigz; then
        run dnf install -y erlang elixir pigz || die "falha ao instalar erlang/elixir/pigz com dnf"
      fi
      if ! install_awscli_with_fallback dnf; then
        log "aviso: aws cli não instalado automaticamente; instale manualmente se necessário"
      fi
    elif command -v yum >/dev/null 2>&1; then
      run yum makecache -y || true
      run yum install -y erlang elixir pigz || die "falha ao instalar erlang/elixir/pigz com yum"
      if ! install_awscli_with_fallback yum; then
        log "aviso: aws cli não instalado automaticamente; instale manualmente se necessário"
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

if ! ensure_minimum_version; then
  if [[ "${FORCE_LATEST_BUILD}" == "1" ]]; then
    log "forçando atualização via fluxo de build/correção para garantir versão recente"
  elif [[ "${FORCE_LATEST_BUILD}" != "1" ]]; then
    log "tentando primeiro ajustar via pacotes de correção para atingir ${MIN_ELIXIR_VERSION}"
  fi

  if ! install_via_fix_script; then
    log "falha ao atingir versão mínima automaticamente via fluxo de correção"
    log "estado final atual (se disponível):"
    command -v elixir || true
    if command -v elixir >/dev/null 2>&1; then
      elixir --version || true
    fi
    die "versão do Elixir ainda abaixo da mínima (${MIN_ELIXIR_VERSION})."
  fi
fi

if ! command -v /opt/elixir/bin/elixir >/dev/null 2>&1; then
  PATH="/opt/elixir/bin:${PATH}"
fi
hash -r

if command -v /opt/elixir/bin/elixir >/dev/null 2>&1; then
  ln -sf /opt/elixir/bin/elixir /usr/local/bin/elixir || true
  ln -sf /opt/elixir/bin/mix /usr/local/bin/mix || true
  ln -sf /opt/elixir/bin/iex /usr/local/bin/iex || true
fi

if command -v elixir >/dev/null 2>&1; then
  log "elixir path: $(command -v elixir)"
  elixir --version | sed -n '1,2p'
  log "versão atual: $(read_elixir_version || echo desconhecida)"
else
  die "elixir não foi encontrado no PATH após atualização"
fi

if command -v erl >/dev/null 2>&1; then
  log "otp_release: $(erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt()' 2>/dev/null)"
fi

which -a elixir || true
log "concluído"
