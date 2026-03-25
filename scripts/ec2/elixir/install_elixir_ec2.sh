#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-elixir-ec2] %s\n' "$*"
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  install_elixir_ec2.sh

Objetivo:
  Instala e valida no EC2:
  - Elixir/Erlang (elixir, mix, erl)
  - pigz
  - aws cli
  - mongodump (MongoDB Database Tools)
USAGE
}

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi

SUDO_CMD=()
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo é necessário para instalar pacotes"
  SUDO_CMD=(sudo)
fi

run_with_sudo() {
  "${SUDO_CMD[@]}" "$@"
}

detect_distro() {
  [[ -f /etc/os-release ]] || die "arquivo /etc/os-release não encontrado"
  # shellcheck disable=SC1091
  source /etc/os-release
  printf '%s\n' "${ID:-unknown}"
}

install_on_amazon_linux() {
  local base_packages=(elixir erlang pigz curl tar gzip unzip)

  if command -v dnf >/dev/null 2>&1; then
    run_with_sudo dnf makecache -y
    if ! run_with_sudo dnf install -y "${base_packages[@]}"; then
      die "falha ao instalar pacotes com dnf"
    fi
    if ! run_with_sudo dnf install -y awscli; then
      run_with_sudo dnf install -y awscli2 || die "falha ao instalar aws cli com dnf"
    fi
    return
  fi

  if command -v yum >/dev/null 2>&1; then
    run_with_sudo yum makecache -y
    if run_with_sudo yum install -y "${base_packages[@]}"; then
      if ! run_with_sudo yum install -y awscli; then
        run_with_sudo yum install -y awscli2 || die "falha ao instalar aws cli com yum"
      fi
      return
    fi

    if command -v amazon-linux-extras >/dev/null 2>&1; then
      run_with_sudo amazon-linux-extras install -y epel
      run_with_sudo yum makecache -y
      run_with_sudo yum install -y "${base_packages[@]}"
      if ! run_with_sudo yum install -y awscli; then
        run_with_sudo yum install -y awscli2 || die "falha ao instalar aws cli com yum"
      fi
      return
    fi

    die "falha ao instalar pacotes com yum"
  fi

  die "nem dnf nem yum foram encontrados"
}

install_on_debian_family() {
  run_with_sudo apt-get update
  run_with_sudo apt-get install -y elixir erlang pigz awscli curl tar gzip unzip
}

install_mongodb_database_tools() {
  if command -v mongodump >/dev/null 2>&1; then
    return
  fi

  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local installer_path="${script_dir}/install_mongodb_database_tools.sh"

  [[ -f "${installer_path}" ]] || die "instalador de MongoDB tools não encontrado: ${installer_path}"
  bash "${installer_path}" --use-sudo auto
}

configure_elixir() {
  if ! mix local.hex --force >/dev/null 2>&1; then
    log "aviso: não foi possível configurar mix local.hex automaticamente"
  fi

  if ! mix local.rebar --force >/dev/null 2>&1; then
    log "aviso: não foi possível configurar mix local.rebar automaticamente"
  fi
}

print_version_line() {
  local binary="$1"
  local output
  output="$("${binary}" --version 2>&1 | head -n 1 || true)"
  [[ -n "${output}" ]] || output="versão não disponível"
  printf '[install-elixir-ec2] %s -> %s\n' "${binary}" "${output}"
}

validate_binaries() {
  local binaries=(elixir mix erl mongodump pigz aws)
  local binary

  for binary in "${binaries[@]}"; do
    command -v "${binary}" >/dev/null 2>&1 || die "binário não encontrado após instalação: ${binary}"
    print_version_line "${binary}"
  done
}

main() {
  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn)
      install_on_amazon_linux
      ;;
    ubuntu|debian)
      install_on_debian_family
      ;;
    *)
      die "distribuição não suportada por este instalador: ${distro}"
      ;;
  esac

  install_mongodb_database_tools
  configure_elixir
  validate_binaries

  log "instalação concluída"
}

main "$@"
