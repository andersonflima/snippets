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
  $0 [--method auto|package|source] [--min-elixir-version x.y.z] [--elixir-tag vX.Y.Z] [--remove-old] [--force]

Objetivo:
  Atualiza Elixir no EC2 até atingir versão mínima.
  Método padrão: pacote -> se não atingir a versão mínima, tenta instalação por fonte via git.

Opções:
  --method              auto (padrão), package ou source
  --min-elixir-version  Versão mínima aceitável (padrão: 1.14.0)
  --elixir-tag          Tag específica para instalação por fonte (ex: v1.17.3)
  --remove-old          Remove pacotes antigos antes da instalação
  --force               Não valida versão mínima, apenas instala
  -h, --help            Exibe esta mensagem
USAGE
}

METHOD="auto"
MIN_ELIXIR_VERSION="1.14.0"
ELIXIR_TAG=""
REMOVE_OLD=0
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --method)
      METHOD="${2:-auto}"
      shift 2
      ;;
    --min-elixir-version)
      MIN_ELIXIR_VERSION="${2:-}"
      shift 2
      ;;
    --elixir-tag)
      ELIXIR_TAG="${2:-}"
      shift 2
      ;;
    --remove-old)
      REMOVE_OLD=1
      shift
      ;;
    --force)
      FORCE=1
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

[[ "$METHOD" == "auto" || "$METHOD" == "package" || "$METHOD" == "source" ]] || die "método inválido: $METHOD"
[[ -n "$MIN_ELIXIR_VERSION" ]] || die "--min-elixir-version não pode ser vazio"

if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo não encontrado"
  SUDO=(sudo)
else
  SUDO=()
fi

run() {
  "${SUDO[@]}" "$@"
}

[[ -f /etc/os-release ]] || die "/etc/os-release não encontrado"
# shellcheck disable=SC1091
source /etc/os-release
DISTRIB_ID="${ID:-unknown}"

version_ge() {
  local current="$1"
  local minimum="$2"
  local norm_current
  local norm_minimum

  norm_current="$(printf '%s' "$current" | awk -F. '{printf "%03d%03d%03d", $1+0, $2+0, $3+0}')"
  norm_minimum="$(printf '%s' "$minimum" | awk -F. '{printf "%03d%03d%03d", $1+0, $2+0, $3+0}')"

  (( norm_current >= norm_minimum ))
}

read_elixir_version() {
  elixir --version 2>/dev/null | awk '/^Elixir / {print $2}' | head -n 1 || true
}

assert_minimum_version() {
  [[ FORCE -eq 1 ]] && return 0

  local current
  current="$(read_elixir_version || true)"
  [[ -n "$current" ]] || return 1
  version_ge "$current" "$MIN_ELIXIR_VERSION"
}

install_elixir_package() {
  log "instalando Elixir via gerenciador de pacotes"

  if [[ "$DISTRIB_ID" == "amzn" || "$DISTRIB_ID" == "amzn2" ]]; then
    if command -v dnf >/dev/null 2>&1; then
      run dnf makecache -y || true
      run dnf install -y erlang git make gcc gcc-c++ || true
      run dnf install -y elixir pigz || run dnf install -y elixir
    elif command -v yum >/dev/null 2>&1; then
      run yum makecache -y || true
      run yum install -y erlang git make gcc gcc-c++ || true
      run yum install -y elixir pigz || run yum install -y elixir
    else
      die "dnf/yum não encontrado"
    fi
  elif [[ "$DISTRIB_ID" == "ubuntu" || "$DISTRIB_ID" == "debian" ]]; then
    run apt-get update
    run DEBIAN_FRONTEND=noninteractive apt-get install -y erlang git make gcc g++
    run DEBIAN_FRONTEND=noninteractive apt-get install -y elixir pigz
  else
    die "distribuição não suportada: $DISTRIB_ID"
  fi
}

select_elixir_tag() {
  local tag_hint="$1"
  if [[ -n "$tag_hint" ]]; then
    echo "$tag_hint"
    return
  fi

  local latest
  latest="$(git ls-remote --tags --refs https://github.com/elixir-lang/elixir.git 'refs/tags/v*' \
    | awk -F/ '{print $NF}' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V -r \
    | head -n 1 || true)"

  if [[ -n "$latest" ]]; then
    echo "$latest"
    return
  fi

  echo "v1.17.3"
}

install_elixir_source() {
  local tag="$1"
  local workdir

  log "instalando Elixir por fonte: $tag"
  workdir="$(mktemp -d -t elixir-source-XXXXXX)"

  run git clone --depth 1 --branch "$tag" --single-branch https://github.com/elixir-lang/elixir.git "$workdir"
  run bash -lc "cd '$workdir' && make clean"
  run bash -lc "cd '$workdir' && make"

  run rm -rf /opt/elixir
  run mkdir -p /opt/elixir
  run cp -a "$workdir/." /opt/elixir/

  run ln -sf /opt/elixir/bin/elixir /usr/local/bin/elixir
  run ln -sf /opt/elixir/bin/iex /usr/local/bin/iex
  run ln -sf /opt/elixir/bin/mix /usr/local/bin/mix
  run rm -rf "$workdir"
}

remove_old_packages_if_requested() {
  [[ "$REMOVE_OLD" == "1" ]] || return 0

  log "removendo pacotes antigos"
  if [[ "$DISTRIB_ID" == "amzn" || "$DISTRIB_ID" == "amzn2" ]]; then
    if command -v dnf >/dev/null 2>&1; then
      run dnf remove -y elixir erlang || true
    elif command -v yum >/dev/null 2>&1; then
      run yum remove -y elixir erlang || true
    fi
  elif [[ "$DISTRIB_ID" == "ubuntu" || "$DISTRIB_ID" == "debian" ]]; then
    run apt-get remove -y elixir erlang || true
  fi
}

validate_runtime() {
  hash -r
  PATH="/usr/local/bin:/opt/elixir/bin:${PATH}"
  hash -r

  if ! command -v elixir >/dev/null 2>&1; then
    die "elixir não encontrado no PATH"
  fi

  log "elixir path: $(command -v elixir)"
  elixir --version | sed -n '1,2p'
  which -a elixir || true

  if [[ FORCE -eq 1 ]]; then
    return 0
  fi

  assert_minimum_version || die "versão atual $(read_elixir_version || echo desconhecida) abaixo de $MIN_ELIXIR_VERSION"
}

main() {
  remove_old_packages_if_requested

  if [[ "$METHOD" == "auto" || "$METHOD" == "package" ]]; then
    install_elixir_package
    hash -r

    if assert_minimum_version; then
      log "versão mínima alcançada por pacote"
      validate_runtime
      return 0
    fi

    log "versão do pacote ficou abaixo do mínimo, tentando fonte"
  fi

  if [[ "$METHOD" == "source" || "$METHOD" == "auto" ]]; then
    local install_tag
    install_tag="$(select_elixir_tag "$ELIXIR_TAG")"

    install_elixir_source "$install_tag"

    validate_runtime
    return 0
  fi

  validate_runtime
}

main "$@"
