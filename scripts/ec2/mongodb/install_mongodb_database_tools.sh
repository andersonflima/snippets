#!/usr/bin/env bash
set -euo pipefail

VERSION="100.6.1"
INSTALL_DIR="/usr/local/bin"
USE_SUDO="auto"
KEEP_WORKDIR="0"

log() {
  printf '[install-mongo-tools] %s\n' "$*"
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<USAGE
Uso:
  $(basename "$0") [--version 100.6.1] [--install-dir /usr/local/bin] [--use-sudo auto|yes|no] [--keep-workdir]

Exemplos:
  $(basename "$0")
  $(basename "$0") --version 100.6.1 --install-dir /usr/local/bin
  $(basename "$0") --use-sudo no --install-dir "\$HOME/.local/bin"
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --use-sudo)
      USE_SUDO="${2:-}"
      shift 2
      ;;
    --keep-workdir)
      KEEP_WORKDIR="1"
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

[[ -n "$VERSION" ]] || die "--version não pode ser vazio"
[[ -n "$INSTALL_DIR" ]] || die "--install-dir não pode ser vazio"
[[ "$USE_SUDO" =~ ^(auto|yes|no)$ ]] || die "--use-sudo deve ser auto, yes ou no"

command -v curl >/dev/null 2>&1 || die "curl não encontrado"
command -v tar >/dev/null 2>&1 || die "tar não encontrado"
command -v install >/dev/null 2>&1 || die "install não encontrado"

ARCH="$(uname -m)"
CANDIDATES=()
case "$ARCH" in
  x86_64)
    CANDIDATES+=(
      "mongodb-database-tools-amazon2-x86_64-${VERSION}.tgz"
      "mongodb-database-tools-rhel90-x86_64-${VERSION}.tgz"
      "mongodb-database-tools-rhel80-x86_64-${VERSION}.tgz"
    )
    ;;
  aarch64|arm64)
    CANDIDATES+=(
      "mongodb-database-tools-amazon2-aarch64-${VERSION}.tgz"
      "mongodb-database-tools-rhel90-arm64-${VERSION}.tgz"
      "mongodb-database-tools-rhel82-arm64-${VERSION}.tgz"
    )
    ;;
  *)
    die "arquitetura não suportada: ${ARCH}"
    ;;
esac

WORKDIR="$(mktemp -d -t mongo-tools-XXXXXX)"
cleanup() {
  if [[ "$KEEP_WORKDIR" == "1" ]]; then
    log "workdir preservado: ${WORKDIR}"
    return
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

ARCHIVE_PATH=""
for filename in "${CANDIDATES[@]}"; do
  url="https://fastdl.mongodb.org/tools/db/${filename}"
  log "tentando baixar: ${url}"
  if curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 "$url" -o "${WORKDIR}/${filename}"; then
    ARCHIVE_PATH="${WORKDIR}/${filename}"
    log "download concluído: ${filename}"
    break
  fi
  log "não disponível para esta plataforma: ${filename}"
done

[[ -n "$ARCHIVE_PATH" ]] || die "não foi possível baixar o pacote para a arquitetura ${ARCH}"

tar -xzf "$ARCHIVE_PATH" -C "$WORKDIR"
BIN_DIR="$(find "$WORKDIR" -maxdepth 3 -type d -name bin | head -n 1 || true)"
[[ -n "$BIN_DIR" ]] || die "não foi possível localizar a pasta bin no pacote"

SUDO_CMD=()
if [[ "$USE_SUDO" == "yes" ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo não encontrado"
  SUDO_CMD=(sudo)
elif [[ "$USE_SUDO" == "auto" ]]; then
  if [[ ! -w "$INSTALL_DIR" ]]; then
    command -v sudo >/dev/null 2>&1 || die "sem permissão de escrita em ${INSTALL_DIR} e sudo não disponível"
    SUDO_CMD=(sudo)
  fi
fi

"${SUDO_CMD[@]}" mkdir -p "$INSTALL_DIR"
for bin in "$BIN_DIR"/*; do
  [[ -f "$bin" ]] || continue
  "${SUDO_CMD[@]}" install -m 0755 "$bin" "$INSTALL_DIR/"
done

"$INSTALL_DIR/mongodump" --version | head -n 1
log "instalação concluída em ${INSTALL_DIR}"
log "verifique o PATH com: command -v mongodump"
