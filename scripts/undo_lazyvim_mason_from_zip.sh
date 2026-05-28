#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[undo-lazyvim-mason] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash scripts/undo_lazyvim_mason_from_zip.sh

Comportamento:
- Remove instalacao aplicada pelo setup de ZIP.
- Restaura backup, quando existir.
USAGE
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

STATE_ROOT="${HOME}/.local/share/nvim-zip-bootstrap"
STATE_FILE="${STATE_ROOT}/state.env"

[ -f "$STATE_FILE" ] || die "state file nao encontrado: $STATE_FILE"

# shellcheck source=/dev/null
. "$STATE_FILE"

[ -n "${NVIM_CONFIG_DIR:-}" ] || die "NVIM_CONFIG_DIR ausente no state"
[ -n "${NVIM_DATA_DIR:-}" ] || die "NVIM_DATA_DIR ausente no state"
[ -n "${NVIM_CACHE_DIR:-}" ] || die "NVIM_CACHE_DIR ausente no state"

rm_path() {
  local target="$1"
  if [ -e "$target" ] || [ -L "$target" ]; then
    rm -rf "$target"
    log "removido: $target"
  fi
}

restore_backup() {
  local source_path="$1"
  local target_path="$2"
  if [ -e "$source_path" ] || [ -L "$source_path" ]; then
    mkdir -p "$(dirname "$target_path")"
    mv "$source_path" "$target_path"
    log "restaurado: $target_path"
  fi
}

rm_path "$NVIM_CONFIG_DIR"
rm_path "${NVIM_DATA_DIR}/lazy"
rm_path "${NVIM_CACHE_DIR}/mason-registry-main"

if [ -n "${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
  restore_backup "$BACKUP_DIR/nvim-config" "$NVIM_CONFIG_DIR"
  restore_backup "$BACKUP_DIR/nvim-lazy" "${NVIM_DATA_DIR}/lazy"
  restore_backup "$BACKUP_DIR/mason-registry-main" "${NVIM_CACHE_DIR}/mason-registry-main"
fi

rm -f "$STATE_FILE"
log "concluido"
