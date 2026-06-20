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
  bash config/lazyvim/undo_lazyvim_mason_from_zip.sh

Comportamento:
- Remove instalacao aplicada pelo setup de ZIP.
- Restaura backup, quando existir.
- Limpa legado dos scripts antigos (wrappers/shims/PATH e caches do Mason).
USAGE
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

STATE_ROOT="${HOME}/.local/share/nvim-zip-bootstrap"
STATE_FILE="${STATE_ROOT}/state.env"

NVIM_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nvim"
NVIM_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/nvim"
NVIM_CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/nvim"
NVIM_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/nvim"
BACKUP_DIR=""
MANAGE_CONFIG="0"

if [ -f "$STATE_FILE" ]; then
  # shellcheck source=/dev/null
  . "$STATE_FILE"
fi

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

remove_path_block_from_rc() {
  local rc_file="$1"
  local marker_begin="# >>> nvim-wrappers PATH >>>"
  local marker_end="# <<< nvim-wrappers PATH <<<"

  [ -f "$rc_file" ] || return 0
  local tmp_file
  tmp_file="$(mktemp)"
  awk -v begin="$marker_begin" -v end="$marker_end" '
    $0 == begin { in_block=1; next }
    $0 == end { in_block=0; next }
    in_block == 1 { next }
    { print }
  ' "$rc_file" > "$tmp_file"

  if ! cmp -s "$rc_file" "$tmp_file"; then
    mv "$tmp_file" "$rc_file"
    log "limpo bloco PATH de wrappers: $rc_file"
  else
    rm -f "$tmp_file"
  fi
}

cleanup_legacy_wrappers() {
  local wrapper_bin_dir="${HOME}/.local/share/nvim/wrappers/bin"
  rm_path "${wrapper_bin_dir}/git"
  rm_path "${wrapper_bin_dir}/curl"
  rm_path "${wrapper_bin_dir}/wget"
  rm_path "${wrapper_bin_dir}/http_fetch.py"
  rm_path "${wrapper_bin_dir}/lazy_zip_sync.py"
  rm_path "${wrapper_bin_dir}/lazy-check"
  rm_path "${wrapper_bin_dir}/lazy-install"
  rm_path "${wrapper_bin_dir}/lazy-update"
  rm_path "${HOME}/.local/share/nvim/wrappers/lazy-plugins.manifest"
  if [ -d "$wrapper_bin_dir" ] && [ -z "$(find "$wrapper_bin_dir" -mindepth 1 -print -quit 2>/dev/null)" ]; then
    rmdir "$wrapper_bin_dir" 2>/dev/null || true
    log "removido diretorio vazio: $wrapper_bin_dir"
  fi

  remove_path_block_from_rc "${HOME}/.zshrc"
  remove_path_block_from_rc "${HOME}/.bashrc"
  remove_path_block_from_rc "${HOME}/.profile"
}

cleanup_legacy_mason_state() {
  rm_path "${NVIM_DATA_DIR}/mason"
  rm_path "${NVIM_CACHE_DIR}/mason"
  rm_path "${NVIM_STATE_DIR}/mason"
}

if [ "${MANAGE_CONFIG:-0}" = "1" ]; then
  rm_path "$NVIM_CONFIG_DIR"
else
  log "preservando config em $NVIM_CONFIG_DIR (MANAGE_CONFIG=0)"
fi
rm_path "${NVIM_DATA_DIR}/lazy"
rm_path "${NVIM_CACHE_DIR}/mason-registry-main"
cleanup_legacy_mason_state
cleanup_legacy_wrappers

if [ -n "${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
  if [ "${MANAGE_CONFIG:-0}" = "1" ]; then
    restore_backup "$BACKUP_DIR/nvim-config" "$NVIM_CONFIG_DIR"
  fi
  restore_backup "$BACKUP_DIR/nvim-lazy" "${NVIM_DATA_DIR}/lazy"
  restore_backup "$BACKUP_DIR/mason-registry-main" "${NVIM_CACHE_DIR}/mason-registry-main"
fi

rm -f "$STATE_FILE" 2>/dev/null || true
log "concluido"
