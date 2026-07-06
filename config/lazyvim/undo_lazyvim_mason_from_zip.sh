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
  bash config/lazyvim/undo_lazyvim_mason_from_zip.sh [opcoes]

Opcoes:
  --purge     Remove tambem o STATE_ROOT inteiro, incluindo os backups
              (~/.local/share/nvim-zip-bootstrap). Sem isso, os backups sao
              preservados.
  -h, --help  Mostra ajuda.

Comportamento:
- Remove instalacao aplicada pelo setup de ZIP e restaura backup quando existir.
- Limpa wrappers/shims/PATH, caches do Mason e a venv de bootstrap.
- Reverte o git insteadOf (https -> ssh) e o bloco homebrew-proxy nos rc.
- Remove a fonte Crowquill Mono instalada por --install-crowquill.
USAGE
}

PURGE=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --purge) PURGE=1; shift ;;
    *) die "parametro invalido: $1" ;;
  esac
done

STATE_ROOT="${HOME}/.local/share/nvim-zip-bootstrap"
STATE_FILE="${STATE_ROOT}/state.env"
VENV_DIR="${STATE_ROOT}/.venv"

NVIM_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nvim"
NVIM_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/nvim"
NVIM_CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/nvim"
NVIM_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/nvim"
BACKUP_DIR=""
MANAGE_CONFIG="0"

if [ -f "$STATE_FILE" ]; then
  # shellcheck source=/dev/null
  . "$STATE_FILE"
elif [ -z "${BACKUP_DIR:-}" ]; then
  # state.env ausente (ex.: setup abortado no meio): recupera pelo backup mais
  # recente em ${STATE_ROOT}/backup_* para ainda permitir restauracao.
  newest_backup="$(ls -dt "${STATE_ROOT}"/backup_* 2>/dev/null | head -n1 || true)"
  if [ -n "$newest_backup" ] && [ -d "$newest_backup" ]; then
    BACKUP_DIR="$newest_backup"
    log "state.env ausente; usando backup mais recente: $BACKUP_DIR"
  fi
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

remove_managed_rc_block() {
  local rc_file="$1"
  local marker_begin="$2"
  local marker_end="$3"
  local label="$4"

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
    log "limpo bloco ${label}: $rc_file"
  else
    rm -f "$tmp_file"
  fi
}

remove_path_block_from_rc() {
  remove_managed_rc_block "$1" \
    "# >>> nvim-wrappers PATH >>>" "# <<< nvim-wrappers PATH <<<" "PATH de wrappers"
}

remove_homebrew_proxy_block_from_rc() {
  remove_managed_rc_block "$1" \
    "# >>> homebrew-proxy (managed) >>>" "# <<< homebrew-proxy (managed) <<<" "homebrew-proxy"
}

remove_git_insteadof() {
  command -v git >/dev/null 2>&1 || return 0
  local ssh="git@github.com:"
  git config --global --unset "url.${ssh}.insteadOf" '^https://github\.com/$' 2>/dev/null || true
  git config --global --unset "url.${ssh}.pushInsteadOf" '^https://github\.com/$' 2>/dev/null || true
  log "git insteadOf revertido (https://github.com/ -> ${ssh})"
}

remove_crowquill_font() {
  local removed=0 d f
  for d in "${HOME}/Library/Fonts" "${HOME}/.local/share/fonts"; do
    [ -d "$d" ] || continue
    for f in "$d"/CrowquillMono-*.ttf; do
      [ -e "$f" ] || continue
      rm -f "$f"
      removed=$((removed + 1))
    done
  done
  if [ "$removed" -gt 0 ]; then
    log "removidas ${removed} face(s) da fonte Crowquill Mono"
    if command -v fc-cache >/dev/null 2>&1; then
      fc-cache -f >/dev/null 2>&1 || true
    fi
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

  remove_homebrew_proxy_block_from_rc "${HOME}/.zshrc"
  remove_homebrew_proxy_block_from_rc "${HOME}/.bashrc"
  remove_homebrew_proxy_block_from_rc "${HOME}/.profile"
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

# Reverte config global aplicada pelo setup e artefatos do --install-crowquill.
remove_git_insteadof
remove_crowquill_font

if [ -n "${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
  if [ "${MANAGE_CONFIG:-0}" = "1" ]; then
    restore_backup "$BACKUP_DIR/nvim-config" "$NVIM_CONFIG_DIR"
  fi
  restore_backup "$BACKUP_DIR/nvim-lazy" "${NVIM_DATA_DIR}/lazy"
  restore_backup "$BACKUP_DIR/mason-registry-main" "${NVIM_CACHE_DIR}/mason-registry-main"
fi

# Remove a venv de bootstrap (nao e backup).
rm_path "$VENV_DIR"

if [ "$PURGE" = "1" ]; then
  rm_path "$STATE_ROOT"
  log "purge: STATE_ROOT removido (incluindo backups)"
else
  rm -f "$STATE_FILE" 2>/dev/null || true
  rmdir "$STATE_ROOT" 2>/dev/null || true
fi
log "concluido"
