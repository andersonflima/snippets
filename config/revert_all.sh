#!/usr/bin/env bash
set -euo pipefail

# revert_all.sh
# Reverte TUDO que os setups de maquina aplicam, orquestrando os undos dedicados:
#   1) setup LazyVim/Mason  -> wrappers (curl/wget/git/lazy-*), PATH nos rc, venv de
#      bootstrap, caches do Mason/lazy, git insteadOf e a fonte Crowquill Mono.
#   2) setup homebrew-proxy -> bloco gerenciado de env no rc.
#
# NAO desinstala Homebrew, Neovim nem pacotes (preservados de proposito).

log() { printf '[revert-all] %s\n' "$*" >&2; }
die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash config/revert_all.sh [opcoes]

Opcoes:
  --purge          Repassa --purge ao undo do LazyVim (apaga STATE_ROOT + backups).
  --clean-backups  Repassa --clean-backups ao undo do homebrew-proxy (apaga <rc>.bak_*).
  -h, --help       Mostra ajuda.

Reverte, nesta ordem:
  config/lazyvim/undo_lazyvim_mason_from_zip.sh
  config/homebrew/undo_homebrew_proxy.sh

NAO desinstala Homebrew, Neovim nem pacotes.
USAGE
}

PURGE=0
CLEAN_BACKUPS=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --purge) PURGE=1; shift ;;
    --clean-backups) CLEAN_BACKUPS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "parametro invalido: $1" ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAZY_UNDO="${SCRIPT_DIR}/lazyvim/undo_lazyvim_mason_from_zip.sh"
BREW_UNDO="${SCRIPT_DIR}/homebrew/undo_homebrew_proxy.sh"

[ -f "$LAZY_UNDO" ] || die "nao encontrado: $LAZY_UNDO"
[ -f "$BREW_UNDO" ] || die "nao encontrado: $BREW_UNDO"

# Flags como string simples (sem array) para compatibilidade com bash 3.2 do macOS.
lazy_flags=""
[ "$PURGE" = "1" ] && lazy_flags="--purge"
brew_flags=""
[ "$CLEAN_BACKUPS" = "1" ] && brew_flags="--clean-backups"

log "1/2 revertendo setup LazyVim/Mason"
# shellcheck disable=SC2086
bash "$LAZY_UNDO" $lazy_flags

log "2/2 revertendo setup homebrew-proxy"
# shellcheck disable=SC2086
bash "$BREW_UNDO" $brew_flags

log "concluido: ambiente revertido (Homebrew/Neovim/pacotes preservados)"
