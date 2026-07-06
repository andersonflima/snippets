#!/usr/bin/env bash
set -euo pipefail

# undo_homebrew_proxy.sh
# Reverte o que setup_homebrew_proxy.sh --apply aplica: remove o bloco gerenciado
# "# >>> homebrew-proxy (managed) >>>" do shell rc.
#
# NAO desinstala o Homebrew nem formulae/casks (preservados de proposito): o setup
# so escreve o bloco de env no rc; brew/pacotes ficam intactos.

log() { printf '[undo-homebrew-proxy] %s\n' "$*" >&2; }
die() {
  log "erro: $*"
  exit 1
}

BLOCK_BEGIN="# >>> homebrew-proxy (managed) >>>"
BLOCK_END="# <<< homebrew-proxy (managed) <<<"

usage() {
  cat <<'USAGE'
Uso:
  bash config/homebrew/undo_homebrew_proxy.sh [opcoes]

Opcoes:
  --rc-file <path>   rc alvo (default ~/.zshrc). Alem dele, ~/.bashrc e ~/.profile
                     tambem sao varridos por seguranca.
  --clean-backups    Remove tambem os backups <rc>.bak_* criados pelo setup.
  -h, --help         Mostra ajuda.

Observacao: NAO desinstala Homebrew nem formulae/casks (preservados de proposito).
USAGE
}

RC_FILE="${HOME}/.zshrc"
CLEAN_BACKUPS=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --rc-file) RC_FILE="${2:?}"; shift 2 ;;
    --clean-backups) CLEAN_BACKUPS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "parametro invalido: $1" ;;
  esac
done

remove_block() {
  local rc="$1"
  [ -f "$rc" ] || return 0
  local tmp
  tmp="$(mktemp)"
  awk -v b="$BLOCK_BEGIN" -v e="$BLOCK_END" '
    $0 == b { inb=1; next }
    $0 == e { inb=0; next }
    inb == 1 { next }
    { print }
  ' "$rc" > "$tmp"
  if ! cmp -s "$rc" "$tmp"; then
    mv "$tmp" "$rc"
    log "removido bloco homebrew-proxy: $rc"
  else
    rm -f "$tmp"
  fi
}

for rc in "$RC_FILE" "${HOME}/.bashrc" "${HOME}/.profile"; do
  remove_block "$rc"
done

if [ "$CLEAN_BACKUPS" = "1" ]; then
  for bak in "${RC_FILE}".bak_*; do
    [ -e "$bak" ] || continue
    rm -f "$bak"
    log "removido backup: $bak"
  done
fi

log "concluido (Homebrew e pacotes preservados)"
