#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
WRAPPER_BIN_DIR="${HOME}/.local/share/nvim/wrappers/bin"
LAZY_ONLY=0
MASON_ONLY=0
FORCE_WRAPPER_INSTALL=0
AUTO_SHELL_PROFILE=1

log() {
  printf '[lazyvim-mason-update] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/update_lazyvim_mason_wrapped.sh [opcoes]

Opcoes:
  --wrapper-bin-dir <dir>  Diretorio dos shims git/curl.
                           Default: $HOME/.local/share/nvim/wrappers/bin
  --lazy-only              Executa apenas install_lazyvim_archives.sh
  --mason-only             Executa apenas install_mason_from_registry_archive.sh
  --force-wrapper-install  Reinstala os shims de wrapper antes de atualizar
  --no-shell-profile       Nao altera arquivo de perfil durante instalacao dos wrappers
  -h, --help               Mostra esta ajuda.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --wrapper-bin-dir)
      WRAPPER_BIN_DIR="${2:-}"
      shift 2
      ;;
    --lazy-only)
      LAZY_ONLY=1
      shift
      ;;
    --mason-only)
      MASON_ONLY=1
      shift
      ;;
    --force-wrapper-install)
      FORCE_WRAPPER_INSTALL=1
      shift
      ;;
    --no-shell-profile)
      AUTO_SHELL_PROFILE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parametro invalido: $1"
      ;;
  esac
done

if [ "${LAZY_ONLY}" = "1" ] && [ "${MASON_ONLY}" = "1" ]; then
  die "--lazy-only e --mason-only nao podem ser usados juntos"
fi

if [ "${FORCE_WRAPPER_INSTALL}" = "1" ] || [ ! -x "${WRAPPER_BIN_DIR}/git" ] || [ ! -x "${WRAPPER_BIN_DIR}/curl" ]; then
  if [ "${FORCE_WRAPPER_INSTALL}" = "1" ]; then
    if [ "${AUTO_SHELL_PROFILE}" = "0" ]; then
      sh "${SCRIPT_DIR}/install_nvim_wrappers.sh" --target-dir "${WRAPPER_BIN_DIR}" --force --no-shell-profile
    else
      sh "${SCRIPT_DIR}/install_nvim_wrappers.sh" --target-dir "${WRAPPER_BIN_DIR}" --force
    fi
  else
    if [ "${AUTO_SHELL_PROFILE}" = "0" ]; then
      sh "${SCRIPT_DIR}/install_nvim_wrappers.sh" --target-dir "${WRAPPER_BIN_DIR}" --no-shell-profile
    else
      sh "${SCRIPT_DIR}/install_nvim_wrappers.sh" --target-dir "${WRAPPER_BIN_DIR}"
    fi
  fi
fi

PATH="${WRAPPER_BIN_DIR}:$PATH"
export PATH

export GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT="${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK="${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"

if [ "${MASON_ONLY}" != "1" ]; then
  sh "${SCRIPT_DIR}/install_lazyvim_archives.sh"
fi

if [ "${LAZY_ONLY}" != "1" ]; then
  sh "${SCRIPT_DIR}/install_mason_from_registry_archive.sh"
fi

log "atualizacao concluida com wrappers em ${WRAPPER_BIN_DIR}"
