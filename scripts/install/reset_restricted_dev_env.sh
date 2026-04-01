#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[reset-restricted-dev-env] erro: bash é obrigatório para executar o reset\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[reset-restricted-dev-env] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/reset_restricted_dev_env.sh [opções]

Opções:
  --shell-rc <arquivo>         Arquivo rc a ser limpo. Padrão: $HOME/.zshrc
  --keep-shell-rc              Não remove linhas do shell rc.
  --keep-env-files             Não remove env-files em ~/.config.
  --keep-install-dirs          Não remove wrappers instalados em ~/.local/share.
  --keep-hex-config            Não restaura/remove a config persistida do Hex.
  -h, --help                   Mostra esta ajuda.
USAGE
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
STATE_HELPER="${SCRIPT_DIR}/restricted_dev_env_state.sh"

# shellcheck disable=SC1090
. "${STATE_HELPER}"

SHELL_RC_PATH="${HOME}/.zshrc"
RESET_SHELL_RC="1"
RESET_ENV_FILES="1"
RESET_INSTALL_DIRS="1"
RESET_HEX_CONFIG="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shell-rc)
      SHELL_RC_PATH="${2:-}"
      shift 2
      ;;
    --keep-shell-rc)
      RESET_SHELL_RC="0"
      shift
      ;;
    --keep-env-files)
      RESET_ENV_FILES="0"
      shift
      ;;
    --keep-install-dirs)
      RESET_INSTALL_DIRS="0"
      shift
      ;;
    --keep-hex-config)
      RESET_HEX_CONFIG="0"
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

restricted_dev_env_load_state
RESTRICTED_DEV_ENV_MANAGED_SHELL_RC="${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}"
RESTRICTED_DEV_ENV_HEX_MANAGED="${RESTRICTED_DEV_ENV_HEX_MANAGED:-0}"
RESTRICTED_DEV_ENV_HEX_CONFIG_PATH="${RESTRICTED_DEV_ENV_HEX_CONFIG_PATH:-}"
RESTRICTED_DEV_ENV_HEX_BACKUP_PATH="${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH:-${RESTRICTED_DEV_ENV_HEX_BACKUP_FILE}}"
RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE="${RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE:-0}"

append_shell_rc_target() {
  local candidate existing
  candidate="$1"

  [[ -n "${candidate}" ]] || return 0

  for existing in "${SHELL_RC_TARGETS[@]:-}"; do
    if [[ "${existing}" == "${candidate}" ]]; then
      return 0
    fi
  done

  SHELL_RC_TARGETS+=("${candidate}")
}

restore_hex_config_from_state() {
  local hex_config_path hex_backup_path

  if [[ "${RESET_HEX_CONFIG}" != "1" || "${RESTRICTED_DEV_ENV_HEX_MANAGED}" != "1" ]]; then
    return 0
  fi

  hex_config_path="${RESTRICTED_DEV_ENV_HEX_CONFIG_PATH}"
  hex_backup_path="${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH}"

  [[ -n "${hex_config_path}" ]] || return 0

  if [[ "${RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE}" == "1" ]]; then
    [[ -n "${hex_backup_path}" ]] || die "backup do Hex não encontrado no estado"
    [[ -f "${hex_backup_path}" ]] || die "arquivo de backup do Hex não encontrado: ${hex_backup_path}"
    mkdir -p "$(dirname "${hex_config_path}")"
    cp "${hex_backup_path}" "${hex_config_path}"
    log "configuração do Hex restaurada: ${hex_config_path}"
    return 0
  fi

  if [[ -e "${hex_config_path}" ]]; then
    rm -f "${hex_config_path}"
    log "configuração do Hex removida: ${hex_config_path}"
  fi
}

remove_file_if_exists() {
  local target
  target="$1"
  if [[ -e "${target}" ]]; then
    rm -f "${target}"
    log "arquivo removido: ${target}"
  fi
}

remove_dir_if_exists() {
  local target
  target="$1"
  if [[ -d "${target}" ]]; then
    rm -rf "${target}"
    log "diretório removido: ${target}"
  fi
}

if [[ "${RESET_SHELL_RC}" == "1" ]]; then
  SHELL_RC_TARGETS=()
  append_shell_rc_target "${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC}"
  append_shell_rc_target "${SHELL_RC_PATH}"

  for shell_rc_target in "${SHELL_RC_TARGETS[@]}"; do
    restricted_dev_env_remove_shell_rc_block "${shell_rc_target}"
    log "shell rc limpo: ${shell_rc_target}"
  done
fi

if [[ "${RESET_ENV_FILES}" == "1" ]]; then
  remove_file_if_exists "${HOME}/.config/mix-via-ec2-envs.sh"
  remove_file_if_exists "${HOME}/.config/wrapper-envs.sh"
  remove_file_if_exists "${HOME}/.config/mix-hex-envs.sh"
fi

if [[ "${RESET_INSTALL_DIRS}" == "1" ]]; then
  remove_dir_if_exists "${HOME}/.local/share/mix-ec2-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/homebrew-install-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/curl-python-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/git-zip-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/nvim-ec2-wrapper"
fi

restore_hex_config_from_state

if [[ "${RESET_SHELL_RC}" == "1" &&
  "${RESET_ENV_FILES}" == "1" &&
  "${RESET_INSTALL_DIRS}" == "1" &&
  "${RESET_HEX_CONFIG}" == "1" ]]; then
  restricted_dev_env_clear_state
fi

cat <<EOF
Reset concluído.

Shell rc:
  ${SHELL_RC_PATH}

Para abrir uma sessão limpa agora:
  zsh -f

Para limpar a sessão atual:
  exec "${SHELL:-/bin/zsh}" -l
EOF
