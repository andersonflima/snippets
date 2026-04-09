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
  --shell-rc <arquivo>         Arquivo rc a ser limpo.
  --keep-shell-rc              Não remove linhas do shell rc.
  --keep-env-files             Não remove env-files em ~/.config.
  --keep-install-dirs          Não remove wrappers instalados em ~/.local/share.
  --keep-wrapper-cache         Não remove cache/seed do wrapper em ~/.cache.
  --keep-mason-elixir-ls       Não remove artefatos do elixir-ls sob ~/.local/share/nvim/mason.
  --keep-hex-config            Não restaura/remove a config persistida do Hex.
  -h, --help                   Mostra esta ajuda.

Ambiente:
  RESTRICTED_DEV_ENV_TARGET_SHELL
                               Shell alvo para o shell rc padrão. Default: zsh.
USAGE
}

resolve_target_shell_name() {
  local target_shell
  target_shell="${RESTRICTED_DEV_ENV_TARGET_SHELL:-zsh}"
  target_shell="${target_shell##*/}"

  case "${target_shell}" in
    zsh|bash|fish|sh)
      printf '%s\n' "${target_shell}"
      ;;
    *)
      printf '%s\n' "zsh"
      ;;
  esac
}

resolve_target_shell_executable() {
  local shell_name
  shell_name="$(resolve_target_shell_name)"

  case "${shell_name}" in
    zsh)
      command -v zsh 2>/dev/null || printf '%s\n' "/bin/zsh"
      ;;
    bash)
      command -v bash 2>/dev/null || printf '%s\n' "/bin/bash"
      ;;
    fish)
      command -v fish 2>/dev/null || printf '%s\n' "/usr/bin/env fish"
      ;;
    *)
      command -v sh 2>/dev/null || printf '%s\n' "/bin/sh"
      ;;
  esac
}

detect_default_shell_rc() {
  local shell_name
  shell_name="$(resolve_target_shell_name)"

  case "${shell_name}" in
    fish)
      printf '%s\n' "${HOME}/.config/fish/config.fish"
      ;;
    zsh)
      printf '%s\n' "${HOME}/.zshrc"
      ;;
    bash)
      printf '%s\n' "${HOME}/.bashrc"
      ;;
    *)
      printf '%s\n' "${HOME}/.profile"
      ;;
  esac
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
STATE_HELPER="${SCRIPT_DIR}/restricted_dev_env_state.sh"

# shellcheck disable=SC1090
. "${STATE_HELPER}"

SHELL_RC_PATH="$(detect_default_shell_rc)"
ELIXIR_LS_SETUP_SH="${HOME}/.config/elixir_ls/setup.sh"
ELIXIR_LS_SETUP_FISH="${HOME}/.config/elixir_ls/setup.fish"
RESET_SHELL_RC="1"
RESET_ENV_FILES="1"
RESET_INSTALL_DIRS="1"
RESET_WRAPPER_CACHE="1"
RESET_MASON_ELIXIR_LS="1"
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
    --keep-wrapper-cache)
      RESET_WRAPPER_CACHE="0"
      shift
      ;;
    --keep-mason-elixir-ls)
      RESET_MASON_ELIXIR_LS="0"
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

TARGET_SHELL_EXECUTABLE="$(resolve_target_shell_executable)"

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

remove_glob_matches() {
  local pattern target
  pattern="$1"
  for target in ${pattern}; do
    [[ -e "${target}" ]] || continue
    if [[ -d "${target}" ]]; then
      remove_dir_if_exists "${target}"
    else
      remove_file_if_exists "${target}"
    fi
  done
}

if [[ "${RESET_SHELL_RC}" == "1" ]]; then
  SHELL_RC_TARGETS=()
  append_shell_rc_target "${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC}"
  append_shell_rc_target "${SHELL_RC_PATH}"

  for shell_rc_target in "${SHELL_RC_TARGETS[@]}"; do
    restricted_dev_env_remove_shell_rc_block "${shell_rc_target}"
    log "shell rc limpo: ${shell_rc_target}"
  done

  remove_file_if_exists "${ELIXIR_LS_SETUP_SH}"
  remove_file_if_exists "${ELIXIR_LS_SETUP_FISH}"
fi

if [[ "${RESET_ENV_FILES}" == "1" ]]; then
  remove_file_if_exists "${HOME}/.config/wrapper-envs.sh"
  remove_file_if_exists "${HOME}/.config/restricted-dev-env.fish"
  remove_glob_matches "${HOME}/.config/mix-*-envs.sh"
fi

if [[ "${RESET_INSTALL_DIRS}" == "1" ]]; then
  remove_dir_if_exists "${HOME}/.local/share/homebrew-install-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/curl-python-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/git-zip-wrapper"
  remove_glob_matches "${HOME}/.local/share/mix-*-wrapper"
  remove_glob_matches "${HOME}/.local/share/nvim-*-wrapper"
fi

if [[ "${RESET_WRAPPER_CACHE}" == "1" ]]; then
  remove_dir_if_exists "${HOME}/.cache/curl-python-wrapper"
  remove_dir_if_exists "${HOME}/.cache/mason-seeds"
fi

if [[ "${RESET_MASON_ELIXIR_LS}" == "1" ]]; then
  remove_dir_if_exists "${HOME}/.local/share/nvim/mason/packages/elixir-ls"
  remove_dir_if_exists "${HOME}/.local/share/nvim/mason/staging/elixir-ls"
  remove_file_if_exists "${HOME}/.local/share/nvim/mason/bin/elixir-ls"
  remove_file_if_exists "${HOME}/.local/share/nvim/mason/bin/elixir-ls-debugger"
  remove_file_if_exists "${HOME}/.local/share/nvim/mason/receipts/elixir-ls.json"
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
  exec "${TARGET_SHELL_EXECUTABLE}" -l

Para limpar a sessão atual:
  exec "${TARGET_SHELL_EXECUTABLE}" -l

Para reconfigurar do zero:
  sh scripts/configure.sh
EOF
