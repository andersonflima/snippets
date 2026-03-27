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
  -h, --help                   Mostra esta ajuda.
USAGE
}

SHELL_RC_PATH="${HOME}/.zshrc"
RESET_SHELL_RC="1"
RESET_ENV_FILES="1"
RESET_INSTALL_DIRS="1"

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
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parâmetro inválido: $1"
      ;;
  esac
done

remove_lines_from_shell_rc() {
  local rc_file tmp_file
  rc_file="$1"
  [[ -f "${rc_file}" ]] || return 0

  tmp_file="$(mktemp "/tmp/reset-restricted-dev-env.XXXXXX")"
  awk '
    index($0, ".config/mix-via-ec2-envs.sh") == 0 &&
    index($0, ".config/wrapper-envs.sh") == 0
  ' "${rc_file}" > "${tmp_file}"
  mv "${tmp_file}" "${rc_file}"
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
  remove_lines_from_shell_rc "${SHELL_RC_PATH}"
  log "shell rc limpo: ${SHELL_RC_PATH}"
fi

if [[ "${RESET_ENV_FILES}" == "1" ]]; then
  remove_file_if_exists "${HOME}/.config/mix-via-ec2-envs.sh"
  remove_file_if_exists "${HOME}/.config/wrapper-envs.sh"
fi

if [[ "${RESET_INSTALL_DIRS}" == "1" ]]; then
  remove_dir_if_exists "${HOME}/.local/share/mix-ec2-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/curl-python-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/git-zip-wrapper"
  remove_dir_if_exists "${HOME}/.local/share/nvim-ec2-wrapper"
fi

cat <<EOF
Reset concluído.

Shell rc:
  ${SHELL_RC_PATH}

Para abrir uma sessão limpa agora:
  zsh -f
EOF
