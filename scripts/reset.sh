#!/usr/bin/env sh
set -eu

DRY_RUN=0
RESTORE_NVIM_BACKUPS=1
RESET_HEX_CONFIG=1

log() {
  printf '[lazyvim-machine-reset] %s\n' "$*" >&2
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/reset.sh [opcoes]

Opcoes:
  --dry-run                    Mostra o que seria removido/restaurado, sem alterar arquivos.
  --no-restore-nvim-backups    Nao restaura backups .offline-backup.* do Neovim.
  --keep-hex-config            Nao restaura/remove configuracao persistida do Hex.
  -h, --help                   Mostra esta ajuda.

Este script reverte configuracoes aplicadas pelos scripts antigos de LazyVim em
ambiente restrito: wrappers locais, env-files, blocos em shell rc, setup do
ElixirLS, caches/seeds, estado do Mason, estado restricted-dev-env e Hex quando
ha backup registrado.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --no-restore-nvim-backups)
      RESTORE_NVIM_BACKUPS=0
      shift
      ;;
    --keep-hex-config)
      RESET_HEX_CONFIG=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "erro: parametro invalido: $1"
      usage >&2
      exit 1
      ;;
  esac
done

run() {
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '[dry-run] %s\n' "$*" >&2
    return 0
  fi

  "$@"
}

remove_file() {
  target="$1"
  [ -e "${target}" ] || return 0
  log "removendo arquivo: ${target}"
  run rm -f "${target}"
}

remove_dir() {
  target="$1"
  [ -d "${target}" ] || return 0
  log "removendo diretorio: ${target}"
  run rm -rf "${target}"
}

remove_path() {
  target="$1"
  [ -e "${target}" ] || [ -L "${target}" ] || return 0

  if [ -d "${target}" ] && [ ! -L "${target}" ]; then
    remove_dir "${target}"
    return 0
  fi

  remove_file "${target}"
}

remove_matches() {
  pattern="$1"

  set +f
  for target in ${pattern}; do
    [ -e "${target}" ] || [ -L "${target}" ] || continue
    remove_path "${target}"
  done
}

read_state_value() {
  key="$1"
  state_file="${HOME}/.config/restricted-dev-env/state.sh"

  [ -f "${state_file}" ] || return 0

  sed -n "s/^export ${key}=//p" "${state_file}" \
    | tail -n 1 \
    | sed \
      -e 's/^"//' \
      -e 's/"$//' \
      -e "s/^'//" \
      -e "s/'$//" \
      -e 's/\\ / /g' \
      -e 's/\\\\/\\/g'
}

append_unique_file() {
  file="$1"
  list_file="$2"

  [ -n "${file}" ] || return 0
  grep -Fxq "${file}" "${list_file}" 2>/dev/null && return 0
  printf '%s\n' "${file}" >> "${list_file}"
}

clean_shell_rc_file() {
  rc_file="$1"

  [ -f "${rc_file}" ] || return 0

  tmp_file="$(mktemp "${TMPDIR:-/tmp}/lazyvim-machine-reset-shell-rc.XXXXXX")"

  awk '
    $0 == "# >>> restricted-dev-env >>>" {
      inside_managed_block = 1
      next
    }

    $0 == "# <<< restricted-dev-env <<<" {
      inside_managed_block = 0
      next
    }

    inside_managed_block == 1 {
      next
    }

    $0 ~ /\.config\/wrapper-envs\.sh/ {
      next
    }

    $0 ~ /\.config\/restricted-dev-env\.fish/ {
      next
    }

    $0 ~ /\.config\/mix-[^[:space:]]+-envs\.sh/ {
      next
    }

    $0 ~ /^[[:space:]]*(export|unset)[[:space:]]*(CURL_WRAPPER_|WGET_WRAPPER_|GIT_ZIP_WRAPPER_|BREW_WRAPPER_|HTTPS_PROXY|HTTP_PROXY|ALL_PROXY|NO_PROXY|SSL_CERT_FILE|REQUESTS_CA_BUNDLE|CURL|WGET|GIT|BREW)=/ {
      next
    }

    $0 ~ /^[[:space:]]*unset[[:space:]]*(CURL_WRAPPER_|WGET_WRAPPER_|GIT_ZIP_WRAPPER_|BREW_WRAPPER_|HTTPS_PROXY|HTTP_PROXY|ALL_PROXY|NO_PROXY|SSL_CERT_FILE|REQUESTS_CA_BUNDLE|CURL|WGET|GIT|BREW)([[:space:]]|$)/ {
      next
    }

    $0 ~ /^[[:space:]]*export[[:space:]]+PATH=.*((\.local\/share\/curl-python-wrapper)|(\.local\/share\/git-zip-wrapper)|(\.local\/share\/homebrew-install-wrapper)|(\.local\/share\/mix-[^[:space:]]*-wrapper\/bin)|(\.local\/share\/nvim-[^[:space:]]*-wrapper\/bin)|(\.local\/bin))/ {
      next
    }

    $0 ~ /# wrapper do mix via [A-Za-z0-9_-]+/ {
      next
    }

    $0 ~ /# wrappers? (locais )?de curl\/git para ambiente restrito/ {
      next
    }

    {
      print
    }
  ' "${rc_file}" > "${tmp_file}"

  if cmp -s "${rc_file}" "${tmp_file}"; then
    rm -f "${tmp_file}"
    return 0
  fi

  log "limpando shell rc: ${rc_file}"
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '[dry-run] mv %s %s\n' "${tmp_file}" "${rc_file}" >&2
    rm -f "${tmp_file}"
    return 0
  fi

  mv "${tmp_file}" "${rc_file}"
}

clean_shell_rc_files() {
  list_file="$(mktemp "${TMPDIR:-/tmp}/lazyvim-machine-reset-rc-list.XXXXXX")"
  managed_shell_rc="$(read_state_value RESTRICTED_DEV_ENV_MANAGED_SHELL_RC || true)"

  append_unique_file "${managed_shell_rc}" "${list_file}"
  append_unique_file "${HOME}/.zshrc" "${list_file}"
  append_unique_file "${HOME}/.bashrc" "${list_file}"
  append_unique_file "${HOME}/.profile" "${list_file}"
  append_unique_file "${HOME}/.config/fish/config.fish" "${list_file}"

  while IFS= read -r rc_file; do
    clean_shell_rc_file "${rc_file}"
  done < "${list_file}"

  rm -f "${list_file}"
}

remove_managed_shim() {
  command_name="$1"
  expected_target="$2"
  shim_path="${HOME}/.local/bin/${command_name}"

  [ -L "${shim_path}" ] || return 0
  actual_target="$(readlink "${shim_path}" 2>/dev/null || true)"

  [ "${actual_target}" = "${expected_target}" ] || return 0
  log "removendo shim gerenciado: ${shim_path}"
  run rm -f "${shim_path}"
}

restore_hex_config() {
  [ "${RESET_HEX_CONFIG}" -eq 1 ] || return 0

  hex_managed="$(read_state_value RESTRICTED_DEV_ENV_HEX_MANAGED || true)"
  [ "${hex_managed:-0}" = "1" ] || return 0

  hex_config_path="$(read_state_value RESTRICTED_DEV_ENV_HEX_CONFIG_PATH || true)"
  hex_backup_path="$(read_state_value RESTRICTED_DEV_ENV_HEX_BACKUP_PATH || true)"
  hex_existed_before="$(read_state_value RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE || true)"

  [ -n "${hex_config_path}" ] || return 0

  if [ "${hex_existed_before:-0}" = "1" ]; then
    if [ -f "${hex_backup_path}" ]; then
      log "restaurando configuracao do Hex: ${hex_config_path}"
      if [ "${DRY_RUN}" -eq 1 ]; then
        printf '[dry-run] cp %s %s\n' "${hex_backup_path}" "${hex_config_path}" >&2
        return 0
      fi

      mkdir -p "$(dirname "${hex_config_path}")"
      cp "${hex_backup_path}" "${hex_config_path}"
      return 0
    fi

    log "aviso: backup do Hex nao encontrado: ${hex_backup_path}"
    return 0
  fi

  remove_file "${hex_config_path}"
}

latest_backup_for() {
  target="$1"
  latest=""

  set +f
  for backup in "${target}".offline-backup.*; do
    [ -e "${backup}" ] || continue
    latest="${backup}"
  done

  printf '%s\n' "${latest}"
}

restore_latest_backup() {
  target="$1"
  latest_backup="$(latest_backup_for "${target}")"

  [ -n "${latest_backup}" ] || return 0

  if [ -e "${target}" ] || [ -L "${target}" ]; then
    log "removendo estado atual antes de restaurar backup: ${target}"
    run rm -rf "${target}"
  fi

  log "restaurando backup mais recente: ${latest_backup} -> ${target}"
  run mv "${latest_backup}" "${target}"
}

remove_env_files() {
  remove_file "${HOME}/.config/wrapper-envs.sh"
  remove_file "${HOME}/.config/restricted-dev-env.fish"
  remove_matches "${HOME}/.config/mix-*-envs.sh"
}

remove_elixir_ls_setup() {
  remove_file "${HOME}/.config/elixir_ls/setup.sh"
  remove_file "${HOME}/.config/elixir_ls/setup.fish"
  if [ -d "${HOME}/.config/elixir_ls" ]; then
    log "removendo diretorio vazio se possivel: ${HOME}/.config/elixir_ls"
    run rmdir "${HOME}/.config/elixir_ls" 2>/dev/null || true
  fi
}

remove_wrapper_installations() {
  remove_managed_shim "git" "${HOME}/.local/share/git-zip-wrapper/bin/git"
  remove_managed_shim "curl" "${HOME}/.local/share/curl-python-wrapper/bin/curl"
  remove_managed_shim "wget" "${HOME}/.local/share/curl-python-wrapper/bin/wget"

  remove_dir "${HOME}/.local/share/homebrew-install-wrapper"
  remove_dir "${HOME}/.local/share/curl-python-wrapper"
  remove_dir "${HOME}/.local/share/git-zip-wrapper"
  remove_matches "${HOME}/.local/share/mix-*-wrapper"
  remove_matches "${HOME}/.local/share/nvim-*-wrapper"
}

remove_wrapper_caches() {
  remove_dir "${HOME}/.cache/curl-python-wrapper"
  remove_dir "${HOME}/.cache/mason-seeds"
  remove_dir "${HOME}/.cache/nvim/mason"
}

remove_mason_state() {
  remove_dir "${HOME}/.local/share/nvim/mason"
  remove_dir "${HOME}/.local/state/nvim/mason"
  remove_dir "${HOME}/.cache/nvim/mason"
  remove_dir "${HOME}/.local/share/nvim/mason-tools"
  remove_dir "${HOME}/.local/share/nvim/site/pack/mason"
  remove_matches "${HOME}/.local/share/mason-*"
}

remove_restricted_state() {
  remove_file "${HOME}/.config/restricted-dev-env/state.sh"
  remove_file "${HOME}/.config/restricted-dev-env/hex.config.backup"
  if [ -d "${HOME}/.config/restricted-dev-env" ]; then
    log "removendo diretorio vazio se possivel: ${HOME}/.config/restricted-dev-env"
    run rmdir "${HOME}/.config/restricted-dev-env" 2>/dev/null || true
  fi
}

restore_nvim_offline_backups() {
  [ "${RESTORE_NVIM_BACKUPS}" -eq 1 ] || return 0

  restore_latest_backup "${HOME}/.config/nvim"
  restore_latest_backup "${HOME}/.local/share/nvim"
}

clean_shell_rc_files
restore_hex_config
remove_env_files
remove_elixir_ls_setup
remove_wrapper_installations
remove_wrapper_caches
remove_mason_state
restore_nvim_offline_backups
remove_restricted_state

cat <<'EOF'
Reset concluido.

Para limpar variaveis ja carregadas na sessao atual, abra um novo shell de login
ou execute novamente seu shell, por exemplo:

  exec "$SHELL" -l
EOF
