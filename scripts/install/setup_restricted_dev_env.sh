#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[setup-restricted-dev-env] erro: bash é obrigatório para executar o bootstrap\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[setup-restricted-dev-env] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_wrapper_binary_path() {
  local binary_name candidate_path wrapper_path
  binary_name="$1"
  candidate_path="$2"

  case "${binary_name}" in
    curl)
      wrapper_path="${HOME}/.local/share/curl-python-wrapper/bin/curl"
      ;;
    wget)
      wrapper_path="${HOME}/.local/share/curl-python-wrapper/bin/wget"
      ;;
    git)
      wrapper_path="${HOME}/.local/share/git-zip-wrapper/bin/git"
      ;;
    mix)
      case "${candidate_path}" in
        "${HOME}"/.local/share/mix-*-wrapper/bin/mix)
          return 0
          ;;
      esac
      return 1
      ;;
    brew)
      wrapper_path="${HOME}/.local/share/homebrew-install-wrapper/bin/brew"
      ;;
    *)
      return 1
      ;;
  esac

  [[ "${candidate_path}" == "${wrapper_path}" ]]
}

resolve_real_binary() {
  local binary_name candidate
  binary_name="$1"

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if is_wrapper_binary_path "${binary_name}" "${candidate}"; then
      continue
    fi
    printf '%s\n' "${candidate}"
    return 0
  done <<EOF2
$(which -a "${binary_name}" 2>/dev/null || true)
EOF2

  return 1
}

detect_default_shell_rc() {
  local active_shell shell_name
  active_shell="${SHELL:-}"
  shell_name="${active_shell##*/}"

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
      if [[ -f "${HOME}/.zshrc" ]]; then
        printf '%s\n' "${HOME}/.zshrc"
      elif [[ -f "${HOME}/.bashrc" ]]; then
        printf '%s\n' "${HOME}/.bashrc"
      else
        printf '%s\n' "${HOME}/.profile"
      fi
      ;;
  esac
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/setup_restricted_dev_env.sh [opções]

Opções:
  --shell-rc <arquivo>         Arquivo rc do shell (padrão detectado a partir de $SHELL).
  --apply-shell-rc             Persiste os env-files no shell rc.
  --real-curl <path>           Binário real do curl.
  --real-wget <path>           Binário real do wget.
  --real-git <path>            Binário real do git.
  --real-mix <path>            Binário real do mix. Necessário apenas com --configure-hex.
  --real-brew <path>           Legado. Ignorado; o wrapper de brew foi removido.
  --proxy <url>                Proxy para wrappers e, opcionalmente, Hex.
  --ca-cert <arquivo>          CA customizada para wrappers/Hex.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no curl wrapper.
  --mason-seed-dir <dir>       Diretório seed para artefatos do Mason.
  --configure-hex              Também aplica mix hex.config no host local.
  --hex-unsafe-https           Define unsafe_https/registry/origin no Hex.
  --hex-no-test                Não executa mix hex.info ao final da config do Hex.
  --no-shell-rc                Não altera o arquivo rc do shell.
  -h, --help                   Mostra esta ajuda.
USAGE
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_HELPER="${SCRIPT_DIR}/restricted_dev_env_state.sh"

# shellcheck disable=SC1090
. "${STATE_HELPER}"

SHELL_RC_PATH="$(detect_default_shell_rc)"
APPLY_SHELL_RC="0"
REAL_CURL_BIN=""
REAL_WGET_BIN=""
REAL_GIT_BIN=""
REAL_MIX_BIN=""
PROXY_URL="${HTTPS_PROXY:-${https_proxy:-${ALL_PROXY:-${all_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}}}"
CA_CERT_PATH="${GIT_ZIP_WRAPPER_CURL_CACERT:-${HEX_CACERTS_PATH:-${SSL_CERT_FILE:-${REQUESTS_CA_BUNDLE:-${AWS_CA_BUNDLE:-}}}}}"
AUTO_INSECURE_ON_CERT_ERROR="${CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR:-0}"
MASON_SEED_DIR="${CURL_WRAPPER_MASON_SEED_DIR:-}"
CONFIGURE_HEX="0"
HEX_UNSAFE_HTTPS="${HEX_UNSAFE_HTTPS:-0}"
HEX_RUN_TEST="1"
WRAPPER_ENV_FILE="${HOME}/.config/wrapper-envs.sh"
FISH_ENV_FILE="${HOME}/.config/restricted-dev-env.fish"
ELIXIR_LS_SETUP_SH="${HOME}/.config/elixir_ls/setup.sh"
ELIXIR_LS_SETUP_FISH="${HOME}/.config/elixir_ls/setup.fish"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shell-rc)
      SHELL_RC_PATH="${2:-}"
      APPLY_SHELL_RC="1"
      shift 2
      ;;
    --apply-shell-rc)
      APPLY_SHELL_RC="1"
      shift
      ;;
    --real-curl)
      REAL_CURL_BIN="${2:-}"
      shift 2
      ;;
    --real-wget)
      REAL_WGET_BIN="${2:-}"
      shift 2
      ;;
    --real-git)
      REAL_GIT_BIN="${2:-}"
      shift 2
      ;;
    --real-mix)
      REAL_MIX_BIN="${2:-}"
      shift 2
      ;;
    --real-brew)
      shift 2
      ;;
    --proxy)
      PROXY_URL="${2:-}"
      shift 2
      ;;
    --ca-cert)
      CA_CERT_PATH="${2:-}"
      shift 2
      ;;
    --auto-insecure-on-cert-error)
      AUTO_INSECURE_ON_CERT_ERROR="1"
      shift
      ;;
    --mason-seed-dir)
      MASON_SEED_DIR="${2:-}"
      shift 2
      ;;
    --configure-hex)
      CONFIGURE_HEX="1"
      shift
      ;;
    --hex-unsafe-https)
      CONFIGURE_HEX="1"
      HEX_UNSAFE_HTTPS="1"
      shift
      ;;
    --hex-no-test)
      HEX_RUN_TEST="0"
      shift
      ;;
    --no-shell-rc)
      APPLY_SHELL_RC="0"
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

if [[ -z "${REAL_CURL_BIN}" ]]; then
  REAL_CURL_BIN="$(resolve_real_binary curl || true)"
fi
if [[ -z "${REAL_WGET_BIN}" ]]; then
  REAL_WGET_BIN="$(resolve_real_binary wget || true)"
fi
if [[ -z "${REAL_GIT_BIN}" ]]; then
  REAL_GIT_BIN="$(resolve_real_binary git || true)"
fi
if [[ "${CONFIGURE_HEX}" == "1" ]] && [[ -z "${REAL_MIX_BIN}" ]]; then
  REAL_MIX_BIN="$(resolve_real_binary mix || true)"
fi

[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_CURL_BIN}" ]] || die "curl inválido/não executável: ${REAL_CURL_BIN}"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
is_wrapper_binary_path curl "${REAL_CURL_BIN}" && die "curl real não pode apontar para o wrapper instalado: ${REAL_CURL_BIN}"
is_wrapper_binary_path git "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"
if [[ -n "${REAL_WGET_BIN}" ]]; then
  [[ -x "${REAL_WGET_BIN}" ]] || die "wget inválido/não executável: ${REAL_WGET_BIN}"
  is_wrapper_binary_path wget "${REAL_WGET_BIN}" && die "wget real não pode apontar para o wrapper instalado: ${REAL_WGET_BIN}"
fi
if [[ "${CONFIGURE_HEX}" == "1" ]]; then
  [[ -n "${REAL_MIX_BIN}" ]] || die "não foi possível localizar mix no PATH para configurar o Hex"
  [[ -x "${REAL_MIX_BIN}" ]] || die "mix inválido/não executável: ${REAL_MIX_BIN}"
  is_wrapper_binary_path mix "${REAL_MIX_BIN}" && die "mix real não pode apontar para o wrapper instalado: ${REAL_MIX_BIN}"
fi
if [[ -n "${CA_CERT_PATH}" ]]; then
  [[ -f "${CA_CERT_PATH}" ]] || die "CA customizada não encontrada: ${CA_CERT_PATH}"
fi
case "${AUTO_INSECURE_ON_CERT_ERROR}" in
  0|1)
    ;;
  *)
    die "CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR inválido: ${AUTO_INSECURE_ON_CERT_ERROR}"
    ;;
esac

run_step() {
  local description
  description="$1"
  shift
  log "${description}"
  "$@"
}

install_curl_wrapper() {
  if [[ -n "${REAL_WGET_BIN}" ]]; then
    sh "${ROOT_DIR}/install/install_curl_python_wrapper.sh" \
      --real-curl "${REAL_CURL_BIN}" \
      --real-wget "${REAL_WGET_BIN}" \
      >/dev/null
    return 0
  fi

  sh "${ROOT_DIR}/install/install_curl_python_wrapper.sh" \
    --real-curl "${REAL_CURL_BIN}" \
    >/dev/null
}

install_git_wrapper() {
  sh "${ROOT_DIR}/install/install_git_zip_wrapper.sh" \
    --real-git "${REAL_GIT_BIN}" \
    >/dev/null
}

remove_legacy_brew_wrapper_installation() {
  local brew_wrapper_root
  brew_wrapper_root="${HOME}/.local/share/homebrew-install-wrapper"

  if [[ ! -d "${brew_wrapper_root}" ]]; then
    return 0
  fi

  rm -rf "${brew_wrapper_root}"
  log "wrapper legado do brew removido: ${brew_wrapper_root}"
}

resolve_hex_config_path() {
  local hex_dump config_home
  hex_dump="$(mix hex.config 2>/dev/null || true)"
  config_home="$(printf '%s\n' "${hex_dump}" | awk -F'"' '/^config_home:/ { print $2; exit }')"

  if [[ -z "${config_home}" ]]; then
    config_home="${HEX_HOME:-${HOME}/.hex}"
  fi

  printf '%s/hex.config\n' "${config_home}"
}

snapshot_hex_config_state_if_needed() {
  local hex_config_path

  if [[ "${CONFIGURE_HEX}" != "1" ]]; then
    return 0
  fi

  if [[ "${RESTRICTED_DEV_ENV_HEX_MANAGED}" == "1" &&
    -n "${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH}" &&
    -f "${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH}" ]]; then
    return 0
  fi

  hex_config_path="$(resolve_hex_config_path)"
  RESTRICTED_DEV_ENV_HEX_MANAGED="1"
  RESTRICTED_DEV_ENV_HEX_CONFIG_PATH="${hex_config_path}"
  RESTRICTED_DEV_ENV_HEX_BACKUP_PATH="${RESTRICTED_DEV_ENV_HEX_BACKUP_FILE}"

  if [[ -f "${hex_config_path}" ]]; then
    restricted_dev_env_ensure_state_dir
    cp "${hex_config_path}" "${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH}"
    RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE="1"
    return 0
  fi

  rm -f "${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH}"
  RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE="0"
}

shell_rc_looks_like_fish() {
  local shell_rc_path
  shell_rc_path="$1"
  case "${shell_rc_path}" in
    *.fish|*/fish/config.fish)
      return 0
      ;;
  esac
  return 1
}

collect_export_keys_from_env_file() {
  local env_file
  env_file="$1"
  [[ -f "${env_file}" ]] || return 0
  awk '
    $1 == "export" && $2 ~ /^[A-Za-z_][A-Za-z0-9_]*=.*/ {
      split($2, parts, "=")
      print parts[1]
    }
  ' "${env_file}"
}

collect_unset_keys_from_env_file() {
  local env_file
  env_file="$1"
  [[ -f "${env_file}" ]] || return 0
  awk '
    $1 == "unset" && $2 ~ /^[A-Za-z_][A-Za-z0-9_]*$/ {
      print $2
    }
  ' "${env_file}"
}

escape_double_quotes_for_fish() {
  local value
  value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//\$/\\\$}"
  printf '%s' "${value}"
}

write_fish_env_file() {
  local -a exported_keys unset_keys path_entries
  local env_entry key value escaped_value escaped_path_entry

  mapfile -t exported_keys < <(
    collect_export_keys_from_env_file "${WRAPPER_ENV_FILE}" | awk '!seen[$0]++'
  )

  mapfile -t unset_keys < <(
    collect_unset_keys_from_env_file "${WRAPPER_ENV_FILE}" | awk '!seen[$0]++'
  )

  mkdir -p "$(dirname "${FISH_ENV_FILE}")"
  {
    cat <<'EOF2'
#!/usr/bin/env fish
# Gerado por scripts/install/setup_restricted_dev_env.sh

EOF2
  } > "${FISH_ENV_FILE}"

  if [[ ${#exported_keys[@]} -gt 0 ]]; then
    while IFS= read -r -d '' env_entry; do
      key="${env_entry%%=*}"
      value="${env_entry#*=}"
      if [[ "${key}" == "PATH" ]]; then
        IFS=':' read -r -a path_entries <<< "${value}"
        printf 'set -gx PATH' >> "${FISH_ENV_FILE}"
        for value in "${path_entries[@]}"; do
          [[ -n "${value}" ]] || continue
          escaped_path_entry="$(escape_double_quotes_for_fish "${value}")"
          printf ' "%s"' "${escaped_path_entry}" >> "${FISH_ENV_FILE}"
        done
        printf '\n' >> "${FISH_ENV_FILE}"
        continue
      fi

      escaped_value="$(escape_double_quotes_for_fish "${value}")"
      printf 'set -gx %s "%s"\n' "${key}" "${escaped_value}" >> "${FISH_ENV_FILE}"
    done < <(
      set +u
      # shellcheck disable=SC1090
      . "${WRAPPER_ENV_FILE}"
      set -u
      for key in "${exported_keys[@]}"; do
        eval "value=\${${key}:-}"
        printf '%s=%s\0' "${key}" "${value}"
      done
    )
    printf '\n' >> "${FISH_ENV_FILE}"
  fi

  for key in "${unset_keys[@]}"; do
    printf 'set -e %s\n' "${key}" >> "${FISH_ENV_FILE}"
  done

  chmod 0644 "${FISH_ENV_FILE}"
}

sync_shell_rc_state() {
  local previous_shell_rc
  previous_shell_rc="${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}"

  if [[ -n "${previous_shell_rc}" ]]; then
    restricted_dev_env_remove_shell_rc_block "${previous_shell_rc}"
  fi

  if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
    if shell_rc_looks_like_fish "${SHELL_RC_PATH}"; then
      restricted_dev_env_apply_shell_rc_fish_block "${SHELL_RC_PATH}" "${FISH_ENV_FILE}"
    else
      restricted_dev_env_apply_shell_rc_block "${SHELL_RC_PATH}" "${WRAPPER_ENV_FILE}"
    fi
    RESTRICTED_DEV_ENV_MANAGED_SHELL_RC="${SHELL_RC_PATH}"
    return 0
  fi

  RESTRICTED_DEV_ENV_MANAGED_SHELL_RC=""
}

sync_elixir_ls_setup_state() {
  restricted_dev_env_apply_elixir_ls_setup_sh_block \
    "${ELIXIR_LS_SETUP_SH}" \
    "${WRAPPER_ENV_FILE}"
  restricted_dev_env_apply_elixir_ls_setup_fish_block \
    "${ELIXIR_LS_SETUP_FISH}" \
    "${FISH_ENV_FILE}"
}

validate_persisted_env_files() {
  [[ -f "${WRAPPER_ENV_FILE}" ]] || die "env-file compartilhado dos wrappers não foi criado: ${WRAPPER_ENV_FILE}"
  [[ -f "${FISH_ENV_FILE}" ]] || die "env-file fish compartilhado não foi criado: ${FISH_ENV_FILE}"
}

validate_elixir_ls_setup_files() {
  [[ -f "${ELIXIR_LS_SETUP_SH}" ]] || die "setup.sh do elixir_ls não foi criado: ${ELIXIR_LS_SETUP_SH}"
  [[ -f "${ELIXIR_LS_SETUP_FISH}" ]] || die "setup.fish do elixir_ls não foi criado: ${ELIXIR_LS_SETUP_FISH}"
  grep -Fq "${WRAPPER_ENV_FILE}" "${ELIXIR_LS_SETUP_SH}" || die "setup.sh do elixir_ls não referencia ${WRAPPER_ENV_FILE}"
  grep -Fq "${FISH_ENV_FILE}" "${ELIXIR_LS_SETUP_FISH}" || die "setup.fish do elixir_ls não referencia ${FISH_ENV_FILE}"
}

validate_installed_wrappers() {
  [[ -x "${HOME}/.local/share/curl-python-wrapper/bin/curl" ]] || die "wrapper do curl não foi instalado"
  [[ -x "${HOME}/.local/share/curl-python-wrapper/bin/wget" ]] || die "wrapper do wget não foi instalado"
  [[ -x "${HOME}/.local/share/git-zip-wrapper/bin/git" ]] || die "wrapper do git não foi instalado"
}

validate_shell_rc_persistence() {
  local managed_shell_rc
  managed_shell_rc="${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}"

  if [[ "${APPLY_SHELL_RC}" != "1" ]]; then
    return 0
  fi

  [[ -n "${managed_shell_rc}" ]] || die "shell rc gerenciado não foi registrado no estado"
  [[ -f "${managed_shell_rc}" ]] || die "shell rc gerenciado não existe: ${managed_shell_rc}"
  grep -Fq "${RESTRICTED_DEV_ENV_SHELL_RC_BEGIN}" "${managed_shell_rc}" || die "bloco gerenciado não foi gravado em ${managed_shell_rc}"
  if shell_rc_looks_like_fish "${managed_shell_rc}"; then
    grep -Fq "${FISH_ENV_FILE}" "${managed_shell_rc}" || die "shell rc fish não referencia ${FISH_ENV_FILE}"
  else
    grep -Fq "${WRAPPER_ENV_FILE}" "${managed_shell_rc}" || die "shell rc não referencia ${WRAPPER_ENV_FILE}"
  fi
}

validate_restricted_dev_env_result() {
  validate_persisted_env_files
  validate_elixir_ls_setup_files
  validate_installed_wrappers
  validate_shell_rc_persistence
}

WRAPPER_ENV_ARGS=(
  --real-curl "${REAL_CURL_BIN}"
  --real-git "${REAL_GIT_BIN}"
)

if [[ -n "${REAL_WGET_BIN}" ]]; then
  WRAPPER_ENV_ARGS+=(--real-wget "${REAL_WGET_BIN}")
fi
if [[ -n "${PROXY_URL}" ]]; then
  WRAPPER_ENV_ARGS+=(--proxy "${PROXY_URL}")
fi
if [[ -n "${CA_CERT_PATH}" ]]; then
  WRAPPER_ENV_ARGS+=(--ca-cert "${CA_CERT_PATH}")
fi
if [[ -n "${MASON_SEED_DIR}" ]]; then
  WRAPPER_ENV_ARGS+=(--mason-seed-dir "${MASON_SEED_DIR}")
fi
if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
  WRAPPER_ENV_ARGS+=(--auto-insecure-on-cert-error)
fi
WRAPPER_ENV_ARGS+=(--no-shell-rc)

run_step "instalando wrapper do curl" install_curl_wrapper
run_step "instalando wrapper do git" install_git_wrapper
run_step "removendo wrapper legado do brew" remove_legacy_brew_wrapper_installation
run_step "configurando ambiente compartilhado dos wrappers em modo local-only" \
  sh "${ROOT_DIR}/install/configure_wrapper_envs.sh" "${WRAPPER_ENV_ARGS[@]}"
run_step "gerando env fish compartilhado para wrappers" write_fish_env_file

if [[ "${CONFIGURE_HEX}" == "1" ]]; then
  snapshot_hex_config_state_if_needed

  HEX_ARGS=()
  if [[ -n "${PROXY_URL}" ]]; then
    HEX_ARGS+=(--proxy "${PROXY_URL}")
  fi
  if [[ -n "${CA_CERT_PATH}" ]]; then
    HEX_ARGS+=(--ca-cert "${CA_CERT_PATH}")
  fi
  if [[ "${HEX_UNSAFE_HTTPS}" == "1" ]]; then
    HEX_ARGS+=(--unsafe-https)
  fi
  if [[ "${HEX_RUN_TEST}" == "0" ]]; then
    HEX_ARGS+=(--no-test)
  fi

  run_step "configurando Hex no host local" \
    sh "${ROOT_DIR}/install/configure_hex_config.sh" "${HEX_ARGS[@]}"
fi

run_step "sincronizando persistência do ambiente restrito" sync_shell_rc_state
run_step "sincronizando setup do ElixirLS (sh/fish)" sync_elixir_ls_setup_state
run_step "persistindo estado do ambiente restrito" restricted_dev_env_write_state
run_step "validando artefatos persistidos do bootstrap" validate_restricted_dev_env_result

cat <<EOF2
Bootstrap concluído.

Modo:
  local-only

Persistência:
  shell rc: ${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-não alterado}
  arquivo env sh: ${WRAPPER_ENV_FILE}
  arquivo env fish: ${FISH_ENV_FILE}
  elixir_ls setup.sh: ${ELIXIR_LS_SETUP_SH}
  elixir_ls setup.fish: ${ELIXIR_LS_SETUP_FISH}
  state: ${RESTRICTED_DEV_ENV_STATE_FILE}

Rede corporativa efetiva:
  proxy wrappers: ${PROXY_URL:-não definido}
  ca cert: ${CA_CERT_PATH:-não definida}
  hex unsafe: ${HEX_UNSAFE_HTTPS}

Para aplicar na sessão atual (zsh/bash/sh):
  arquivo: ${WRAPPER_ENV_FILE}
  . "${WRAPPER_ENV_FILE}"
  rehash 2>/dev/null || true
  hash -r 2>/dev/null || true
  # reinicie o nvim/tmux já aberto depois disso

Para aplicar na sessão atual (fish):
  arquivo: ${FISH_ENV_FILE}
  source "${FISH_ENV_FILE}"
  # reinicie o nvim/tmux já aberto depois disso

Para validar o env persistido:
  sh "${ROOT_DIR}/install/validate_wrappers.sh"

Para validar o shell atual:
  sh "${ROOT_DIR}/install/validate_wrappers.sh" --current-shell

Para validar o Neovim atual:
  nvim --headless '+lua print(vim.fn.exepath("git"))' +qa
EOF2
