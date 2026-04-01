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
    mix)
      wrapper_path="${HOME}/.local/share/mix-ec2-wrapper/bin/mix"
      ;;
    curl)
      wrapper_path="${HOME}/.local/share/curl-python-wrapper/bin/curl"
      ;;
    wget)
      wrapper_path="${HOME}/.local/share/curl-python-wrapper/bin/wget"
      ;;
    git)
      wrapper_path="${HOME}/.local/share/git-zip-wrapper/bin/git"
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
  done <<EOF
$(which -a "${binary_name}" 2>/dev/null || true)
EOF

  return 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/setup_restricted_dev_env.sh --s3-bucket <bucket> [opções]

Opções:
  --s3-bucket <bucket>         Bucket compartilhado pelos wrappers e pelo mix.
  --instance-name <nome>       Instância EC2. Padrão: Dander
  --aws-region <region>        Region AWS. Padrão: sa-east-1
  --aws-profile <profile>      Profile AWS.
  --s3-prefix <prefixo>        Prefixo compartilhado para os wrappers. Padrão: wrappers-via-ec2
  --mix-s3-prefix <prefixo>    Prefixo específico do mix. Padrão: mix-via-ec2
  --enable-ec2-backend         Liga backend remoto via EC2 nos wrappers (opcional).
  --disable-ec2-backend        Desliga backend remoto via EC2 nos wrappers.
  --shell-rc <arquivo>         Arquivo rc do shell.
  --apply-shell-rc             Persiste os env-files no shell rc.
  --real-mix <path>            Binário real do mix.
  --real-curl <path>           Binário real do curl.
  --real-git <path>            Binário real do git.
  --real-brew <path>           Binário real do brew, quando houver Homebrew.
  --ssh-identity <arquivo>     Chave SSH opcional para o mix via EC2.
  --proxy <url>                Proxy para wrappers e, opcionalmente, Hex.
  --ec2-proxy <url>            Proxy exclusivo para o backend remoto no EC2.
  --ca-cert <arquivo>          CA customizada para wrappers/Hex.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no curl wrapper.
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

S3_BUCKET=""
INSTANCE_NAME="Dander"
AWS_REGION_NAME="sa-east-1"
AWS_PROFILE_NAME=""
WRAPPERS_S3_PREFIX="wrappers-via-ec2"
MIX_S3_PREFIX="mix-via-ec2"
ENABLE_WRAPPER_EC2_BACKEND="0"
SHELL_RC_PATH="${HOME}/.zshrc"
APPLY_SHELL_RC="0"
REAL_MIX_BIN=""
REAL_CURL_BIN=""
REAL_WGET_BIN=""
REAL_GIT_BIN=""
REAL_BREW_BIN=""
SSH_IDENTITY_PATH=""
PROXY_URL=""
EC2_PROXY_URL=""
CA_CERT_PATH=""
AUTO_INSECURE_ON_CERT_ERROR="0"
CONFIGURE_HEX="0"
HEX_UNSAFE_HTTPS="0"
HEX_RUN_TEST="1"
MIX_ENV_FILE="${HOME}/.config/mix-via-ec2-envs.sh"
WRAPPER_ENV_FILE="${HOME}/.config/wrapper-envs.sh"
ELIXIR_LS_SETUP_SH="${HOME}/.config/elixir_ls/setup.sh"
ELIXIR_LS_SETUP_FISH="${HOME}/.config/elixir_ls/setup.fish"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --s3-bucket)
      S3_BUCKET="${2:-}"
      shift 2
      ;;
    --instance-name)
      INSTANCE_NAME="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION_NAME="${2:-}"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE_NAME="${2:-}"
      shift 2
      ;;
    --s3-prefix)
      WRAPPERS_S3_PREFIX="${2:-}"
      shift 2
      ;;
    --mix-s3-prefix)
      MIX_S3_PREFIX="${2:-}"
      shift 2
      ;;
    --enable-ec2-backend)
      ENABLE_WRAPPER_EC2_BACKEND="1"
      shift
      ;;
    --disable-ec2-backend)
      ENABLE_WRAPPER_EC2_BACKEND="0"
      shift
      ;;
    --shell-rc)
      SHELL_RC_PATH="${2:-}"
      APPLY_SHELL_RC="1"
      shift 2
      ;;
    --apply-shell-rc)
      APPLY_SHELL_RC="1"
      shift
      ;;
    --real-mix)
      REAL_MIX_BIN="${2:-}"
      shift 2
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
    --real-brew)
      REAL_BREW_BIN="${2:-}"
      shift 2
      ;;
    --ssh-identity)
      SSH_IDENTITY_PATH="${2:-}"
      shift 2
      ;;
    --proxy)
      PROXY_URL="${2:-}"
      shift 2
      ;;
    --ec2-proxy)
      EC2_PROXY_URL="${2:-}"
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

[[ -n "${S3_BUCKET}" ]] || die "--s3-bucket é obrigatório"

restricted_dev_env_load_state
RESTRICTED_DEV_ENV_MANAGED_SHELL_RC="${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}"
RESTRICTED_DEV_ENV_HEX_MANAGED="${RESTRICTED_DEV_ENV_HEX_MANAGED:-0}"
RESTRICTED_DEV_ENV_HEX_CONFIG_PATH="${RESTRICTED_DEV_ENV_HEX_CONFIG_PATH:-}"
RESTRICTED_DEV_ENV_HEX_BACKUP_PATH="${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH:-${RESTRICTED_DEV_ENV_HEX_BACKUP_FILE}}"
RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE="${RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE:-0}"

if [[ -z "${REAL_MIX_BIN}" ]]; then
  REAL_MIX_BIN="$(resolve_real_binary mix || true)"
fi
if [[ -z "${REAL_CURL_BIN}" ]]; then
  REAL_CURL_BIN="$(resolve_real_binary curl || true)"
fi
if [[ -z "${REAL_WGET_BIN}" ]]; then
  REAL_WGET_BIN="$(resolve_real_binary wget || true)"
fi
if [[ -z "${REAL_GIT_BIN}" ]]; then
  REAL_GIT_BIN="$(resolve_real_binary git || true)"
fi
if [[ -z "${REAL_BREW_BIN}" ]]; then
  REAL_BREW_BIN="$(resolve_real_binary brew || true)"
fi

[[ -n "${REAL_MIX_BIN}" ]] || die "não foi possível localizar mix no PATH"
[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
is_wrapper_binary_path mix "${REAL_MIX_BIN}" && die "mix real não pode apontar para o wrapper instalado: ${REAL_MIX_BIN}"
is_wrapper_binary_path curl "${REAL_CURL_BIN}" && die "curl real não pode apontar para o wrapper instalado: ${REAL_CURL_BIN}"
is_wrapper_binary_path git "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"
if [[ -n "${REAL_BREW_BIN}" ]]; then
  is_wrapper_binary_path brew "${REAL_BREW_BIN}" && die "brew real não pode apontar para o wrapper instalado: ${REAL_BREW_BIN}"
fi

run_step() {
  local description
  description="$1"
  shift
  log "${description}"
  "$@"
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

sync_shell_rc_state() {
  local previous_shell_rc
  previous_shell_rc="${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}"

  if [[ -n "${previous_shell_rc}" ]]; then
    restricted_dev_env_remove_shell_rc_block "${previous_shell_rc}"
  fi

  if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
    restricted_dev_env_apply_shell_rc_block "${SHELL_RC_PATH}" "${MIX_ENV_FILE}" "${WRAPPER_ENV_FILE}"
    RESTRICTED_DEV_ENV_MANAGED_SHELL_RC="${SHELL_RC_PATH}"
    return 0
  fi

  RESTRICTED_DEV_ENV_MANAGED_SHELL_RC=""
}

sync_elixir_ls_setup_state() {
  restricted_dev_env_apply_elixir_ls_setup_sh_block \
    "${ELIXIR_LS_SETUP_SH}" \
    "${MIX_ENV_FILE}" \
    "${WRAPPER_ENV_FILE}"
  restricted_dev_env_apply_elixir_ls_setup_fish_block \
    "${ELIXIR_LS_SETUP_FISH}" \
    "${MIX_ENV_FILE}" \
    "${WRAPPER_ENV_FILE}"
}

MIX_ENV_ARGS=(
  --instance-name "${INSTANCE_NAME}"
  --aws-region "${AWS_REGION_NAME}"
  --s3-bucket "${S3_BUCKET}"
  --s3-prefix "${MIX_S3_PREFIX}"
  --real-mix "${REAL_MIX_BIN}"
)
WRAPPER_ENV_ARGS=(
  --instance-name "${INSTANCE_NAME}"
  --aws-region "${AWS_REGION_NAME}"
  --s3-bucket "${S3_BUCKET}"
  --s3-prefix "${WRAPPERS_S3_PREFIX}"
  --real-curl "${REAL_CURL_BIN}"
  --real-wget "${REAL_WGET_BIN}"
  --real-git "${REAL_GIT_BIN}"
)

if [[ -n "${AWS_PROFILE_NAME}" ]]; then
  MIX_ENV_ARGS+=(--aws-profile "${AWS_PROFILE_NAME}")
  WRAPPER_ENV_ARGS+=(--aws-profile "${AWS_PROFILE_NAME}")
fi
if [[ -n "${SSH_IDENTITY_PATH}" ]]; then
  MIX_ENV_ARGS+=(--ssh-identity "${SSH_IDENTITY_PATH}")
fi
if [[ -n "${EC2_PROXY_URL}" ]]; then
  MIX_ENV_ARGS+=(--proxy "${EC2_PROXY_URL}")
elif [[ -n "${PROXY_URL}" ]]; then
  MIX_ENV_ARGS+=(--proxy "${PROXY_URL}")
fi
if [[ -n "${CA_CERT_PATH}" ]]; then
  MIX_ENV_ARGS+=(--ca-cert "${CA_CERT_PATH}")
fi
if [[ "${HEX_UNSAFE_HTTPS}" == "1" ]]; then
  MIX_ENV_ARGS+=(--hex-unsafe-https)
fi
if [[ -n "${PROXY_URL}" ]]; then
  WRAPPER_ENV_ARGS+=(--proxy "${PROXY_URL}")
fi
if [[ -n "${EC2_PROXY_URL}" ]]; then
  WRAPPER_ENV_ARGS+=(--ec2-proxy "${EC2_PROXY_URL}")
fi
if [[ -n "${CA_CERT_PATH}" ]]; then
  WRAPPER_ENV_ARGS+=(--ca-cert "${CA_CERT_PATH}")
fi
if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
  WRAPPER_ENV_ARGS+=(--auto-insecure-on-cert-error)
fi
if [[ "${ENABLE_WRAPPER_EC2_BACKEND}" == "1" ]]; then
  WRAPPER_ENV_ARGS+=(--enable-ec2-backend)
else
  WRAPPER_ENV_ARGS+=(--disable-ec2-backend)
fi
MIX_ENV_ARGS+=(--no-shell-rc)
WRAPPER_ENV_ARGS+=(--no-shell-rc)

run_step "instalando wrapper do mix" \
  sh "${ROOT_DIR}/install/install_mix_ec2_wrapper.sh" --real-mix "${REAL_MIX_BIN}"
run_step "instalando wrapper do curl" \
  sh "${ROOT_DIR}/install/install_curl_python_wrapper.sh" --real-curl "${REAL_CURL_BIN}"
run_step "instalando wrapper do git" \
  sh "${ROOT_DIR}/install/install_git_zip_wrapper.sh" --real-git "${REAL_GIT_BIN}"
if [[ -n "${REAL_BREW_BIN}" ]]; then
  run_step "instalando wrapper do brew" \
    sh "${ROOT_DIR}/install/install_homebrew_wrapper.sh" --real-brew "${REAL_BREW_BIN}"
else
  log "brew não encontrado no PATH; pulando wrapper do brew"
fi
run_step "configurando ambiente do mix via EC2" \
  sh "${ROOT_DIR}/install/configure_mix_via_ec2_envs.sh" "${MIX_ENV_ARGS[@]}"
if [[ -n "${REAL_BREW_BIN}" ]]; then
  WRAPPER_ENV_ARGS+=(--real-brew "${REAL_BREW_BIN}")
fi
run_step "configurando ambiente compartilhado dos wrappers" \
  sh "${ROOT_DIR}/install/configure_wrapper_envs.sh" "${WRAPPER_ENV_ARGS[@]}"

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
    sh "${ROOT_DIR}/ec2/elixir/configure_hex_config.sh" "${HEX_ARGS[@]}"
fi

run_step "sincronizando persistência do ambiente restrito" sync_shell_rc_state
run_step "sincronizando setup do ElixirLS (sh/fish)" sync_elixir_ls_setup_state
run_step "persistindo estado do ambiente restrito" restricted_dev_env_write_state

cat <<EOF
Bootstrap concluído.

Instância EC2:
  ${INSTANCE_NAME}

Region AWS:
  ${AWS_REGION_NAME}

Bucket compartilhado:
  ${S3_BUCKET}

Prefixos:
  mix: ${MIX_S3_PREFIX}
  wrappers: ${WRAPPERS_S3_PREFIX}

Wrappers EC2 backend:
  ${ENABLE_WRAPPER_EC2_BACKEND}

Persistência:
  shell rc: ${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-não alterado}
  elixir_ls setup.sh: ${ELIXIR_LS_SETUP_SH}
  elixir_ls setup.fish: ${ELIXIR_LS_SETUP_FISH}
  state: ${RESTRICTED_DEV_ENV_STATE_FILE}

Para aplicar na sessão atual:
  . "${MIX_ENV_FILE}"
  . "${WRAPPER_ENV_FILE}"
  rehash 2>/dev/null || true
  hash -r 2>/dev/null || true

Para validar se o Mason está vendo os wrappers:
  sh "${ROOT_DIR}/install/validate_wrappers.sh"
EOF
