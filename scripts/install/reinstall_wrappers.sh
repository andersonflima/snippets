#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[reinstall-wrappers] erro: bash é obrigatório para reinstalar os wrappers\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[reinstall-wrappers] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/reinstall_wrappers.sh [opções]

Opções:
  --env-file <arquivo>         Arquivo de env gerado pelo configure_wrapper_envs.
  --shell-rc <arquivo>         Persiste source no rc indicado.
  --apply-shell-rc             Persiste source no shell rc detectado.
  --no-shell-rc                Não altera shell rc.
  --skip-configure             Reinstala binários, mas não regenera env-file.
  --with-mix-wrapper           Também reinstala o wrapper de mix.
  --real-curl <path>           Binário real do curl.
  --real-wget <path>           Binário real do wget.
  --real-git <path>            Binário real do git.
  --real-brew <path>           Binário real do brew.
  --instance-name <nome>       Nome da instância EC2 dos wrappers.
  --aws-profile <profile>      Profile AWS.
  --aws-region <region>        Região AWS.
  --s3-bucket <bucket>         Bucket S3 dos wrappers.
  --s3-prefix <prefixo>        Prefixo S3 dos wrappers.
  --enable-ec2-backend         Liga backend remoto via EC2 nos wrappers.
  --disable-ec2-backend        Desliga backend EC2 nos wrappers.
  --proxy <url>                Proxy local para wrappers.
  --ec2-proxy <url>            Proxy exclusivo para backend EC2.
  --ca-cert <arquivo>          CA customizada para wrapper de git.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no wrapper de curl.
  --mason-seed-dir <dir>       Diretório seed para artefatos do Mason.
  --git-lfs-mode <modo>        local|ec2 para pós-clone do git wrapper.
  -h, --help                   Mostra esta ajuda.

Comportamento:
  - Reinstala wrappers de curl, git e brew (quando brew existir).
  - Se --skip-configure não for usado, regenera o env-file.
  - Quando possível, reaproveita parâmetros do env-file atual.
USAGE
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC=""
SKIP_CONFIGURE="0"
WITH_MIX_WRAPPER="0"

REAL_CURL_BIN=""
REAL_WGET_BIN=""
REAL_GIT_BIN=""
REAL_BREW_BIN=""

INSTANCE_NAME=""
AWS_PROFILE_NAME=""
AWS_REGION_NAME=""
S3_BUCKET_NAME=""
S3_PREFIX_NAME=""
ENABLE_EC2_BACKEND=""
PROXY_URL=""
EC2_PROXY_URL=""
CA_CERT_PATH=""
AUTO_INSECURE_ON_CERT_ERROR="0"
MASON_SEED_DIR=""
GIT_LFS_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --shell-rc)
      SHELL_RC="${2:-}"
      APPLY_SHELL_RC="1"
      shift 2
      ;;
    --apply-shell-rc)
      APPLY_SHELL_RC="1"
      shift
      ;;
    --no-shell-rc)
      APPLY_SHELL_RC="0"
      shift
      ;;
    --skip-configure)
      SKIP_CONFIGURE="1"
      shift
      ;;
    --with-mix-wrapper)
      WITH_MIX_WRAPPER="1"
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
    --real-brew)
      REAL_BREW_BIN="${2:-}"
      shift 2
      ;;
    --instance-name)
      INSTANCE_NAME="${2:-}"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE_NAME="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION_NAME="${2:-}"
      shift 2
      ;;
    --s3-bucket)
      S3_BUCKET_NAME="${2:-}"
      shift 2
      ;;
    --s3-prefix)
      S3_PREFIX_NAME="${2:-}"
      shift 2
      ;;
    --enable-ec2-backend)
      ENABLE_EC2_BACKEND="1"
      shift
      ;;
    --disable-ec2-backend)
      ENABLE_EC2_BACKEND="0"
      shift
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
    --mason-seed-dir)
      MASON_SEED_DIR="${2:-}"
      shift 2
      ;;
    --git-lfs-mode)
      GIT_LFS_MODE="${2:-}"
      shift 2
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

load_existing_env_defaults() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    return 0
  fi

  set +u
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set -u

  [[ -n "${REAL_CURL_BIN}" ]] || REAL_CURL_BIN="${CURL_WRAPPER_REAL_CURL:-}"
  [[ -n "${REAL_WGET_BIN}" ]] || REAL_WGET_BIN="${WGET_WRAPPER_REAL_WGET:-}"
  [[ -n "${REAL_GIT_BIN}" ]] || REAL_GIT_BIN="${GIT_ZIP_WRAPPER_REAL_GIT:-}"
  [[ -n "${REAL_BREW_BIN}" ]] || REAL_BREW_BIN="${BREW_WRAPPER_REAL_BREW:-}"

  [[ -n "${INSTANCE_NAME}" ]] || INSTANCE_NAME="${WRAPPERS_VIA_EC2_INSTANCE_NAME:-}"
  [[ -n "${AWS_PROFILE_NAME}" ]] || AWS_PROFILE_NAME="${WRAPPERS_VIA_EC2_AWS_PROFILE:-}"
  [[ -n "${AWS_REGION_NAME}" ]] || AWS_REGION_NAME="${WRAPPERS_VIA_EC2_AWS_REGION:-}"
  [[ -n "${S3_BUCKET_NAME}" ]] || S3_BUCKET_NAME="${WRAPPERS_VIA_EC2_S3_BUCKET:-}"
  [[ -n "${S3_PREFIX_NAME}" ]] || S3_PREFIX_NAME="${WRAPPERS_VIA_EC2_S3_PREFIX:-}"
  [[ -n "${ENABLE_EC2_BACKEND}" ]] || ENABLE_EC2_BACKEND="${WRAPPERS_VIA_EC2_ENABLED:-}"
  [[ -n "${PROXY_URL}" ]] || PROXY_URL="${CURL_WRAPPER_PROXY:-${HTTPS_PROXY:-${HTTP_PROXY:-}}}"
  [[ -n "${EC2_PROXY_URL}" ]] || EC2_PROXY_URL="${WRAPPERS_VIA_EC2_PROXY:-}"
  [[ -n "${CA_CERT_PATH}" ]] || CA_CERT_PATH="${GIT_ZIP_WRAPPER_CURL_CACERT:-}"
  [[ -n "${MASON_SEED_DIR}" ]] || MASON_SEED_DIR="${CURL_WRAPPER_MASON_SEED_DIR:-}"
  [[ -n "${GIT_LFS_MODE}" ]] || GIT_LFS_MODE="${GIT_ZIP_WRAPPER_LFS_MODE:-}"
}

load_existing_env_defaults

if [[ -z "${ENABLE_EC2_BACKEND}" ]]; then
  ENABLE_EC2_BACKEND="0"
fi

install_wrapper_binaries() {
  local -a curl_install_args=()
  local -a git_install_args=()
  local -a brew_install_args=()

  [[ -n "${REAL_CURL_BIN}" ]] && curl_install_args+=(--real-curl "${REAL_CURL_BIN}")
  [[ -n "${REAL_WGET_BIN}" ]] && curl_install_args+=(--real-wget "${REAL_WGET_BIN}")
  [[ -n "${REAL_GIT_BIN}" ]] && git_install_args+=(--real-git "${REAL_GIT_BIN}")
  [[ -n "${REAL_BREW_BIN}" ]] && brew_install_args+=(--real-brew "${REAL_BREW_BIN}")

  log "reinstalando wrapper de curl/wget"
  sh "${SCRIPT_DIR}/install_curl_python_wrapper.sh" "${curl_install_args[@]}"

  log "reinstalando wrapper de git"
  sh "${SCRIPT_DIR}/install_git_zip_wrapper.sh" "${git_install_args[@]}"

  if [[ -n "${REAL_BREW_BIN}" ]] || command -v brew >/dev/null 2>&1; then
    log "reinstalando wrapper de brew"
    sh "${SCRIPT_DIR}/install_homebrew_wrapper.sh" "${brew_install_args[@]}"
  else
    log "brew não encontrado no host atual; reinstalação do wrapper de brew foi ignorada"
  fi

  if [[ "${WITH_MIX_WRAPPER}" == "1" ]]; then
    log "reinstalando wrapper de mix"
    sh "${SCRIPT_DIR}/install_mix_ec2_wrapper.sh"
  fi
}

configure_wrapper_env_file() {
  local -a configure_args=()

  configure_args+=(--env-file "${ENV_FILE}")

  case "${APPLY_SHELL_RC}" in
    1)
      if [[ -n "${SHELL_RC}" ]]; then
        configure_args+=(--shell-rc "${SHELL_RC}")
      else
        configure_args+=(--apply-shell-rc)
      fi
      ;;
    0)
      configure_args+=(--no-shell-rc)
      ;;
  esac

  [[ -n "${REAL_CURL_BIN}" ]] && configure_args+=(--real-curl "${REAL_CURL_BIN}")
  [[ -n "${REAL_WGET_BIN}" ]] && configure_args+=(--real-wget "${REAL_WGET_BIN}")
  [[ -n "${REAL_GIT_BIN}" ]] && configure_args+=(--real-git "${REAL_GIT_BIN}")
  [[ -n "${REAL_BREW_BIN}" ]] && configure_args+=(--real-brew "${REAL_BREW_BIN}")
  [[ -n "${INSTANCE_NAME}" ]] && configure_args+=(--instance-name "${INSTANCE_NAME}")
  [[ -n "${AWS_PROFILE_NAME}" ]] && configure_args+=(--aws-profile "${AWS_PROFILE_NAME}")
  [[ -n "${AWS_REGION_NAME}" ]] && configure_args+=(--aws-region "${AWS_REGION_NAME}")
  [[ -n "${S3_BUCKET_NAME}" ]] && configure_args+=(--s3-bucket "${S3_BUCKET_NAME}")
  [[ -n "${S3_PREFIX_NAME}" ]] && configure_args+=(--s3-prefix "${S3_PREFIX_NAME}")
  [[ -n "${PROXY_URL}" ]] && configure_args+=(--proxy "${PROXY_URL}")
  [[ -n "${EC2_PROXY_URL}" ]] && configure_args+=(--ec2-proxy "${EC2_PROXY_URL}")
  [[ -n "${CA_CERT_PATH}" ]] && configure_args+=(--ca-cert "${CA_CERT_PATH}")
  [[ -n "${MASON_SEED_DIR}" ]] && configure_args+=(--mason-seed-dir "${MASON_SEED_DIR}")

  if [[ "${ENABLE_EC2_BACKEND}" == "1" ]]; then
    configure_args+=(--enable-ec2-backend)
  else
    configure_args+=(--disable-ec2-backend)
  fi
  if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
    configure_args+=(--auto-insecure-on-cert-error)
  fi

  log "regenerando env dos wrappers em ${ENV_FILE}"
  if [[ -n "${GIT_LFS_MODE}" ]]; then
    GIT_ZIP_WRAPPER_LFS_MODE="${GIT_LFS_MODE}" \
      sh "${SCRIPT_DIR}/configure_wrapper_envs.sh" "${configure_args[@]}"
    return 0
  fi

  sh "${SCRIPT_DIR}/configure_wrapper_envs.sh" "${configure_args[@]}"
}

install_wrapper_binaries

if [[ "${SKIP_CONFIGURE}" == "1" ]]; then
  cat <<EOF
Reinstalação concluída.

Env-file:
  ${ENV_FILE}

Próximo passo:
  sh "${ROOT_DIR}/validate_wrappers.sh"
EOF
  exit 0
fi

configure_wrapper_env_file

cat <<EOF
Reinstalação concluída.

Env-file:
  ${ENV_FILE}

Próximo passo:
  . "${ENV_FILE}"
  sh "${ROOT_DIR}/validate_wrappers.sh"
EOF
