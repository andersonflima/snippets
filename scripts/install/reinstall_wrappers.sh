#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[reinstall-wrappers] erro: bash é obrigatório para reinstalar os wrappers\n' >&2
  exit 1
}

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
COMMON_HELPER="${SCRIPT_DIR}/lib/common.sh"

# shellcheck disable=SC1090
. "${COMMON_HELPER}"

RESTRICTED_SCRIPT_NAME="reinstall-wrappers"

log() {
  restricted_log "$@"
}

die() {
  restricted_die "$@"
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/install/reinstall_wrappers.sh [opções]

Opções:
  --env-file <arquivo>         Arquivo de env gerado por configure_wrapper_envs.
  --shell-rc <arquivo>         Persiste source no rc indicado.
  --apply-shell-rc             Persiste source no shell rc detectado.
  --no-shell-rc                Não altera shell rc.
  --skip-configure             Reinstala binários, mas não regenera env-file.
  --real-curl <path>           Binário real do curl.
  --real-wget <path>           Binário real do wget.
  --real-git <path>            Binário real do git.
  --real-brew <path>           Legado. Ignorado; o wrapper de brew foi removido.
  --proxy <url>                Proxy local para wrappers.
  --ca-cert <arquivo>          CA customizada para wrapper de git.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no wrapper de curl.
  --mason-seed-dir <dir>       Diretório seed para artefatos do Mason.
  --git-lfs-mode <modo>        Aceita apenas local.
  -h, --help                   Mostra esta ajuda.

Comportamento:
  - Reinstala wrappers de curl, wget e git.
  - Remove instalação legada do wrapper de brew, quando existir.
  - Se --skip-configure não for usado, regenera o env-file.
  - Quando possível, reaproveita parâmetros do env-file atual.
USAGE
}

ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC=""
SKIP_CONFIGURE="0"

REAL_CURL_BIN=""
REAL_WGET_BIN=""
REAL_GIT_BIN=""
PROXY_URL=""
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
  [[ -n "${PROXY_URL}" ]] || PROXY_URL="${CURL_WRAPPER_PROXY:-${HTTPS_PROXY:-${HTTP_PROXY:-}}}"
  [[ -n "${CA_CERT_PATH}" ]] || CA_CERT_PATH="${GIT_ZIP_WRAPPER_CURL_CACERT:-}"
  [[ -n "${MASON_SEED_DIR}" ]] || MASON_SEED_DIR="${CURL_WRAPPER_MASON_SEED_DIR:-}"
  [[ -n "${GIT_LFS_MODE}" ]] || GIT_LFS_MODE="${GIT_ZIP_WRAPPER_LFS_MODE:-local}"
}

load_existing_env_defaults

case "${GIT_LFS_MODE:-local}" in
  ""|local)
    GIT_LFS_MODE="local"
    ;;
  *)
    log "GIT_ZIP_WRAPPER_LFS_MODE=${GIT_LFS_MODE} ignorado; forçando local"
    GIT_LFS_MODE="local"
    ;;
esac

install_wrapper_binaries() {
  local -a curl_install_args=()
  local -a git_install_args=()

  [[ -n "${REAL_CURL_BIN}" ]] && curl_install_args+=(--real-curl "${REAL_CURL_BIN}")
  [[ -n "${REAL_WGET_BIN}" ]] && curl_install_args+=(--real-wget "${REAL_WGET_BIN}")
  [[ -n "${REAL_GIT_BIN}" ]] && git_install_args+=(--real-git "${REAL_GIT_BIN}")

  log "reinstalando wrapper de curl/wget"
  sh "${SCRIPT_DIR}/install_curl_python_wrapper.sh" "${curl_install_args[@]+"${curl_install_args[@]}"}"

  log "reinstalando wrapper de git"
  sh "${SCRIPT_DIR}/install_git_zip_wrapper.sh" "${git_install_args[@]+"${git_install_args[@]}"}"

  restricted_remove_legacy_brew_wrapper_installation
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
  [[ -n "${PROXY_URL}" ]] && configure_args+=(--proxy "${PROXY_URL}")
  [[ -n "${CA_CERT_PATH}" ]] && configure_args+=(--ca-cert "${CA_CERT_PATH}")
  [[ -n "${MASON_SEED_DIR}" ]] && configure_args+=(--mason-seed-dir "${MASON_SEED_DIR}")

  if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
    configure_args+=(--auto-insecure-on-cert-error)
  fi

  log "regenerando env dos wrappers em ${ENV_FILE}"
  GIT_ZIP_WRAPPER_LFS_MODE="${GIT_LFS_MODE}" \
    sh "${SCRIPT_DIR}/configure_wrapper_envs.sh" "${configure_args[@]+"${configure_args[@]}"}"
}

install_wrapper_binaries

if [[ "${SKIP_CONFIGURE}" == "1" ]]; then
  cat <<EOF2
Reinstalação concluída.

Env-file:
  ${ENV_FILE}

Próximo passo:
  sh "${ROOT_DIR}/install/validate_wrappers.sh"
EOF2
  exit 0
fi

configure_wrapper_env_file

cat <<EOF2
Reinstalação concluída.

Env-file:
  ${ENV_FILE}

Próximo passo:
  . "${ENV_FILE}"
  sh "${ROOT_DIR}/install/validate_wrappers.sh"
EOF2
