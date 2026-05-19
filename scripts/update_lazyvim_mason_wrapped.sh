#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
WRAPPER_BIN_DIR="${HOME}/.local/share/nvim/wrappers/bin"
LAZY_ONLY=0
MASON_ONLY=0
FORCE_WRAPPER_INSTALL=0
AUTO_SHELL_PROFILE=1
EC2_HOST=""
EC2_INSTANCE_ID=""
EC2_S3_URI=""
CLONE_ORDER=""

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
  --ec2-host <host>        Host SSH da EC2 usado para git clone de repositorios externos.
                           Reinstala os shims com clone order ec2-first.
  --ec2-instance-id <id>   Instance ID da EC2 gerenciada pelo SSM.
  --ec2-s3-uri <uri>       Prefixo S3 usado para transportar clones da EC2 para a maquina.
                           Junto com --ec2-instance-id, reinstala com ec2-s3-first.
  --clone-order <ordem>    Ordem do wrapper git: local-first, git-first, ec2-first ou ec2-s3-first.
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
    --ec2-host)
      EC2_HOST="${2:-}"
      CLONE_ORDER="ec2-first"
      FORCE_WRAPPER_INSTALL=1
      shift 2
      ;;
    --ec2-instance-id)
      EC2_INSTANCE_ID="${2:-}"
      if [ -n "${EC2_S3_URI}" ]; then
        CLONE_ORDER="ec2-s3-first"
      fi
      FORCE_WRAPPER_INSTALL=1
      shift 2
      ;;
    --ec2-s3-uri)
      EC2_S3_URI="${2:-}"
      if [ -n "${EC2_INSTANCE_ID}" ]; then
        CLONE_ORDER="ec2-s3-first"
      fi
      FORCE_WRAPPER_INSTALL=1
      shift 2
      ;;
    --clone-order)
      CLONE_ORDER="${2:-}"
      FORCE_WRAPPER_INSTALL=1
      shift 2
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

build_install_args() {
  printf '%s\n' "--target-dir"
  printf '%s\n' "${WRAPPER_BIN_DIR}"

  if [ "${FORCE_WRAPPER_INSTALL}" = "1" ]; then
    printf '%s\n' "--force"
  fi

  if [ -n "${EC2_HOST}" ]; then
    printf '%s\n' "--ec2-host"
    printf '%s\n' "${EC2_HOST}"
  fi

  if [ -n "${EC2_INSTANCE_ID}" ]; then
    printf '%s\n' "--ec2-instance-id"
    printf '%s\n' "${EC2_INSTANCE_ID}"
  fi

  if [ -n "${EC2_S3_URI}" ]; then
    printf '%s\n' "--ec2-s3-uri"
    printf '%s\n' "${EC2_S3_URI}"
  fi

  if [ -n "${CLONE_ORDER}" ]; then
    printf '%s\n' "--clone-order"
    printf '%s\n' "${CLONE_ORDER}"
  fi

  if [ "${AUTO_SHELL_PROFILE}" = "0" ]; then
    printf '%s\n' "--no-shell-profile"
  fi
}

if [ "${FORCE_WRAPPER_INSTALL}" = "1" ] || [ ! -x "${WRAPPER_BIN_DIR}/git" ] || [ ! -x "${WRAPPER_BIN_DIR}/curl" ]; then
  install_args_file="$(mktemp)"
  build_install_args > "${install_args_file}"
  xargs sh "${SCRIPT_DIR}/install_nvim_wrappers.sh" < "${install_args_file}"
  rm -f "${install_args_file}"
fi

PATH="${WRAPPER_BIN_DIR}:$PATH"
export PATH

if [ -n "${EC2_HOST}" ]; then
  export GIT_ZIP_WRAPPER_EC2_HOST="${GIT_ZIP_WRAPPER_EC2_HOST:-${EC2_HOST}}"
  export GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-ec2-first}"
fi
if [ -n "${EC2_INSTANCE_ID}" ] && [ -n "${EC2_S3_URI}" ]; then
  export GIT_ZIP_WRAPPER_EC2_INSTANCE_ID="${GIT_ZIP_WRAPPER_EC2_INSTANCE_ID:-${EC2_INSTANCE_ID}}"
  export GIT_ZIP_WRAPPER_EC2_S3_URI="${GIT_ZIP_WRAPPER_EC2_S3_URI:-${EC2_S3_URI}}"
  export GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-ec2-s3-first}"
fi
if [ -n "${CLONE_ORDER}" ]; then
  export GIT_ZIP_WRAPPER_CLONE_ORDER="${GIT_ZIP_WRAPPER_CLONE_ORDER:-${CLONE_ORDER}}"
fi
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
