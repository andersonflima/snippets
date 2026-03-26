#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-mix-via-ec2-envs] erro: bash é obrigatório para configurar o wrapper do mix\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[configure-mix-via-ec2-envs] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/configure_mix_via_ec2_envs.sh [opções]

Opções:
  --env-file <arquivo>         Arquivo com exports persistidos.
  --shell-rc <arquivo>         Arquivo rc do shell que vai carregar o env-file.
  --apply-shell-rc             Persiste o source do env-file no shell rc.
  --no-shell-rc                Não altera arquivo rc do shell.
  --mix-install-dir <dir>      Diretório do wrapper instalado. Padrão: $HOME/.local/share/mix-ec2-wrapper/bin
  --real-mix <path>            Caminho do mix real.
  --instance-name <nome>       Nome da instância EC2. Padrão: Dander
  --aws-profile <profile>      Profile AWS.
  --aws-region <region>        Region AWS. Padrão: sa-east-1
  --s3-bucket <bucket>         Bucket compartilhado com os demais wrappers.
  --s3-prefix <prefixo>        Prefixo S3 compartilhado. Padrão: mix-via-ec2
  --ssh-identity <arquivo>     Chave SSH privada para o EC2.
  --remote-commands <csv>      Lista CSV de comandos do mix roteados para o EC2.
  -h, --help                   Mostra esta ajuda.
USAGE
}

ENV_FILE="${HOME}/.config/mix-via-ec2-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC="0"
MIX_INSTALL_DIR="${HOME}/.local/share/mix-ec2-wrapper/bin"
REAL_MIX_BIN="${MIX_WRAPPER_REAL_MIX:-}"
INSTANCE_NAME="${MIX_VIA_EC2_INSTANCE_NAME:-Dander}"
AWS_PROFILE_NAME="${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}"
AWS_REGION_NAME="${MIX_VIA_EC2_AWS_REGION:-sa-east-1}"
S3_BUCKET_NAME="${MIX_VIA_EC2_S3_BUCKET:-}"
S3_PREFIX_NAME="${MIX_VIA_EC2_S3_PREFIX:-mix-via-ec2}"
SSH_IDENTITY_PATH="${MIX_VIA_EC2_SSH_IDENTITY:-}"
REMOTE_COMMANDS="${MIX_WRAPPER_REMOTE_COMMANDS:-deps.get,deps.compile,deps.update,deps.unlock,local.hex,local.rebar,archive.install,archive.build,phx.new,hex.info}"

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
    --mix-install-dir)
      MIX_INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --real-mix)
      REAL_MIX_BIN="${2:-}"
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
    --ssh-identity)
      SSH_IDENTITY_PATH="${2:-}"
      shift 2
      ;;
    --remote-commands)
      REMOTE_COMMANDS="${2:-}"
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

if [[ -z "${REAL_MIX_BIN}" ]]; then
  REAL_MIX_BIN="$(command -v mix || true)"
fi

[[ -n "${REAL_MIX_BIN}" ]] || die "não foi possível localizar mix no PATH"
[[ -x "${REAL_MIX_BIN}" ]] || die "mix inválido/não executável: ${REAL_MIX_BIN}"

detect_shell_rc() {
  local active_shell shell_name
  active_shell="${SHELL:-}"
  shell_name="${active_shell##*/}"

  case "${shell_name}" in
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

shell_quote() {
  printf "%q" "$1"
}

write_env_file() {
  mkdir -p "$(dirname "${ENV_FILE}")"
  cat > "${ENV_FILE}" <<EOF
#!/usr/bin/env sh
# Gerado por scripts/install/configure_mix_via_ec2_envs.sh

export MIX_WRAPPER_REAL_MIX=$(shell_quote "${REAL_MIX_BIN}")
export PATH=$(shell_quote "${MIX_INSTALL_DIR}"):"\$PATH"
export MIX_VIA_EC2_INSTANCE_NAME=$(shell_quote "${INSTANCE_NAME}")
export MIX_VIA_EC2_AWS_REGION=$(shell_quote "${AWS_REGION_NAME}")
export MIX_VIA_EC2_S3_PREFIX=$(shell_quote "${S3_PREFIX_NAME}")
export MIX_WRAPPER_REMOTE_COMMANDS=$(shell_quote "${REMOTE_COMMANDS}")
export WRAPPERS_VIA_EC2_ENABLED="1"
export WRAPPERS_VIA_EC2_INSTANCE_NAME=$(shell_quote "${INSTANCE_NAME}")
export WRAPPERS_VIA_EC2_AWS_REGION=$(shell_quote "${AWS_REGION_NAME}")
export WRAPPERS_VIA_EC2_S3_PREFIX=$(shell_quote "${S3_PREFIX_NAME}")
EOF

  if [[ -n "${AWS_PROFILE_NAME}" ]]; then
    printf 'export MIX_VIA_EC2_AWS_PROFILE=%s\n' "$(shell_quote "${AWS_PROFILE_NAME}")" >> "${ENV_FILE}"
    printf 'export WRAPPERS_VIA_EC2_AWS_PROFILE=%s\n' "$(shell_quote "${AWS_PROFILE_NAME}")" >> "${ENV_FILE}"
  fi

  if [[ -n "${S3_BUCKET_NAME}" ]]; then
    printf 'export MIX_VIA_EC2_S3_BUCKET=%s\n' "$(shell_quote "${S3_BUCKET_NAME}")" >> "${ENV_FILE}"
    printf 'export WRAPPERS_VIA_EC2_S3_BUCKET=%s\n' "$(shell_quote "${S3_BUCKET_NAME}")" >> "${ENV_FILE}"
  fi

  if [[ -n "${SSH_IDENTITY_PATH}" ]]; then
    printf 'export MIX_VIA_EC2_SSH_IDENTITY=%s\n' "$(shell_quote "${SSH_IDENTITY_PATH}")" >> "${ENV_FILE}"
  fi

  chmod 0644 "${ENV_FILE}"
}

ensure_source_line() {
  local rc_file source_line
  rc_file="$1"
  source_line=". $(shell_quote "${ENV_FILE}")"

  mkdir -p "$(dirname "${rc_file}")"
  touch "${rc_file}"

  if grep -Fq "${ENV_FILE}" "${rc_file}" || grep -Fq "${source_line}" "${rc_file}"; then
    return 0
  fi

  {
    printf '\n'
    printf '# wrapper do mix via EC2\n'
    printf '%s\n' "${source_line}"
  } >> "${rc_file}"
}

write_env_file

if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
  if [[ -z "${SHELL_RC}" ]]; then
    SHELL_RC="$(detect_shell_rc)"
  fi
  ensure_source_line "${SHELL_RC}"
fi

cat <<EOF
Configuração concluída.

Arquivo de ambiente:
  ${ENV_FILE}

Instância padrão:
  ${INSTANCE_NAME}

Region padrão:
  ${AWS_REGION_NAME}
EOF

if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
  cat <<EOF

Arquivo rc atualizado:
  ${SHELL_RC}
EOF
fi

cat <<EOF

Para aplicar na sessão atual:
  . ${ENV_FILE}
EOF
