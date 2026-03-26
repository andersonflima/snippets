#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-wrapper-envs] erro: bash é obrigatório para configurar o ambiente\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[configure-wrapper-envs] %s\n' "$*" >&2
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
      wrapper_path="${CURL_INSTALL_DIR}/curl"
      ;;
    git)
      wrapper_path="${GIT_INSTALL_DIR}/git"
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
  scripts/install/configure_wrapper_envs.sh [opções]

Opções:
  --env-file <arquivo>         Arquivo com exports persistidos.
  --shell-rc <arquivo>         Arquivo rc do shell que vai carregar o env-file.
  --apply-shell-rc             Persiste o source do env-file no shell rc.
  --no-shell-rc                Não altera arquivo rc do shell.
  --curl-install-dir <dir>     Diretório do wrapper instalado de curl.
  --git-install-dir <dir>      Diretório do wrapper instalado de git.
  --real-curl <path>           Caminho do curl real.
  --real-git <path>            Caminho do git real.
  --mason-seed-dir <dir>       Diretório com artefatos seed do Mason.
  --instance-name <nome>       Instância EC2 compartilhada. Padrão: Dander
  --aws-profile <profile>      Profile AWS para backend remoto dos wrappers.
  --aws-region <region>        Region AWS do backend remoto. Padrão: sa-east-1
  --s3-bucket <bucket>         Bucket compartilhado pelos wrappers no backend EC2.
  --s3-prefix <prefixo>        Prefixo S3 compartilhado. Padrão: wrappers-via-ec2
  --disable-ec2-backend        Desliga o backend remoto via EC2 nos wrappers.
  --proxy <url>                Define proxy para wrappers e env padrão.
  --ca-cert <arquivo>          Define CA customizada para o wrapper de git.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no wrapper de curl.
  -h, --help                   Mostra esta ajuda.

Padrões:
  --env-file: $HOME/.config/wrapper-envs.sh
  --shell-rc: só usado quando combinado com --apply-shell-rc
  --curl-install-dir: $HOME/.local/share/curl-python-wrapper/bin
  --git-install-dir: $HOME/.local/share/git-zip-wrapper/bin
USAGE
}

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC="0"
CURL_INSTALL_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
GIT_INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
REAL_CURL_BIN="${CURL_WRAPPER_REAL_CURL:-}"
REAL_GIT_BIN="${GIT_ZIP_WRAPPER_REAL_GIT:-}"
PROXY_URL=""
CA_CERT_PATH=""
AUTO_INSECURE_ON_CERT_ERROR="0"
MASON_SEED_DIR="${CURL_WRAPPER_MASON_SEED_DIR:-}"
INSTANCE_NAME="${WRAPPERS_VIA_EC2_INSTANCE_NAME:-${MIX_VIA_EC2_INSTANCE_NAME:-Dander}}"
AWS_PROFILE_NAME="${WRAPPERS_VIA_EC2_AWS_PROFILE:-${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}}"
AWS_REGION_NAME="${WRAPPERS_VIA_EC2_AWS_REGION:-${MIX_VIA_EC2_AWS_REGION:-sa-east-1}}"
S3_BUCKET_NAME="${WRAPPERS_VIA_EC2_S3_BUCKET:-${MIX_VIA_EC2_S3_BUCKET:-}}"
S3_PREFIX_NAME="${WRAPPERS_VIA_EC2_S3_PREFIX:-${MIX_VIA_EC2_S3_PREFIX:-wrappers-via-ec2}}"
ENABLE_EC2_BACKEND="${WRAPPERS_VIA_EC2_ENABLED:-1}"

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
    --curl-install-dir)
      CURL_INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --git-install-dir)
      GIT_INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --real-curl)
      REAL_CURL_BIN="${2:-}"
      shift 2
      ;;
    --real-git)
      REAL_GIT_BIN="${2:-}"
      shift 2
      ;;
    --mason-seed-dir)
      MASON_SEED_DIR="${2:-}"
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
    --disable-ec2-backend)
      ENABLE_EC2_BACKEND="0"
      shift
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
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parâmetro inválido: $1"
      ;;
  esac
done

[[ -n "${ENV_FILE}" ]] || die "--env-file não pode ser vazio"
[[ -n "${CURL_INSTALL_DIR}" ]] || die "--curl-install-dir não pode ser vazio"
[[ -n "${GIT_INSTALL_DIR}" ]] || die "--git-install-dir não pode ser vazio"

if [[ -z "${REAL_CURL_BIN}" ]]; then
  REAL_CURL_BIN="$(resolve_real_binary curl || true)"
fi

if [[ -z "${REAL_GIT_BIN}" ]]; then
  REAL_GIT_BIN="$(resolve_real_binary git || true)"
fi

[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -x "${REAL_CURL_BIN}" ]] || die "curl inválido/não executável: ${REAL_CURL_BIN}"
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
is_wrapper_binary_path curl "${REAL_CURL_BIN}" && die "curl real não pode apontar para o wrapper instalado: ${REAL_CURL_BIN}"
is_wrapper_binary_path git "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"

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

render_optional_exports() {
  printf 'export WRAPPERS_VIA_EC2_ENABLED=%s\n' "$(shell_quote "${ENABLE_EC2_BACKEND}")"
  if [[ "${ENABLE_EC2_BACKEND}" == "1" ]]; then
    printf 'export WRAPPERS_VIA_EC2_INSTANCE_NAME=%s\n' "$(shell_quote "${INSTANCE_NAME}")"
    printf 'export WRAPPERS_VIA_EC2_AWS_REGION=%s\n' "$(shell_quote "${AWS_REGION_NAME}")"
    printf 'export WRAPPERS_VIA_EC2_S3_PREFIX=%s\n' "$(shell_quote "${S3_PREFIX_NAME}")"
    printf 'export CURL_WRAPPER_USE_EC2=%s\n' "$(shell_quote "1")"
    printf 'export GIT_ZIP_WRAPPER_USE_EC2=%s\n' "$(shell_quote "1")"
    if [[ -n "${AWS_PROFILE_NAME}" ]]; then
      printf 'export WRAPPERS_VIA_EC2_AWS_PROFILE=%s\n' "$(shell_quote "${AWS_PROFILE_NAME}")"
    fi
    if [[ -n "${S3_BUCKET_NAME}" ]]; then
      printf 'export WRAPPERS_VIA_EC2_S3_BUCKET=%s\n' "$(shell_quote "${S3_BUCKET_NAME}")"
    fi
  fi

  if [[ -n "${PROXY_URL}" ]]; then
    cat <<EOF
export HTTPS_PROXY=$(shell_quote "${PROXY_URL}")
export HTTP_PROXY=$(shell_quote "${PROXY_URL}")
export ALL_PROXY=$(shell_quote "${PROXY_URL}")
export CURL_WRAPPER_PROXY=$(shell_quote "${PROXY_URL}")
export GIT_ZIP_WRAPPER_PROXY=$(shell_quote "${PROXY_URL}")
EOF
  fi

  if [[ -n "${CA_CERT_PATH}" ]]; then
    printf 'export GIT_ZIP_WRAPPER_CURL_CACERT=%s\n' "$(shell_quote "${CA_CERT_PATH}")"
  fi

  if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
    printf 'export CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR=%s\n' "$(shell_quote "1")"
  fi

  if [[ -n "${MASON_SEED_DIR}" ]]; then
    printf 'export CURL_WRAPPER_MASON_SEED_DIR=%s\n' "$(shell_quote "${MASON_SEED_DIR}")"
  fi
}

write_env_file() {
  local env_dir
  env_dir="$(dirname "${ENV_FILE}")"
  mkdir -p "${env_dir}"

  {
    cat <<EOF
#!/usr/bin/env sh
# Gerado por scripts/install/configure_wrapper_envs.sh

export CURL_WRAPPER_REAL_CURL=$(shell_quote "${REAL_CURL_BIN}")
export GIT_ZIP_WRAPPER_REAL_GIT=$(shell_quote "${REAL_GIT_BIN}")
export CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES="1"
export CURL_WRAPPER_RELEASE_FALLBACK_REPOS="elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
export CURL_WRAPPER_RELEASE_CACHE_DIR=$(shell_quote "${HOME}/.cache/curl-python-wrapper/releases")
export CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS="elixir-lsp/elixir-ls,omnisharp/omnisharp-roslyn"
export CURL_WRAPPER_MASON_BUILDERS="elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"
export CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS="tar.gz,tgz,tar"

export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT="tar.gz"
EOF
    printf 'export PATH=%s:"$PATH"\n' "$(shell_quote "${CURL_INSTALL_DIR}:${GIT_INSTALL_DIR}")"
    render_optional_exports
  } > "${ENV_FILE}"

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
    printf '# wrappers de curl/git para ambiente restrito\n'
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

Wrapper dirs:
  curl: ${CURL_INSTALL_DIR}
  git:  ${GIT_INSTALL_DIR}

Backend EC2:
  enabled: ${ENABLE_EC2_BACKEND}
  instance: ${INSTANCE_NAME}
  region: ${AWS_REGION_NAME}
  s3-prefix: ${S3_PREFIX_NAME}

Binários reais:
  curl: ${REAL_CURL_BIN}
  git:  ${REAL_GIT_BIN}
EOF

if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
  cat <<EOF

Arquivo rc atualizado:
  ${SHELL_RC}

Para aplicar na sessão atual:
  . ${ENV_FILE}
EOF
else
  cat <<EOF

Nenhum arquivo rc foi alterado.

Para aplicar manualmente:
  . ${ENV_FILE}
EOF
fi
