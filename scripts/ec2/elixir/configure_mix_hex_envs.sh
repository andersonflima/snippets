#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-mix-hex-envs] erro: bash é obrigatório para configurar o ambiente do mix/hex\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[configure-mix-hex-envs] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/elixir/configure_mix_hex_envs.sh [opções]

Opções:
  --env-file <arquivo>         Arquivo com exports persistidos.
  --shell-rc <arquivo>         Arquivo rc do shell que vai carregar o env-file.
  --no-shell-rc                Não altera arquivo rc do shell.
  --mix-home <dir>             Diretório do MIX_HOME.
  --hex-home <dir>             Diretório do HEX_HOME.
  --proxy <url>                Proxy HTTP/HTTPS/ALL para Mix/Hex.
  --ca-cert <arquivo>          Caminho do certificado CA corporativo.
  --hex-mirror <url>           Mirror do Hex para pacotes e builds.
  --hex-api-url <url>          API URL do Hex.
  --hex-builds-url <url>       Mirror específico para builds/rebar.
  --http-concurrency <n>       HEX_HTTP_CONCURRENCY. Padrão: 1
  --http-timeout <seg>         HEX_HTTP_TIMEOUT. Padrão: 120
  --unsafe-https               Desativa verificação TLS do Hex.
  --bootstrap                  Tenta configurar hex e rebar após gravar o env-file.
  -h, --help                   Mostra esta ajuda.

Padrões:
  --env-file: $HOME/.config/mix-hex-envs.sh
  --shell-rc: detectado a partir de $SHELL (.zshrc ou .bashrc)
  --mix-home: $HOME/.mix
  --hex-home: $HOME/.hex
USAGE
}

ENV_FILE="${HOME}/.config/mix-hex-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC="1"
MIX_HOME_DIR="${HOME}/.mix"
HEX_HOME_DIR="${HOME}/.hex"
PROXY_URL=""
CA_CERT_PATH=""
HEX_MIRROR_URL=""
HEX_API_URL_VALUE=""
HEX_BUILDS_URL_VALUE=""
HEX_HTTP_CONCURRENCY_VALUE="${HEX_HTTP_CONCURRENCY:-1}"
HEX_HTTP_TIMEOUT_VALUE="${HEX_HTTP_TIMEOUT:-120}"
HEX_UNSAFE_HTTPS_VALUE="0"
RUN_BOOTSTRAP="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --shell-rc)
      SHELL_RC="${2:-}"
      shift 2
      ;;
    --no-shell-rc)
      APPLY_SHELL_RC="0"
      shift
      ;;
    --mix-home)
      MIX_HOME_DIR="${2:-}"
      shift 2
      ;;
    --hex-home)
      HEX_HOME_DIR="${2:-}"
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
    --hex-mirror)
      HEX_MIRROR_URL="${2:-}"
      shift 2
      ;;
    --hex-api-url)
      HEX_API_URL_VALUE="${2:-}"
      shift 2
      ;;
    --hex-builds-url)
      HEX_BUILDS_URL_VALUE="${2:-}"
      shift 2
      ;;
    --http-concurrency)
      HEX_HTTP_CONCURRENCY_VALUE="${2:-}"
      shift 2
      ;;
    --http-timeout)
      HEX_HTTP_TIMEOUT_VALUE="${2:-}"
      shift 2
      ;;
    --unsafe-https)
      HEX_UNSAFE_HTTPS_VALUE="1"
      shift
      ;;
    --bootstrap)
      RUN_BOOTSTRAP="1"
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
[[ -n "${MIX_HOME_DIR}" ]] || die "--mix-home não pode ser vazio"
[[ -n "${HEX_HOME_DIR}" ]] || die "--hex-home não pode ser vazio"

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
  if [[ -n "${PROXY_URL}" ]]; then
    cat <<EOF
export HTTPS_PROXY=$(shell_quote "${PROXY_URL}")
export HTTP_PROXY=$(shell_quote "${PROXY_URL}")
export ALL_PROXY=$(shell_quote "${PROXY_URL}")
EOF
  fi

  if [[ -n "${CA_CERT_PATH}" ]]; then
    printf 'export HEX_CACERTS_PATH=%s\n' "$(shell_quote "${CA_CERT_PATH}")"
  fi

  if [[ -n "${HEX_MIRROR_URL}" ]]; then
    printf 'export HEX_MIRROR=%s\n' "$(shell_quote "${HEX_MIRROR_URL}")"
  fi

  if [[ -n "${HEX_API_URL_VALUE}" ]]; then
    printf 'export HEX_API_URL=%s\n' "$(shell_quote "${HEX_API_URL_VALUE}")"
  fi

  if [[ -n "${HEX_BUILDS_URL_VALUE}" ]]; then
    printf 'export HEX_BUILDS_URL=%s\n' "$(shell_quote "${HEX_BUILDS_URL_VALUE}")"
  fi

  if [[ "${HEX_UNSAFE_HTTPS_VALUE}" == "1" ]]; then
    printf 'export HEX_UNSAFE_HTTPS=%s\n' "$(shell_quote "1")"
    printf 'export HEX_UNSAFE_REGISTRY=%s\n' "$(shell_quote "1")"
    printf 'export HEX_NO_VERIFY_REPO_ORIGIN=%s\n' "$(shell_quote "1")"
  fi
}

write_env_file() {
  local env_dir
  env_dir="$(dirname "${ENV_FILE}")"
  mkdir -p "${env_dir}" "${MIX_HOME_DIR}" "${HEX_HOME_DIR}"

  {
    cat <<EOF
#!/usr/bin/env sh
# Gerado por scripts/ec2/elixir/configure_mix_hex_envs.sh

export MIX_XDG=1
export MIX_HOME=$(shell_quote "${MIX_HOME_DIR}")
export HEX_HOME=$(shell_quote "${HEX_HOME_DIR}")
export HEX_HTTP_CONCURRENCY=$(shell_quote "${HEX_HTTP_CONCURRENCY_VALUE}")
export HEX_HTTP_TIMEOUT=$(shell_quote "${HEX_HTTP_TIMEOUT_VALUE}")
EOF
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
    printf '# mix/hex para ambiente restrito\n'
    printf '%s\n' "${source_line}"
  } >> "${rc_file}"
}

bootstrap_mix_tooling() {
  command -v mix >/dev/null 2>&1 || die "mix não encontrado no PATH"

  (
    set -e
    . "${ENV_FILE}"

    if ! mix local.hex --force --if-missing; then
      mix archive.install github hexpm/hex branch latest --force
    fi

    mix local.rebar --force --if-missing
  )
}

write_env_file

if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
  if [[ -z "${SHELL_RC}" ]]; then
    SHELL_RC="$(detect_shell_rc)"
  fi

  ensure_source_line "${SHELL_RC}"
fi

if [[ "${RUN_BOOTSTRAP}" == "1" ]]; then
  bootstrap_mix_tooling
fi

cat <<EOF
Configuração concluída.

Arquivo de ambiente:
  ${ENV_FILE}

MIX_HOME:
  ${MIX_HOME_DIR}

HEX_HOME:
  ${HEX_HOME_DIR}
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
