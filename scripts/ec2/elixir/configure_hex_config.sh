#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-hex-config] erro: bash é obrigatório para configurar o Hex\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[configure-hex-config] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/elixir/configure_hex_config.sh [opções]

Opções:
  --proxy <url>                Define http_proxy e https_proxy no Hex.
  --ca-cert <arquivo>          Define cacerts_path no Hex.
  --unsafe-https               Define unsafe_https, unsafe_registry e no_verify_repo_origin.
  --http-concurrency <n>       Define http_concurrency no Hex. Padrão: 1
  --http-timeout <seg>         Define http_timeout no Hex. Padrão: 120
  --api-url <url>              Define api_url no Hex.
  --mirror-url <url>           Define mirror_url no Hex.
  --test-package <nome>        Executa mix hex.info <nome> ao final. Padrão: phx_new
  --no-test                    Não executa teste ao final.
  -h, --help                   Mostra esta ajuda.
USAGE
}

PROXY_URL=""
CA_CERT_PATH=""
UNSAFE_HTTPS="0"
HTTP_CONCURRENCY_VALUE="1"
HTTP_TIMEOUT_VALUE="120"
API_URL_VALUE=""
MIRROR_URL_VALUE=""
TEST_PACKAGE="phx_new"
RUN_TEST="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --proxy)
      PROXY_URL="${2:-}"
      shift 2
      ;;
    --ca-cert)
      CA_CERT_PATH="${2:-}"
      shift 2
      ;;
    --unsafe-https)
      UNSAFE_HTTPS="1"
      shift
      ;;
    --http-concurrency)
      HTTP_CONCURRENCY_VALUE="${2:-}"
      shift 2
      ;;
    --http-timeout)
      HTTP_TIMEOUT_VALUE="${2:-}"
      shift 2
      ;;
    --api-url)
      API_URL_VALUE="${2:-}"
      shift 2
      ;;
    --mirror-url)
      MIRROR_URL_VALUE="${2:-}"
      shift 2
      ;;
    --test-package)
      TEST_PACKAGE="${2:-}"
      shift 2
      ;;
    --no-test)
      RUN_TEST="0"
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

command -v mix >/dev/null 2>&1 || die "mix não encontrado no PATH"

apply_mix_runtime_env() {
  export HEX_HTTP_CONCURRENCY="${HTTP_CONCURRENCY_VALUE}"
  export HEX_HTTP_TIMEOUT="${HTTP_TIMEOUT_VALUE}"

  if [[ -n "${PROXY_URL}" ]]; then
    export HTTPS_PROXY="${PROXY_URL}"
    export HTTP_PROXY="${PROXY_URL}"
    export ALL_PROXY="${PROXY_URL}"
    export https_proxy="${PROXY_URL}"
    export http_proxy="${PROXY_URL}"
    export all_proxy="${PROXY_URL}"
  fi

  if [[ -n "${CA_CERT_PATH}" ]]; then
    export HEX_CACERTS_PATH="${CA_CERT_PATH}"
  fi

  if [[ -n "${API_URL_VALUE}" ]]; then
    export HEX_API_URL="${API_URL_VALUE}"
  fi

  if [[ -n "${MIRROR_URL_VALUE}" ]]; then
    export HEX_MIRROR="${MIRROR_URL_VALUE}"
  fi

  if [[ "${UNSAFE_HTTPS}" == "1" ]]; then
    export HEX_UNSAFE_HTTPS="1"
    export HEX_UNSAFE_REGISTRY="1"
    export HEX_NO_VERIFY_REPO_ORIGIN="1"
  fi
}

ensure_hex_installed() {
  apply_mix_runtime_env

  if mix local.hex --force --if-missing; then
    return 0
  fi

  mix archive.install github hexpm/hex branch latest --force
}

run_mix_hex_config() {
  local key value
  key="$1"
  value="$2"
  log "hex.config ${key}"
  mix hex.config "${key}" "${value}"
}

ensure_hex_installed

run_mix_hex_config "http_concurrency" "${HTTP_CONCURRENCY_VALUE}"
run_mix_hex_config "http_timeout" "${HTTP_TIMEOUT_VALUE}"

if [[ -n "${PROXY_URL}" ]]; then
  run_mix_hex_config "http_proxy" "${PROXY_URL}"
  run_mix_hex_config "https_proxy" "${PROXY_URL}"
fi

if [[ -n "${CA_CERT_PATH}" ]]; then
  run_mix_hex_config "cacerts_path" "${CA_CERT_PATH}"
fi

if [[ -n "${API_URL_VALUE}" ]]; then
  run_mix_hex_config "api_url" "${API_URL_VALUE}"
fi

if [[ -n "${MIRROR_URL_VALUE}" ]]; then
  run_mix_hex_config "mirror_url" "${MIRROR_URL_VALUE}"
fi

if [[ "${UNSAFE_HTTPS}" == "1" ]]; then
  run_mix_hex_config "unsafe_https" "true"
  run_mix_hex_config "unsafe_registry" "true"
  run_mix_hex_config "no_verify_repo_origin" "true"
fi

cat <<EOF
Configuração do Hex aplicada.

Verificação atual:
EOF
mix hex.config

if [[ "${RUN_TEST}" == "1" ]]; then
  printf '\nTeste:\n'
  mix hex.info "${TEST_PACKAGE}"
fi
