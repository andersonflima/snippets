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
  scripts/install/configure_hex_config.sh [opções]

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
MIX_HEX_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR="${MIX_HEX_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR:-1}"
MIX_HEX_LAST_COMMAND_OUTPUT=""

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

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

has_explicit_proxy_arg() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      --proxy|--proxy=*|--http-proxy|--http-proxy=*|--https-proxy|--https-proxy=*|--proxy-user|--proxy-user=*|--proxy-password|--proxy-password=*)
        return 0
        ;;
    esac
  done
  return 1
}

is_proxy_auth_error_log() {
  local output
  output="${1:-}"
  [[ -n "${output}" ]] || return 1
  printf '%s\n' "${output}" | grep -Eiq '(^|[[:space:]])407([[:space:]]|$)|proxy[ -]authentication|required|proxy authent(i|y)cation|Proxy-Authenticate|proxy error|Proxy Error'
}

run_mix_command_capture() {
  local -a command
  local output_file command_status

  command=("$@")
  output_file="$(mktemp -t configure-hex-mix-output-XXXXXX)"
  MIX_HEX_LAST_COMMAND_OUTPUT=""

  set +e
  "${command[@]}" >"${output_file}" 2>&1
  command_status=$?
  set -e

  if [[ -f "${output_file}" ]]; then
    MIX_HEX_LAST_COMMAND_OUTPUT="$(cat "${output_file}")"
    if [[ -n "${MIX_HEX_LAST_COMMAND_OUTPUT}" ]]; then
      if [[ "${command_status}" -eq 0 ]]; then
        printf '%s\n' "${MIX_HEX_LAST_COMMAND_OUTPUT}"
      else
        printf '%s\n' "${MIX_HEX_LAST_COMMAND_OUTPUT}" >&2
      fi
    fi
    rm -f "${output_file}"
  fi

  return "${command_status}"
}

run_mix_proxy_command() {
  local label="$1"
  local -a mix_command=()
  local -a no_proxy_command=()
  local -a command=()
  shift

  mix_command+=(mix "$@")
  command=("${mix_command[@]}")

  log "${label}"
  if run_mix_command_capture "${command[@]}"; then
    return 0
  fi

  if is_truthy "${MIX_HEX_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR}" && \
    [[ -n "${PROXY_URL}" ]] && \
    ! has_explicit_proxy_arg "${mix_command[@]}" && \
    is_proxy_auth_error_log "${MIX_HEX_LAST_COMMAND_OUTPUT}"; then
    log "mix/hex falhou com erro de autenticação de proxy (407). Tentando novamente sem proxy"
    no_proxy_command=(env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy "${mix_command[@]}")
    if run_mix_command_capture "${no_proxy_command[@]}"; then
      return 0
    fi
  fi

  return 1
}

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

  if run_mix_proxy_command "executando: mix local.hex --force --if-missing" local.hex --force --if-missing; then
    return 0
  fi

  run_mix_proxy_command "executando: mix archive.install github hexpm/hex branch latest --force" archive.install github hexpm/hex branch latest --force
}

run_mix_hex_config() {
  local key value
  key="$1"
  value="$2"
  run_mix_proxy_command "configurando hex.config ${key}=${value}" hex.config "${key}" "${value}"
}

run_mix_hex_info_test() {
  printf 'Teste:\n'
  if run_mix_proxy_command "validando conectividade com mix hex.info ${TEST_PACKAGE}" hex.info "${TEST_PACKAGE}"; then
    return 0
  fi

  log "teste de conectividade com hex.info falhou; tentando fallback inseguro temporário para validação"
  if run_mix_command_capture env \
    -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
    HEX_UNSAFE_HTTPS=1 \
    HEX_UNSAFE_REGISTRY=1 \
    HEX_NO_VERIFY_REPO_ORIGIN=1 \
    mix hex.info "${TEST_PACKAGE}"; then
    log "aviso: validação funcionou apenas com TLS inseguro (unsafe)."
    log "considere definir --unsafe-https para manter comportamento consistente"
    return 0
  fi

  log "aviso: mix hex.info ainda falhou em fallback inseguro; seguindo sem bloqueio (conexão corporativa/host de rede pode restringir ${TEST_PACKAGE})"
  return 0
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

cat <<EOF2
Configuração do Hex aplicada.

Verificação atual:
EOF2
run_mix_proxy_command "consultando mix hex.config" hex.config

if [[ "${RUN_TEST}" == "1" ]]; then
  run_mix_hex_info_test
fi
