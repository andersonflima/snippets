#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[list-github-repos-by-prefix] erro: bash e obrigatorio\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[list-github-repos-by-prefix] %s\n' "$*" >&2
}

progress() {
  if [[ "${SHOW_PROGRESS}" == "1" ]]; then
    log "$*"
  fi
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/list_github_repos_by_prefix.sh --owner <owner> --prefix <prefixo> [opcoes]

Opcoes:
  --owner <owner>         Organizacao ou usuario no GitHub.
  --prefix <prefixo>      Prefixo usado para filtrar nome dos repositorios.
  --owner-type <tipo>     auto|org|user. Padrao: auto.
  --with-branches         Inclui branches de cada repositorio.
  --max-api-retries <n>   Tentativas maximas para falhas transitórias da API. Padrao: 6.
  --retry-delay <seg>     Delay inicial entre retries da API. Padrao: 2.
  --quiet                 Nao mostra logs de progresso.
  --output <arquivo>      Salva saida em arquivo.
  -h, --help              Mostra esta ajuda.

Formato de saida:
  Sem --with-branches:
    owner/repo

  Com --with-branches:
    owner/repo<TAB>branch1,branch2,branch3
USAGE
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "comando obrigatorio nao encontrado: $1"
}

gh_api() {
  GH_PROMPT_DISABLED=1 gh api "$@"
}

is_positive_integer() {
  case "${1:-}" in
    ''|*[!0-9]*)
      return 1
      ;;
    0)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

should_retry_gh_error() {
  local status message normalized
  status="$1"
  message="${2:-}"
  normalized="$(printf '%s' "${message}" | tr '[:upper:]' '[:lower:]')"

  if [[ "${status}" -eq 0 ]]; then
    return 1
  fi

  case "${normalized}" in
    *"http 429"*|*"secondary rate limit"*|*"rate limit"*|*"http 500"*|*"http 502"*|*"http 503"*|*"http 504"*|*"502"*|*"503"*|*"504"*|*"bad gateway"*|*"gateway timeout"*|*"timeout"*|*"timed out"*|*"connection reset"*|*"unexpected eof"*|*"eof"*|*"temporarily unavailable"*|*"temporary failure"*|*"try again"*)
      return 0
      ;;
  esac

  return 1
}

gh_api_with_retry() {
  local endpoint attempt max_attempts delay_seconds error_file payload status error_message
  endpoint="$1"
  attempt=1
  max_attempts="${MAX_API_RETRIES}"
  delay_seconds="${RETRY_DELAY_SECONDS}"

  while :; do
    error_file="$(mktemp "/tmp/list-github-repos-by-prefix-gherr.XXXXXX")"

    set +e
    payload="$(gh_api "${endpoint}" 2>"${error_file}")"
    status=$?
    set -e

    error_message="$(tr '\n' ' ' < "${error_file}")"
    rm -f "${error_file}"

    if [[ "${status}" -eq 0 ]]; then
      printf '%s' "${payload}"
      return 0
    fi

    if should_retry_gh_error "${status}" "${error_message}" && [[ "${attempt}" -lt "${max_attempts}" ]]; then
      progress "falha transitória na API (tentativa ${attempt}/${max_attempts}) para ${endpoint}; retry em ${delay_seconds}s"
      sleep "${delay_seconds}"
      attempt=$((attempt + 1))
      delay_seconds=$((delay_seconds * 2))
      continue
    fi

    log "falha ao consultar API: ${endpoint}"
    if [[ -n "${error_message}" ]]; then
      printf '%s\n' "${error_message}" >&2
    fi
    return "${status}"
  done
}

assert_github_auth() {
  local auth_status_output
  if auth_status_output="$(GH_PROMPT_DISABLED=1 gh auth status 2>&1)"; then
    return 0
  fi

  printf '%s\n' "${auth_status_output}" >&2
  die "autenticacao da API do GitHub invalida no gh. SSH cobre git clone/push, mas este script usa API REST."
}

parse_arguments() {
  OWNER=""
  PREFIX=""
  OWNER_TYPE="auto"
  WITH_BRANCHES="0"
  MAX_API_RETRIES="6"
  RETRY_DELAY_SECONDS="2"
  SHOW_PROGRESS="1"
  OUTPUT_FILE=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --owner)
        OWNER="${2:-}"
        shift 2
        ;;
      --prefix)
        PREFIX="${2:-}"
        shift 2
        ;;
      --owner-type)
        OWNER_TYPE="${2:-}"
        shift 2
        ;;
      --with-branches)
        WITH_BRANCHES="1"
        shift
        ;;
      --max-api-retries)
        MAX_API_RETRIES="${2:-}"
        shift 2
        ;;
      --retry-delay)
        RETRY_DELAY_SECONDS="${2:-}"
        shift 2
        ;;
      --quiet)
        SHOW_PROGRESS="0"
        shift
        ;;
      --output)
        OUTPUT_FILE="${2:-}"
        shift 2
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

  [[ -n "${OWNER}" ]] || die "--owner e obrigatorio"
  [[ -n "${PREFIX}" ]] || die "--prefix e obrigatorio"

  case "${OWNER_TYPE}" in
    auto|org|user)
      ;;
    *)
      die "--owner-type invalido: ${OWNER_TYPE}. Use auto, org ou user."
      ;;
  esac

  is_positive_integer "${MAX_API_RETRIES}" || die "--max-api-retries deve ser inteiro > 0"
  is_positive_integer "${RETRY_DELAY_SECONDS}" || die "--retry-delay deve ser inteiro > 0"
}

resolve_authenticated_login() {
  gh_api /user --jq '.login' 2>/dev/null || true
}

resolve_owner_type() {
  local owner requested_type
  owner="$1"
  requested_type="$2"

  if [[ "${requested_type}" != "auto" ]]; then
    printf '%s\n' "${requested_type}"
    return 0
  fi

  if gh_api "/orgs/${owner}" >/dev/null 2>&1; then
    printf 'org\n'
    return 0
  fi

  if gh_api "/users/${owner}" >/dev/null 2>&1; then
    printf 'user\n'
    return 0
  fi

  die "owner nao encontrado no GitHub: ${owner}"
}

repositories_endpoint() {
  local owner owner_type authenticated_login
  owner="$1"
  owner_type="$2"
  authenticated_login="$3"

  if [[ "${owner_type}" == "org" ]]; then
    printf '/orgs/%s/repos?per_page=100&type=all&sort=full_name&direction=asc\n' "${owner}"
    return 0
  fi

  if [[ -n "${authenticated_login}" && "${authenticated_login}" == "${owner}" ]]; then
    printf '/user/repos?per_page=100&type=owner&sort=full_name&direction=asc\n'
    return 0
  fi

  printf '/users/%s/repos?per_page=100&type=owner&sort=full_name&direction=asc\n' "${owner}"
}

cleanup() {
  if [[ -n "${MATCHING_REPOSITORIES_FILE:-}" && -f "${MATCHING_REPOSITORIES_FILE}" ]]; then
    rm -f "${MATCHING_REPOSITORIES_FILE}"
  fi
}

list_repositories_matching_prefix() {
  local owner prefix owner_type authenticated_login endpoint
  local page page_endpoint page_payload page_count
  owner="$1"
  prefix="$2"
  owner_type="$3"
  authenticated_login="$4"
  endpoint="$(repositories_endpoint "${owner}" "${owner_type}" "${authenticated_login}")"
  page=1

  while :; do
    page_endpoint="${endpoint}&page=${page}"
    progress "consultando repositorios via API: ${page_endpoint}"
    page_payload="$(gh_api_with_retry "${page_endpoint}")"
    page_count="$(printf '%s' "${page_payload}" | jq 'length')"
    progress "pagina ${page}: ${page_count} repositorios retornados"

    if [[ "${page_count}" == "0" ]]; then
      break
    fi

    printf '%s\n' "${page_payload}" \
      | jq -r --arg owner "${owner}" --arg prefix "${prefix}" '
        .[]
        | select(.owner.login == $owner)
        | select(.name | startswith($prefix))
        | .name
      '

    if [[ "${page_count}" != "100" ]]; then
      break
    fi

    page=$((page + 1))
  done | sort -u
}

prepare_matching_repositories() {
  MATCHING_REPOSITORIES_FILE="$(mktemp "/tmp/list-github-repos-by-prefix.XXXXXX")"
  list_repositories_matching_prefix "${OWNER}" "${PREFIX}" "${RESOLVED_OWNER_TYPE}" "${AUTHENTICATED_LOGIN}" > "${MATCHING_REPOSITORIES_FILE}"
  MATCHING_REPOSITORIES_COUNT="$(awk 'NF { count += 1 } END { print count + 0 }' "${MATCHING_REPOSITORIES_FILE}")"
  progress "repositorios encontrados com prefixo '${PREFIX}': ${MATCHING_REPOSITORIES_COUNT}"
}

list_repository_branches() {
  local owner repo
  local page page_endpoint page_payload page_count
  owner="$1"
  repo="$2"
  page=1

  while :; do
    page_endpoint="/repos/${owner}/${repo}/branches?per_page=100&page=${page}"
    page_payload="$(gh_api_with_retry "${page_endpoint}")"
    page_count="$(printf '%s' "${page_payload}" | jq 'length')"

    if [[ "${page_count}" == "0" ]]; then
      break
    fi

    printf '%s\n' "${page_payload}" | jq -r '.[] | .name'

    if [[ "${page_count}" != "100" ]]; then
      break
    fi

    page=$((page + 1))
  done | sort -u
}

emit_repositories_only() {
  local repo_name
  while IFS= read -r repo_name; do
    [[ -n "${repo_name}" ]] || continue
    printf '%s/%s\n' "${OWNER}" "${repo_name}"
  done < "${MATCHING_REPOSITORIES_FILE}"
}

emit_repositories_with_branches() {
  local repo_name branches_csv index
  index=0

  while IFS= read -r repo_name; do
    [[ -n "${repo_name}" ]] || continue
    index=$((index + 1))
    progress "coletando branches [${index}/${MATCHING_REPOSITORIES_COUNT}]: ${OWNER}/${repo_name}"
    branches_csv="$(list_repository_branches "${OWNER}" "${repo_name}" | paste -sd ',' -)"
    printf '%s/%s\t%s\n' "${OWNER}" "${repo_name}" "${branches_csv}"
  done < "${MATCHING_REPOSITORIES_FILE}"
}

emit_output() {
  if [[ "${WITH_BRANCHES}" == "1" ]]; then
    emit_repositories_with_branches
    return 0
  fi

  emit_repositories_only
}

main() {
  parse_arguments "$@"
  require_command gh
  require_command jq
  trap cleanup EXIT

  progress "validando autenticacao do gh"
  assert_github_auth

  AUTHENTICATED_LOGIN="$(resolve_authenticated_login)"
  progress "usuario autenticado no gh: ${AUTHENTICATED_LOGIN:-desconhecido}"

  RESOLVED_OWNER_TYPE="$(resolve_owner_type "${OWNER}" "${OWNER_TYPE}")"
  progress "tipo de owner resolvido: ${RESOLVED_OWNER_TYPE}"

  prepare_matching_repositories

  if [[ -n "${OUTPUT_FILE}" ]]; then
    mkdir -p "$(dirname "${OUTPUT_FILE}")"
    progress "gravando resultado em arquivo: ${OUTPUT_FILE}"
    emit_output > "${OUTPUT_FILE}"
    log "arquivo gerado: ${OUTPUT_FILE}"
    exit 0
  fi

  progress "emitindo resultado no stdout"
  emit_output
}

main "$@"
