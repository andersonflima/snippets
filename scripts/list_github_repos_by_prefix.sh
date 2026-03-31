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

assert_github_auth() {
  gh auth status >/dev/null 2>&1 || die "voce precisa autenticar no gh: gh auth login"
}

parse_arguments() {
  OWNER=""
  PREFIX=""
  OWNER_TYPE="auto"
  WITH_BRANCHES="0"
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
}

resolve_authenticated_login() {
  gh api /user --jq '.login' 2>/dev/null || true
}

resolve_owner_type() {
  local owner requested_type
  owner="$1"
  requested_type="$2"

  if [[ "${requested_type}" != "auto" ]]; then
    printf '%s\n' "${requested_type}"
    return 0
  fi

  if gh api "/orgs/${owner}" >/dev/null 2>&1; then
    printf 'org\n'
    return 0
  fi

  if gh api "/users/${owner}" >/dev/null 2>&1; then
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

list_repositories_matching_prefix() {
  local owner prefix owner_type authenticated_login endpoint
  owner="$1"
  prefix="$2"
  owner_type="$3"
  authenticated_login="$4"
  endpoint="$(repositories_endpoint "${owner}" "${owner_type}" "${authenticated_login}")"

  gh api --paginate "${endpoint}" \
    | jq -r --arg owner "${owner}" --arg prefix "${prefix}" '
      .[]
      | select(.owner.login == $owner)
      | select(.name | startswith($prefix))
      | .name
    ' \
    | sort -u
}

list_repository_branches() {
  local owner repo
  owner="$1"
  repo="$2"

  gh api --paginate "/repos/${owner}/${repo}/branches?per_page=100" \
    | jq -r '.[] | .name' \
    | sort -u
}

emit_repositories_only() {
  local repo_name
  list_repositories_matching_prefix "${OWNER}" "${PREFIX}" "${RESOLVED_OWNER_TYPE}" "${AUTHENTICATED_LOGIN}" \
    | while IFS= read -r repo_name; do
      [[ -n "${repo_name}" ]] || continue
      printf '%s/%s\n' "${OWNER}" "${repo_name}"
    done
}

emit_repositories_with_branches() {
  local repo_name branches_csv
  list_repositories_matching_prefix "${OWNER}" "${PREFIX}" "${RESOLVED_OWNER_TYPE}" "${AUTHENTICATED_LOGIN}" \
    | while IFS= read -r repo_name; do
      [[ -n "${repo_name}" ]] || continue
      branches_csv="$(list_repository_branches "${OWNER}" "${repo_name}" | paste -sd ',' -)"
      printf '%s/%s\t%s\n' "${OWNER}" "${repo_name}" "${branches_csv}"
    done
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
  assert_github_auth

  AUTHENTICATED_LOGIN="$(resolve_authenticated_login)"
  RESOLVED_OWNER_TYPE="$(resolve_owner_type "${OWNER}" "${OWNER_TYPE}")"

  if [[ -n "${OUTPUT_FILE}" ]]; then
    mkdir -p "$(dirname "${OUTPUT_FILE}")"
    emit_output > "${OUTPUT_FILE}"
    log "arquivo gerado: ${OUTPUT_FILE}"
    exit 0
  fi

  emit_output
}

main "$@"
