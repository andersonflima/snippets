#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[build-mason-seed-artifact] erro: bash é obrigatório para gerar o artefato seed do Mason\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[build-mason-seed-artifact] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/build_mason_seed_artifact.sh --release-url <url> [--seed-dir <dir>]

Exemplo:
  sh scripts/install/build_mason_seed_artifact.sh \
    --release-url https://github.com/elixir-lsp/elixir-ls/releases/download/v0.30.0/elixir-ls-v0.30.0.zip \
    --seed-dir "$HOME/.cache/mason-seeds"

Saída:
  <seed-dir>/<owner>/<repo>/<tag>/<asset>
USAGE
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"
RELEASE_URL=""
SEED_DIR="${HOME}/.cache/mason-seeds"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-url)
      RELEASE_URL="${2:-}"
      shift 2
      ;;
    --seed-dir)
      SEED_DIR="${2:-}"
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

[[ -n "${RELEASE_URL}" ]] || die "--release-url é obrigatório"
[[ "${RELEASE_URL}" =~ ^https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)$ ]] || die "release-url inválida: ${RELEASE_URL}"

OWNER="${BASH_REMATCH[1]}"
REPO="${BASH_REMATCH[2]}"
TAG="${BASH_REMATCH[3]}"
ASSET="${BASH_REMATCH[4]}"
SLUG="$(printf '%s/%s' "${OWNER}" "${REPO}" | tr '[:upper:]' '[:lower:]')"
OUTPUT_PATH="${SEED_DIR%/}/${SLUG}/${TAG}/${ASSET}"

mkdir -p "$(dirname "${OUTPUT_PATH}")"

CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES=1 \
CURL_WRAPPER_RELEASE_FALLBACK_REPOS="${CURL_WRAPPER_RELEASE_FALLBACK_REPOS:-elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn}" \
CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS="${CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS:-elixir-lsp/elixir-ls,omnisharp/omnisharp-roslyn}" \
CURL_WRAPPER_MASON_BUILDERS="${CURL_WRAPPER_MASON_BUILDERS:-elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish}" \
"${REPO_ROOT}/wrappers/curl_python_wrapper.sh" \
  -fLo "${OUTPUT_PATH}" \
  --create-dirs \
  "${RELEASE_URL}"

cat <<EOF
Artefato seed gerado com sucesso.

Release URL:
  ${RELEASE_URL}

Seed path:
  ${OUTPUT_PATH}

No host restrito, exporte:
  export CURL_WRAPPER_MASON_SEED_DIR="${SEED_DIR}"
EOF
