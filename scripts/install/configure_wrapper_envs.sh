#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[configure-wrapper-envs] erro: bash é obrigatório para configurar o ambiente\n' >&2
  exit 1
}

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
COMMON_HELPER="${SCRIPT_DIR}/lib/common.sh"

# shellcheck disable=SC1090
. "${COMMON_HELPER}"

RESTRICTED_SCRIPT_NAME="configure-wrapper-envs"

log() {
  restricted_log "$@"
}

die() {
  restricted_die "$@"
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
  --brew-install-dir <dir>     Legado. Ignorado; usado apenas para limpar PATH antigo.
  --real-curl <path>           Caminho do curl real.
  --real-wget <path>           Caminho do wget real.
  --real-git <path>            Caminho do git real.
  --real-brew <path>           Legado. Ignorado; o wrapper de brew foi removido.
  --mason-seed-dir <dir>       Diretório com artefatos seed do Mason.
  --proxy <url>                Define proxy para wrappers e env padrão.
  --ca-cert <arquivo>          Define CA customizada para o wrapper de git.
  --auto-insecure-on-cert-error
                               Ativa retry inseguro no wrapper de curl.
  -h, --help                   Mostra esta ajuda.
USAGE
}

ENV_FILE="${HOME}/.config/wrapper-envs.sh"
SHELL_RC=""
APPLY_SHELL_RC="0"
WRAPPER_SHIM_DIR="${HOME}/.local/bin"
CURL_INSTALL_DIR="${HOME}/.local/share/curl-python-wrapper/bin"
GIT_INSTALL_DIR="${HOME}/.local/share/git-zip-wrapper/bin"
BREW_INSTALL_DIR="${HOME}/.local/share/homebrew-install-wrapper/bin"
REAL_CURL_BIN="${CURL_WRAPPER_REAL_CURL:-}"
REAL_WGET_BIN="${WGET_WRAPPER_REAL_WGET:-}"
REAL_GIT_BIN="${GIT_ZIP_WRAPPER_REAL_GIT:-}"
GIT_LFS_MODE="${GIT_ZIP_WRAPPER_LFS_MODE:-local}"
PROXY_URL=""
CA_CERT_PATH=""
AUTO_INSECURE_ON_CERT_ERROR="0"
MASON_SEED_DIR="${CURL_WRAPPER_MASON_SEED_DIR:-}"

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
    --brew-install-dir)
      BREW_INSTALL_DIR="${2:-}"
      shift 2
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
    --mason-seed-dir)
      MASON_SEED_DIR="${2:-}"
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
[[ -n "${BREW_INSTALL_DIR}" ]] || die "--brew-install-dir não pode ser vazio"

if [[ -z "${REAL_CURL_BIN}" ]]; then
  RESTRICTED_CURL_INSTALL_DIR="${CURL_INSTALL_DIR}"
  RESTRICTED_GIT_INSTALL_DIR="${GIT_INSTALL_DIR}"
  RESTRICTED_WRAPPER_SHIM_DIR="${WRAPPER_SHIM_DIR}"
  REAL_CURL_BIN="$(restricted_resolve_real_binary curl || true)"
fi
if [[ -z "${REAL_WGET_BIN}" ]]; then
  RESTRICTED_CURL_INSTALL_DIR="${CURL_INSTALL_DIR}"
  RESTRICTED_GIT_INSTALL_DIR="${GIT_INSTALL_DIR}"
  RESTRICTED_WRAPPER_SHIM_DIR="${WRAPPER_SHIM_DIR}"
  REAL_WGET_BIN="$(restricted_resolve_real_binary wget || true)"
fi
if [[ -z "${REAL_GIT_BIN}" ]]; then
  RESTRICTED_CURL_INSTALL_DIR="${CURL_INSTALL_DIR}"
  RESTRICTED_GIT_INSTALL_DIR="${GIT_INSTALL_DIR}"
  RESTRICTED_WRAPPER_SHIM_DIR="${WRAPPER_SHIM_DIR}"
  REAL_GIT_BIN="$(restricted_resolve_real_binary git || true)"
fi

[[ -n "${REAL_CURL_BIN}" ]] || die "não foi possível localizar curl no PATH"
[[ -x "${REAL_CURL_BIN}" ]] || die "curl inválido/não executável: ${REAL_CURL_BIN}"
[[ -n "${REAL_GIT_BIN}" ]] || die "não foi possível localizar git no PATH"
[[ -x "${REAL_GIT_BIN}" ]] || die "git inválido/não executável: ${REAL_GIT_BIN}"
RESTRICTED_CURL_INSTALL_DIR="${CURL_INSTALL_DIR}"
RESTRICTED_GIT_INSTALL_DIR="${GIT_INSTALL_DIR}"
RESTRICTED_WRAPPER_SHIM_DIR="${WRAPPER_SHIM_DIR}"
restricted_is_wrapper_binary_path curl "${REAL_CURL_BIN}" && die "curl real não pode apontar para o wrapper instalado: ${REAL_CURL_BIN}"
restricted_is_wrapper_binary_path git "${REAL_GIT_BIN}" && die "git real não pode apontar para o wrapper instalado: ${REAL_GIT_BIN}"
if [[ -n "${REAL_WGET_BIN}" && ! -x "${REAL_WGET_BIN}" ]]; then
  die "wget inválido/não executável: ${REAL_WGET_BIN}"
fi
if [[ -n "${REAL_WGET_BIN}" ]] && restricted_is_wrapper_binary_path wget "${REAL_WGET_BIN}"; then
  die "wget real não pode apontar para o wrapper instalado: ${REAL_WGET_BIN}"
fi
if [[ -n "${CA_CERT_PATH}" && ! -f "${CA_CERT_PATH}" ]]; then
  die "CA customizada não encontrada: ${CA_CERT_PATH}"
fi
case "${AUTO_INSECURE_ON_CERT_ERROR}" in
  0|1)
    ;;
  *)
    die "CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR inválido: ${AUTO_INSECURE_ON_CERT_ERROR}"
    ;;
esac
case "$(printf '%s' "${GIT_LFS_MODE}" | tr '[:upper:]' '[:lower:]')" in
  ""|local)
    GIT_LFS_MODE="local"
    ;;
  *)
    log "GIT_ZIP_WRAPPER_LFS_MODE=${GIT_LFS_MODE} ignorado; forçando local"
    GIT_LFS_MODE="local"
    ;;
esac

detect_shell_rc() {
  restricted_default_shell_rc
}

shell_quote() {
  printf '%q' "$1"
}

render_path_prefix() {
  local -a entries=()
  local joined

  entries+=("${WRAPPER_SHIM_DIR}" "${CURL_INSTALL_DIR}" "${GIT_INSTALL_DIR}")

  joined="$(IFS=:; printf '%s' "${entries[*]}")"
  printf '%s\n' "${joined}"
}

render_local_exports() {
  printf 'export GIT_ZIP_WRAPPER_CLONE_ORDER=%s\n' "$(shell_quote "local-first")"
  printf 'export GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS=%s\n' "$(shell_quote "1")"
  printf 'export GIT_ZIP_WRAPPER_STRICT=%s\n' "$(shell_quote "0")"
  printf 'export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=%s\n' "$(shell_quote "zip")"
  printf 'export GIT_ZIP_WRAPPER_USE_JS_ENGINE=%s\n' "$(shell_quote "1")"
  printf 'export GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK=%s\n' "$(shell_quote "1")"
  printf 'export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=%s\n' "$(shell_quote "0")"
  printf 'export GIT_ZIP_WRAPPER_LFS_MODE=%s\n' "$(shell_quote "${GIT_LFS_MODE}")"
  printf 'export CURL_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR=%s\n' "$(shell_quote "1")"
  printf 'export GIT_ZIP_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR=%s\n' "$(shell_quote "1")"
  printf 'export WGET_WRAPPER_RETRY_WITHOUT_PROXY_ON_AUTH_ERROR=%s\n' "$(shell_quote "1")"

  if [[ -n "${PROXY_URL}" ]]; then
    cat <<EOF2
export HTTPS_PROXY=$(shell_quote "${PROXY_URL}")
export HTTP_PROXY=$(shell_quote "${PROXY_URL}")
export ALL_PROXY=$(shell_quote "${PROXY_URL}")
export CURL_WRAPPER_PROXY=$(shell_quote "${PROXY_URL}")
export WGET_WRAPPER_PROXY=$(shell_quote "${PROXY_URL}")
export GIT_ZIP_WRAPPER_PROXY=$(shell_quote "${PROXY_URL}")
EOF2
  else
    cat <<'EOF2'
unset CURL_WRAPPER_PROXY
unset WGET_WRAPPER_PROXY
unset GIT_ZIP_WRAPPER_PROXY
EOF2
  fi

  if [[ -n "${CA_CERT_PATH}" ]]; then
    printf 'export GIT_ZIP_WRAPPER_CURL_CACERT=%s\n' "$(shell_quote "${CA_CERT_PATH}")"
  else
    printf 'unset GIT_ZIP_WRAPPER_CURL_CACERT\n'
  fi

  if [[ "${AUTO_INSECURE_ON_CERT_ERROR}" == "1" ]]; then
    printf 'export CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR=%s\n' "$(shell_quote "1")"
  else
    printf 'unset CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR\n'
  fi

  if [[ -n "${MASON_SEED_DIR}" ]]; then
    printf 'export CURL_WRAPPER_MASON_SEED_DIR=%s\n' "$(shell_quote "${MASON_SEED_DIR}")"
  else
    printf 'unset CURL_WRAPPER_MASON_SEED_DIR\n'
  fi
}

write_env_file() {
  local env_dir
  env_dir="$(dirname "${ENV_FILE}")"
  mkdir -p "${env_dir}"

  {
    cat <<EOF2
#!/usr/bin/env sh
# Gerado por scripts/install/configure_wrapper_envs.sh

export CURL_WRAPPER_REAL_CURL=$(shell_quote "${REAL_CURL_BIN}")
export WGET_WRAPPER_REAL_WGET=$(shell_quote "${REAL_WGET_BIN}")
export GIT_ZIP_WRAPPER_REAL_GIT=$(shell_quote "${REAL_GIT_BIN}")
__wrapper_env_curl_bin=$(shell_quote "${REAL_CURL_BIN}")
if [ -x $(shell_quote "${CURL_INSTALL_DIR}/curl") ]; then
  __wrapper_env_curl_bin=$(shell_quote "${CURL_INSTALL_DIR}/curl")
fi
export CURL="\${__wrapper_env_curl_bin}"

if [ -n $(shell_quote "${REAL_WGET_BIN}") ]; then
  __wrapper_env_wget_bin=$(shell_quote "${REAL_WGET_BIN}")
  if [ -x $(shell_quote "${CURL_INSTALL_DIR}/wget") ]; then
    __wrapper_env_wget_bin=$(shell_quote "${CURL_INSTALL_DIR}/wget")
  fi
  export WGET="\${__wrapper_env_wget_bin}"
else
  unset WGET
fi

__wrapper_env_git_bin=$(shell_quote "${REAL_GIT_BIN}")
if [ -x $(shell_quote "${GIT_INSTALL_DIR}/git") ]; then
  __wrapper_env_git_bin=$(shell_quote "${GIT_INSTALL_DIR}/git")
fi
export GIT="\${__wrapper_env_git_bin}"

export BREW_WRAPPER_ENABLED="0"
unset BREW_WRAPPER_REAL_BREW
unset BREW_WRAPPER_CURL_BIN
unset BREW_WRAPPER_GIT_BIN
unset BREW_WRAPPER_NO_AUTO_UPDATE
unset BREW

export CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES="1"
export CURL_WRAPPER_RELEASE_FALLBACK_REPOS="mason-org/mason-registry,elixir-lsp/elixir-ls,johnnymorganz/stylua,luals/lua-language-server,omnisharp/omnisharp-roslyn"
export CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK="1"
export CURL_WRAPPER_RELEASE_CACHE_DIR=$(shell_quote "${HOME}/.cache/curl-python-wrapper/releases")
export CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS="omnisharp/omnisharp-roslyn"
export CURL_WRAPPER_MASON_BUILDERS="elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"
export CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS="tar.gz,tgz,tar"
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT="zip"
export GIT_ZIP_WRAPPER_USE_JS_ENGINE="1"
export GIT_ZIP_WRAPPER_CLONE_ORDER="local-first"
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK="0"
export GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK="1"
EOF2
    cat <<EOF2
__wrapper_env_original_path="\${PATH:-}"
__wrapper_env_sanitized_path=""
__wrapper_env_old_ifs="\${IFS}"
IFS=':'
for __wrapper_env_entry in \${__wrapper_env_original_path}; do
  case "\${__wrapper_env_entry}" in
    ""|$(shell_quote "${BREW_INSTALL_DIR}")|$(shell_quote "${CURL_INSTALL_DIR}")|$(shell_quote "${GIT_INSTALL_DIR}")|$(shell_quote "${WRAPPER_SHIM_DIR}")|$(shell_quote "${HOME}/.local/share/mix-"*"-wrapper/bin")|$(shell_quote "${HOME}/.local/share/nvim-"*"-wrapper/bin"))
      continue
      ;;
  esac

  if [ -z "\${__wrapper_env_sanitized_path}" ]; then
    __wrapper_env_sanitized_path="\${__wrapper_env_entry}"
  else
    __wrapper_env_sanitized_path="\${__wrapper_env_sanitized_path}:\${__wrapper_env_entry}"
  fi
done
IFS="\${__wrapper_env_old_ifs}"

if [ -n "\${__wrapper_env_sanitized_path}" ]; then
  export PATH=$(shell_quote "$(render_path_prefix)"):"\${__wrapper_env_sanitized_path}"
else
  export PATH=$(shell_quote "$(render_path_prefix)")
fi

unset __wrapper_env_entry
unset __wrapper_env_old_ifs
unset __wrapper_env_original_path
unset __wrapper_env_sanitized_path
unset __wrapper_env_curl_bin
unset __wrapper_env_wget_bin
unset __wrapper_env_git_bin
EOF2
    render_local_exports
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
    printf '# wrappers locais de curl/git para ambiente restrito\n'
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

cat <<EOF2
Configuração concluída.

Arquivo de ambiente:
  ${ENV_FILE}

Wrapper dirs:
  brew: desabilitado
  curl: ${CURL_INSTALL_DIR}
  git:  ${GIT_INSTALL_DIR}

Modo:
  local-only

Binários reais:
  curl: ${REAL_CURL_BIN}
  wget: ${REAL_WGET_BIN:-não encontrado}
  git:  ${REAL_GIT_BIN}
EOF2

if [[ "${APPLY_SHELL_RC}" == "1" ]]; then
  cat <<EOF2

Arquivo rc atualizado:
  ${SHELL_RC}

Para aplicar na sessão atual:
  . ${ENV_FILE}
EOF2
else
  cat <<EOF2

Nenhum arquivo rc foi alterado.

Para aplicar manualmente:
  . ${ENV_FILE}
EOF2
fi
