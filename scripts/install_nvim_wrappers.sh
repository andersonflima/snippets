#!/usr/bin/env sh
set -eu

TARGET_DIR="${HOME}/.local/share/nvim/wrappers/bin"
FORCE=0

log() {
  printf '[nvim-wrappers] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/install_nvim_wrappers.sh [opcoes]

Opcoes:
  --target-dir <dir>  Diretorio onde os shims git/curl serao instalados.
                      Default: $HOME/.local/share/nvim/wrappers/bin
  --force             Sobrescreve shims existentes.
  -h, --help          Mostra esta ajuda.

Resultado:
  - instala shims "git" e "curl" para wrappers locais do repositorio
  - imprime o export de PATH para aplicar na maquina do servico
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target-dir)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --force)
      FORCE=1
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

[ -n "${TARGET_DIR}" ] || die "--target-dir nao pode ser vazio"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WRAPPER_DIR="${REPO_ROOT}/scripts/wrappers"

[ -x "${WRAPPER_DIR}/git_zip_clone_wrapper.sh" ] || die "wrapper git ausente em ${WRAPPER_DIR}"
[ -x "${WRAPPER_DIR}/curl_python_wrapper.sh" ] || die "wrapper curl ausente em ${WRAPPER_DIR}"

REAL_GIT_BIN="$(command -v git 2>/dev/null || true)"
REAL_CURL_BIN="$(command -v curl 2>/dev/null || true)"
[ -n "${REAL_GIT_BIN}" ] || die "git real nao encontrado no PATH"
[ -n "${REAL_CURL_BIN}" ] || die "curl real nao encontrado no PATH"

mkdir -p "${TARGET_DIR}"

if [ -e "${TARGET_DIR}/git" ] && [ "${FORCE}" != "1" ]; then
  die "shim git ja existe em ${TARGET_DIR}/git (use --force para sobrescrever)"
fi

if [ -e "${TARGET_DIR}/curl" ] && [ "${FORCE}" != "1" ]; then
  die "shim curl ja existe em ${TARGET_DIR}/curl (use --force para sobrescrever)"
fi

# Escrever o shim final de forma segura com escaped dollars.
cat > "${TARGET_DIR}/git" <<EOF2
#!/usr/bin/env sh
set -eu

export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export GIT_ZIP_WRAPPER_CLONE_ORDER="\${GIT_ZIP_WRAPPER_CLONE_ORDER:-local-first}"
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT="\${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK="\${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"
export GIT_ZIP_WRAPPER_STRICT="\${GIT_ZIP_WRAPPER_STRICT:-0}"
exec "${WRAPPER_DIR}/git_zip_clone_wrapper.sh" "\$@"
EOF2

cat > "${TARGET_DIR}/curl" <<EOF2
#!/usr/bin/env sh
set -eu

export CURL_WRAPPER_REAL_CURL="${REAL_CURL_BIN}"
exec "${WRAPPER_DIR}/curl_python_wrapper.sh" "\$@"
EOF2

chmod +x "${TARGET_DIR}/git" "${TARGET_DIR}/curl"

log "wrappers instalados em ${TARGET_DIR}"
log "adicione no ambiente da maquina de servico:"
printf 'export PATH="%s:$PATH"\n' "${TARGET_DIR}"
