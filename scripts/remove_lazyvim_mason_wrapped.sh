#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DRY_RUN=0
WRAPPER_BIN_DIR="${HOME}/.local/share/nvim/wrappers/bin"
NVIM_DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/nvim"
REGISTRY_DIR="${XDG_CACHE_HOME:-${HOME}/.cache}/nvim/mason-registry-main"
EXTRA_RC_FILE=""
FORCE_MANIFEST_PLUGINS=0
TMP_PATHS=""

log() {
  printf '[lazyvim-mason-remove] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

remember_tmp_path() {
  TMP_PATHS="${TMP_PATHS}${TMP_PATHS:+ }$1"
}

cleanup() {
  for tmp_path in ${TMP_PATHS}; do
    [ -n "${tmp_path}" ] || continue
    rm -f "${tmp_path}" 2>/dev/null || true
  done
}

trap cleanup EXIT HUP INT TERM

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/remove_lazyvim_mason_wrapped.sh [opcoes]

Opcoes:
  --wrapper-bin-dir <dir>  Diretorio dos shims git/curl.
                           Default: $HOME/.local/share/nvim/wrappers/bin
  --data-dir <dir>         Diretorio data do Neovim.
                           Default: ${XDG_DATA_HOME:-$HOME/.local/share}/nvim
  --registry-dir <dir>     Diretorio do mason-registry local.
                           Default: ${XDG_CACHE_HOME:-$HOME/.cache}/nvim/mason-registry-main
  --rc-file <arquivo>      Arquivo shell rc extra para limpar o bloco de PATH dos wrappers.
  --force-manifest-plugins Remove todos os plugins do manifesto do install_lazyvim_archives.sh,
                           mesmo sem .lazyvim-archive-meta.
  --dry-run                Mostra acoes sem alterar arquivos.
  -h, --help               Mostra esta ajuda.

Escopo removido:
  - bloco '# >>> nvim-wrappers PATH >>>' de ~/.zshrc, ~/.bashrc, ~/.profile e --rc-file
  - shims gerenciados em <wrapper-bin-dir>/git e <wrapper-bin-dir>/curl
  - plugins LazyVim instalados por install_lazyvim_archives.sh (por padrao, so diretorios com .lazyvim-archive-meta)
  - mason-registry local em --registry-dir
  - estado/caches do Mason em data/state/cache do Neovim
USAGE
}

run() {
  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] %s' "$1" >&2
    shift
    while [ "$#" -gt 0 ]; do
      printf ' %s' "$1" >&2
      shift
    done
    printf '\n' >&2
    return 0
  fi

  "$@"
}

remove_file() {
  target="$1"
  [ -e "${target}" ] || [ -L "${target}" ] || return 0
  log "removendo arquivo: ${target}"
  run rm -f "${target}"
}

remove_dir() {
  target="$1"
  [ -d "${target}" ] || return 0
  log "removendo diretorio: ${target}"
  run rm -rf "${target}"
}

remove_path() {
  target="$1"

  [ -e "${target}" ] || [ -L "${target}" ] || return 0

  if [ -d "${target}" ] && [ ! -L "${target}" ]; then
    remove_dir "${target}"
    return 0
  fi

  remove_file "${target}"
}

rmdir_if_empty() {
  target="$1"
  [ -d "${target}" ] || return 0

  has_entries="$(find "${target}" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null || true)"
  [ -z "${has_entries}" ] || return 0

  log "removendo diretorio vazio: ${target}"
  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] rmdir %s\n' "${target}" >&2
    return 0
  fi

  rmdir "${target}" 2>/dev/null || true
  return 0
}

is_managed_wrapper() {
  wrapper_path="$1"
  signature="$2"

  [ -f "${wrapper_path}" ] || return 1
  grep -Fq "${signature}" "${wrapper_path}" 2>/dev/null
}

remove_managed_wrapper() {
  wrapper_name="$1"
  signature="$2"
  wrapper_path="${WRAPPER_BIN_DIR%/}/${wrapper_name}"

  [ -e "${wrapper_path}" ] || [ -L "${wrapper_path}" ] || return 0

  if is_managed_wrapper "${wrapper_path}" "${signature}"; then
    remove_file "${wrapper_path}"
    return 0
  fi

  log "mantendo ${wrapper_path}: nao parece ser shim gerenciado"
}

clean_wrapper_path_block() {
  rc_file="$1"
  marker_begin="# >>> nvim-wrappers PATH >>>"
  marker_end="# <<< nvim-wrappers PATH <<<"

  [ -f "${rc_file}" ] || return 0

  tmp_file="$(mktemp)"
  remember_tmp_path "${tmp_file}"

  awk -v begin="${marker_begin}" -v end="${marker_end}" '
    $0 == begin { in_block=1; next }
    $0 == end { in_block=0; next }
    in_block == 1 { next }
    { print }
  ' "${rc_file}" > "${tmp_file}"

  if cmp -s "${rc_file}" "${tmp_file}"; then
    return 0
  fi

  log "limpando PATH de wrappers em ${rc_file}"
  run mv "${tmp_file}" "${rc_file}"
}

clean_shell_rc_files() {
  clean_wrapper_path_block "${HOME}/.zshrc"
  clean_wrapper_path_block "${HOME}/.bashrc"
  clean_wrapper_path_block "${HOME}/.profile"
  if [ -n "${EXTRA_RC_FILE}" ]; then
    clean_wrapper_path_block "${EXTRA_RC_FILE}"
  fi
}

remove_lazyvim_plugins() {
  lazy_root="${NVIM_DATA_DIR%/}/lazy"
  manifest_script="${SCRIPT_DIR}/install_lazyvim_archives.sh"

  [ -x "${manifest_script}" ] || die "script de manifesto nao encontrado: ${manifest_script}"
  [ -d "${lazy_root}" ] || return 0

  manifest_file="$(mktemp)"
  remember_tmp_path "${manifest_file}"
  "${manifest_script}" --list > "${manifest_file}"

  while IFS='|' read -r plugin_name _rest; do
    [ -n "${plugin_name}" ] || continue

    plugin_dir="${lazy_root}/${plugin_name}"
    meta_file="${plugin_dir}/.lazyvim-archive-meta"

    [ -d "${plugin_dir}" ] || continue

    if [ -f "${meta_file}" ] || [ "${FORCE_MANIFEST_PLUGINS}" = "1" ]; then
      remove_dir "${plugin_dir}"
      continue
    fi

    log "mantendo ${plugin_dir}: sem ${meta_file}"
  done < "${manifest_file}"

  rmdir_if_empty "${lazy_root}"
}

remove_mason_state() {
  nvim_cache_dir="${XDG_CACHE_HOME:-${HOME}/.cache}/nvim"
  nvim_state_dir="${XDG_STATE_HOME:-${HOME}/.local/state}/nvim"

  remove_path "${REGISTRY_DIR}"
  remove_path "${NVIM_DATA_DIR%/}/mason"
  remove_path "${nvim_cache_dir}/mason"
  remove_path "${nvim_state_dir}/mason"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --wrapper-bin-dir)
      WRAPPER_BIN_DIR="${2:-}"
      shift 2
      ;;
    --data-dir)
      NVIM_DATA_DIR="${2:-}"
      shift 2
      ;;
    --registry-dir)
      REGISTRY_DIR="${2:-}"
      shift 2
      ;;
    --rc-file)
      EXTRA_RC_FILE="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --force-manifest-plugins)
      FORCE_MANIFEST_PLUGINS=1
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

[ -n "${WRAPPER_BIN_DIR}" ] || die "--wrapper-bin-dir nao pode ser vazio"
[ -n "${NVIM_DATA_DIR}" ] || die "--data-dir nao pode ser vazio"
[ -n "${REGISTRY_DIR}" ] || die "--registry-dir nao pode ser vazio"

clean_shell_rc_files
remove_managed_wrapper "git" "git_zip_clone_wrapper.sh"
remove_managed_wrapper "curl" "curl_python_wrapper.sh"
rmdir_if_empty "${WRAPPER_BIN_DIR}"
remove_lazyvim_plugins
remove_mason_state

log "limpeza concluida"
