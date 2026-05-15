#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DRY_RUN=0
ONLY_PLUGIN=""
NVIM_DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/nvim"
LOCK_FILE="${XDG_CONFIG_HOME:-${HOME}/.config}/nvim/lazy-lock.json"
CURL_BIN="${CURL_BIN:-${SCRIPT_DIR}/wrappers/curl_python_wrapper.sh}"
GIT_BIN="${GIT_BIN:-git}"
LOCK_INDEX_FILE=""
LOCK_INDEX_READY=0
META_FILE_NAME=".lazyvim-archive-meta"
TMP_PATHS=""

log() {
  printf '[lazyvim-archives] %s\n' "$*" >&2
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
    rm -rf "${tmp_path}" 2>/dev/null || true
  done
}

trap cleanup EXIT HUP INT TERM

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/install_lazyvim_archives.sh [opcoes]

Opcoes:
  --data-dir <dir>       Diretorio data do Neovim. Default: ${XDG_DATA_HOME:-$HOME/.local/share}/nvim
  --lock-file <arquivo>  Caminho do lazy-lock.json. Default: ${XDG_CONFIG_HOME:-$HOME/.config}/nvim/lazy-lock.json
  --only <plugin>        Instala/atualiza apenas um plugin pelo nome do diretorio.
  --list                 Lista os plugins gerenciados e sai.
  --dry-run              Mostra as acoes sem baixar nem alterar arquivos.
  -h, --help             Mostra esta ajuda.

Comportamento:
  - Nao usa git clone.
  - Usa lazy-lock.json quando disponivel para baixar commits fixos.
  - Sem lazy-lock, tenta resolver o SHA remoto da branch com git ls-remote.
  - Evita reinstalar plugins ja atualizados (instala apenas o que falta ou mudou).
  - Substitui diretorios existentes de forma atomica com backup temporario.
  - Deve ser executado manualmente quando quiser atualizar os plugins.
USAGE
}

plugins_manifest() {
  cat <<'EOF'
LazyVim|LazyVim/LazyVim|main
SchemaStore.nvim|b0o/SchemaStore.nvim|main
blink.cmp|saghen/blink.cmp|main
bufferline.nvim|akinsho/bufferline.nvim|main
catppuccin|catppuccin/nvim|main
codex.nvim|kkrampis/codex.nvim|main
conform.nvim|stevearc/conform.nvim|master
copilot.lua|zbirenbaum/copilot.lua|master
crates.nvim|Saecki/crates.nvim|main
dial.nvim|monaqa/dial.nvim|master
friendly-snippets|rafamadriz/friendly-snippets|main
flash.nvim|folke/flash.nvim|main
fzf-lua|ibhagwan/fzf-lua|main
git.nvim|dinhhuy258/git.nvim|main
gitsigns.nvim|lewis6991/gitsigns.nvim|main
grug-far.nvim|MagicDuck/grug-far.nvim|main
inc-rename.nvim|smjonas/inc-rename.nvim|main
incline.nvim|b0o/incline.nvim|main
lazy.nvim|folke/lazy.nvim|main
lazydev.nvim|folke/lazydev.nvim|main
lspsaga.nvim|glepnir/lspsaga.nvim|main
lualine.nvim|nvim-lualine/lualine.nvim|master
luarocks.nvim|vhyrro/luarocks.nvim|main
markdown-preview.nvim|iamcco/markdown-preview.nvim|master
mason-lspconfig.nvim|mason-org/mason-lspconfig.nvim|main
mason-nvim-dap.nvim|jay-babu/mason-nvim-dap.nvim|main
mason.nvim|mason-org/mason.nvim|main
mini.ai|nvim-mini/mini.ai|main
mini.animate|nvim-mini/mini.animate|main
mini.bracketed|nvim-mini/mini.bracketed|main
mini.hipatterns|nvim-mini/mini.hipatterns|main
mini.icons|nvim-mini/mini.icons|main
mini.pairs|nvim-mini/mini.pairs|main
neogen|danymat/neogen|main
noice.nvim|folke/noice.nvim|main
nui.nvim|MunifTanjim/nui.nvim|main
nvim-dap|mfussenegger/nvim-dap|master
nvim-dap-go|leoluz/nvim-dap-go|main
nvim-dap-python|mfussenegger/nvim-dap-python|master
nvim-dap-ui|rcarriga/nvim-dap-ui|master
nvim-dap-virtual-text|theHamsta/nvim-dap-virtual-text|master
nvim-jdtls|mfussenegger/nvim-jdtls|master
nvim-lint|mfussenegger/nvim-lint|master
nvim-lspconfig|neovim/nvim-lspconfig|master
nvim-nio|nvim-neotest/nvim-nio|master
nvim-notify|rcarriga/nvim-notify|master
nvim-treesitter|nvim-treesitter/nvim-treesitter|main
nvim-treesitter-textobjects|nvim-treesitter/nvim-treesitter-textobjects|main
nvim-ts-autotag|windwp/nvim-ts-autotag|main
persistence.nvim|folke/persistence.nvim|main
pingu_ai_codding_pair_programming|andersonflima/pingu_ai_codding_pair_programming|main
playground|nvim-treesitter/playground|master
plenary.nvim|nvim-lua/plenary.nvim|master
render-markdown.nvim|MeanderingProgrammer/render-markdown.nvim|main
rest.nvim|rest-nvim/rest.nvim|main
rustaceanvim|mrcjkb/rustaceanvim|main
snacks.nvim|folke/snacks.nvim|main
solarized-osaka.nvim|craftzdog/solarized-osaka.nvim|main
symbols-outline.nvim|simrat39/symbols-outline.nvim|master
telescope-file-browser.nvim|nvim-telescope/telescope-file-browser.nvim|master
telescope-fzf-native.nvim|nvim-telescope/telescope-fzf-native.nvim|main
telescope.nvim|nvim-telescope/telescope.nvim|master
todo-comments.nvim|folke/todo-comments.nvim|main
toggleterm.nvim|akinsho/toggleterm.nvim|main
tokyonight.nvim|folke/tokyonight.nvim|main
trouble.nvim|folke/trouble.nvim|main
ts-comments.nvim|folke/ts-comments.nvim|main
venv-selector.nvim|linux-cultist/venv-selector.nvim|main
which-key.nvim|folke/which-key.nvim|main
zen-mode.nvim|folke/zen-mode.nvim|main
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --data-dir)
      NVIM_DATA_DIR="${2:-}"
      shift 2
      ;;
    --lock-file)
      LOCK_FILE="${2:-}"
      shift 2
      ;;
    --only)
      ONLY_PLUGIN="${2:-}"
      shift 2
      ;;
    --list)
      plugins_manifest
      exit 0
      ;;
    --dry-run)
      DRY_RUN=1
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

[ -n "${NVIM_DATA_DIR}" ] || die "--data-dir nao pode ser vazio"
[ -n "${LOCK_FILE}" ] || die "--lock-file nao pode ser vazio"

download() {
  url="$1"
  output="$2"

  if [ -x "${CURL_BIN}" ]; then
    "${CURL_BIN}" -fL --retry 3 --connect-timeout 20 "${url}" -o "${output}"
    return
  fi

  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --connect-timeout 20 "${url}" -o "${output}"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -O "${output}" "${url}"
    return
  fi

  die "curl ou wget e obrigatorio"
}

is_commit_sha() {
  value="$1"
  printf '%s' "${value}" | grep -Eq '^[0-9a-f]{40}$'
}

resolve_archive_url() {
  repo="$1"
  ref="$2"
  if is_commit_sha "${ref}"; then
    printf 'https://github.com/%s/archive/%s.tar.gz\n' "${repo}" "${ref}"
    return
  fi
  printf 'https://codeload.github.com/%s/tar.gz/refs/heads/%s\n' "${repo}" "${ref}"
}

resolve_branch_head_sha() {
  repo="$1"
  branch="$2"

  if ! command -v "${GIT_BIN}" >/dev/null 2>&1; then
    return 1
  fi

  "${GIT_BIN}" ls-remote --heads "https://github.com/${repo}.git" "${branch}" 2>/dev/null | awk 'NR==1 {print $1}'
}

repo_default_plugin_name() {
  repo="$1"
  printf '%s\n' "${repo##*/}"
}

build_lock_index() {
  if [ ! -f "${LOCK_FILE}" ]; then
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    log "aviso: python3 nao encontrado; lazy-lock.json sera ignorado (fallback: remote-head/branch)"
    return 0
  fi

  LOCK_INDEX_FILE="$(mktemp)"
  remember_tmp_path "${LOCK_INDEX_FILE}"

  if ! python3 - "${LOCK_FILE}" > "${LOCK_INDEX_FILE}" <<'PY'
import json
import sys

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("conteudo nao e objeto JSON")
except Exception as exc:
    print(f"lock-parse-error: {exc}", file=sys.stderr)
    sys.exit(1)

for plugin_name, meta in data.items():
    if not isinstance(meta, dict):
        continue
    branch = str(meta.get("branch", "")).strip()
    commit = str(meta.get("commit", "")).strip()
    if not commit:
        continue
    print(f"{plugin_name}|{branch}|{commit}")
PY
  then
    log "aviso: falha ao analisar lock file ${LOCK_FILE}; fallback para remote-head/branch"
    rm -f "${LOCK_INDEX_FILE}" 2>/dev/null || true
    LOCK_INDEX_FILE=""
    LOCK_INDEX_READY=0
    return 0
  fi

  if [ ! -s "${LOCK_INDEX_FILE}" ]; then
    log "aviso: lock file sem entradas validas; fallback para remote-head/branch"
    LOCK_INDEX_READY=0
    return 0
  fi

  LOCK_INDEX_READY=1
  log "lock file habilitado: ${LOCK_FILE}"
}

lookup_lock_by_name() {
  plugin_name="$1"
  if [ "${LOCK_INDEX_READY}" != "1" ] || [ ! -s "${LOCK_INDEX_FILE}" ]; then
    return 1
  fi

  awk -F'|' -v plugin="${plugin_name}" '$1 == plugin { print $2 "|" $3; found=1; exit } END { if (!found) exit 1 }' "${LOCK_INDEX_FILE}"
}

lookup_lock_plugin() {
  plugin_name="$1"
  repo="$2"

  lock_info="$(lookup_lock_by_name "${plugin_name}" || true)"
  if [ -n "${lock_info}" ]; then
    printf '%s\n' "${lock_info}"
    return 0
  fi

  repo_plugin_name="$(repo_default_plugin_name "${repo}")"
  if [ -n "${repo_plugin_name}" ] && [ "${repo_plugin_name}" != "${plugin_name}" ]; then
    lock_info="$(lookup_lock_by_name "${repo_plugin_name}" || true)"
    if [ -n "${lock_info}" ]; then
      log "aviso: usando lock alias ${repo_plugin_name} para ${plugin_name}"
      printf '%s\n' "${lock_info}"
      return 0
    fi
  fi

  return 1
}

plugin_sentinel_relative_path() {
  plugin_name="$1"
  case "${plugin_name}" in
    flash.nvim)
      printf '%s\n' "lua/flash/init.lua"
      ;;
    *)
      printf '%s\n' ""
      ;;
  esac
}

read_metadata_value() {
  key="$1"
  file="$2"
  sed -n "s/^${key}=//p" "${file}" | sed -n '1p'
}

install_plugin() {
  plugin_name="$1"
  repo="$2"
  branch="$3"
  install_root="${NVIM_DATA_DIR%/}/lazy"
  target_dir="${install_root}/${plugin_name}"
  meta_file="${target_dir}/${META_FILE_NAME}"
  desired_ref="${branch}"
  resolved_branch="${branch}"
  ref_source="manifest-branch"

  if [ -n "${ONLY_PLUGIN}" ] && [ "${ONLY_PLUGIN}" != "${plugin_name}" ]; then
    return 0
  fi

  lock_info="$(lookup_lock_plugin "${plugin_name}" "${repo}" || true)"
  if [ -n "${lock_info}" ]; then
    lock_branch="$(printf '%s' "${lock_info}" | cut -d'|' -f1)"
    lock_ref="$(printf '%s' "${lock_info}" | cut -d'|' -f2)"
    [ -n "${lock_branch}" ] || lock_branch="${branch}"

    if is_commit_sha "${lock_ref}"; then
      resolved_branch="${lock_branch}"
      desired_ref="${lock_ref}"
      ref_source="lazy-lock"
    else
      log "aviso: lock entry invalida para ${plugin_name} (commit='${lock_ref}'); fallback para remote-head/branch"
    fi
  fi

  if [ "${ref_source}" != "lazy-lock" ]; then
    remote_sha="$(resolve_branch_head_sha "${repo}" "${branch}" || true)"
    if [ -n "${remote_sha}" ]; then
      desired_ref="${remote_sha}"
      ref_source="remote-head"
    fi
  fi

  archive_url="$(resolve_archive_url "${repo}" "${desired_ref}")"

  sentinel_rel="$(plugin_sentinel_relative_path "${plugin_name}")"

  if [ ! -d "${target_dir}" ]; then
    action="install"
    reason="diretorio ausente"
  elif [ -n "${sentinel_rel}" ] && [ ! -f "${target_dir}/${sentinel_rel}" ]; then
    action="update"
    reason="integridade ausente (${sentinel_rel})"
  else
    installed_repo=""
    installed_branch=""
    installed_ref=""
    installed_source=""

    if [ -f "${meta_file}" ]; then
      installed_repo="$(read_metadata_value "repo" "${meta_file}")"
      installed_branch="$(read_metadata_value "branch" "${meta_file}")"
      installed_ref="$(read_metadata_value "ref" "${meta_file}")"
      installed_source="$(read_metadata_value "source" "${meta_file}")"
    fi

    if [ "${installed_repo}" = "${repo}" ] && [ "${installed_branch}" = "${resolved_branch}" ] && [ "${installed_ref}" = "${desired_ref}" ]; then
      log "mantendo ${plugin_name} (${installed_source:-sem-origem}) - ja esta em ${desired_ref}"
      return 0
    fi

    if [ -f "${meta_file}" ]; then
      action="update"
      reason="metadados divergentes"
    else
      action="update"
      reason="metadados ausentes"
    fi
  fi

  log "${action} ${plugin_name} de ${repo}#${resolved_branch} (ref=${desired_ref}, origem=${ref_source}, motivo=${reason})"

  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] %s: download %s -> %s\n' "${action}" "${archive_url}" "${target_dir}" >&2
    return 0
  fi

  tmp_dir="$(mktemp -d)"
  remember_tmp_path "${tmp_dir}"
  archive_path="${tmp_dir}/${plugin_name}.tar.gz"
  extract_dir="${tmp_dir}/extract"
  new_dir="${tmp_dir}/new"
  timestamp="$(date +%Y%m%d_%H%M%S)"
  backup_dir="${target_dir}.archive-backup.${timestamp}"

  mkdir -p "${extract_dir}" "${install_root}"
  download "${archive_url}" "${archive_path}"
  tar -xzf "${archive_path}" -C "${extract_dir}"

  source_dir="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | sed -n '1p')"
  [ -n "${source_dir}" ] || die "archive sem diretorio raiz para ${plugin_name}"

  mv "${source_dir}" "${new_dir}"
  cat > "${new_dir}/${META_FILE_NAME}" <<EOF
repo=${repo}
branch=${resolved_branch}
ref=${desired_ref}
source=${ref_source}
EOF

  if [ -e "${target_dir}" ]; then
    mv "${target_dir}" "${backup_dir}"
  fi

  if ! mv "${new_dir}" "${target_dir}"; then
    if [ -d "${backup_dir}" ] && [ ! -e "${target_dir}" ]; then
      mv "${backup_dir}" "${target_dir}"
    fi
    die "falha ao publicar ${plugin_name}"
  fi

  rm -rf "${backup_dir}" "${tmp_dir}"
}

command -v tar >/dev/null 2>&1 || die "tar e obrigatorio"
build_lock_index

manifest_file="$(mktemp)"
remember_tmp_path "${manifest_file}"
plugins_manifest > "${manifest_file}"

matched=0
while IFS='|' read -r plugin_name repo branch; do
  [ -n "${plugin_name}" ] && [ -n "${repo}" ] && [ -n "${branch}" ] || continue
  if [ -z "${ONLY_PLUGIN}" ] || [ "${ONLY_PLUGIN}" = "${plugin_name}" ]; then
    matched=1
  fi
  install_plugin "${plugin_name}" "${repo}" "${branch}"
done < "${manifest_file}"

if [ -n "${ONLY_PLUGIN}" ] && [ "${matched}" != "1" ]; then
  die "plugin nao encontrado no manifesto: ${ONLY_PLUGIN}"
fi

log "plugins LazyVim processados em ${NVIM_DATA_DIR%/}/lazy"
