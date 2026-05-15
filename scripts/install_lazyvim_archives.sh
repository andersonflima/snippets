#!/usr/bin/env sh
set -eu

DRY_RUN=0
ONLY_PLUGIN=""
NVIM_DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/nvim"
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
  --only <plugin>        Instala/atualiza apenas um plugin pelo nome do diretorio.
  --list                 Lista os plugins gerenciados e sai.
  --dry-run              Mostra as acoes sem baixar nem alterar arquivos.
  -h, --help             Mostra esta ajuda.

Comportamento:
  - Nao usa git clone.
  - Baixa cada plugin via archive da branch registrada no lazy-lock.json analisado.
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

download() {
  url="$1"
  output="$2"

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

install_plugin() {
  plugin_name="$1"
  repo="$2"
  branch="$3"
  install_root="${NVIM_DATA_DIR%/}/lazy"
  target_dir="${install_root}/${plugin_name}"
  archive_url="https://codeload.github.com/${repo}/tar.gz/refs/heads/${branch}"

  if [ -n "${ONLY_PLUGIN}" ] && [ "${ONLY_PLUGIN}" != "${plugin_name}" ]; then
    return 0
  fi

  log "instalando ${plugin_name} de ${repo}#${branch}"

  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] download %s -> %s\n' "${archive_url}" "${target_dir}" >&2
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
