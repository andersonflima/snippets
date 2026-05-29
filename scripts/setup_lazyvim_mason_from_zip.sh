#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[setup-lazyvim-mason] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash scripts/setup_lazyvim_mason_from_zip.sh [opcoes]

Opcoes:
  --force                Sobrescreve instalacao atual (com backup automatico).
  --skip-plugins         Instala somente configuracao do LazyVim e registry do Mason.
  --config-dir <dir>     Default: ${XDG_CONFIG_HOME:-$HOME/.config}/nvim
  --data-dir <dir>       Default: ${XDG_DATA_HOME:-$HOME/.local/share}/nvim
  --cache-dir <dir>      Default: ${XDG_CACHE_HOME:-$HOME/.cache}/nvim
  --github-base <url>    Default: https://github.com
  -h, --help             Mostra ajuda.

Observacoes:
- Nao usa curl/wget.
- Download feito por codigo Python em venv dedicado.
- Usa ZIP no formato /archive/refs/heads/<branch>.zip.
USAGE
}

FORCE=0
SKIP_PLUGINS=0
NVIM_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nvim"
NVIM_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/nvim"
NVIM_CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/nvim"
GITHUB_BASE_URL="https://github.com"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    --skip-plugins)
      SKIP_PLUGINS=1
      shift
      ;;
    --config-dir)
      NVIM_CONFIG_DIR="${2:-}"
      shift 2
      ;;
    --data-dir)
      NVIM_DATA_DIR="${2:-}"
      shift 2
      ;;
    --cache-dir)
      NVIM_CACHE_DIR="${2:-}"
      shift 2
      ;;
    --github-base)
      GITHUB_BASE_URL="${2:-}"
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

command -v python3 >/dev/null 2>&1 || die "python3 nao encontrado"

[ -n "$NVIM_CONFIG_DIR" ] || die "--config-dir vazio"
[ -n "$NVIM_DATA_DIR" ] || die "--data-dir vazio"
[ -n "$NVIM_CACHE_DIR" ] || die "--cache-dir vazio"

STATE_ROOT="${HOME}/.local/share/nvim-zip-bootstrap"
STATE_FILE="${STATE_ROOT}/state.env"
VENV_DIR="${STATE_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
WRAPPER_ROOT="${HOME}/.local/share/nvim/wrappers"
WRAPPER_BIN_DIR="${WRAPPER_ROOT}/bin"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="${STATE_ROOT}/backup_${TIMESTAMP}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$STATE_ROOT"

backup_if_exists() {
  local target_path="$1"
  local backup_name="$2"
  if [ -e "$target_path" ] || [ -L "$target_path" ]; then
    mkdir -p "$BACKUP_DIR"
    mv "$target_path" "$BACKUP_DIR/$backup_name"
    log "backup: $target_path -> $BACKUP_DIR/$backup_name"
  fi
}

ensure_absent_or_force() {
  local target_path="$1"
  if [ -e "$target_path" ] || [ -L "$target_path" ]; then
    if [ "$FORCE" != "1" ]; then
      die "caminho ja existe: $target_path (use --force)"
    fi
  fi
}

ensure_python_runtime() {
  if [ ! -x "$VENV_PYTHON" ]; then
    log "criando venv em $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi
}

append_wrapper_path_to_rc() {
  local rc_file="$1"
  local marker_begin="# >>> nvim-wrappers PATH >>>"
  local marker_end="# <<< nvim-wrappers PATH <<<"
  local export_line="export PATH=\"${WRAPPER_BIN_DIR}:\$PATH\""
  [ -f "$rc_file" ] || touch "$rc_file"
  if grep -Fq "$marker_begin" "$rc_file"; then
    return 0
  fi
  {
    printf '\n%s\n' "$marker_begin"
    printf '%s\n' "$export_line"
    printf '%s\n' "$marker_end"
  } >> "$rc_file"
}

install_http_wrappers() {
  mkdir -p "$WRAPPER_BIN_DIR"

  cat > "${WRAPPER_BIN_DIR}/http_fetch.py" <<'PY'
#!/usr/bin/env python3
import os
import ssl
import sys
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

RETRYABLE = {429, 500, 502, 503, 504}

def die(msg: str, code: int = 2):
    print(msg, file=sys.stderr)
    raise SystemExit(code)

def parse_args(argv):
    mode = os.path.basename(argv[0])
    follow = False
    fail = False
    silent = False
    show_error = False
    output = None
    tries = 5
    timeout = 180
    headers = []
    method = "GET"
    user_agent = ""
    data = None
    url = None
    wants_version = False
    wants_help = False
    i = 1
    while i < len(argv):
        a = argv[i]
        if a in ("-L", "--location"):
            follow = True
        elif a in ("-f", "--fail"):
            fail = True
        elif a in ("-s", "--silent", "-q"):
            silent = True
        elif a in ("-S", "--show-error"):
            show_error = True
        elif a.startswith("-") and not a.startswith("--") and len(a) > 2:
            # suporta flags curtas combinadas como -fsSL
            combined_ok = {"f", "s", "S", "L"}
            unknown = [flag for flag in a[1:] if flag not in combined_ok]
            if unknown:
                die(f"{mode}: unsupported option {a}")
            fail = fail or ("f" in a)
            silent = silent or ("s" in a)
            show_error = show_error or ("S" in a)
            follow = follow or ("L" in a)
        elif a in ("-o", "--output", "-O"):
            if mode == "wget":
                if a == "-o":
                    i += 1
                    if i >= len(argv):
                        die(f"{mode}: option {a} requires an argument")
                    # wget: -o define arquivo de log; ignorado no wrapper
                    _ = argv[i]
                else:
                    i += 1
                    if i >= len(argv):
                        die(f"{mode}: option {a} requires an argument")
                    output = argv[i]
            elif a == "-O":
                output = "__remote__"
            else:
                i += 1
                if i >= len(argv):
                    die(f"{mode}: option {a} requires an argument")
                output = argv[i]
        elif a in ("--retry", "--tries"):
            i += 1
            tries = int(argv[i])
        elif a.startswith("--retry="):
            tries = int(a.split("=", 1)[1])
        elif a.startswith("--tries="):
            tries = int(a.split("=", 1)[1])
        elif a in ("--max-time", "--timeout"):
            i += 1
            timeout = int(argv[i])
        elif a in ("-T",):
            i += 1
            timeout = int(argv[i])
        elif a in ("--connect-timeout",):
            i += 1
            timeout = int(argv[i])
        elif a.startswith("--max-time="):
            timeout = int(a.split("=", 1)[1])
        elif a.startswith("--timeout="):
            timeout = int(a.split("=", 1)[1])
        elif a.startswith("--connect-timeout="):
            timeout = int(a.split("=", 1)[1])
        elif a in ("-H", "--header"):
            i += 1
            headers.append(argv[i])
        elif a.startswith("--header="):
            headers.append(a.split("=", 1)[1])
        elif a in ("-X", "--request", "--method"):
            i += 1
            method = (argv[i] or "GET").upper()
        elif a.startswith("--request="):
            method = (a.split("=", 1)[1] or "GET").upper()
        elif a.startswith("--method="):
            method = (a.split("=", 1)[1] or "GET").upper()
        elif a in ("-A", "--user-agent"):
            i += 1
            user_agent = argv[i]
        elif a.startswith("--user-agent="):
            user_agent = a.split("=", 1)[1]
        elif a in ("-d", "--data", "--data-raw", "--data-binary", "--post-data"):
            i += 1
            if i >= len(argv):
                die(f"{mode}: option {a} requires an argument")
            data = argv[i]
            if method == "GET":
                method = "POST"
        elif a.startswith("--data=") or a.startswith("--data-raw=") or a.startswith("--data-binary="):
            data = a.split("=", 1)[1]
            if method == "GET":
                method = "POST"
        elif a in ("--version", "-V"):
            wants_version = True
        elif a in ("--help", "-h"):
            wants_help = True
        elif a in ("--compressed", "--progress-bar", "-#", "-nv", "--no-verbose"):
            pass
        elif a in ("-w", "--write-out", "--proxy", "--noproxy"):
            i += 1
            # opcao ignorada por compatibilidade
            _ = argv[i]
        elif a.startswith("--write-out=") or a.startswith("--proxy=") or a.startswith("--noproxy="):
            pass
        elif a.startswith("-"):
            die(f"{mode}: unsupported option {a}")
        else:
            url = a
        i += 1
    if wants_version:
        return {"meta_only": "version", "mode": mode}
    if wants_help:
        return {"meta_only": "help", "mode": mode}
    if not url:
        die(f"{mode}: missing URL")
    if output == "__remote__":
        output = os.path.basename(urlparse(url).path) or "download.bin"
    return {
        "mode": mode,
        "follow": follow,
        "fail": fail,
        "silent": silent,
        "show_error": show_error,
        "output": output,
        "tries": max(1, tries),
        "timeout": max(1, timeout),
        "headers": headers,
        "method": method,
        "user_agent": user_agent,
        "data": data,
        "url": url,
    }

def do_fetch(cfg):
    if cfg.get("meta_only") == "version":
        sys.stdout.write(f"{cfg.get('mode', 'http-fetch')} wrapper 1.0\n")
        return 0
    if cfg.get("meta_only") == "help":
        sys.stdout.write(f"{cfg.get('mode', 'http-fetch')} wrapper (curl/wget compatible subset)\n")
        return 0

    req_headers = {"User-Agent": "nvim-http-wrapper/1.0"}
    if cfg["user_agent"]:
        req_headers["User-Agent"] = cfg["user_agent"]
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token and "github.com" in cfg["url"]:
        req_headers["Authorization"] = f"Bearer {token}"
    for header in cfg["headers"]:
        if ":" in header:
            k, v = header.split(":", 1)
            req_headers[k.strip()] = v.strip()
    context = ssl._create_unverified_context()
    last = None
    for attempt in range(1, cfg["tries"] + 1):
        payload = None
        if cfg.get("data"):
            if cfg["data"] == "@-":
                payload = sys.stdin.buffer.read()
            else:
                payload = cfg["data"].encode("utf-8")
        req = Request(cfg["url"], headers=req_headers, method=cfg["method"], data=payload)
        try:
            with urlopen(req, timeout=cfg["timeout"], context=context) as resp:
                status = getattr(resp, "status", 200)
                if cfg["fail"] and (status < 200 or status >= 300):
                    raise HTTPError(cfg["url"], status, f"status {status}", resp.headers, None)
                data = resp.read()
                if cfg["output"]:
                    with open(cfg["output"], "wb") as f:
                        f.write(data)
                else:
                    sys.stdout.buffer.write(data)
                return 0
        except HTTPError as e:
            last = e
            if e.code not in RETRYABLE:
                break
        except (URLError, TimeoutError, socket.timeout, ConnectionError) as e:
            last = e
        if attempt == cfg["tries"]:
            break
    if not cfg["silent"] or cfg["show_error"]:
        die(f"{cfg['mode']}: request failed: {last}", 22)
    return 22

if __name__ == "__main__":
    cfg = parse_args(sys.argv)
    raise SystemExit(do_fetch(cfg))
PY
  chmod +x "${WRAPPER_BIN_DIR}/http_fetch.py"

  cat > "${WRAPPER_BIN_DIR}/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/http_fetch.py" "$@"
SH
  chmod +x "${WRAPPER_BIN_DIR}/curl"

  cat > "${WRAPPER_BIN_DIR}/wget" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/http_fetch.py" "$@"
SH
  chmod +x "${WRAPPER_BIN_DIR}/wget"

  cat > "${WRAPPER_BIN_DIR}/git" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

wrapper_dir="$(cd "$(dirname "$0")" && pwd)"
real_git=""
while IFS= read -r candidate; do
  candidate_dir="$(cd "$(dirname "$candidate")" && pwd)"
  if [ "$candidate_dir" != "$wrapper_dir" ]; then
    real_git="$candidate"
    break
  fi
done < <(command -v -a git)

if [ -z "$real_git" ]; then
  echo "git wrapper: real git binary not found" >&2
  exit 127
fi

rewrite_github_url() {
  local value="$1"
  if [[ "$value" == https://github.com/* ]]; then
    local path="${value#https://github.com/}"
    printf 'git@github.com:%s' "$path"
    return 0
  fi
  printf '%s' "$value"
}

args=()
for arg in "$@"; do
  args+=("$(rewrite_github_url "$arg")")
done

exec "$real_git" "${args[@]}"
SH
  chmod +x "${WRAPPER_BIN_DIR}/git"

  append_wrapper_path_to_rc "${HOME}/.zshrc"
  append_wrapper_path_to_rc "${HOME}/.bashrc"
  append_wrapper_path_to_rc "${HOME}/.profile"
}

download_and_extract_branch_zip() {
  local repo="$1"
  local branch="$2"
  local destination_dir="$3"
  local temp_zip="$TMP_DIR/${repo##*/}_${branch}.zip"
  local extract_root="$TMP_DIR/extract_${repo##*/}_${branch}"
  local url="${GITHUB_BASE_URL}/${repo}/archive/refs/heads/${branch}.zip"

  mkdir -p "$extract_root"

  "$VENV_PYTHON" "$(dirname "$0")/github_zip_download_extract.py" \
    "$url" "$temp_zip" "$extract_root" "$destination_dir"
}

write_mason_local_registry_override() {
  local plugin_dir="${NVIM_CONFIG_DIR}/lua/plugins"
  local target_file="${plugin_dir}/mason_local_registry.lua"
  mkdir -p "$plugin_dir"
  cat > "$target_file" <<'LUA'
return {
  {
    "mason-org/mason.nvim",
    opts = function(_, opts)
      opts = opts or {}
      local registry_path = os.getenv("MASON_REGISTRY_DIR")
      if not registry_path or registry_path == "" then
        registry_path = vim.fn.stdpath("cache") .. "/mason-registry-main"
      end
      opts.registries = { "file:" .. registry_path }
      return opts
    end,
  },
}
LUA
}

write_http_wrapper_path_override() {
  local plugin_dir="${NVIM_CONFIG_DIR}/lua/plugins"
  local target_file="${plugin_dir}/http_wrappers_path.lua"
  mkdir -p "$plugin_dir"
  cat > "$target_file" <<LUA
return {
  {
    "folke/lazy.nvim",
    init = function()
      local wrapper_bin = "${WRAPPER_BIN_DIR}"
      if vim.fn.isdirectory(wrapper_bin) == 1 then
        local current_path = vim.env.PATH or ""
        if not string.find(current_path, wrapper_bin, 1, true) then
          vim.env.PATH = wrapper_bin .. ":" .. current_path
        end
      end
    end,
  end
}
LUA
}

install_plugins_from_manifest() {
  local lazy_dir="${NVIM_DATA_DIR}/lazy"
  mkdir -p "$lazy_dir"

  while IFS='|' read -r plugin_name repo branch; do
    [ -n "$plugin_name" ] || continue
    [ -n "$repo" ] || continue
    [ -n "$branch" ] || branch="main"
    log "plugin: $plugin_name (${repo}@${branch})"
    download_and_extract_branch_zip "$repo" "$branch" "${lazy_dir}/${plugin_name}"
    printf '%s\n' "${repo}@${branch}" > "${lazy_dir}/${plugin_name}/.zip-source"
  done <<'MANIFEST'
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
MANIFEST
}

ensure_absent_or_force "$NVIM_CONFIG_DIR"
ensure_absent_or_force "${NVIM_DATA_DIR}/lazy"
ensure_absent_or_force "${NVIM_CACHE_DIR}/mason-registry-main"
ensure_python_runtime
install_http_wrappers

if [ "$FORCE" = "1" ]; then
  backup_if_exists "$NVIM_CONFIG_DIR" "nvim-config"
  backup_if_exists "${NVIM_DATA_DIR}/lazy" "nvim-lazy"
  backup_if_exists "${NVIM_CACHE_DIR}/mason-registry-main" "mason-registry-main"
fi

log "instalando LazyVim starter"
download_and_extract_branch_zip "LazyVim/starter" "main" "$NVIM_CONFIG_DIR"

log "instalando registry local do Mason"
download_and_extract_branch_zip "mason-org/mason-registry" "main" "${NVIM_CACHE_DIR}/mason-registry-main"

log "configurando Mason para usar registry local"
write_mason_local_registry_override
log "configurando PATH interno de wrappers HTTP no Neovim"
write_http_wrapper_path_override

if [ "$SKIP_PLUGINS" != "1" ]; then
  log "instalando plugins do LazyVim por ZIP"
  install_plugins_from_manifest
fi

cat > "$STATE_FILE" <<EOFSTATE
NVIM_CONFIG_DIR='${NVIM_CONFIG_DIR}'
NVIM_DATA_DIR='${NVIM_DATA_DIR}'
NVIM_CACHE_DIR='${NVIM_CACHE_DIR}'
BACKUP_DIR='${BACKUP_DIR}'
SETUP_TIMESTAMP='${TIMESTAMP}'
EOFSTATE

log "concluido"
log "proximo passo: abrir o neovim e rodar :checkhealth e :Mason"
