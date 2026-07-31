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
  bash config/lazyvim/setup_lazyvim_mason_from_zip.sh [opcoes]

Opcoes:
  --force                Sobrescreve instalacao atual (com backup automatico).
  --skip-plugins         Nao instala plugins em ~/.local/share/nvim/lazy.
  --plugins-only         So instala lazy.nvim + plugins do manifesto por ZIP
                         (sem Mason/config/fonte/estado) — use com --data-dir
                         para popular o XDG do toolchain docker.
  --with-homebrew-proxy  Aplica antes o bloco de proxy/Homebrew no shell rc
                         (chama setup_homebrew_proxy.sh --apply).
  --manage-config        Permite alterar ~/.config/nvim (opt-in).
  --install-crowquill    Instala a fonte Crowquill Mono (macOS: ~/Library/Fonts;
                         Linux: ~/.local/share/fonts). O tema Crowquill Ink ja vem
                         versionado em config/nvim e e aplicado com --manage-config.
  --config-source-dir    Copia config nvim de origem para --config-dir (requer --manage-config).
  --config-dir <dir>     Default: ${XDG_CONFIG_HOME:-$HOME/.config}/nvim
  --data-dir <dir>       Default: ${XDG_DATA_HOME:-$HOME/.local/share}/nvim
  --cache-dir <dir>      Default: ${XDG_CACHE_HOME:-$HOME/.cache}/nvim
  --github-base <url>    Default: https://github.com
  --github-transport <modo>
                         Transporte GitHub para git/wrappers/patch da config:
                         `ssh` (default, comportamento atual) ou `http`
                         para ambientes corporativos HTTP-only.
  -h, --help             Mostra ajuda.

Observacoes:
- Nao usa curl/wget.
- Download feito por codigo Python em venv dedicado.
- Usa ZIP no formato /archive/refs/heads/<branch>.zip.
- A branch "default" resolve dinamicamente a branch padrao do repositorio.
USAGE
}

FORCE=0
SKIP_PLUGINS=0
PLUGINS_ONLY=0
WITH_HOMEBREW_PROXY=0
MANAGE_CONFIG=0
INSTALL_CROWQUILL=0
CONFIG_SOURCE_DIR=""
NVIM_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nvim"
NVIM_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/nvim"
NVIM_CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/nvim"
GITHUB_BASE_URL="https://github.com"
GITHUB_TRANSPORT="ssh"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Estrutura do repo: config/lazyvim/<este script>, config/homebrew/, config/nvim/.
CONFIG_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DEFAULT_CONFIG_SOURCE_DIR="${CONFIG_ROOT}/nvim"

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
    --plugins-only)
      PLUGINS_ONLY=1
      shift
      ;;
    --with-homebrew-proxy)
      WITH_HOMEBREW_PROXY=1
      shift
      ;;
    --manage-config)
      MANAGE_CONFIG=1
      shift
      ;;
    --install-crowquill)
      INSTALL_CROWQUILL=1
      shift
      ;;
    --config-source-dir)
      CONFIG_SOURCE_DIR="${2:-}"
      shift 2
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
    --github-transport)
      GITHUB_TRANSPORT="${2:-}"
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
[ "$GITHUB_TRANSPORT" = "ssh" ] || [ "$GITHUB_TRANSPORT" = "http" ] \
  || die "--github-transport deve ser 'ssh' ou 'http'"
if [ -n "$CONFIG_SOURCE_DIR" ] && [ "$MANAGE_CONFIG" != "1" ]; then
  die "--config-source-dir requer --manage-config"
fi

STATE_ROOT="${HOME}/.local/share/nvim-zip-bootstrap"
STATE_FILE="${STATE_ROOT}/state.env"
VENV_DIR="${STATE_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
WRAPPER_ROOT="${HOME}/.local/share/nvim/wrappers"
WRAPPER_BIN_DIR="${WRAPPER_ROOT}/bin"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="${STATE_ROOT}/backup_${TIMESTAMP}_$$"
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

venv_python_works() {
  [ -x "$VENV_PYTHON" ] && "$VENV_PYTHON" -c 'import sys' >/dev/null 2>&1
}

ensure_python_runtime() {
  if venv_python_works; then
    return 0
  fi

  command -v python3 >/dev/null 2>&1 || die "python3 nao encontrado para criar a venv"

  # Uma venv pode existir mas estar quebrada (interpretador removido/atualizado,
  # ex.: 3.13 -> 3.14, deixando bin/python como symlink morto) ou ter sido criada
  # pela metade. `python3 -m venv` sem --clear nao recria o bin/python existente,
  # entao removemos e recriamos do zero para garantir um runtime utilizavel.
  if [ -e "$VENV_DIR" ] || [ -L "$VENV_DIR" ]; then
    log "venv invalida em $VENV_DIR; recriando"
    rm -rf "$VENV_DIR"
  else
    log "criando venv em $VENV_DIR"
  fi

  python3 -m venv "$VENV_DIR" || die "falha ao criar venv em $VENV_DIR (python3 -m venv)"
  venv_python_works || die "venv criada mas $VENV_PYTHON nao esta utilizavel"
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

emit_lazy_plugin_manifest() {
  cat <<'MANIFEST'
LazyVim|LazyVim/LazyVim|default
SchemaStore.nvim|b0o/SchemaStore.nvim|default
LuaSnip|L3MON4D3/LuaSnip|default
blink.cmp|saghen/blink.cmp|default
bufferline.nvim|akinsho/bufferline.nvim|default
catppuccin|catppuccin/nvim|default
claudecode.nvim|coder/claudecode.nvim|default
cmp-buffer|hrsh7th/cmp-buffer|default
cmp-cmdline|hrsh7th/cmp-cmdline|default
cmp-nvim-lsp|hrsh7th/cmp-nvim-lsp|default
cmp-path|hrsh7th/cmp-path|default
codex.nvim|kkrampis/codex.nvim|default
conform.nvim|stevearc/conform.nvim|default
copilot.lua|zbirenbaum/copilot.lua|default
crates.nvim|Saecki/crates.nvim|default
dial.nvim|monaqa/dial.nvim|default
friendly-snippets|rafamadriz/friendly-snippets|default
flash.nvim|folke/flash.nvim|default
fzf-lua|ibhagwan/fzf-lua|default
git.nvim|dinhhuy258/git.nvim|default
gitsigns.nvim|lewis6991/gitsigns.nvim|default
grug-far.nvim|MagicDuck/grug-far.nvim|default
inc-rename.nvim|smjonas/inc-rename.nvim|default
incline.nvim|b0o/incline.nvim|default
lazy.nvim|folke/lazy.nvim|default
lazydev.nvim|folke/lazydev.nvim|default
lspsaga.nvim|glepnir/lspsaga.nvim|default
lualine.nvim|nvim-lualine/lualine.nvim|default
luarocks.nvim|vhyrro/luarocks.nvim|default
markdown-preview.nvim|iamcco/markdown-preview.nvim|default
mason-lspconfig.nvim|mason-org/mason-lspconfig.nvim|default
mason-nvim-dap.nvim|jay-babu/mason-nvim-dap.nvim|default
mason.nvim|mason-org/mason.nvim|default
mini.ai|nvim-mini/mini.ai|default
mini.animate|nvim-mini/mini.animate|default
mini.bracketed|nvim-mini/mini.bracketed|default
mini.hipatterns|nvim-mini/mini.hipatterns|default
mini.icons|nvim-mini/mini.icons|default
mini.pairs|nvim-mini/mini.pairs|default
neogen|danymat/neogen|default
neo-tree.nvim|nvim-neo-tree/neo-tree.nvim|default
crowquill-theme|andersonflima/crowquill-theme|default
noice.nvim|folke/noice.nvim|default
nui.nvim|MunifTanjim/nui.nvim|default
nvim-cmp|hrsh7th/nvim-cmp|default
nvim-dap|mfussenegger/nvim-dap|default
nvim-dap-go|leoluz/nvim-dap-go|default
nvim-dap-python|mfussenegger/nvim-dap-python|default
nvim-dap-ui|rcarriga/nvim-dap-ui|default
nvim-dap-virtual-text|theHamsta/nvim-dap-virtual-text|default
nvim-jdtls|mfussenegger/nvim-jdtls|default
nvim-lint|mfussenegger/nvim-lint|default
nvim-lspconfig|neovim/nvim-lspconfig|default
nvim-nio|nvim-neotest/nvim-nio|default
nvim-notify|rcarriga/nvim-notify|default
nvim-treesitter|nvim-treesitter/nvim-treesitter|default
nvim-treesitter-textobjects|nvim-treesitter/nvim-treesitter-textobjects|default
nvim-ts-autotag|windwp/nvim-ts-autotag|default
nvim-web-devicons|nvim-tree/nvim-web-devicons|default
persistence.nvim|folke/persistence.nvim|default
pingu_ai_codding_pair_programming|andersonflima/pingu_ai_codding_pair_programming|default
playground|nvim-treesitter/playground|default
plenary.nvim|nvim-lua/plenary.nvim|default
render-markdown.nvim|MeanderingProgrammer/render-markdown.nvim|default
rest.nvim|rest-nvim/rest.nvim|default
rustaceanvim|mrcjkb/rustaceanvim|default
snacks.nvim|folke/snacks.nvim|default
solarized-osaka.nvim|craftzdog/solarized-osaka.nvim|default
symbols-outline.nvim|simrat39/symbols-outline.nvim|default
telescope-file-browser.nvim|nvim-telescope/telescope-file-browser.nvim|default
telescope-fzf-native.nvim|nvim-telescope/telescope-fzf-native.nvim|default
telescope.nvim|nvim-telescope/telescope.nvim|default
todo-comments.nvim|folke/todo-comments.nvim|default
toggleterm.nvim|akinsho/toggleterm.nvim|default
tokyonight.nvim|folke/tokyonight.nvim|default
trouble.nvim|folke/trouble.nvim|default
ts-comments.nvim|folke/ts-comments.nvim|default
venv-selector.nvim|linux-cultist/venv-selector.nvim|default
which-key.nvim|folke/which-key.nvim|default
zen-mode.nvim|folke/zen-mode.nvim|default
MANIFEST
}

install_http_wrappers() {
  mkdir -p "$WRAPPER_BIN_DIR"
  emit_lazy_plugin_manifest > "${WRAPPER_ROOT}/lazy-plugins.manifest"

  cat > "${WRAPPER_BIN_DIR}/http_fetch.py" <<'PY'
#!/usr/bin/env python3
import os
import ssl
import sys
import socket
import tempfile
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.parse import urlsplit, urlunsplit
from urllib.request import (
    HTTPPasswordMgrWithDefaultRealm,
    ProxyBasicAuthHandler,
    ProxyHandler,
    Request,
    build_opener,
    HTTPSHandler,
)

RETRYABLE = {429, 500, 502, 503, 504}
PROXY_ENV_KEYS = {
    "http": ("HTTP_PROXY", "http_proxy"),
    "https": ("HTTPS_PROXY", "https_proxy"),
    "all": ("ALL_PROXY", "all_proxy"),
}
PROXY_CREDENTIAL_ENV_KEYS = {
    "http": (
        ("HTTP_PROXY_USERNAME", "http_proxy_username"),
        ("HTTP_PROXY_PASSWORD", "http_proxy_password"),
    ),
    "https": (
        ("HTTPS_PROXY_USERNAME", "https_proxy_username"),
        ("HTTPS_PROXY_PASSWORD", "https_proxy_password"),
    ),
    "all": (
        ("PROXY_USERNAME", "proxy_username"),
        ("PROXY_PASSWORD", "proxy_password"),
    ),
}

# Letras curtas que consomem valor (dentro de combos como -fsSLo /tmp/x).
VALUE_LETTERS = set("oOHAdXTbeuCP")
# Letras curtas sem valor (toggles ou aceitas-e-ignoradas).
TOGGLE_LETTERS = set("fsSLkqg46Ic#")
# Tokens curtos exatos aceitos e ignorados.
SHORT_EXACT_IGNORE = {"-nv"}
# Opcoes longas aceitas e ignoradas (sem valor).
IGNORE_NOARG_LONG = {
    "--insecure", "--no-check-certificate", "--head", "--globoff",
    "--http1.1", "--compressed", "--content-disposition", "--create-dirs",
    "--progress-bar", "--no-verbose", "--continue",
}
# Opcoes longas aceitas e ignoradas (com valor a descartar).
IGNORE_VALUE_LONG = {
    "--continue-at", "--cookie", "--referer", "--execute",
    "--write-out", "--proxy", "--noproxy",
}

def die(msg: str, code: int = 2):
    print(msg, file=sys.stderr)
    raise SystemExit(code)

def first_env(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""

def normalize_proxy_url(raw_url: str) -> str:
    if "://" in raw_url:
        return raw_url
    return f"http://{raw_url}"

def proxy_credentials_for(scheme: str) -> tuple[str, str]:
    username_keys, password_keys = PROXY_CREDENTIAL_ENV_KEYS.get(
        scheme, PROXY_CREDENTIAL_ENV_KEYS["all"]
    )
    username = first_env(*username_keys) or first_env(
        *PROXY_CREDENTIAL_ENV_KEYS["all"][0]
    )
    password = first_env(*password_keys) or first_env(
        *PROXY_CREDENTIAL_ENV_KEYS["all"][1]
    )
    return username, password

def split_proxy_auth(proxy_url: str, scheme: str) -> tuple[str, str, str]:
    normalized = normalize_proxy_url(proxy_url)
    parsed = urlsplit(normalized)
    username = parsed.username or ""
    password = parsed.password or ""
    if not username:
        username, password = proxy_credentials_for(scheme)
    hostname = parsed.hostname or ""
    if not hostname:
        return normalized, "", ""
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    sanitized = urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    )
    return sanitized, username, password

def build_proxy_opener(url: str, ssl_context: ssl.SSLContext):
    scheme = urlsplit(url).scheme or "https"
    explicit_proxy = first_env(*PROXY_ENV_KEYS.get(scheme, ()))
    fallback_proxy = first_env(*PROXY_ENV_KEYS["all"])
    proxy_url = explicit_proxy or fallback_proxy
    if not proxy_url:
        return build_opener(HTTPSHandler(context=ssl_context))

    sanitized_proxy, username, password = split_proxy_auth(proxy_url, scheme)
    handlers = [
        ProxyHandler({"http": sanitized_proxy, "https": sanitized_proxy}),
        HTTPSHandler(context=ssl_context),
    ]
    if username:
        password_manager = HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, sanitized_proxy, username, password)
        handlers.append(ProxyBasicAuthHandler(password_manager))
    return build_opener(*handlers)

def parse_args(argv):
    mode = os.path.basename(argv[0])
    follow = False
    fail = False
    silent = False
    show_error = False
    output = None
    directory_prefix = None
    tries = 5
    timeout = 180
    headers = []
    method = "GET"
    user_agent = ""
    data = None
    url = None
    wants_version = False
    wants_help = False
    argc = len(argv)
    i = 1

    def need_int(raw, opt):
        try:
            return int(raw)
        except (TypeError, ValueError):
            die(f"{mode}: option {opt} requires a numeric argument")

    def long_value(key, val):
        nonlocal i
        if val is not None:
            return val
        i += 1
        if i >= argc:
            die(f"{mode}: option {key} requires an argument")
        return argv[i]

    while i < argc:
        a = argv[i]

        # "-" ou qualquer token sem "-" inicial e a URL/posicional.
        if a == "-" or not a.startswith("-"):
            url = a
            i += 1
            continue

        # Opcoes longas (--xxx / --xxx=valor).
        if a.startswith("--"):
            if "=" in a:
                key, val = a.split("=", 1)
            else:
                key, val = a, None
            if key == "--location":
                follow = True
            elif key == "--fail":
                fail = True
            elif key == "--silent":
                silent = True
            elif key == "--show-error":
                show_error = True
            elif key == "--output":
                output = long_value(key, val)
            elif key == "--directory-prefix":
                directory_prefix = long_value(key, val)
            elif key in ("--retry", "--tries"):
                tries = need_int(long_value(key, val), key)
            elif key in ("--max-time", "--timeout", "--connect-timeout"):
                timeout = need_int(long_value(key, val), key)
            elif key == "--header":
                headers.append(long_value(key, val))
            elif key in ("--request", "--method"):
                method = (long_value(key, val) or "GET").upper()
            elif key == "--user-agent":
                user_agent = long_value(key, val)
            elif key in ("--data", "--data-raw", "--data-binary", "--post-data"):
                data = long_value(key, val)
                if method == "GET":
                    method = "POST"
            elif key == "--version":
                wants_version = True
            elif key == "--help":
                wants_help = True
            elif key in IGNORE_NOARG_LONG:
                pass
            elif key in IGNORE_VALUE_LONG:
                long_value(key, val)
            else:
                die(f"{mode}: unsupported option {a}")
            i += 1
            continue

        # Tokens curtos exatos aceitos e ignorados (ex.: -nv).
        if a in SHORT_EXACT_IGNORE:
            i += 1
            continue

        # Token de opcoes curtas, possivelmente combinadas (ex.: -fsSLo /tmp/x).
        letters = a[1:]
        j = 0
        while j < len(letters):
            c = letters[j]
            if c in TOGGLE_LETTERS:
                if c == "f":
                    fail = True
                elif c in ("s", "q"):
                    silent = True
                elif c == "S":
                    show_error = True
                elif c == "L":
                    follow = True
                # k, g, 4, 6, I, c, # -> aceitos e ignorados
                j += 1
                continue
            if c in VALUE_LETTERS:
                remainder = letters[j + 1:]
                if c == "O":
                    # curl: remote-name (sem argumento); wget: arquivo de saida;
                    # valor "-" (ex.: -qO-, -O -) => stdout.
                    if remainder:
                        val = remainder
                    elif i + 1 < argc and argv[i + 1] == "-":
                        i += 1
                        val = "-"
                    elif mode == "wget":
                        i += 1
                        if i >= argc:
                            die(f"{mode}: option -O requires an argument")
                        val = argv[i]
                    else:
                        val = "__remote__"
                    output = val
                    break
                if remainder:
                    val = remainder
                else:
                    i += 1
                    if i >= argc:
                        die(f"{mode}: option -{c} requires an argument")
                    val = argv[i]
                if c == "o":
                    if mode == "wget":
                        pass  # wget: -o e arquivo de log; ignorado no wrapper
                    else:
                        output = val
                elif c == "H":
                    headers.append(val)
                elif c == "A":
                    user_agent = val
                elif c == "d":
                    data = val
                    if method == "GET":
                        method = "POST"
                elif c == "X":
                    method = (val or "GET").upper()
                elif c == "T":
                    timeout = need_int(val, "-T")
                elif c == "P":
                    directory_prefix = val
                # b, e, u, C -> aceitos e ignorados (valor descartado)
                break
            die(f"{mode}: unsupported option -{c}")
        i += 1

    if wants_version:
        return {"meta_only": "version", "mode": mode}
    if wants_help:
        return {"meta_only": "help", "mode": mode}
    if not url:
        die(f"{mode}: missing URL")
    if output == "__remote__":
        base = os.path.basename(urlparse(url).path) or "download.bin"
        output = os.path.join(directory_prefix, base) if directory_prefix else base
    elif output not in (None, "-") and directory_prefix and not os.path.isabs(output):
        output = os.path.join(directory_prefix, output)
    if output == "-":
        output = None
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
    opener = build_proxy_opener(cfg["url"], context)
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
            with opener.open(req, timeout=cfg["timeout"]) as resp:
                status = getattr(resp, "status", 200)
                if cfg["fail"] and (status < 200 or status >= 300):
                    raise HTTPError(cfg["url"], status, f"status {status}", resp.headers, None)
                data = resp.read()
                if cfg["output"]:
                    dest = cfg["output"]
                    parent = os.path.dirname(dest)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    tmp_path = None
                    try:
                        fd, tmp_path = tempfile.mkstemp(
                            dir=parent or ".", prefix=".http_fetch_"
                        )
                        with os.fdopen(fd, "wb") as handle:
                            handle.write(data)
                        os.replace(tmp_path, dest)
                        tmp_path = None
                    finally:
                        if tmp_path is not None and os.path.exists(tmp_path):
                            os.remove(tmp_path)
                else:
                    sys.stdout.buffer.write(data)
                return 0
        except HTTPError as e:
            last = e
            if e.code == 407:
                die(
                    f"{cfg['mode']}: proxy exige autenticacao. Configure HTTPS_PROXY/HTTP_PROXY "
                    "com usuario:senha ou use *_PROXY_USERNAME/*_PROXY_PASSWORD.",
                    22,
                )
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
if command -v which >/dev/null 2>&1; then
  while IFS= read -r candidate; do
    [ -n "$candidate" ] || continue
    [ -x "$candidate" ] || continue
    candidate_dir="$(cd "$(dirname "$candidate")" && pwd)"
    if [ "$candidate_dir" != "$wrapper_dir" ]; then
      real_git="$candidate"
      break
    fi
  done < <(which -a git 2>/dev/null || true)
fi

if [ -z "$real_git" ]; then
  for candidate in /usr/bin/git /opt/homebrew/bin/git /home/linuxbrew/.linuxbrew/bin/git; do
    [ -x "$candidate" ] || continue
    candidate_dir="$(cd "$(dirname "$candidate")" && pwd)"
    [ "$candidate_dir" != "$wrapper_dir" ] || continue
    real_git="$candidate"
    break
  done
fi

if [ -z "$real_git" ]; then
  echo "git wrapper: real git binary not found" >&2
  exit 127
fi

rewrite_github_url() {
  local value="$1"
  local transport="${NVIM_GITHUB_TRANSPORT:-ssh}"
  if [ "$transport" = "ssh" ] && [[ "$value" == https://github.com/* ]]; then
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

exec "$real_git" ${args[@]+"${args[@]}"}
SH
  chmod +x "${WRAPPER_BIN_DIR}/git"

  cat > "${WRAPPER_BIN_DIR}/lazy_zip_sync.py" <<'PY'
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import socket
import ssl
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import (
    HTTPPasswordMgrWithDefaultRealm,
    ProxyBasicAuthHandler,
    ProxyHandler,
    Request,
    build_opener,
    HTTPSHandler,
)

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 1.0
PROXY_ENV_KEYS = {
    "http": ("HTTP_PROXY", "http_proxy"),
    "https": ("HTTPS_PROXY", "https_proxy"),
    "all": ("ALL_PROXY", "all_proxy"),
}
PROXY_CREDENTIAL_ENV_KEYS = {
    "http": (
        ("HTTP_PROXY_USERNAME", "http_proxy_username"),
        ("HTTP_PROXY_PASSWORD", "http_proxy_password"),
    ),
    "https": (
        ("HTTPS_PROXY_USERNAME", "https_proxy_username"),
        ("HTTPS_PROXY_PASSWORD", "https_proxy_password"),
    ),
    "all": (
        ("PROXY_USERNAME", "proxy_username"),
        ("PROXY_PASSWORD", "proxy_password"),
    ),
}


def die(message: str) -> None:
    print(f"[lazy-zip-sync] erro: {message}", file=sys.stderr)
    raise SystemExit(1)


def assert_zip_file(zip_path: Path, url: str) -> None:
    if zipfile.is_zipfile(zip_path):
        return

    preview = zip_path.read_bytes()[:200]
    text_preview = preview.decode("utf-8", errors="replace").replace("\n", "\\n")
    raise RuntimeError(
        "download nao retornou um ZIP valido: "
        f"{url}. Inicio da resposta: {text_preview!r}"
    )


def first_env(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""


def normalize_proxy_url(raw_url: str) -> str:
    if "://" in raw_url:
        return raw_url
    return f"http://{raw_url}"


def proxy_credentials_for(scheme: str) -> tuple[str, str]:
    username_keys, password_keys = PROXY_CREDENTIAL_ENV_KEYS.get(
        scheme, PROXY_CREDENTIAL_ENV_KEYS["all"]
    )
    username = first_env(*username_keys) or first_env(
        *PROXY_CREDENTIAL_ENV_KEYS["all"][0]
    )
    password = first_env(*password_keys) or first_env(
        *PROXY_CREDENTIAL_ENV_KEYS["all"][1]
    )
    return username, password


def split_proxy_auth(proxy_url: str, scheme: str) -> tuple[str, str, str]:
    normalized = normalize_proxy_url(proxy_url)
    parsed = urlsplit(normalized)
    username = parsed.username or ""
    password = parsed.password or ""
    if not username:
        username, password = proxy_credentials_for(scheme)
    hostname = parsed.hostname or ""
    if not hostname:
        return normalized, "", ""
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    sanitized = urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    )
    return sanitized, username, password


def build_proxy_opener(url: str, ssl_context: ssl.SSLContext):
    scheme = urlsplit(url).scheme or "https"
    explicit_proxy = first_env(*PROXY_ENV_KEYS.get(scheme, ()))
    fallback_proxy = first_env(*PROXY_ENV_KEYS["all"])
    proxy_url = explicit_proxy or fallback_proxy
    if not proxy_url:
        return build_opener(HTTPSHandler(context=ssl_context))

    sanitized_proxy, username, password = split_proxy_auth(proxy_url, scheme)
    handlers = [
        ProxyHandler({"http": sanitized_proxy, "https": sanitized_proxy}),
        HTTPSHandler(context=ssl_context),
    ]
    if username:
        password_manager = HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, sanitized_proxy, username, password)
        handlers.append(ProxyBasicAuthHandler(password_manager))
    return build_opener(*handlers)


def download_zip(url: str, zip_path: Path, token: str) -> None:
    headers = {"User-Agent": "lazy-zip-sync/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    context = ssl._create_unverified_context()
    opener = build_proxy_opener(url, context)
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = Request(url=url, headers=headers, method="GET")
        try:
            with opener.open(request, timeout=180) as response:
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise HTTPError(url, status, f"status {status}", response.headers, None)
                with zip_path.open("wb") as handle:
                    shutil.copyfileobj(response, handle, length=1024 * 64)
                assert_zip_file(zip_path, url)
                return
        except HTTPError as error:
            last_error = error
            if error.code == 407:
                raise RuntimeError(
                    "proxy exige autenticacao. Configure HTTPS_PROXY/HTTP_PROXY com usuario:senha "
                    "ou use *_PROXY_USERNAME/*_PROXY_PASSWORD."
                ) from error
            if error.code not in RETRYABLE_HTTP_STATUS or attempt == MAX_ATTEMPTS:
                break
        except (URLError, TimeoutError, socket.timeout, ConnectionError) as error:
            last_error = error
            if attempt == MAX_ATTEMPTS:
                break
        time.sleep(BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"falha no download ZIP {url}: {last_error}")


def candidate_branches(branch: str) -> list[str]:
    if branch != "default":
        branches = [branch]
    else:
        branches = ["default", "main", "master"]

    result: list[str] = []
    for candidate in branches:
        if candidate and candidate not in result:
            result.append(candidate)
    if branch == "main" and "master" not in result:
        result.append("master")
    elif branch == "master" and "main" not in result:
        result.append("main")
    return result


def archive_url(repo: str, branch: str) -> str:
    if branch == "default":
        return f"https://github.com/{repo}/archive/HEAD.zip"
    return f"https://github.com/{repo}/archive/refs/heads/{branch}.zip"


def extract_single_dir(zip_path: Path, destination: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="lazy-zip-sync-") as td:
        extract_root = Path(td) / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zipped:
            zipped.extractall(extract_root)
        dirs = [entry for entry in extract_root.iterdir() if entry.is_dir()]
        if len(dirs) != 1:
            raise RuntimeError(f"arquivo zip com formato inesperado: {zip_path}")
        src = dirs[0]
        tmp_dest = Path(td) / "new_plugin"
        shutil.move(str(src), str(tmp_dest))
        if destination.exists() or destination.is_symlink():
            if destination.is_dir() and not destination.is_symlink():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp_dest), str(destination))


def parse_manifest_line(line: str) -> tuple[str, str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    parts = [part.strip() for part in stripped.split("|")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError(f"linha de manifesto invalida: {line.rstrip()}")
    return parts[0], parts[1], parts[2]


def read_manifest(manifest_path: Path) -> dict[str, tuple[str, str]]:
    entries: dict[str, tuple[str, str]] = {}
    if not manifest_path.exists():
        return entries
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        parsed = parse_manifest_line(line)
        if parsed is None:
            continue
        name, repo, branch = parsed
        entries[name] = (repo, branch)
    return entries


def parse_zip_source(path: Path) -> tuple[str, str]:
    source_file = path / ".zip-source"
    if not source_file.exists():
        raise RuntimeError("arquivo .zip-source ausente")
    content = source_file.read_text(encoding="utf-8").strip()
    if "@" not in content:
        raise RuntimeError(f".zip-source invalido: {content}")
    repo, branch = content.split("@", 1)
    repo = repo.strip()
    branch = branch.strip()
    if not repo or not branch:
        raise RuntimeError(f".zip-source invalido: {content}")
    return repo, branch


def sync_plugin(
    *,
    lazy_root: Path,
    name: str,
    repo: str,
    branch: str,
    action: str,
    token: str,
) -> tuple[bool, str]:
    path = lazy_root / name
    zip_source_file = path / ".zip-source"
    if action == "check":
        status = "instalado" if path.exists() else "ausente"
        return False, f"{name}: {status}; check remoto desabilitado no modo ZIP-only"

    last_error: Exception | None = None
    for candidate_branch in candidate_branches(branch):
        zip_url = archive_url(repo, candidate_branch)
        try:
            with tempfile.TemporaryDirectory(prefix="lazy-zip-sync-") as td:
                zip_path = Path(td) / f"{path.name}.zip"
                download_zip(zip_url, zip_path, token)
                extract_single_dir(zip_path, path)
            zip_source_file.write_text(f"{repo}@{candidate_branch}\n", encoding="utf-8")
            return True, f"{name}: instalado/atualizado por ZIP ({candidate_branch})"
        except Exception as error:  # noqa: BLE001
            last_error = error

    raise RuntimeError(f"falha ao instalar {repo}@{branch}: {last_error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lazy plugin sync over GitHub ZIP archives")
    parser.add_argument("action", choices=["check", "install", "update"], help="check, install or update plugins")
    parser.add_argument(
        "--lazy-root",
        default=str(Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))) / "nvim" / "lazy"),
        help="lazy plugins root directory",
    )
    parser.add_argument(
        "--manifest",
        default=str(Path(__file__).resolve().parents[1] / "lazy-plugins.manifest"),
        help="ZIP plugin manifest path",
    )
    args = parser.parse_args()

    lazy_root = Path(args.lazy_root)
    if not lazy_root.exists():
        die(f"diretorio nao encontrado: {lazy_root}")

    token = os.environ.get("GITHUB_TOKEN", "").strip()
    entries = read_manifest(Path(args.manifest))
    for plugin_dir in sorted([p for p in lazy_root.iterdir() if p.is_dir() and (p / ".zip-source").exists()]):
        if plugin_dir.name not in entries:
            repo, branch = parse_zip_source(plugin_dir)
            entries[plugin_dir.name] = (repo, branch)
    if not entries:
        die(f"nenhum plugin encontrado em manifesto ou .zip-source: {lazy_root}")

    changed = 0
    for name, (repo, branch) in sorted(entries.items()):
        try:
            did_change, msg = sync_plugin(
                lazy_root=lazy_root,
                name=name,
                repo=repo,
                branch=branch,
                action=args.action,
                token=token,
            )
            print(msg)
            if did_change:
                changed += 1
        except Exception as error:  # noqa: BLE001
            print(f"{name}: erro: {error}", file=sys.stderr)

    print(f"[lazy-zip-sync] acao={args.action} alteracoes={changed} total={len(entries)}")


if __name__ == "__main__":
    main()
PY
  chmod +x "${WRAPPER_BIN_DIR}/lazy_zip_sync.py"

  cat > "${WRAPPER_BIN_DIR}/lazy-check" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/lazy_zip_sync.py" check "$@"
SH
  chmod +x "${WRAPPER_BIN_DIR}/lazy-check"

  cat > "${WRAPPER_BIN_DIR}/lazy-install" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/lazy_zip_sync.py" install "$@"
SH
  chmod +x "${WRAPPER_BIN_DIR}/lazy-install"

  cat > "${WRAPPER_BIN_DIR}/lazy-update" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/lazy_zip_sync.py" update "$@"
SH
  chmod +x "${WRAPPER_BIN_DIR}/lazy-update"

  append_wrapper_path_to_rc "${HOME}/.zshrc"
  append_wrapper_path_to_rc "${HOME}/.bashrc"
  append_wrapper_path_to_rc "${HOME}/.profile"
}

emit_candidate_branches() {
  local repo="$1"
  local branch="$2"

  if [ "$branch" = "default" ]; then
    printf '%s\n' default main master
  elif [ "$branch" = "main" ]; then
    printf '%s\n' main master
  elif [ "$branch" = "master" ]; then
    printf '%s\n' master main
  else
    printf '%s\n' "$branch"
  fi
}

download_and_extract_branch_zip() {
  local repo="$1"
  local branch="$2"
  local destination_dir="$3"
  local resolved_branch=""
  local attempted=" "
  local last_status=0

  while IFS= read -r candidate_branch; do
    [ -n "$candidate_branch" ] || continue
    case "$attempted" in
      *" ${candidate_branch} "*) continue ;;
    esac
    attempted="${attempted}${candidate_branch} "

    local temp_zip="$TMP_DIR/${repo##*/}_${candidate_branch}.zip"
    local extract_root="$TMP_DIR/extract_${repo##*/}_${candidate_branch}"
    local url="${GITHUB_BASE_URL}/${repo}/archive/refs/heads/${candidate_branch}.zip"
    if [ "$candidate_branch" = "default" ]; then
      url="${GITHUB_BASE_URL}/${repo}/archive/HEAD.zip"
    fi

    mkdir -p "$extract_root"
    if "$VENV_PYTHON" "${SCRIPT_DIR}/github_zip_download_extract.py" \
      "$url" "$temp_zip" "$extract_root" "$destination_dir"; then
      resolved_branch="$candidate_branch"
      break
    else
      # Dentro do else, $? é o status do download que falhou; fora dele seria o
      # status do if (0 quando a condição falha sem else) — e um 404 viraria
      # "sucesso" na chamada.
      last_status=$?
    fi
  done < <(emit_candidate_branches "$repo" "$branch")

  if [ -z "$resolved_branch" ]; then
    [ "$last_status" -ne 0 ] || last_status=1
    return "$last_status"
  fi
}

install_crowquill_font() {
  local font_dir
  if [ -d "$HOME/Library/Fonts" ]; then
    font_dir="$HOME/Library/Fonts"
  else
    font_dir="$HOME/.local/share/fonts"
  fi
  mkdir -p "$font_dir"

  if ls "$font_dir"/CrowquillMono-*.ttf >/dev/null 2>&1; then
    log "fonte Crowquill Mono ja presente em ${font_dir} (pulando download)"
  else
    local src_dir="${TMP_DIR}/crowquill-mono"
    mkdir -p "$src_dir"
    log "baixando Crowquill Mono (andersonflima/crowquill-mono) por ZIP"
    download_and_extract_branch_zip "andersonflima/crowquill-mono" "default" "$src_dir" \
      || die "falha ao baixar a fonte Crowquill Mono"
    local installed=0
    for ttf in "$src_dir"/dist/*.ttf; do
      [ -e "$ttf" ] || continue
      cp "$ttf" "$font_dir/"
      installed=$((installed + 1))
    done
    [ "$installed" -gt 0 ] || die "nenhum .ttf encontrado em dist/ do crowquill-mono"
    log "instaladas ${installed} face(s) da Crowquill Mono em ${font_dir}"
  fi

  if command -v fc-cache >/dev/null 2>&1; then
    fc-cache -f "$font_dir" >/dev/null 2>&1 || true
  fi
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
        vim.env.NVIM_GITHUB_TRANSPORT = "${GITHUB_TRANSPORT}"
      end
    end,
  },
}
LUA
}

write_lazy_offline_mode_override() {
  local plugin_dir="${NVIM_CONFIG_DIR}/lua/plugins"
  local target_file="${plugin_dir}/lazy_offline_mode.lua"
  mkdir -p "$plugin_dir"
  cat > "$target_file" <<LUA
return {
  {
    "folke/lazy.nvim",
    init = function()
      local function run_zip_action(action)
        local wrapper_bin = "${WRAPPER_BIN_DIR}"
        local binary = wrapper_bin .. "/lazy-" .. action
        if vim.fn.executable(binary) ~= 1 then
          vim.notify("Comando nao encontrado: " .. binary, vim.log.levels.ERROR)
          return
        end
        require("lazy.util").float_term({ binary }, {
          cwd = vim.fn.stdpath("data") .. "/lazy",
        })
      end

      vim.api.nvim_create_user_command("Lazy", function(cmd)
        local commands = require("lazy.view.commands")
        local prefix, args = commands.parse(cmd.args)
        if prefix == "install" then
          run_zip_action("install")
          return
        end
        if prefix == "update" or prefix == "sync" then
          run_zip_action("update")
          return
        end
        if prefix == "check" then
          run_zip_action("check")
          return
        end

        local opts = { wait = cmd.bang == true }
        if #args == 1 and args[1] == "all" then
          args = vim.tbl_keys(require("lazy.core.config").plugins)
        end
        if #args > 0 then
          opts.plugins = vim.tbl_map(function(plugin)
            return require("lazy.core.config").plugins[plugin]
          end, args)
        end
        commands.cmd(prefix, opts)
      end, {
        bar = true,
        bang = true,
        nargs = "?",
        desc = "Lazy",
        force = true,
        complete = function(_, line)
          local commands = require("lazy.view.commands")
          local prefix, args = commands.parse(line)
          if #args > 0 then
            return commands.complete(prefix, args[#args])
          end
          return vim.tbl_filter(function(key)
            return key:find(prefix, 1, true) == 1
          end, vim.tbl_keys(commands.commands))
        end,
      })
    end,
    opts = function(_, opts)
      opts = opts or {}
      local wrapper_bin = "${WRAPPER_BIN_DIR}"
      local manifest_path = wrapper_bin .. "/../lazy-plugins.manifest"
      local function normalize_github_repo(url)
        if not url or url == "" then
          return nil
        end
        url = url:gsub("%.git$", "")
        url = url:gsub("^git@github%.com:", "")
        url = url:gsub("^ssh://git@github%.com/", "")
        url = url:gsub("^https://github%.com/", "")
        url = url:gsub("^http://github%.com/", "")
        local owner, repo = url:match("^([^/]+)/([^/]+)$")
        if owner and repo then
          return owner .. "/" .. repo
        end
        return nil
      end
      local function refresh_zip_manifest()
        local entries = {}
        local order = {}
        local function put(name, repo, branch)
          if not name or not repo or not branch then
            return
          end
          if not entries[name] then
            table.insert(order, name)
          end
          entries[name] = { repo = repo, branch = branch }
        end
        local existing = io.open(manifest_path, "r")
        if existing then
          for line in existing:lines() do
            local name, repo, branch = line:match("^([^|]+)|([^|]+)|([^|]+)$")
            put(name, repo, branch)
          end
          existing:close()
        end
        local ok_config, lazy_config = pcall(require, "lazy.core.config")
        if ok_config and lazy_config.plugins then
          for name, plugin in pairs(lazy_config.plugins) do
            local plugin_name = plugin.name or name
            local repo = normalize_github_repo(plugin.url)
            local branch = plugin.branch or "default"
            if repo then
              put(plugin_name, repo, branch)
            end
          end
        end
        table.sort(order)
        local manifest = assert(io.open(manifest_path, "w"))
        for _, name in ipairs(order) do
          local entry = entries[name]
          manifest:write(("%s|%s|%s\n"):format(name, entry.repo, entry.branch))
        end
        manifest:close()
      end
      local function run_zip_action(action)
        local binary = wrapper_bin .. "/lazy-" .. action
        if vim.fn.executable(binary) ~= 1 then
          vim.notify("Comando nao encontrado: " .. binary, vim.log.levels.ERROR)
          return false
        end
        refresh_zip_manifest()
        require("lazy.util").float_term({ binary }, {
          cwd = vim.fn.stdpath("data") .. "/lazy",
        })
        return true
      end

      local ok_cmd, lazy_commands = pcall(require, "lazy.view.commands")
      if ok_cmd and lazy_commands and lazy_commands.commands then
        lazy_commands.commands.install = function()
          run_zip_action("install")
        end
        lazy_commands.commands.update = function()
          run_zip_action("update")
        end
        lazy_commands.commands.sync = function()
          run_zip_action("update")
        end
        lazy_commands.commands.check = function()
          run_zip_action("check")
        end
      end

      opts.checker = vim.tbl_deep_extend("force", opts.checker or {}, { enabled = false })
      opts.change_detection = vim.tbl_deep_extend("force", opts.change_detection or {}, {
        enabled = false,
        notify = false,
      })
      opts.ui = opts.ui or {}
      opts.ui.custom_keys = opts.ui.custom_keys or {}
      opts.ui.custom_keys["I"] = {
        function()
          run_zip_action("install")
        end,
        desc = "ZIP install (offline)",
      }
      opts.ui.custom_keys["U"] = {
        function()
          run_zip_action("update")
        end,
        desc = "ZIP update (offline)",
      }
      opts.ui.custom_keys["S"] = {
        function()
          run_zip_action("update")
        end,
        desc = "ZIP sync (offline)",
      }
      opts.ui.custom_keys["C"] = {
        function()
          run_zip_action("check")
        end,
        desc = "ZIP check (offline)",
      }
      return opts
    end,
  },
}
LUA
}

write_lazy_command_override_after_plugin() {
  local after_plugin_dir="${NVIM_CONFIG_DIR}/after/plugin"
  local target_file="${after_plugin_dir}/lazy_zip_command_override.lua"
  mkdir -p "$after_plugin_dir"
  cat > "$target_file" <<LUA
local wrapper_bin = "${WRAPPER_BIN_DIR}"
local manifest_path = wrapper_bin .. "/../lazy-plugins.manifest"

local function normalize_github_repo(url)
  if not url or url == "" then
    return nil
  end
  url = url:gsub("%.git$", "")
  url = url:gsub("^git@github%.com:", "")
  url = url:gsub("^ssh://git@github%.com/", "")
  url = url:gsub("^https://github%.com/", "")
  url = url:gsub("^http://github%.com/", "")
  local owner, repo = url:match("^([^/]+)/([^/]+)$")
  if owner and repo then
    return owner .. "/" .. repo
  end
  return nil
end

local function refresh_zip_manifest()
  local entries = {}
  local order = {}
  local function put(name, repo, branch)
    if not name or not repo or not branch then
      return
    end
    if not entries[name] then
      table.insert(order, name)
    end
    entries[name] = { repo = repo, branch = branch }
  end
  local existing = io.open(manifest_path, "r")
  if existing then
    for line in existing:lines() do
      local name, repo, branch = line:match("^([^|]+)|([^|]+)|([^|]+)$")
      put(name, repo, branch)
    end
    existing:close()
  end
  local ok_config, lazy_config = pcall(require, "lazy.core.config")
  if ok_config and lazy_config.plugins then
    for name, plugin in pairs(lazy_config.plugins) do
      local plugin_name = plugin.name or name
      local repo = normalize_github_repo(plugin.url)
      local branch = plugin.branch or "default"
      if repo then
        put(plugin_name, repo, branch)
      end
    end
  end
  table.sort(order)
  local manifest = assert(io.open(manifest_path, "w"))
  for _, name in ipairs(order) do
    local entry = entries[name]
    manifest:write(("%s|%s|%s\n"):format(name, entry.repo, entry.branch))
  end
  manifest:close()
end

local function run_zip_action(action)
  local binary = wrapper_bin .. "/lazy-" .. action
  if vim.fn.executable(binary) ~= 1 then
    vim.notify("Comando nao encontrado: " .. binary, vim.log.levels.ERROR)
    return false
  end
  refresh_zip_manifest()
  require("lazy.util").float_term({ binary }, {
    cwd = vim.fn.stdpath("data") .. "/lazy",
  })
  return true
end

local ok, commands = pcall(require, "lazy.view.commands")
if not ok or not commands then
  return
end

if commands.commands then
  commands.commands.install = function()
    run_zip_action("install")
  end
  commands.commands.update = function()
    run_zip_action("update")
  end
  commands.commands.sync = function()
    run_zip_action("update")
  end
  commands.commands.check = function()
    run_zip_action("check")
  end
end

vim.api.nvim_create_user_command("Lazy", function(cmd)
  local prefix, args = commands.parse(cmd.args)
  if prefix == "install" then
    run_zip_action("install")
    return
  end
  if prefix == "update" or prefix == "sync" then
    run_zip_action("update")
    return
  end
  if prefix == "check" then
    run_zip_action("check")
    return
  end

  local opts = { wait = cmd.bang == true }
  if #args == 1 and args[1] == "all" then
    args = vim.tbl_keys(require("lazy.core.config").plugins)
  end
  if #args > 0 then
    opts.plugins = vim.tbl_map(function(plugin)
      return require("lazy.core.config").plugins[plugin]
    end, args)
  end
  commands.cmd(prefix, opts)
end, {
  bar = true,
  bang = true,
  nargs = "?",
  desc = "Lazy",
  force = true,
  complete = function(_, line)
    local pfx, a = commands.parse(line)
    if #a > 0 then
      return commands.complete(pfx, a[#a])
    end
    return vim.tbl_filter(function(key)
      return key:find(pfx, 1, true) == 1
    end, vim.tbl_keys(commands.commands))
  end,
})
LUA
}

patch_lazy_transport() {
  local lazy_config_file="${NVIM_CONFIG_DIR}/lua/config/lazy.lua"
  [ -f "$lazy_config_file" ] || return 0

  if [ "$GITHUB_TRANSPORT" = "ssh" ]; then
    # Ajusta bootstrap manual de lazy.nvim para SSH.
    if grep -Fq "https://github.com/folke/lazy.nvim.git" "$lazy_config_file"; then
      local tmp_file_url
      tmp_file_url="$(mktemp)"
      awk '{ gsub(/https:\/\/github\.com\/folke\/lazy\.nvim\.git/, "git@github.com:folke/lazy.nvim.git"); print }' \
        "$lazy_config_file" > "$tmp_file_url"
      mv "$tmp_file_url" "$lazy_config_file"
    fi

    # Garante url_format SSH no setup do lazy.nvim.
    if ! grep -Fq 'url_format = "git@github.com:%s.git"' "$lazy_config_file"; then
      local tmp_file
      tmp_file="$(mktemp)"
      awk '
        BEGIN { inserted=0 }
        {
          print
          if (!inserted && $0 ~ /require\("lazy"\)\.setup\(\{/) {
            print "\tgit = {"
            print "\t\turl_format = \"git@github.com:%s.git\","
            print "\t},"
            inserted=1
          }
        }
      ' "$lazy_config_file" > "$tmp_file"
      mv "$tmp_file" "$lazy_config_file"
    else
      local tmp_file_git
      tmp_file_git="$(mktemp)"
      awk '{ gsub(/url_format = "https:\/\/github\.com\/%s\.git"/, "url_format = \"git@github.com:%s.git\""); print }' \
        "$lazy_config_file" > "$tmp_file_git"
      mv "$tmp_file_git" "$lazy_config_file"
    fi
  else
    # Ajusta bootstrap manual de lazy.nvim para HTTPS em ambiente HTTP-only.
    if grep -Fq "git@github.com:folke/lazy.nvim.git" "$lazy_config_file"; then
      local tmp_file_url_http
      tmp_file_url_http="$(mktemp)"
      awk '{ gsub(/git@github\.com:folke\/lazy\.nvim\.git/, "https://github.com/folke/lazy.nvim.git"); print }' \
        "$lazy_config_file" > "$tmp_file_url_http"
      mv "$tmp_file_url_http" "$lazy_config_file"
    fi

    if grep -Fq 'url_format = "git@github.com:%s.git"' "$lazy_config_file"; then
      local tmp_file_url_format
      tmp_file_url_format="$(mktemp)"
      awk '{ gsub(/url_format = "git@github.com:%s.git"/, "url_format = \"https://github.com/%s.git\""); print }' \
        "$lazy_config_file" > "$tmp_file_url_format"
      mv "$tmp_file_url_format" "$lazy_config_file"
    fi
  fi

  # Em ambiente bloqueado para git externo, evita checks automáticos via git fetch.
  local tmp_file_checker
  tmp_file_checker="$(mktemp)"
  awk '{ gsub(/checker = \{ enabled = true \}/, "checker = { enabled = false }"); print }' \
    "$lazy_config_file" > "$tmp_file_checker"
  mv "$tmp_file_checker" "$lazy_config_file"

  if ! grep -Fq 'change_detection = { enabled = false' "$lazy_config_file"; then
    local tmp_file_2
    tmp_file_2="$(mktemp)"
    awk '
      BEGIN { inserted=0 }
      {
        print
        if (!inserted && $0 ~ /checker = \{ enabled = false \}/) {
          print "\tchange_detection = { enabled = false, notify = false },"
          inserted=1
        }
      }
    ' "$lazy_config_file" > "$tmp_file_2"
    mv "$tmp_file_2" "$lazy_config_file"
  fi
}

install_plugins_from_manifest() {
  local lazy_dir="${NVIM_DATA_DIR}/lazy"
  mkdir -p "$lazy_dir"

  local failed=()
  local total=0
  # Resiliencia por plugin: uma falha nao aborta a instalacao inteira
  # (espelha o caminho Lua em runtime, que tolera erros por plugin).
  while IFS='|' read -r plugin_name repo branch; do
    [ -n "$plugin_name" ] || continue
    [ -n "$repo" ] || continue
    [ -n "$branch" ] || branch="main"
    total=$((total + 1))
    log "plugin: $plugin_name (${repo}@${branch})"
    if download_and_extract_branch_zip "$repo" "$branch" "${lazy_dir}/${plugin_name}"; then
      printf '%s\n' "${repo}@${branch}" > "${lazy_dir}/${plugin_name}/.zip-source"
    else
      log "falha ao instalar plugin: $plugin_name (${repo}@${branch})"
      failed+=("$plugin_name")
    fi
  done < <(emit_lazy_plugin_manifest)

  if [ "${#failed[@]}" -gt 0 ]; then
    log "plugins com falha (${#failed[@]}/${total}): ${failed[*]}"
    return 1
  fi
  log "todos os plugins instalados (${total})"
  return 0
}

configure_git_transport() {
  # Faz submodules/ls-remote (que o shim de argv nao alcanca) usarem SSH.
  # Idempotente e opt-out-safe: so adiciona se ainda nao estiver presente.
  if [ "$GITHUB_TRANSPORT" != "ssh" ]; then
    log "modo GitHub HTTP-only: pulando git insteadOf global"
    return 0
  fi
  command -v git >/dev/null 2>&1 || {
    log "git nao encontrado; pulando configuracao insteadOf"
    return 0
  }
  local https="https://github.com/"
  local ssh="git@github.com:"
  if ! git config --global --get-all "url.${ssh}.insteadOf" 2>/dev/null | grep -Fxq "$https"; then
    git config --global "url.${ssh}.insteadOf" "$https"
    log "git insteadOf configurado: ${https} -> ${ssh}"
  fi
  if ! git config --global --get-all "url.${ssh}.pushInsteadOf" 2>/dev/null | grep -Fxq "$https"; then
    git config --global "url.${ssh}.pushInsteadOf" "$https"
    log "git pushInsteadOf configurado: ${https} -> ${ssh}"
  fi
}

write_state_file() {
  cat > "$STATE_FILE" <<EOFSTATE
NVIM_CONFIG_DIR='${NVIM_CONFIG_DIR}'
NVIM_DATA_DIR='${NVIM_DATA_DIR}'
NVIM_CACHE_DIR='${NVIM_CACHE_DIR}'
BACKUP_DIR='${BACKUP_DIR}'
SETUP_TIMESTAMP='${TIMESTAMP}'
MANAGE_CONFIG='${MANAGE_CONFIG}'
CONFIG_SOURCE_DIR='${CONFIG_SOURCE_DIR}'
EOFSTATE
}

if [ "$WITH_HOMEBREW_PROXY" = "1" ]; then
  homebrew_proxy_script="${CONFIG_ROOT}/homebrew/setup_homebrew_proxy.sh"
  [ -f "$homebrew_proxy_script" ] || die "script nao encontrado: $homebrew_proxy_script"
  log "aplicando bloco de proxy/Homebrew no shell rc"
  bash "$homebrew_proxy_script" --apply
fi

# plugins-only só toca ${NVIM_DATA_DIR}/lazy — pré-checagens de Mason/config
# pertencem ao fluxo completo.
ensure_absent_or_force "${NVIM_DATA_DIR}/lazy"
if [ "$PLUGINS_ONLY" != "1" ]; then
  ensure_absent_or_force "${NVIM_CACHE_DIR}/mason-registry-main"
  if [ "$MANAGE_CONFIG" = "1" ]; then
    ensure_absent_or_force "$NVIM_CONFIG_DIR"
  fi
fi
ensure_python_runtime

if [ "$PLUGINS_ONLY" = "1" ]; then
  log "modo plugins-only: lazy.nvim + plugins do manifesto por ZIP em ${NVIM_DATA_DIR}/lazy"
  install_plugins_from_manifest
  log "concluido (plugins-only)"
  exit 0
fi

install_http_wrappers
log "configurando transporte GitHub: ${GITHUB_TRANSPORT}"
configure_git_transport

if [ "$FORCE" = "1" ]; then
  if [ "$MANAGE_CONFIG" = "1" ]; then
    backup_if_exists "$NVIM_CONFIG_DIR" "nvim-config"
  fi
  backup_if_exists "${NVIM_DATA_DIR}/lazy" "nvim-lazy"
  backup_if_exists "${NVIM_CACHE_DIR}/mason-registry-main" "mason-registry-main"
fi

# Registra o estado (BACKUP_DIR/MANAGE_CONFIG) imediatamente apos o backup,
# antes de qualquer download/patch, para que um aborto no meio seja recuperavel.
write_state_file

if [ "$MANAGE_CONFIG" = "1" ]; then
  source_config_dir="$CONFIG_SOURCE_DIR"
  if [ -z "$source_config_dir" ]; then
    source_config_dir="$REPO_DEFAULT_CONFIG_SOURCE_DIR"
  fi

  if [ -n "$source_config_dir" ]; then
    if [ ! -d "$source_config_dir" ]; then
      die "diretorio de origem da config nao encontrado: $source_config_dir"
    fi
    log "copiando config nvim de origem: $source_config_dir -> $NVIM_CONFIG_DIR"
    local_source_dir="$(cd "$source_config_dir" && pwd)"
    local_target_parent="$(dirname "$NVIM_CONFIG_DIR")"
    mkdir -p "$local_target_parent"
    local_target_dir="$(cd "$local_target_parent" && pwd)/$(basename "$NVIM_CONFIG_DIR")"

    snapshot_dir="$TMP_DIR/config_source_snapshot"
    mkdir -p "$snapshot_dir"
    cp -R "$local_source_dir"/. "$snapshot_dir"/

    rm -rf "$local_target_dir"
    mkdir -p "$local_target_dir"
    cp -R "$snapshot_dir"/. "$local_target_dir"/
  fi
else
  log "modo padrao: preservando ${NVIM_CONFIG_DIR} (sem alteracoes)"
fi

log "instalando registry local do Mason"
download_and_extract_branch_zip "mason-org/mason-registry" "default" "${NVIM_CACHE_DIR}/mason-registry-main"

if [ "$MANAGE_CONFIG" = "1" ]; then
  log "configurando Mason para usar registry local"
  write_mason_local_registry_override
  log "configurando PATH interno de wrappers HTTP no Neovim"
  write_http_wrapper_path_override
  log "forcando modo offline do lazy.nvim (sem checker externo)"
  write_lazy_offline_mode_override
  log "forcando override global de :Lazy update/sync/check para ZIP"
  write_lazy_command_override_after_plugin
  log "forcando transporte ${GITHUB_TRANSPORT} no bootstrap/update do lazy.nvim"
  patch_lazy_transport
fi

if [ "$SKIP_PLUGINS" != "1" ]; then
  log "instalando plugins do LazyVim por ZIP"
  install_plugins_from_manifest
fi

if [ "$INSTALL_CROWQUILL" = "1" ]; then
  log "instalando fonte Crowquill Mono"
  install_crowquill_font
fi

# Reescreve o estado ao final (refresh apos conclusao bem-sucedida).
write_state_file

log "concluido"
log "proximo passo: abrir o neovim e rodar :checkhealth e :Mason"
