#!/usr/bin/env bash
set -euo pipefail

# setup_ec2_dev.sh
# Provisiona um Amazon Linux 2023 como ambiente de desenvolvimento remoto:
# code-server (VS Code no browser) + Neovim + toolchain + fonte Crowquill Mono
# e tema Crowquill Ink Light.
#
# Rode NO EC2 (login via SSM Session Manager / EC2 Instance Connect), com um
# usuario que tenha sudo (ex.: ec2-user). Idempotente: pode rodar de novo.
#
# Acesso ao code-server: ele escuta em 127.0.0.1 (nunca exposto na internet).
# Chegue nele por SSM port forwarding a partir da sua maquina:
#   aws ssm start-session --target <instance-id> \
#     --document-name AWS-StartPortForwardingSession \
#     --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
# e abra http://localhost:8080 no browser.

log() { printf '[setup-ec2-dev] %s\n' "$*" >&2; }
die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  bash config/ec2/setup_ec2_dev.sh [opcoes]

Opcoes:
  --code-server-port <n>  Porta local do code-server (default 8080).
  --node-version <v>      Versao do Node via nvm (default: --lts).
  --with-docker           Instala e habilita o Docker.
  --with-go               Instala o Go (tarball oficial).
  --skip-nvim             Nao instala/gerencia o Neovim e a config.
  --skip-code-server      Nao instala/configura o code-server.
  -h, --help              Mostra ajuda.

Componentes (default):
- Pacotes base (dnf): git, build tools, fontconfig, tmux, jq, ripgrep/fd/fzf (best-effort).
- Node (nvm), Python3, starship.
- Neovim (release oficial) + config do snippets + tema crowquill-light + fonte Crowquill Mono.
- code-server em 127.0.0.1:<porta> (senha gerada) + extensao Crowquill Ink Light + settings.
USAGE
}

CODE_SERVER_PORT=8080
NODE_VERSION="--lts"
WITH_DOCKER=0
WITH_GO=0
SKIP_NVIM=0
SKIP_CODE_SERVER=0
GO_VERSION="1.23.5"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --code-server-port) CODE_SERVER_PORT="${2:?}"; shift 2 ;;
    --node-version) NODE_VERSION="${2:?}"; shift 2 ;;
    --with-docker) WITH_DOCKER=1; shift ;;
    --with-go) WITH_GO=1; shift ;;
    --skip-nvim) SKIP_NVIM=1; shift ;;
    --skip-code-server) SKIP_CODE_SERVER=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "parametro invalido: $1" ;;
  esac
done

SNIPPETS_REPO="https://github.com/andersonflima/snippets.git"
CROWQUILL_MONO_REPO="https://github.com/andersonflima/crowquill-mono.git"
CROWQUILL_THEME_REPO="https://github.com/andersonflima/crowquill-theme.git"

SRC_ROOT="${HOME}/.local/share/ec2-dev-src"
FONT_DIR="${HOME}/.local/share/fonts"
BIN_DIR="${HOME}/.local/bin"
NVIM_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nvim"
CODE_SERVER_DATA="${HOME}/.local/share/code-server"
CODE_SERVER_CONFIG="${HOME}/.config/code-server"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

command -v dnf >/dev/null 2>&1 || die "dnf nao encontrado (este script e para Amazon Linux 2023)"
command -v sudo >/dev/null 2>&1 || die "sudo necessario"
[ "$(id -u)" -ne 0 ] || log "aviso: rodando como root; code-server prefere um usuario normal com sudo"

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) NVIM_ARCH="x86_64"; GO_ARCH="amd64" ;;
  aarch64|arm64) NVIM_ARCH="arm64"; GO_ARCH="arm64" ;;
  *) die "arquitetura nao suportada: $ARCH" ;;
esac

mkdir -p "$SRC_ROOT" "$FONT_DIR" "$BIN_DIR"

# Garante ~/.local/bin no PATH via bloco gerenciado idempotente.
ensure_local_bin_on_path() {
  local rc="${HOME}/.bashrc"
  local marker="# >>> ec2-dev PATH >>>"
  [ -f "$rc" ] || touch "$rc"
  grep -Fq "$marker" "$rc" && return 0
  {
    printf '\n%s\n' "$marker"
    printf '%s\n' 'export PATH="$HOME/.local/bin:$PATH"'
    printf '%s\n' "# <<< ec2-dev PATH <<<"
  } >> "$rc"
  log "adicionado ~/.local/bin ao PATH em $rc"
}

# Clona repo em SRC_ROOT (ou atualiza se ja existir).
sync_repo() {
  local url="$1" dest="$2"
  if [ -d "$dest/.git" ]; then
    git -C "$dest" pull --ff-only --quiet 2>/dev/null || log "aviso: pull falhou em $dest (seguindo com o que ha)"
  else
    rm -rf "$dest"
    git clone --depth 1 --quiet "$url" "$dest"
  fi
}

install_base_packages() {
  log "instalando pacotes base (dnf)"
  sudo dnf install -y \
    git tar gzip unzip zip jq gcc gcc-c++ make openssl openssl-devel \
    fontconfig tmux which findutils procps-ng ca-certificates curl >/dev/null
  # Utilitarios de produtividade: best-effort (podem nao estar nos repos do AL2023).
  for pkg in ripgrep fd-find fzf; do
    sudo dnf install -y "$pkg" >/dev/null 2>&1 && log "instalado: $pkg" || log "nao disponivel via dnf: $pkg (pulando)"
  done
}

install_starship() {
  command -v starship >/dev/null 2>&1 && { log "starship ja instalado"; return 0; }
  log "instalando starship em $BIN_DIR"
  curl -fsSL https://starship.rs/install.sh | sh -s -- --yes --bin-dir "$BIN_DIR" >/dev/null
}

install_node() {
  if [ ! -d "${HOME}/.nvm" ]; then
    log "instalando nvm"
    curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash >/dev/null
  fi
  # shellcheck disable=SC1090
  export NVM_DIR="${HOME}/.nvm"
  . "${NVM_DIR}/nvm.sh"
  log "instalando Node (${NODE_VERSION})"
  nvm install "$NODE_VERSION" >/dev/null
  nvm alias default "$NODE_VERSION" >/dev/null 2>&1 || true
}

install_go() {
  command -v go >/dev/null 2>&1 && { log "go ja instalado"; return 0; }
  local tarball="go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
  log "instalando Go ${GO_VERSION}"
  curl -fsSL "https://go.dev/dl/${tarball}" -o "/tmp/${tarball}"
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf "/tmp/${tarball}"
  rm -f "/tmp/${tarball}"
  ln -sfn /usr/local/go/bin/go "${BIN_DIR}/go"
  ln -sfn /usr/local/go/bin/gofmt "${BIN_DIR}/gofmt"
}

install_docker() {
  command -v docker >/dev/null 2>&1 && { log "docker ja instalado"; return 0; }
  log "instalando Docker"
  sudo dnf install -y docker >/dev/null
  sudo systemctl enable --now docker >/dev/null 2>&1 || true
  sudo usermod -aG docker "$USER" || true
  log "docker instalado (relogar para usar o grupo docker sem sudo)"
}

install_neovim() {
  local tarball="nvim-linux-${NVIM_ARCH}.tar.gz"
  local target="/opt/nvim-${NVIM_ARCH}"
  if command -v nvim >/dev/null 2>&1 && [ -x "${target}/bin/nvim" ]; then
    log "neovim ja instalado"
  else
    log "instalando Neovim (${NVIM_ARCH})"
    curl -fsSL "https://github.com/neovim/neovim/releases/latest/download/${tarball}" -o "/tmp/${tarball}"
    sudo rm -rf "$target"
    sudo mkdir -p "$target"
    sudo tar -C "$target" --strip-components=1 -xzf "/tmp/${tarball}"
    rm -f "/tmp/${tarball}"
    sudo ln -sfn "${target}/bin/nvim" /usr/local/bin/nvim
  fi
}

install_nvim_config() {
  sync_repo "$SNIPPETS_REPO" "${SRC_ROOT}/snippets"
  local src="${SRC_ROOT}/snippets/config/nvim"
  [ -d "$src" ] || die "config/nvim nao encontrada no snippets"

  if [ -d "$NVIM_CONFIG_DIR" ]; then
    mv "$NVIM_CONFIG_DIR" "${NVIM_CONFIG_DIR}.bak_${TIMESTAMP}"
    log "backup da config nvim: ${NVIM_CONFIG_DIR}.bak_${TIMESTAMP}"
  fi
  mkdir -p "$NVIM_CONFIG_DIR"
  cp -R "$src"/. "$NVIM_CONFIG_DIR"/

  # Garante o tema Crowquill Ink Light independente do estado do snippets.
  cat > "${NVIM_CONFIG_DIR}/lua/plugins/colorscheme.lua" <<'LUA'
return {
	{
		"andersonflima/crowquill-theme",
		lazy = false,
		priority = 1000,
		config = function()
			vim.o.background = "light"
			vim.cmd.colorscheme("crowquill-light")
		end,
	},
}
LUA

  log "sincronizando plugins do LazyVim (headless)"
  nvim --headless "+Lazy! sync" +qa >/dev/null 2>&1 || log "aviso: Lazy sync falhou no headless (rode 'nvim' e ':Lazy sync' depois)"
}

install_crowquill_font() {
  if ls "$FONT_DIR"/CrowquillMono-*.ttf >/dev/null 2>&1; then
    log "fonte Crowquill Mono ja presente em ${FONT_DIR}"
  else
    sync_repo "$CROWQUILL_MONO_REPO" "${SRC_ROOT}/crowquill-mono"
    local installed=0 ttf
    for ttf in "${SRC_ROOT}/crowquill-mono/dist"/*.ttf; do
      [ -e "$ttf" ] || continue
      cp "$ttf" "$FONT_DIR/"
      installed=$((installed + 1))
    done
    [ "$installed" -gt 0 ] || die "nenhum .ttf em dist/ do crowquill-mono"
    log "instaladas ${installed} face(s) da Crowquill Mono"
  fi
  command -v fc-cache >/dev/null 2>&1 && fc-cache -f "$FONT_DIR" >/dev/null 2>&1 || true
}

install_code_server() {
  if ! command -v code-server >/dev/null 2>&1; then
    log "instalando code-server"
    curl -fsSL https://code-server.dev/install.sh | sh >/dev/null
  else
    log "code-server ja instalado"
  fi

  mkdir -p "$CODE_SERVER_CONFIG"
  local config_yaml="${CODE_SERVER_CONFIG}/config.yaml"
  if [ ! -f "$config_yaml" ] || ! grep -q '^password:' "$config_yaml"; then
    local password
    password="$(openssl rand -hex 24)"
    cat > "$config_yaml" <<YAML
bind-addr: 127.0.0.1:${CODE_SERVER_PORT}
auth: password
password: ${password}
cert: false
YAML
    chmod 600 "$config_yaml"
    log "config.yaml gerado com senha nova (bind 127.0.0.1:${CODE_SERVER_PORT})"
  else
    # Preserva a senha existente; apenas garante a porta/bind.
    sed -i "s|^bind-addr:.*|bind-addr: 127.0.0.1:${CODE_SERVER_PORT}|" "$config_yaml"
    log "config.yaml existente preservado (senha mantida), bind 127.0.0.1:${CODE_SERVER_PORT}"
  fi

  install_code_server_theme
  write_code_server_settings

  sudo systemctl enable --now "code-server@${USER}" >/dev/null 2>&1 \
    && log "code-server@${USER} habilitado e iniciado" \
    || log "aviso: nao consegui habilitar o servico code-server@${USER} (verifique com systemctl)"
}

install_code_server_theme() {
  sync_repo "$CROWQUILL_THEME_REPO" "${SRC_ROOT}/crowquill-theme"
  local ext_src="${SRC_ROOT}/crowquill-theme/vscode"
  [ -d "$ext_src" ] || { log "aviso: extensao VS Code do tema nao encontrada"; return 0; }
  local ext_dest="${CODE_SERVER_DATA}/extensions/andersonflima.crowquill-theme"
  mkdir -p "${CODE_SERVER_DATA}/extensions"
  rm -rf "$ext_dest"
  cp -R "$ext_src" "$ext_dest"
  log "extensao Crowquill Ink instalada no code-server"
}

write_code_server_settings() {
  local user_dir="${CODE_SERVER_DATA}/User"
  local settings="${user_dir}/settings.json"
  mkdir -p "$user_dir"
  [ -f "$settings" ] && cp "$settings" "${settings}.bak_${TIMESTAMP}"
  cat > "$settings" <<'JSON'
{
  "workbench.colorTheme": "Crowquill Ink Light",
  "editor.fontFamily": "Crowquill Mono, Menlo, Consolas, monospace",
  "editor.fontLigatures": true,
  "terminal.integrated.fontFamily": "Crowquill Mono, monospace"
}
JSON
  log "settings.json do code-server escrito (tema + fonte Crowquill)"
}

print_summary() {
  cat >&2 <<SUMMARY

==================== CONCLUIDO ====================
Ambiente de dev provisionado no Amazon Linux 2023.

code-server: escutando em 127.0.0.1:${CODE_SERVER_PORT} (NUNCA exposto na internet).
Senha:       ${CODE_SERVER_CONFIG}/config.yaml (campo 'password')

Para acessar do seu notebook, via SSM port forwarding:
  aws ssm start-session --target <INSTANCE_ID> \\
    --document-name AWS-StartPortForwardingSession \\
    --parameters '{"portNumber":["${CODE_SERVER_PORT}"],"localPortNumber":["${CODE_SERVER_PORT}"]}'
Depois abra:  http://localhost:${CODE_SERVER_PORT}

Neovim:  'nvim' (tema crowquill-light). Rode :checkhealth se precisar.
Fonte:   Crowquill Mono instalada no servidor. No browser, a fonte do editor
         depende do cliente; se quiser garanti-la, instale a Crowquill Mono na
         maquina que abre o browser, ou injete um webfont no code-server.
Recarregue o shell:  source ~/.bashrc
==================================================
SUMMARY
}

# ---- execucao ----
ensure_local_bin_on_path
install_base_packages
install_starship
install_node
[ "$WITH_GO" = "1" ] && install_go
[ "$WITH_DOCKER" = "1" ] && install_docker

if [ "$SKIP_NVIM" != "1" ]; then
  install_neovim
  install_nvim_config
fi

install_crowquill_font

if [ "$SKIP_CODE_SERVER" != "1" ]; then
  install_code_server
fi

print_summary
