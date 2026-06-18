#!/usr/bin/env bash
set -euo pipefail

# setup_homebrew_proxy.sh
# Analisa e melhora a configuracao de proxy/no_proxy e Homebrew no shell rc
# (default ~/.zshrc) para permitir `brew install` atras de proxy corporativo
# sem erro 403 nos bottles/artefatos (ghcr.io / mirror interno).
#
# Por padrao roda em modo analise (dry-run): nada e escrito, apenas o relatorio
# do bloco gerenciado que SERIA aplicado. Use --apply para gravar.

log() { printf '[homebrew-proxy] %s\n' "$*" >&2; }
die() {
  log "erro: $*"
  exit 1
}

BLOCK_BEGIN="# >>> homebrew-proxy (managed) >>>"
BLOCK_END="# <<< homebrew-proxy (managed) <<<"

usage() {
  cat <<'USAGE'
Uso:
  bash scripts/setup_homebrew_proxy.sh [opcoes]

Modo:
  (sem flags)            Analisa o rc e mostra o bloco que SERIA aplicado (dry-run).
  --apply                Grava o bloco gerenciado no rc (com backup automatico).
  --doctor               Testa conectividade dos endpoints do Homebrew e sai.

Arquivo alvo:
  --rc-file <path>       Default: ~/.zshrc

Proxy (se omitido, herda do ambiente atual e do rc existente):
  --http-proxy <url>
  --https-proxy <url>
  --all-proxy <url>
  --no-proxy <lista>     Itens extra; mesclados com defaults e valor atual.

Homebrew anti-403 (mirror interno da empresa):
  --artifact-domain <url>   HOMEBREW_ARTIFACT_DOMAIN
  --bottle-domain <url>     HOMEBREW_BOTTLE_DOMAIN
  --api-domain <url>        HOMEBREW_API_DOMAIN
  --registry-token <token>  HOMEBREW_DOCKER_REGISTRY_TOKEN (sensivel)
  --brew-git-remote <url>   HOMEBREW_BREW_GIT_REMOTE
  --core-git-remote <url>   HOMEBREW_CORE_GIT_REMOTE

Instalacao opcional:
  --install-brew         Instala o Homebrew se ausente (usa o proxy configurado).
  --install-packages     Roda `brew install` do conjunto p/ LazyVim/Mason.
  --packages "a b c"     Override das formulae (default: neovim ripgrep fd fzf ...).
  --casks "a b"          Casks a instalar.
  -h, --help             Mostra ajuda.

Observacoes:
- Valores conhecidos (flag > ambiente > rc) entram como `export`.
- Variaveis importantes sem valor entram comentadas, como template a preencher.
- O token e mascarado no relatorio; so e gravado no rc com --apply.
USAGE
}

APPLY=0
DOCTOR=0
INSTALL_BREW=0
INSTALL_PACKAGES=0
RC_FILE="${HOME}/.zshrc"
DEFAULT_PACKAGES="neovim ripgrep fd fzf git node go lua luarocks tree-sitter"
PACKAGES="$DEFAULT_PACKAGES"
CASKS=""

# Overrides explicitos via flag (vazio = nao informado).
# Sem array associativo (`declare -A`) para manter compatibilidade com o bash 3.2
# do macOS, que e o interpretador disponivel antes do Homebrew existir.
NO_PROXY_EXTRA=""

# flag_set <var> <value>: registra override explicito de flag.
flag_set() {
  printf -v "FLAG_$1" '%s' "$2"
  printf -v "FLAGSET_$1" '%s' "1"
}

# flag_is_set <var>: verdadeiro se a flag foi informada explicitamente.
flag_is_set() {
  local marker="FLAGSET_$1"
  [ -n "${!marker:-}" ]
}

# flag_get <var>: valor do override explicito (vazio se ausente).
flag_get() {
  local var="FLAG_$1"
  printf '%s' "${!var:-}"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --apply) APPLY=1; shift ;;
    --dry-run) APPLY=0; shift ;;
    --doctor) DOCTOR=1; shift ;;
    --install-brew) INSTALL_BREW=1; shift ;;
    --install-packages) INSTALL_PACKAGES=1; shift ;;
    --rc-file) RC_FILE="${2:?}"; shift 2 ;;
    --http-proxy) flag_set HTTP_PROXY "${2:?}"; shift 2 ;;
    --https-proxy) flag_set HTTPS_PROXY "${2:?}"; shift 2 ;;
    --all-proxy) flag_set ALL_PROXY "${2:?}"; shift 2 ;;
    --no-proxy) NO_PROXY_EXTRA="${2:?}"; shift 2 ;;
    --artifact-domain) flag_set HOMEBREW_ARTIFACT_DOMAIN "${2:?}"; shift 2 ;;
    --bottle-domain) flag_set HOMEBREW_BOTTLE_DOMAIN "${2:?}"; shift 2 ;;
    --api-domain) flag_set HOMEBREW_API_DOMAIN "${2:?}"; shift 2 ;;
    --registry-token) flag_set HOMEBREW_DOCKER_REGISTRY_TOKEN "${2:?}"; shift 2 ;;
    --brew-git-remote) flag_set HOMEBREW_BREW_GIT_REMOTE "${2:?}"; shift 2 ;;
    --core-git-remote) flag_set HOMEBREW_CORE_GIT_REMOTE "${2:?}"; shift 2 ;;
    --packages) PACKAGES="${2:?}"; shift 2 ;;
    --casks) CASKS="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "parametro invalido: $1" ;;
  esac
done

[ -n "$RC_FILE" ] || die "--rc-file vazio"

# Ordem importa: proxies primeiro, depois Homebrew.
PROXY_VARS=(HTTP_PROXY HTTPS_PROXY ALL_PROXY)
HOMEBREW_VARS=(
  HOMEBREW_ARTIFACT_DOMAIN
  HOMEBREW_BOTTLE_DOMAIN
  HOMEBREW_API_DOMAIN
  HOMEBREW_DOCKER_REGISTRY_TOKEN
  HOMEBREW_BREW_GIT_REMOTE
  HOMEBREW_CORE_GIT_REMOTE
)
SECRET_VARS=" HOMEBREW_DOCKER_REGISTRY_TOKEN "

is_secret() { case "$SECRET_VARS" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# rc_value <var>: ultimo `export VAR=...` definido no rc (sem aspas externas).
rc_value() {
  local var="$1" line
  [ -f "$RC_FILE" ] || return 0
  line="$(grep -E "^[[:space:]]*export[[:space:]]+${var}=" "$RC_FILE" 2>/dev/null | tail -n1 || true)"
  [ -n "$line" ] || return 0
  line="${line#*=}"
  line="${line%\"}"; line="${line#\"}"
  line="${line%\'}"; line="${line#\'}"
  printf '%s' "$line"
}

# resolve_value <var>: precedencia flag > ambiente > rc.
resolve_value() {
  local var="$1"
  if flag_is_set "$var"; then flag_get "$var"; return 0; fi
  if [ -n "${!var:-}" ]; then printf '%s' "${!var}"; return 0; fi
  rc_value "$var"
}

# normalize_no_proxy: mescla defaults + valor atual + extras, deduplicando.
normalize_no_proxy() {
  local current extra defaults merged
  defaults="localhost,127.0.0.1,::1,*.local"
  current="$(resolve_value NO_PROXY)"
  [ -n "$current" ] || current="${NO_PROXY:-${no_proxy:-}}"
  extra="$NO_PROXY_EXTRA"
  merged="${defaults},${current},${extra}"
  printf '%s' "$merged" \
    | tr ', ' '\n\n' \
    | awk 'NF && !seen[$0]++' \
    | paste -sd ',' -
}

# emit_block <mode>: imprime o bloco gerenciado. mode=display mascara segredos.
emit_block() {
  local mode="$1" var value masked
  printf '%s\n' "$BLOCK_BEGIN"
  printf '%s\n' "# Gerado por setup_homebrew_proxy.sh. Nao editar manualmente este bloco."

  for var in "${PROXY_VARS[@]}"; do
    value="$(resolve_value "$var")"
    if [ -n "$value" ]; then
      printf 'export %s="%s"\n' "$var" "$value"
      printf 'export %s="%s"\n' "$(printf '%s' "$var" | tr 'A-Z' 'a-z')" "$value"
    else
      printf '# export %s="http://proxy.empresa:porta"  # preencher\n' "$var"
    fi
  done

  local np
  np="$(normalize_no_proxy)"
  printf 'export NO_PROXY="%s"\n' "$np"
  printf 'export no_proxy="%s"\n' "$np"

  for var in "${HOMEBREW_VARS[@]}"; do
    value="$(resolve_value "$var")"
    if [ -n "$value" ]; then
      if [ "$mode" = "display" ] && is_secret "$var"; then
        masked="***"
        printf 'export %s="%s"\n' "$var" "$masked"
      else
        printf 'export %s="%s"\n' "$var" "$value"
      fi
    else
      printf '# export %s=""  # preencher com valor da empresa\n' "$var"
    fi
  done

  printf '%s\n' "$BLOCK_END"
}

backup_rc() {
  [ -f "$RC_FILE" ] || return 0
  local ts backup
  ts="$(date +%Y%m%d_%H%M%S)"
  backup="${RC_FILE}.bak_${ts}"
  cp "$RC_FILE" "$backup"
  log "backup: $RC_FILE -> $backup"
}

# write_block: substitui bloco existente ou anexa ao final, de forma idempotente.
write_block() {
  local tmp body
  body="$(emit_block write)"
  tmp="$(mktemp)"
  [ -f "$RC_FILE" ] || touch "$RC_FILE"

  if grep -Fq "$BLOCK_BEGIN" "$RC_FILE"; then
    awk -v b="$BLOCK_BEGIN" -v e="$BLOCK_END" -v body="$body" '
      $0 == b { print body; skip=1; next }
      $0 == e { skip=0; next }
      skip != 1 { print }
    ' "$RC_FILE" > "$tmp"
    log "bloco existente atualizado em $RC_FILE"
  else
    cat "$RC_FILE" > "$tmp"
    if [ -s "$tmp" ] && [ -n "$(tail -c1 "$tmp")" ]; then printf '\n' >> "$tmp"; fi
    printf '%s\n' "$body" >> "$tmp"
    log "bloco adicionado em $RC_FILE"
  fi
  mv "$tmp" "$RC_FILE"
}

# http_status <url>: codigo HTTP via curl respeitando o proxy resolvido.
http_status() {
  local url="$1"
  HTTPS_PROXY="$(resolve_value HTTPS_PROXY)" \
  HTTP_PROXY="$(resolve_value HTTP_PROXY)" \
  ALL_PROXY="$(resolve_value ALL_PROXY)" \
  NO_PROXY="$(normalize_no_proxy)" \
    curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$url" 2>/dev/null || printf '000'
}

doctor() {
  command -v curl >/dev/null 2>&1 || die "curl nao encontrado para diagnostico"
  local api artifact api_url artifact_url s_api s_art

  api="$(resolve_value HOMEBREW_API_DOMAIN)"
  artifact="$(resolve_value HOMEBREW_ARTIFACT_DOMAIN)"
  api_url="${api:-https://formulae.brew.sh/api}"
  artifact_url="${artifact:-https://ghcr.io/v2/}"

  log "diagnostico de conectividade Homebrew"
  log "API domain:      ${api_url}"
  log "Artifact domain: ${artifact_url}"

  s_api="$(http_status "$api_url")"
  s_art="$(http_status "$artifact_url")"
  log "status API:      ${s_api}"
  log "status Artifact: ${s_art}"

  if [ "$s_art" = "403" ]; then
    log "403 no artifact domain: provavel token ausente/invalido."
    log "  -> defina HOMEBREW_DOCKER_REGISTRY_TOKEN (e HOMEBREW_ARTIFACT_DOMAIN do mirror)."
  fi
  if [ "$s_art" = "401" ] && [ -z "$artifact" ]; then
    log "401 anonimo em ghcr.io e esperado; use o mirror/token da empresa para bottles."
  fi
  if [ "$s_api" = "000" ] || [ "$s_art" = "000" ]; then
    log "status 000: sem rota ate o endpoint; verifique proxy/no_proxy e o mirror interno."
  fi
}

install_brew() {
  if command -v brew >/dev/null 2>&1; then
    log "brew ja instalado: $(command -v brew)"
    return 0
  fi
  command -v curl >/dev/null 2>&1 || die "curl necessario para instalar o Homebrew"
  log "instalando Homebrew via instalador oficial (com proxy)"
  HTTPS_PROXY="$(resolve_value HTTPS_PROXY)" \
  HTTP_PROXY="$(resolve_value HTTP_PROXY)" \
  ALL_PROXY="$(resolve_value ALL_PROXY)" \
  NO_PROXY="$(normalize_no_proxy)" \
  HOMEBREW_ARTIFACT_DOMAIN="$(resolve_value HOMEBREW_ARTIFACT_DOMAIN)" \
  HOMEBREW_BOTTLE_DOMAIN="$(resolve_value HOMEBREW_BOTTLE_DOMAIN)" \
  HOMEBREW_API_DOMAIN="$(resolve_value HOMEBREW_API_DOMAIN)" \
  HOMEBREW_DOCKER_REGISTRY_TOKEN="$(resolve_value HOMEBREW_DOCKER_REGISTRY_TOKEN)" \
    /bin/bash -c \
    "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
}

install_packages() {
  command -v brew >/dev/null 2>&1 || die "brew nao encontrado; use --install-brew antes"
  log "instalando formulae: $PACKAGES"
  # shellcheck disable=SC2086
  HTTPS_PROXY="$(resolve_value HTTPS_PROXY)" \
  HTTP_PROXY="$(resolve_value HTTP_PROXY)" \
  ALL_PROXY="$(resolve_value ALL_PROXY)" \
  NO_PROXY="$(normalize_no_proxy)" \
  HOMEBREW_ARTIFACT_DOMAIN="$(resolve_value HOMEBREW_ARTIFACT_DOMAIN)" \
  HOMEBREW_BOTTLE_DOMAIN="$(resolve_value HOMEBREW_BOTTLE_DOMAIN)" \
  HOMEBREW_API_DOMAIN="$(resolve_value HOMEBREW_API_DOMAIN)" \
  HOMEBREW_DOCKER_REGISTRY_TOKEN="$(resolve_value HOMEBREW_DOCKER_REGISTRY_TOKEN)" \
    brew install $PACKAGES
  if [ -n "$CASKS" ]; then
    log "instalando casks: $CASKS"
    # shellcheck disable=SC2086
    HTTPS_PROXY="$(resolve_value HTTPS_PROXY)" \
    HTTP_PROXY="$(resolve_value HTTP_PROXY)" \
    ALL_PROXY="$(resolve_value ALL_PROXY)" \
    NO_PROXY="$(normalize_no_proxy)" \
    HOMEBREW_ARTIFACT_DOMAIN="$(resolve_value HOMEBREW_ARTIFACT_DOMAIN)" \
    HOMEBREW_BOTTLE_DOMAIN="$(resolve_value HOMEBREW_BOTTLE_DOMAIN)" \
    HOMEBREW_API_DOMAIN="$(resolve_value HOMEBREW_API_DOMAIN)" \
    HOMEBREW_DOCKER_REGISTRY_TOKEN="$(resolve_value HOMEBREW_DOCKER_REGISTRY_TOKEN)" \
      brew install --cask $CASKS
  fi
}

main() {
  if [ "$DOCTOR" = "1" ]; then
    doctor
    exit 0
  fi

  log "rc alvo: $RC_FILE"
  if [ "$APPLY" = "1" ]; then
    backup_rc
    write_block
    log "aplicado. rode: source \"$RC_FILE\""
  else
    log "modo analise (dry-run). Bloco que SERIA aplicado (--apply para gravar):"
    printf '%s\n' "----------------------------------------"
    emit_block display
    printf '%s\n' "----------------------------------------"
  fi

  [ "$INSTALL_BREW" = "1" ] && install_brew
  [ "$INSTALL_PACKAGES" = "1" ] && install_packages
  exit 0
}

main
