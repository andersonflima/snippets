#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[install-go-ec2] %s\n' "$*"
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  install_go_ec2.sh [--version <latest|vX.Y.Z>] [--install-dir /usr/local/go] [--profile /etc/profile.d/go.sh] [--force]

Exemplos:
  install_go_ec2.sh
  install_go_ec2.sh --version 1.25.0
  install_go_ec2.sh --version latest --install-dir /opt/go

Observação:
  - Por padrão instala a última versão estável (latest)
  - Requer privilégios de sudo quando instalar em /usr/local/go
USAGE
}

GO_VERSION="latest"
GO_INSTALL_DIR="/usr/local/go"
GO_PROFILE_PATH="/etc/profile.d/go.sh"
FORCE_REINSTALL=0
HTTP_TIMEOUT=15

LATEST_FALLBACK_URLS=(
  "https://go.dev/dl/?mode=json"
  "https://golang.org/dl/?mode=json"
  "https://api.github.com/repos/golang/go/releases/latest"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      GO_VERSION="${2:-}"
      shift 2
      ;;
    --install-dir)
      GO_INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --profile)
      GO_PROFILE_PATH="${2:-}"
      shift 2
      ;;
    --force)
      FORCE_REINSTALL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "opção inválida: $1"
      ;;
  esac
done

[[ -n "${GO_VERSION}" ]] || die "--version não pode ficar vazio"
[[ -n "${GO_INSTALL_DIR}" ]] || die "--install-dir não pode ficar vazio"
[[ -n "${GO_PROFILE_PATH}" ]] || die "--profile não pode ficar vazio"

if [[ "${GO_INSTALL_DIR}" != /* ]]; then
  die "--install-dir precisa ser um caminho absoluto"
fi

SUDO_CMD=()
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo não encontrado. Execute como root ou instale sudo."
  SUDO_CMD=(sudo)
fi

run_with_sudo() {
  "${SUDO_CMD[@]}" "$@"
}

detect_distro() {
  [[ -f /etc/os-release ]] || die "/etc/os-release não encontrado"
  # shellcheck disable=SC1091
  source /etc/os-release
  printf '%s\n' "${ID:-unknown}"
}

install_prereqs() {
  local distro
  distro="$(detect_distro)"

  case "${distro}" in
    amzn|amzn2|amazon|alinux*)
      if command -v dnf >/dev/null 2>&1; then
        run_with_sudo dnf makecache -y
        run_with_sudo dnf install -y tar gzip curl ca-certificates
        return
      fi
      if command -v yum >/dev/null 2>&1; then
        run_with_sudo yum install -y tar gzip curl ca-certificates
        return
      fi
      ;;
    ubuntu|debian)
      run_with_sudo apt-get update
      run_with_sudo apt-get install -y tar gzip ca-certificates curl
      return
      ;;
    *)
      die "distribuição não suportada para instalação automática de dependências: ${distro}"
      ;;
  esac

  die "não foi possível instalar dependências automaticamente para ${distro}"
}

ensure_downloader() {
  if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
    return
  fi

  install_prereqs

  if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
    return
  fi

  die "não foi possível garantir curl ou wget para baixar artefatos Go"
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      echo amd64
      ;;
    aarch64|arm64)
      echo arm64
      ;;
    armv6l)
      echo armv6l
      ;;
    armv7l)
      echo armv7l
      ;;
    *)
      die "arquitetura não suportada: $(uname -m). Use AMD64 ou ARM64"
      ;;
  esac
}

fetch_latest_version() {
  local json
  local url
  local version

  for url in "${LATEST_FALLBACK_URLS[@]}"; do
    if [[ "${DOWNLOADER}" == "curl" ]]; then
      json="$(curl -fsSL --max-time "${HTTP_TIMEOUT}" --retry 2 --retry-delay 1 "${url}" || true)"
    else
      json="$(wget -qO- --timeout="${HTTP_TIMEOUT}" --tries=2 "${url}" || true)"
    fi

    if [[ -z "${json}" ]]; then
      attempts+=("${url}")
      continue
    fi

    if [[ "${url}" == *"api.github.com"* ]]; then
      version="$(printf '%s' "${json}" | grep -o '"tag_name":"go[0-9][^"]*"' | head -n 1 | cut -d'"' -f4)"
    else
      version="$(printf '%s' "${json}" | grep -o '"version":"go[0-9][^"]*"' | head -n 1 | cut -d'"' -f4)"
    fi

    if [[ -n "${version}" ]]; then
      echo "${version}"
      return 0
    fi
  done

  log "falha ao consultar versões do Go nos endpoints:"
  log "  ${LATEST_FALLBACK_URLS[*]}"
  return 1
}

normalize_version() {
  local requested="$1"

  if [[ "${requested}" == "latest" ]]; then
    fetch_latest_version
    return
  fi

  if [[ "${requested}" == v* ]]; then
    echo "go${requested#v}"
    return
  fi

  if [[ "${requested}" == go* ]]; then
    echo "${requested}"
    return
  fi

  echo "go${requested}"
}

current_go_version() {
  local bin="${GO_INSTALL_DIR}/bin/go"
  if [[ -x "${bin}" ]]; then
    "${bin}" version 2>/dev/null | awk '{print $3}'
    return
  fi
  echo ""
}

write_profile() {
  local go_home_escaped
  go_home_escaped="$(printf '%q' "${GO_INSTALL_DIR}")"

  local profile_content
  profile_content="# Go environment\nexport GOROOT=${go_home_escaped}\nexport PATH=\"\\${GOROOT}/bin:\$PATH\"\n"

  if [[ ${#SUDO_CMD[@]} -eq 0 ]]; then
    PROFILE_TARGET="${HOME}/.goenv"
    printf '%b' "${profile_content}" > "${PROFILE_TARGET}"
    if [[ -f "${HOME}/.bashrc" && "$(grep -Fx "source ${PROFILE_TARGET}" "${HOME}/.bashrc" || true)" == "" ]]; then
      printf '\n# Go environment\nsource "%s"\n' "${PROFILE_TARGET}" >> "${HOME}/.bashrc"
    fi
    log "snippet do Go salvo em ${PROFILE_TARGET}"
    return
  fi

  run_with_sudo install -d "$(dirname "${GO_PROFILE_PATH}")"
  run_with_sudo tee "${GO_PROFILE_PATH}" >/dev/null <<EOF
${profile_content}
EOF
  run_with_sudo chmod 0644 "${GO_PROFILE_PATH}"
  log "profile salvo em ${GO_PROFILE_PATH}"
}

install_go() {
  local archive_name
  local arch
  local url
  local download_path
  local temp_dir
  local extracted_dir

  local resolved_version
  local current_version

  ensure_downloader

  resolved_version="$(normalize_version "${GO_VERSION}")"
  if [[ -z "${resolved_version}" ]]; then
    die "não foi possível resolver a versão do Go. Verifique conexão e URL"
  fi

  arch="$(detect_arch)"
  archive_name="${resolved_version}.linux-${arch}.tar.gz"
  url="https://go.dev/dl/${archive_name}"

  current_version="$(current_go_version)"
  if [[ "${current_version}" == "${resolved_version}" && ${FORCE_REINSTALL} -eq 0 ]]; then
    log "Go ${resolved_version} já está instalado em ${GO_INSTALL_DIR}"
    return
  fi

  log "baixando ${archive_name}"
  download_path="$(mktemp /tmp/${archive_name}.XXXXXX)"

  if [[ "${DOWNLOADER}" == "curl" ]]; then
    curl -fsSL "${url}" -o "${download_path}"
  else
    wget -qO "${download_path}" "${url}"
  fi

  temp_dir="$(mktemp -d)"

  if [[ ${FORCE_REINSTALL} -eq 1 && -d "${GO_INSTALL_DIR}" ]]; then
    run_with_sudo rm -rf "${GO_INSTALL_DIR}"
  fi

  run_with_sudo mkdir -p "$(dirname "${GO_INSTALL_DIR}")"
  run_with_sudo tar -xzf "${download_path}" -C "${temp_dir}"

  extracted_dir="${temp_dir}/go"
  if [[ ! -d "${extracted_dir}" ]]; then
    rm -f "${download_path}"
    run_with_sudo rm -rf "${temp_dir}"
    die "estrutura inesperada no pacote baixado"
  fi

  run_with_sudo rm -rf "${GO_INSTALL_DIR}"
  run_with_sudo mv "${extracted_dir}" "${GO_INSTALL_DIR}"

  if [[ ${#SUDO_CMD[@]} -eq 0 ]]; then
    log "Go instalado em ${GO_INSTALL_DIR}"
  else
    run_with_sudo ln -sf "${GO_INSTALL_DIR}/bin/go" /usr/local/bin/go
    run_with_sudo ln -sf "${GO_INSTALL_DIR}/bin/gofmt" /usr/local/bin/gofmt
  fi

  rm -f "${download_path}"
  run_with_sudo rm -rf "${temp_dir}"
  write_profile
}

validate_install() {
  if [[ ! -x "${GO_INSTALL_DIR}/bin/go" ]]; then
    die "instalação falhou: binário go não encontrado em ${GO_INSTALL_DIR}/bin/go"
  fi

  local installed
  installed="$(${GO_INSTALL_DIR}/bin/go version)"
  log "instalado: ${installed}"
}

main() {
  install_prereqs
  install_go
  validate_install

  log "instalação concluída"
  log "para carregar Go nesta sessão atual:"
  log "  source ${GO_PROFILE_PATH}"
  log "para carregar automaticamente nos próximos logins (root):"
  log "  reinicie o shell"
}

main "$@"
