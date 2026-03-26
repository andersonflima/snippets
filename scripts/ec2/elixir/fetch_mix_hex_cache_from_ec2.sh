#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[fetch-mix-hex-cache] erro: bash é obrigatório para copiar cache de Elixir do EC2\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[fetch-mix-hex-cache] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/elixir/fetch_mix_hex_cache_from_ec2.sh [opções]

Opções:
  --host <host>               Hostname ou IP do EC2. Obrigatório.
  --user <user>               Usuário SSH. Padrão: ec2-user
  --identity <arquivo>        Chave privada SSH.
  --port <porta>              Porta SSH. Padrão: 22
  --destination <dir>         Diretório local de destino.
                              Padrão: $HOME/.cache/elixir-ec2-import
  --project-path <dir>        Caminho remoto do projeto Elixir no EC2.
  --mode <modo>               Modo: archives, home, project, all
                              Padrão: all quando --project-path existe, senão home
  --ssh-option <opção>        Opção extra para ssh. Repetível.
  -h, --help                  Mostra esta ajuda.

Estrutura local gerada:
  <destination>/home/.mix
  <destination>/home/.hex
  <destination>/project/deps
  <destination>/project/_build
  <destination>/project/mix.lock

Exemplos:
  sh scripts/fetch_mix_hex_cache_from_ec2.sh \
    --host 10.0.0.10 \
    --identity ~/.ssh/minha-chave.pem

  sh scripts/fetch_mix_hex_cache_from_ec2.sh \
    --host 10.0.0.10 \
    --identity ~/.ssh/minha-chave.pem \
    --project-path /home/ec2-user/app \
    --mode all
USAGE
}

HOST=""
SSH_USER="ec2-user"
SSH_IDENTITY=""
SSH_PORT="22"
DESTINATION_DIR="${HOME}/.cache/elixir-ec2-import"
PROJECT_PATH=""
MODE=""
SSH_OPTIONS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --user)
      SSH_USER="${2:-}"
      shift 2
      ;;
    --identity)
      SSH_IDENTITY="${2:-}"
      shift 2
      ;;
    --port)
      SSH_PORT="${2:-}"
      shift 2
      ;;
    --destination)
      DESTINATION_DIR="${2:-}"
      shift 2
      ;;
    --project-path)
      PROJECT_PATH="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --ssh-option)
      SSH_OPTIONS+=("${2:-}")
      shift 2
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

[[ -n "${HOST}" ]] || die "--host é obrigatório"
[[ -n "${SSH_USER}" ]] || die "--user não pode ser vazio"
[[ -n "${DESTINATION_DIR}" ]] || die "--destination não pode ser vazio"

if [[ -z "${MODE}" ]]; then
  if [[ -n "${PROJECT_PATH}" ]]; then
    MODE="all"
  else
    MODE="home"
  fi
fi

case "${MODE}" in
  archives|home|project|all)
    ;;
  *)
    die "--mode inválido: ${MODE}"
    ;;
esac

if [[ ("${MODE}" == "project" || "${MODE}" == "all") && -z "${PROJECT_PATH}" ]]; then
  die "--project-path é obrigatório quando --mode é project ou all"
fi

SSH_CMD=(ssh -p "${SSH_PORT}" -o BatchMode=yes)

if [[ -n "${SSH_IDENTITY}" ]]; then
  SSH_CMD+=(-i "${SSH_IDENTITY}")
fi

if (( ${#SSH_OPTIONS[@]} > 0 )); then
  for ssh_option in "${SSH_OPTIONS[@]}"; do
    SSH_CMD+=(-o "${ssh_option}")
  done
fi

SSH_CMD+=("${SSH_USER}@${HOST}")

mkdir -p "${DESTINATION_DIR}/home" "${DESTINATION_DIR}/project"

fetch_remote_tarball() {
  local remote_script local_extract_dir
  remote_script="$1"
  local_extract_dir="$2"

  mkdir -p "${local_extract_dir}"
  "${SSH_CMD[@]}" "bash -s" <<EOF | tar -xzf - -C "${local_extract_dir}"
set -euo pipefail
${remote_script}
EOF
}

fetch_home_cache() {
  log "copiando ~/.mix e ~/.hex do EC2"
  fetch_remote_tarball '
home_dir="${HOME}"
entries=()

if [[ -d "${home_dir}/.mix" ]]; then
  entries+=(".mix")
fi

if [[ -d "${home_dir}/.hex" ]]; then
  entries+=(".hex")
fi

if (( ${#entries[@]} == 0 )); then
  printf "[fetch-mix-hex-cache] remoto sem ~/.mix e ~/.hex\n" >&2
  exit 1
fi

tar -czf - -C "${home_dir}" "${entries[@]}"
' "${DESTINATION_DIR}/home"
}

fetch_archives_only() {
  log "copiando ~/.mix/archives do EC2"
  fetch_remote_tarball '
home_dir="${HOME}"
[[ -d "${home_dir}/.mix/archives" ]] || {
  printf "[fetch-mix-hex-cache] remoto sem ~/.mix/archives\n" >&2
  exit 1
}

tar -czf - -C "${home_dir}" .mix/archives
' "${DESTINATION_DIR}/home"
}

fetch_project_cache() {
  local remote_project_path
  remote_project_path="$1"

  log "copiando deps/_build/mix.lock do projeto remoto ${remote_project_path}"
  fetch_remote_tarball "
project_path=$(printf '%q' "${remote_project_path}")
cd \"\${project_path}\"
entries=()

if [[ -d deps ]]; then
  entries+=(deps)
fi

if [[ -d _build ]]; then
  entries+=(_build)
fi

if [[ -f mix.lock ]]; then
  entries+=(mix.lock)
fi

if (( \${#entries[@]} == 0 )); then
  printf '[fetch-mix-hex-cache] projeto remoto sem deps/_build/mix.lock\n' >&2
  exit 1
fi

tar -czf - \"\${entries[@]}\"
" "${DESTINATION_DIR}/project"
}

case "${MODE}" in
  home)
    fetch_home_cache
    ;;
  archives)
    fetch_archives_only
    ;;
  project)
    fetch_project_cache "${PROJECT_PATH}"
    ;;
  all)
    fetch_home_cache
    fetch_project_cache "${PROJECT_PATH}"
    ;;
esac

cat <<EOF
Importação concluída.

Destino local:
  ${DESTINATION_DIR}

Conteúdo esperado:
  ${DESTINATION_DIR}/home/.mix
  ${DESTINATION_DIR}/home/.hex
  ${DESTINATION_DIR}/project/deps
  ${DESTINATION_DIR}/project/_build
  ${DESTINATION_DIR}/project/mix.lock
EOF
