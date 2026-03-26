#!/bin/sh
[ -n "${BASH_VERSION:-}" ] || {
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi

  printf '[mix-via-ec2] erro: bash é obrigatório para executar mix via EC2\n' >&2
  exit 1
}

set -euo pipefail

log() {
  printf '[mix-via-ec2] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/elixir/mix_via_ec2.sh (--host <host> | --instance-name <nome>) [opções] -- <mix args>

Opções:
  --host <host>               Hostname ou IP do EC2.
  --instance-name <nome>      Resolve o EC2 automaticamente por tag Name via AWS CLI.
  --aws-profile <profile>     Profile AWS para resolução automática do EC2.
  --aws-region <region>       Region AWS para resolução automática do EC2.
  --user <user>               Usuário SSH. Padrão: ec2-user
  --identity <arquivo>        Chave privada SSH.
  --port <porta>              Porta SSH. Padrão: 22
  --local-project-path <dir>  Projeto local. Padrão: diretório atual
  --remote-project-path <dir> Projeto remoto fixo. Padrão: ~/.cache/mix-via-ec2/workspaces/<nome>
  --cache-root <dir>          Cache local gerenciado. Padrão: $HOME/.cache/mix-via-ec2/<host>
  --sync-build                Traz _build de volta também.
  --no-sync-home-cache        Não traz ~/.mix ~/.hex ~/.cache/rebar3 do EC2.
  --ssh-option <opção>        Opção extra para ssh. Repetível.
  -h, --help                  Mostra esta ajuda.

Exemplos:
  sh scripts/mix_via_ec2.sh \
    --instance-name Dander \
    --aws-region us-east-1 \
    --identity ~/.ssh/minha-chave.pem \
    -- deps.get

  sh scripts/mix_via_ec2.sh \
    --instance-name Dander \
    --aws-region us-east-1 \
    --identity ~/.ssh/minha-chave.pem \
    -- archive.install hex phx_new --force
USAGE
}

HOST=""
INSTANCE_NAME="${MIX_VIA_EC2_INSTANCE_NAME:-}"
AWS_PROFILE_NAME="${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}"
AWS_REGION_NAME="${MIX_VIA_EC2_AWS_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
SSH_USER="${MIX_VIA_EC2_SSH_USER:-ec2-user}"
SSH_IDENTITY="${MIX_VIA_EC2_SSH_IDENTITY:-}"
SSH_PORT="22"
LOCAL_PROJECT_PATH="$(pwd)"
REMOTE_PROJECT_PATH="${MIX_VIA_EC2_REMOTE_PROJECT_PATH:-}"
SYNC_BUILD="0"
SYNC_HOME_CACHE="1"
SSH_OPTIONS=()
MIX_ARGS=()
CACHE_ROOT="${MIX_VIA_EC2_CACHE_ROOT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --instance-name)
      INSTANCE_NAME="${2:-}"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE_NAME="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION_NAME="${2:-}"
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
    --local-project-path)
      LOCAL_PROJECT_PATH="${2:-}"
      shift 2
      ;;
    --remote-project-path)
      REMOTE_PROJECT_PATH="${2:-}"
      shift 2
      ;;
    --cache-root)
      CACHE_ROOT="${2:-}"
      shift 2
      ;;
    --sync-build)
      SYNC_BUILD="1"
      shift
      ;;
    --no-sync-home-cache)
      SYNC_HOME_CACHE="0"
      shift
      ;;
    --ssh-option)
      SSH_OPTIONS+=("${2:-}")
      shift 2
      ;;
    --)
      shift
      MIX_ARGS=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      MIX_ARGS=("$@")
      break
      ;;
  esac
done

if [[ -z "${HOST}" && -z "${INSTANCE_NAME}" ]]; then
  die "informe --host ou --instance-name"
fi
[[ -n "${SSH_USER}" ]] || die "--user não pode ser vazio"
[[ -n "${LOCAL_PROJECT_PATH}" ]] || die "--local-project-path não pode ser vazio"
(( ${#MIX_ARGS[@]} > 0 )) || die "informe o comando do mix após --"

LOCAL_PROJECT_PATH="$(cd "${LOCAL_PROJECT_PATH}" && pwd)"
[[ -d "${LOCAL_PROJECT_PATH}" ]] || die "projeto local não encontrado: ${LOCAL_PROJECT_PATH}"

PROJECT_NAME="$(basename "${LOCAL_PROJECT_PATH}")"
if [[ -z "${REMOTE_PROJECT_PATH}" ]]; then
  REMOTE_PROJECT_PATH="~/.cache/mix-via-ec2/workspaces/${PROJECT_NAME}"
fi

if [[ -z "${CACHE_ROOT}" ]]; then
  CACHE_ROOT="${HOME}/.cache/mix-via-ec2/${HOST:-${INSTANCE_NAME}}"
fi

MANAGED_HOME_DIR="${CACHE_ROOT}/home"
MANAGED_METADATA_DIR="${CACHE_ROOT}/metadata"
MANAGED_ENV_FILE="${CACHE_ROOT}/env.sh"

mkdir -p "${MANAGED_HOME_DIR}" "${MANAGED_METADATA_DIR}"

SSH_BASE_CMD=(ssh -p "${SSH_PORT}" -o BatchMode=yes)

if [[ -n "${SSH_IDENTITY}" ]]; then
  SSH_BASE_CMD+=(-i "${SSH_IDENTITY}")
fi

if (( ${#SSH_OPTIONS[@]} > 0 )); then
  for ssh_option in "${SSH_OPTIONS[@]}"; do
    SSH_BASE_CMD+=(-o "${ssh_option}")
  done
fi

resolve_host_from_aws() {
  local aws_cmd output line_count selected_line resolved_host resolved_instance_id
  [[ -n "${INSTANCE_NAME}" ]] || return 1

  command -v aws >/dev/null 2>&1 || die "aws CLI não encontrado no PATH para resolver o EC2 automaticamente"

  aws_cmd=(aws)
  if [[ -n "${AWS_PROFILE_NAME}" ]]; then
    aws_cmd+=(--profile "${AWS_PROFILE_NAME}")
  fi
  if [[ -n "${AWS_REGION_NAME}" ]]; then
    aws_cmd+=(--region "${AWS_REGION_NAME}")
  fi

  output="$("${aws_cmd[@]}" ec2 describe-instances \
    --filters "Name=tag:Name,Values=${INSTANCE_NAME}" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].[InstanceId,PublicDnsName,PublicIpAddress,PrivateIpAddress]' \
    --output text)"

  [[ -n "${output}" ]] || die "nenhuma instância running encontrada com tag Name=${INSTANCE_NAME}"

  line_count="$(printf '%s\n' "${output}" | awk 'NF {count++} END {print count+0}')"
  if [[ "${line_count}" -gt 1 ]]; then
    printf '[mix-via-ec2] múltiplas instâncias encontradas para Name=%s:\n%s\n' "${INSTANCE_NAME}" "${output}" >&2
    die "refine a busca ou informe --host explicitamente"
  fi

  selected_line="$(printf '%s\n' "${output}" | awk 'NF {print; exit}')"
  resolved_instance_id="$(printf '%s\n' "${selected_line}" | awk '{print $1}')"
  resolved_host="$(printf '%s\n' "${selected_line}" | awk '{print $2}')"
  if [[ -z "${resolved_host}" || "${resolved_host}" == "None" ]]; then
    resolved_host="$(printf '%s\n' "${selected_line}" | awk '{print $3}')"
  fi
  if [[ -z "${resolved_host}" || "${resolved_host}" == "None" ]]; then
    resolved_host="$(printf '%s\n' "${selected_line}" | awk '{print $4}')"
  fi

  [[ -n "${resolved_host}" && "${resolved_host}" != "None" ]] || die "não foi possível resolver DNS/IP para a instância ${resolved_instance_id}"

  HOST="${resolved_host}"
  log "instância ${INSTANCE_NAME} resolvida via AWS: ${resolved_instance_id} -> ${HOST}"
}

if [[ -z "${HOST}" ]]; then
  resolve_host_from_aws
fi

SSH_CMD=("${SSH_BASE_CMD[@]}" "${SSH_USER}@${HOST}")

mix_command_type() {
  local first second
  first="${MIX_ARGS[0]}"
  second="${MIX_ARGS[1]:-}"

  case "${first}" in
    local.hex|local.rebar)
      printf 'home\n'
      return 0
      ;;
    archive.install|archive.build|archive)
      printf 'home\n'
      return 0
      ;;
    hex.info|hex.search|hex.outdated|hex.audit|hex)
      printf 'home\n'
      return 0
      ;;
    deps.get|deps.compile|deps|compile|test|phx.server|phx.new)
      printf 'project\n'
      return 0
      ;;
  esac
  printf 'project\n'
}

COMMAND_TYPE="$(mix_command_type)"

if [[ "${COMMAND_TYPE}" == "project" ]] && [[ ! -f "${LOCAL_PROJECT_PATH}/mix.exs" ]]; then
  die "mix.exs não encontrado em ${LOCAL_PROJECT_PATH}"
fi

sync_project_to_remote() {
  local remote_parent remote_project
  remote_project="$1"
  remote_parent="${remote_project%/*}"

  log "sincronizando fontes locais para o EC2 em ${remote_project}"

  tar -czf - \
    --exclude=.git \
    --exclude=deps \
    --exclude=_build \
    --exclude=.elixir_ls \
    --exclude=node_modules \
    -C "${LOCAL_PROJECT_PATH}" . | \
    "${SSH_CMD[@]}" "bash -lc 'set -euo pipefail; mkdir -p ${remote_parent@Q}; rm -rf ${remote_project@Q}; mkdir -p ${remote_project@Q}; tar -xzf - -C ${remote_project@Q}'"
}

run_remote_mix() {
  local remote_project remote_mix_args_literal
  remote_project="$1"
  remote_mix_args_literal="$(printf '%q ' "${MIX_ARGS[@]}")"

  log "executando mix no EC2: mix ${MIX_ARGS[*]}"

  "${SSH_CMD[@]}" "bash -lc 'set -euo pipefail; cd ${remote_project@Q}; mix ${remote_mix_args_literal}'"
}

sync_remote_project_back() {
  local remote_project include_build
  remote_project="$1"
  include_build="$2"

  log "trazendo deps e mix.lock do EC2"

  "${SSH_CMD[@]}" "bash -lc '
set -euo pipefail
cd ${remote_project@Q}
entries=()
if [[ -d deps ]]; then entries+=(deps); fi
if [[ -f mix.lock ]]; then entries+=(mix.lock); fi
if [[ ${include_build@Q} == 1 ]] && [[ -d _build ]]; then entries+=(_build); fi
if (( \${#entries[@]} == 0 )); then
  printf \"[mix-via-ec2] remoto sem deps/mix.lock/_build\\n\" >&2
  exit 1
fi
tar -czf - \"\${entries[@]}\"
'" | tar -xzf - -C "${LOCAL_PROJECT_PATH}"
}

sync_remote_home_cache_back() {
  log "trazendo ~/.mix ~/.hex e cache do rebar3 do EC2"

  "${SSH_CMD[@]}" "bash -lc '
set -euo pipefail
cd \"\${HOME}\"
entries=()
if [[ -d .mix ]]; then entries+=(.mix); fi
if [[ -d .hex ]]; then entries+=(.hex); fi
if [[ -d .cache/rebar3 ]]; then entries+=(.cache/rebar3); fi
if [[ -d .config/rebar3 ]]; then entries+=(.config/rebar3); fi
if (( \${#entries[@]} == 0 )); then
  printf \"[mix-via-ec2] remoto sem ~/.mix ~/.hex cache do rebar3\\n\" >&2
  exit 1
fi
tar -czf - \"\${entries[@]}\"
'" | tar -xzf - -C "${MANAGED_HOME_DIR}"
}

write_managed_env_file() {
  mkdir -p "${CACHE_ROOT}"
  cat > "${MANAGED_ENV_FILE}" <<EOF
#!/usr/bin/env sh
export MIX_HOME=$(printf '%q' "${MANAGED_HOME_DIR}/.mix")
export HEX_HOME=$(printf '%q' "${MANAGED_HOME_DIR}/.hex")
EOF
  chmod 0644 "${MANAGED_ENV_FILE}"
}

fetch_remote_metadata() {
  "${SSH_CMD[@]}" "bash -s" <<'EOF' > "${MANAGED_METADATA_DIR}/runtime.txt"
set -euo pipefail
printf 'uname=%s\n' "$(uname -a 2>/dev/null || true)"
printf 'arch=%s\n' "$(uname -m 2>/dev/null || true)"
printf 'elixir=%s\n' "$(elixir --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]$//')"
printf 'mix=%s\n' "$(mix --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]$//')"
printf 'erlang=%s\n' "$(erl -eval 'io:format(\"~s\", [erlang:system_info(system_architecture)]), halt().' -noshell 2>/dev/null || true)"
EOF
}

fetch_remote_metadata

if [[ "${COMMAND_TYPE}" == "project" ]]; then
  sync_project_to_remote "${REMOTE_PROJECT_PATH}"
fi

run_remote_mix "${REMOTE_PROJECT_PATH}"

if [[ "${COMMAND_TYPE}" == "project" ]]; then
  sync_remote_project_back "${REMOTE_PROJECT_PATH}" "${SYNC_BUILD}"
fi

if [[ "${SYNC_HOME_CACHE}" == "1" ]]; then
  sync_remote_home_cache_back
  write_managed_env_file
fi

cat <<EOF
Execução remota concluída.

Comando:
  mix ${MIX_ARGS[*]}

Cache gerenciado:
  ${CACHE_ROOT}

Metadados:
  ${MANAGED_METADATA_DIR}/runtime.txt
EOF

if [[ "${SYNC_HOME_CACHE}" == "1" ]]; then
  cat <<EOF

Para usar o cache gerenciado localmente:
  . ${MANAGED_ENV_FILE}
EOF
fi

if [[ "${SYNC_BUILD}" != "1" ]]; then
  cat <<'EOF'

Observação:
  _build não foi trazido. Isso é intencional por compatibilidade.
  Use --sync-build só quando a máquina local e o EC2 tiverem stack compatível.
EOF
fi
