#!/usr/bin/env bash
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
  --transport <modo>          auto, ssh ou ssm. Padrão: auto
  --aws-profile <profile>     Profile AWS para resolução automática do EC2.
  --aws-region <region>       Region AWS para resolução automática do EC2.
  --s3-bucket <bucket>        Bucket usado no modo SSM para sincronização de artefatos.
  --s3-prefix <prefixo>       Prefixo do bucket no modo SSM. Padrão: mix-via-ec2
  --user <user>               Usuário SSH. Padrão: ec2-user
  --identity <arquivo>        Chave privada SSH.
  --port <porta>              Porta SSH. Padrão: 22
  --local-project-path <dir>  Projeto local. Padrão: diretório atual
  --remote-project-path <dir> Projeto remoto fixo. Padrão: ~/.cache/mix-via-ec2/workspaces/<nome>
  --cache-root <dir>          Cache local gerenciado. Padrão: $HOME/.cache/mix-via-ec2/<instância>
  --sync-build                Traz _build de volta também.
  --no-sync-home-cache        Não traz ~/.mix ~/.hex ~/.cache/rebar3 do EC2.
  --ssh-option <opção>        Opção extra para ssh. Repetível.
  -h, --help                  Mostra esta ajuda.

Exemplos:
  sh scripts/mix_via_ec2.sh \
    --instance-name Dander \
    --aws-region sa-east-1 \
    -- deps.get

  sh scripts/mix_via_ec2.sh \
    --instance-name Dander \
    --aws-region sa-east-1 \
    -- archive.install hex phx_new --force
USAGE
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "comando obrigatório não encontrado no PATH: $1"
}

is_truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

random_suffix() {
  local suffix
  if command -v openssl >/dev/null 2>&1; then
    suffix="$(openssl rand -hex 4 2>/dev/null || true)"
  fi

  if [[ -z "${suffix:-}" ]]; then
    suffix="$(date +%s)"
  fi

  printf '%s\n' "${suffix}"
}

shell_quote() {
  printf '%q' "$1"
}

make_temp_file() {
  local prefix extension temp_path target_path
  prefix="$1"
  extension="${2:-}"

  if temp_path="$(mktemp "/tmp/${prefix}.XXXXXX" 2>/dev/null)"; then
    :
  elif temp_path="$(mktemp -t "${prefix}" 2>/dev/null)"; then
    :
  else
    die "não foi possível criar arquivo temporário para ${prefix}"
  fi

  if [[ -n "${extension}" ]]; then
    target_path="${temp_path}${extension}"
    mv "${temp_path}" "${target_path}" || die "não foi possível renomear arquivo temporário para ${target_path}"
    temp_path="${target_path}"
  fi

  printf '%s\n' "${temp_path}"
}

HOST=""
INSTANCE_ID=""
INSTANCE_PROFILE_ARN=""
INSTANCE_ROLE_ARN=""
INSTANCE_REMOTE_PRINCIPAL_ARN=""
INSTANCE_NAME="${MIX_VIA_EC2_INSTANCE_NAME:-}"
TRANSPORT="${MIX_VIA_EC2_TRANSPORT:-auto}"
AWS_PROFILE_NAME="${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}"
AWS_REGION_NAME="${MIX_VIA_EC2_AWS_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
S3_BUCKET="${MIX_VIA_EC2_S3_BUCKET:-}"
S3_PREFIX="${MIX_VIA_EC2_S3_PREFIX:-mix-via-ec2}"
KEEP_S3_ARTIFACTS="${MIX_VIA_EC2_KEEP_S3_ARTIFACTS:-0}"
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
AWS_CMD=(aws)
AWS_CMD_CONFIGURED="0"
RUN_ID=""
RUN_S3_PREFIX=""
PROJECT_ARCHIVE_KEY=""
REMOTE_SCRIPT_KEY=""
PROJECT_RESULT_KEY=""
HOME_CACHE_KEY=""
RUNTIME_METADATA_KEY=""
STATUS_KEY=""

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
    --transport)
      TRANSPORT="${2:-}"
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
    --s3-bucket)
      S3_BUCKET="${2:-}"
      shift 2
      ;;
    --s3-prefix)
      S3_PREFIX="${2:-}"
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

case "${TRANSPORT}" in
  auto|ssh|ssm)
    ;;
  *)
    die "--transport inválido: ${TRANSPORT}. Use auto, ssh ou ssm"
    ;;
esac

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

configure_aws_cmd() {
  if [[ "${AWS_CMD_CONFIGURED}" == "1" ]]; then
    return 0
  fi

  if [[ -n "${AWS_PROFILE_NAME}" ]]; then
    AWS_CMD+=(--profile "${AWS_PROFILE_NAME}")
  fi
  if [[ -n "${AWS_REGION_NAME}" ]]; then
    AWS_CMD+=(--region "${AWS_REGION_NAME}")
  fi

  AWS_CMD_CONFIGURED="1"
}

resolve_instance_from_aws() {
  local output line_count selected_line resolved_host resolved_instance_id resolved_instance_profile_arn

  [[ -n "${INSTANCE_NAME}" ]] || return 0

  require_command aws
  configure_aws_cmd

  output="$("${AWS_CMD[@]}" ec2 describe-instances \
    --filters "Name=tag:Name,Values=${INSTANCE_NAME}" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].[InstanceId,PublicDnsName,PublicIpAddress,PrivateIpAddress,IamInstanceProfile.Arn]' \
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
  resolved_instance_profile_arn="$(printf '%s\n' "${selected_line}" | awk '{print $5}')"

  if [[ -z "${resolved_host}" || "${resolved_host}" == "None" ]]; then
    resolved_host="$(printf '%s\n' "${selected_line}" | awk '{print $3}')"
  fi
  if [[ -z "${resolved_host}" || "${resolved_host}" == "None" ]]; then
    resolved_host="$(printf '%s\n' "${selected_line}" | awk '{print $4}')"
  fi

  INSTANCE_ID="${resolved_instance_id}"
  if [[ -n "${resolved_instance_profile_arn}" && "${resolved_instance_profile_arn}" != "None" ]]; then
    INSTANCE_PROFILE_ARN="${resolved_instance_profile_arn}"
  fi
  if [[ -n "${resolved_host}" && "${resolved_host}" != "None" ]]; then
    HOST="${resolved_host}"
  fi

  log "instância ${INSTANCE_NAME} resolvida via AWS: ${INSTANCE_ID}${HOST:+ -> ${HOST}}"
}

resolve_transport() {
  if [[ "${TRANSPORT}" != "auto" ]]; then
    return 0
  fi

  if [[ -n "${INSTANCE_NAME}" ]]; then
    TRANSPORT="ssm"
    return 0
  fi

  TRANSPORT="ssh"
}

mix_command_type() {
  local first_arg
  first_arg="${MIX_ARGS[0]}"

  case "${first_arg}" in
    local.hex|local.rebar|archive.install|archive.build|archive|hex.info|hex.search|hex.outdated|hex.audit|hex)
      printf 'home\n'
      ;;
    *)
      printf 'project\n'
      ;;
  esac
}

COMMAND_TYPE="$(mix_command_type)"

if [[ "${COMMAND_TYPE}" == "project" ]] && [[ ! -f "${LOCAL_PROJECT_PATH}/mix.exs" ]]; then
  die "mix.exs não encontrado em ${LOCAL_PROJECT_PATH}"
fi

write_managed_env_file() {
  mkdir -p "${CACHE_ROOT}"
  cat > "${MANAGED_ENV_FILE}" <<EOF
#!/usr/bin/env sh
export MIX_HOME=$(shell_quote "${MANAGED_HOME_DIR}/.mix")
export HEX_HOME=$(shell_quote "${MANAGED_HOME_DIR}/.hex")
EOF
  chmod 0644 "${MANAGED_ENV_FILE}"
}

prepare_run_artifacts() {
  RUN_ID="$(date +%Y%m%d%H%M%S)-$(random_suffix)"
  RUN_S3_PREFIX="${S3_PREFIX%/}/${INSTANCE_NAME:-manual}/${PROJECT_NAME}/${RUN_ID}"
  PROJECT_ARCHIVE_KEY="${RUN_S3_PREFIX}/input/project-source.tgz"
  REMOTE_SCRIPT_KEY="${RUN_S3_PREFIX}/input/run.sh"
  PROJECT_RESULT_KEY="${RUN_S3_PREFIX}/output/project-result.tgz"
  HOME_CACHE_KEY="${RUN_S3_PREFIX}/output/home-cache.tgz"
  RUNTIME_METADATA_KEY="${RUN_S3_PREFIX}/output/runtime.txt"
  STATUS_KEY="${RUN_S3_PREFIX}/output/status.txt"
}

cleanup_s3_run_artifacts() {
  if ! is_truthy "${KEEP_S3_ARTIFACTS}"; then
    "${AWS_CMD[@]}" s3 rm "s3://${S3_BUCKET}/${RUN_S3_PREFIX}/" --recursive >/dev/null 2>&1 || true
  fi
}

ensure_s3_bucket() {
  local account_id bucket_region

  require_command aws
  [[ -n "${AWS_REGION_NAME}" ]] || die "AWS region é obrigatória para o modo SSM"

  if [[ -z "${S3_BUCKET}" ]]; then
    account_id="$("${AWS_CMD[@]}" sts get-caller-identity --query 'Account' --output text)"
    [[ -n "${account_id}" && "${account_id}" != "None" ]] || die "não foi possível obter a account AWS"
    S3_BUCKET="mix-via-ec2-${account_id}-${AWS_REGION_NAME}"
  fi

  if "${AWS_CMD[@]}" s3api head-bucket --bucket "${S3_BUCKET}" >/dev/null 2>&1; then
    return 0
  fi

  log "bucket S3 ${S3_BUCKET} não existe. Criando automaticamente"

  if [[ "${AWS_REGION_NAME}" == "us-east-1" ]]; then
    "${AWS_CMD[@]}" s3api create-bucket --bucket "${S3_BUCKET}" >/dev/null
  else
    bucket_region="LocationConstraint=${AWS_REGION_NAME}"
    "${AWS_CMD[@]}" s3api create-bucket \
      --bucket "${S3_BUCKET}" \
      --create-bucket-configuration "${bucket_region}" >/dev/null
  fi
}

resolve_instance_role_arn() {
  local instance_profile_name resolved_role_arn

  if [[ -n "${INSTANCE_ROLE_ARN}" ]]; then
    return 0
  fi

  [[ -n "${INSTANCE_PROFILE_ARN}" ]] || return 1

  instance_profile_name="${INSTANCE_PROFILE_ARN##*/}"
  resolved_role_arn="$("${AWS_CMD[@]}" iam get-instance-profile \
    --instance-profile-name "${instance_profile_name}" \
    --query 'InstanceProfile.Roles[0].Arn' \
    --output text 2>/dev/null || true)"

  [[ -n "${resolved_role_arn}" && "${resolved_role_arn}" != "None" ]] || die "não foi possível resolver o role ARN do instance profile ${instance_profile_name}"

  INSTANCE_ROLE_ARN="${resolved_role_arn}"
  INSTANCE_REMOTE_PRINCIPAL_ARN="${resolved_role_arn}"
}

normalize_principal_arn() {
  local raw_arn account_id role_name
  raw_arn="$1"

  case "${raw_arn}" in
    arn:aws:sts::*:assumed-role/*)
      account_id="$(printf '%s' "${raw_arn}" | cut -d: -f5)"
      role_name="$(printf '%s' "${raw_arn}" | cut -d/ -f2)"
      [[ -n "${account_id}" && -n "${role_name}" ]] || die "não foi possível normalizar o principal remoto: ${raw_arn}"
      printf 'arn:aws:iam::%s:role/%s\n' "${account_id}" "${role_name}"
      ;;
    *)
      printf '%s\n' "${raw_arn}"
      ;;
  esac
}

resolve_remote_principal_arn_via_ssm() {
  local parameter_file command_id principal_arn

  [[ -n "${INSTANCE_ID}" ]] || die "INSTANCE_ID ausente para resolver principal remoto"

  assert_ssm_managed_instance

  parameter_file="$(make_temp_file "mix-via-ec2-identity-params" ".json")"

  require_command python3
  python3 - "${parameter_file}" "${AWS_REGION_NAME}" <<'PY'
import json
import sys

parameter_file = sys.argv[1]
aws_region = sys.argv[2]

commands = [
    "set -euo pipefail",
    "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE",
    "unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME",
    "unset AWS_EC2_METADATA_DISABLED",
    "export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null",
]

if aws_region:
    commands.append(f'export AWS_REGION="{aws_region}" AWS_DEFAULT_REGION="{aws_region}"')

payload = {
    "commands": commands + [
        "aws sts get-caller-identity --query Arn --output text",
    ]
}

with open(parameter_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

  command_id="$("${AWS_CMD[@]}" ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name 'AWS-RunShellScript' \
    --comment "mix-via-ec2 identity probe ${INSTANCE_ID}" \
    --parameters "file://${parameter_file}" \
    --query 'Command.CommandId' \
    --output text)"

  rm -f "${parameter_file}"

  if ! poll_ssm_command "${command_id}"; then
    show_ssm_command_output "${command_id}"
    die "não foi possível resolver o principal AWS remoto via SSM"
  fi

  principal_arn="$("${AWS_CMD[@]}" ssm get-command-invocation \
    --command-id "${command_id}" \
    --instance-id "${INSTANCE_ID}" \
    --query 'StandardOutputContent' \
    --output text 2>/dev/null || true)"

  principal_arn="$(printf '%s' "${principal_arn}" | tr -d '\r' | awk 'NF {print; exit}')"
  [[ -n "${principal_arn}" && "${principal_arn}" != "None" ]] || die "o host remoto não retornou um principal AWS válido"

  INSTANCE_REMOTE_PRINCIPAL_ARN="${principal_arn}"
  INSTANCE_ROLE_ARN="$(normalize_principal_arn "${principal_arn}")"
}

ensure_s3_bucket_policy_for_instance_role() {
  local current_policy_file merged_policy_file current_policy prefix_root object_resource sid_bucket sid_object
  local bucket_resource
  local principal_patterns=()

  if ! resolve_instance_role_arn; then
    resolve_remote_principal_arn_via_ssm
  fi
  require_command python3

  if [[ -n "${INSTANCE_ROLE_ARN}" ]]; then
    principal_patterns+=("${INSTANCE_ROLE_ARN}")
  fi
  if [[ -n "${INSTANCE_REMOTE_PRINCIPAL_ARN}" && "${INSTANCE_REMOTE_PRINCIPAL_ARN}" != "${INSTANCE_ROLE_ARN}" ]]; then
    principal_patterns+=("${INSTANCE_REMOTE_PRINCIPAL_ARN}")
  fi
  (( ${#principal_patterns[@]} > 0 )) || die "não foi possível determinar um principal AWS remoto para a política do bucket"

  current_policy_file="$(make_temp_file "mix-via-ec2-bucket-policy-current" ".json")"
  merged_policy_file="$(make_temp_file "mix-via-ec2-bucket-policy-merged" ".json")"

  current_policy="$("${AWS_CMD[@]}" s3api get-bucket-policy \
    --bucket "${S3_BUCKET}" \
    --query 'Policy' \
    --output text 2>/dev/null || true)"

  if [[ -n "${current_policy}" && "${current_policy}" != "None" ]]; then
    printf '%s\n' "${current_policy}" > "${current_policy_file}"
  else
    printf '{}\n' > "${current_policy_file}"
  fi

  prefix_root="${S3_PREFIX%/}"
  bucket_resource="arn:aws:s3:::${S3_BUCKET}"
  if [[ -n "${prefix_root}" ]]; then
    object_resource="arn:aws:s3:::${S3_BUCKET}/${prefix_root}/*"
  else
    object_resource="arn:aws:s3:::${S3_BUCKET}/*"
  fi

  sid_bucket="MixViaEc2ListBucket${INSTANCE_ID//-/}"
  sid_object="MixViaEc2ObjectAccess${INSTANCE_ID//-/}"

  python3 - "${current_policy_file}" "${merged_policy_file}" "${bucket_resource}" "${object_resource}" "${sid_bucket}" "${sid_object}" "${principal_patterns[@]}" <<'PY'
import json
import sys

current_path, merged_path, bucket_resource, object_resource, sid_bucket, sid_object = sys.argv[1:7]
principal_patterns = [pattern for pattern in sys.argv[7:] if pattern]

with open(current_path, "r", encoding="utf-8") as handle:
    raw = handle.read().strip() or "{}"

try:
    policy = json.loads(raw)
except json.JSONDecodeError:
    policy = {}

statements = policy.get("Statement", [])
if isinstance(statements, dict):
    statements = [statements]
elif not isinstance(statements, list):
    statements = []

statements = [statement for statement in statements if statement.get("Sid") not in {sid_bucket, sid_object}]

condition_value = principal_patterns[0] if len(principal_patterns) == 1 else principal_patterns

statements.extend([
    {
        "Sid": sid_bucket,
        "Effect": "Allow",
        "Principal": "*",
        "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
        "Resource": bucket_resource,
        "Condition": {"StringLike": {"aws:PrincipalArn": condition_value}},
    },
    {
        "Sid": sid_object,
        "Effect": "Allow",
        "Principal": "*",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:AbortMultipartUpload",
        ],
        "Resource": object_resource,
        "Condition": {"StringLike": {"aws:PrincipalArn": condition_value}},
    },
])

policy["Version"] = policy.get("Version", "2012-10-17")
policy["Statement"] = statements

with open(merged_path, "w", encoding="utf-8") as handle:
    json.dump(policy, handle, indent=2)
    handle.write("\n")
PY

  log "garantindo acesso do principal remoto ao bucket ${S3_BUCKET}"
  "${AWS_CMD[@]}" s3api put-bucket-policy --bucket "${S3_BUCKET}" --policy "file://${merged_policy_file}" >/dev/null

  rm -f "${current_policy_file}" "${merged_policy_file}"
}

assert_ssm_managed_instance() {
  local ping_status
  ping_status="$("${AWS_CMD[@]}" ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=${INSTANCE_ID}" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text 2>/dev/null || true)"

  [[ -n "${ping_status}" && "${ping_status}" != "None" ]] || die "a instância ${INSTANCE_ID} não está registrada no SSM"
  [[ "${ping_status}" == "Online" ]] || die "a instância ${INSTANCE_ID} não está Online no SSM (status atual: ${ping_status})"
}

upload_local_project_archive() {
  local archive_path
  archive_path="$(make_temp_file "mix-via-ec2-project-${RUN_ID}" ".tgz")"

  tar -czf "${archive_path}" \
    --exclude=.git \
    --exclude=deps \
    --exclude=_build \
    --exclude=.elixir_ls \
    --exclude=node_modules \
    -C "${LOCAL_PROJECT_PATH}" .

  log "enviando fontes locais para s3://${S3_BUCKET}/${PROJECT_ARCHIVE_KEY}"
  "${AWS_CMD[@]}" s3 cp "${archive_path}" "s3://${S3_BUCKET}/${PROJECT_ARCHIVE_KEY}" --only-show-errors >/dev/null

  rm -f "${archive_path}"
}

generate_remote_script() {
  local remote_script_path remote_mix_args_literal quoted_remote_project_path quoted_aws_region quoted_mix_first_arg
  remote_script_path="$1"
  remote_mix_args_literal="$(printf '%q ' "${MIX_ARGS[@]}")"
  quoted_remote_project_path="$(shell_quote "${REMOTE_PROJECT_PATH}")"
  quoted_aws_region="$(shell_quote "${AWS_REGION_NAME}")"
  quoted_mix_first_arg="$(shell_quote "${MIX_ARGS[0]}")"

  cat > "${remote_script_path}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[mix-via-ec2-remote] %s\n' "\$*" >&2
}

upload_if_exists() {
  local source_path="\$1"
  local target_key="\$2"

  [[ -e "\${source_path}" ]] || return 0
  aws s3 cp "\${source_path}" "s3://${S3_BUCKET}/\${target_key}" --only-show-errors >/dev/null
}

reset_aws_auth_env() {
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE
  unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME
  unset AWS_EC2_METADATA_DISABLED
  export AWS_SHARED_CREDENTIALS_FILE=/dev/null
  export AWS_CONFIG_FILE=/dev/null
}

resolve_home_dir() {
  local detected_home

  if [[ -n "\${HOME:-}" ]]; then
    printf '%s\n' "\${HOME}"
    return 0
  fi

  detected_home="\$(getent passwd "\$(id -un)" 2>/dev/null | cut -d: -f6 || true)"
  if [[ -n "\${detected_home}" ]]; then
    printf '%s\n' "\${detected_home}"
    return 0
  fi

  detected_home="\$(cd ~ && pwd 2>/dev/null || true)"
  [[ -n "\${detected_home}" ]] || {
    printf '[mix-via-ec2-remote] erro: não foi possível resolver o diretório HOME do usuário remoto\n' >&2
    exit 1
  }

  printf '%s\n' "\${detected_home}"
}

resolve_remote_path() {
  local raw_path
  raw_path="\$1"

  case "\${raw_path}" in
    "~/"*)
      printf '%s/%s\n' "\${HOME_DIR}" "\${raw_path#~/}"
      ;;
    *)
      printf '%s\n' "\${raw_path}"
      ;;
  esac
}

HOME_DIR="\$(resolve_home_dir)"
RUN_ROOT="/tmp/mix-via-ec2/runs/${RUN_ID}"
HOME_CACHE_ROOT="\${RUN_ROOT}/home-cache"
REMOTE_MIX_HOME="\${HOME_CACHE_ROOT}/.mix"
REMOTE_HEX_HOME="\${HOME_CACHE_ROOT}/.hex"
REMOTE_REBAR_CACHE_DIR="\${HOME_CACHE_ROOT}/.cache/rebar3"
REMOTE_REBAR_CONFIG_DIR="\${HOME_CACHE_ROOT}/.config/rebar3"
COMMAND_TYPE="${COMMAND_TYPE}"
MIX_FIRST_ARG=${quoted_mix_first_arg}
SYNC_BUILD="${SYNC_BUILD}"
SYNC_HOME_CACHE="${SYNC_HOME_CACHE}"
REMOTE_PROJECT_PATH_RAW=${quoted_remote_project_path}
REMOTE_AWS_REGION=${quoted_aws_region}
PROJECT_ARCHIVE_KEY="${PROJECT_ARCHIVE_KEY}"
PROJECT_RESULT_KEY="${PROJECT_RESULT_KEY}"
HOME_CACHE_KEY="${HOME_CACHE_KEY}"
RUNTIME_METADATA_KEY="${RUNTIME_METADATA_KEY}"
STATUS_KEY="${STATUS_KEY}"
REMOTE_PROJECT_PATH="\$(resolve_remote_path "\${REMOTE_PROJECT_PATH_RAW}")"
REMOTE_PROJECT_PARENT="\${REMOTE_PROJECT_PATH%/*}"

mkdir -p "\${RUN_ROOT}"
mkdir -p "\${REMOTE_PROJECT_PARENT}"
mkdir -p "\${REMOTE_MIX_HOME}" "\${REMOTE_HEX_HOME}" "\${REMOTE_REBAR_CACHE_DIR}" "\${REMOTE_REBAR_CONFIG_DIR}"
reset_aws_auth_env
if [[ -n "\${REMOTE_AWS_REGION}" ]]; then
  export AWS_REGION="\${REMOTE_AWS_REGION}"
  export AWS_DEFAULT_REGION="\${REMOTE_AWS_REGION}"
fi
unset MIX_PATH MIX_ARCHIVES
export HOME="\${HOME_DIR}"
export MIX_XDG=1
export MIX_HOME="\${REMOTE_MIX_HOME}"
export HEX_HOME="\${REMOTE_HEX_HOME}"
export REBAR_CACHE_DIR="\${REMOTE_REBAR_CACHE_DIR}"
export REBAR_GLOBAL_CONFIG_DIR="\${REMOTE_REBAR_CONFIG_DIR}"
export HEX_HTTP_TIMEOUT="\${HEX_HTTP_TIMEOUT:-120}"
export HEX_HTTP_CONCURRENCY="\${HEX_HTTP_CONCURRENCY:-1}"

run_bootstrap_command() {
  local step_name log_file
  step_name="\$1"
  shift
  log_file="\${RUN_ROOT}/\${step_name}.log"

  if ! "\$@" > "\${log_file}" 2>&1; then
    printf '[mix-via-ec2-remote] erro ao executar %s\n' "\${step_name}" >&2
    if [[ -s "\${log_file}" ]]; then
      tail -n 80 "\${log_file}" >&2 || cat "\${log_file}" >&2
    fi
    exit 1
  fi
}

bootstrap_mix_local_tooling() {
  case "\${MIX_FIRST_ARG}" in
    local.hex)
      return 0
      ;;
  esac

  run_bootstrap_command local_hex mix local.hex --force --if-missing

  case "\${MIX_FIRST_ARG}" in
    local.rebar)
      return 0
      ;;
  esac

  run_bootstrap_command local_rebar mix local.rebar --force --if-missing
}

finish() {
  local exit_code="\$?"
  if [[ "\${exit_code}" -eq 0 ]]; then
    printf 'success\n' > "\${RUN_ROOT}/status.txt"
  else
    printf 'failed\n' > "\${RUN_ROOT}/status.txt"
  fi

  upload_if_exists "\${RUN_ROOT}/runtime.txt" "\${RUNTIME_METADATA_KEY}"
  upload_if_exists "\${RUN_ROOT}/status.txt" "\${STATUS_KEY}"
}

trap finish EXIT

{
  printf 'uname=%s\n' "\$(uname -a 2>/dev/null || true)"
  printf 'arch=%s\n' "\$(uname -m 2>/dev/null || true)"
  printf 'elixir=%s\n' "\$(elixir --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/[[:space:]]\$//')"
  printf 'mix=%s\n' "\$(mix --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/[[:space:]]\$//')"
  printf 'erlang=%s\n' "\$(erl -eval 'io:format(\"~s\", [erlang:system_info(system_architecture)]), halt().' -noshell 2>/dev/null || true)"
} > "\${RUN_ROOT}/runtime.txt"

if [[ "\${COMMAND_TYPE}" == "project" ]]; then
  rm -rf "\${REMOTE_PROJECT_PATH}"
  mkdir -p "\${REMOTE_PROJECT_PATH}"
  aws s3 cp "s3://${S3_BUCKET}/\${PROJECT_ARCHIVE_KEY}" - --only-show-errors | tar -xzf - -C "\${REMOTE_PROJECT_PATH}"
  cd "\${REMOTE_PROJECT_PATH}"
else
  cd "\${HOME_DIR}"
fi

bootstrap_mix_local_tooling
log "executando mix ${MIX_ARGS[*]}"
mix ${remote_mix_args_literal}

if [[ "\${COMMAND_TYPE}" == "project" ]]; then
  cd "\${REMOTE_PROJECT_PATH}"
  entries=()
  if [[ -d deps ]]; then entries+=(deps); fi
  if [[ -f mix.lock ]]; then entries+=(mix.lock); fi
  if [[ "\${SYNC_BUILD}" == "1" ]] && [[ -d _build ]]; then entries+=(_build); fi
  if (( \${#entries[@]} > 0 )); then
    tar -czf "\${RUN_ROOT}/project-result.tgz" "\${entries[@]}"
    upload_if_exists "\${RUN_ROOT}/project-result.tgz" "\${PROJECT_RESULT_KEY}"
  fi
fi

if [[ "\${SYNC_HOME_CACHE}" == "1" ]]; then
  cd "\${HOME_CACHE_ROOT}"
  entries=()
  if [[ -d .mix ]]; then entries+=(.mix); fi
  if [[ -d .hex ]]; then entries+=(.hex); fi
  if [[ -d .cache/rebar3 ]]; then entries+=(.cache/rebar3); fi
  if [[ -d .config/rebar3 ]]; then entries+=(.config/rebar3); fi
  if (( \${#entries[@]} > 0 )); then
    tar -czf "\${RUN_ROOT}/home-cache.tgz" "\${entries[@]}"
    upload_if_exists "\${RUN_ROOT}/home-cache.tgz" "\${HOME_CACHE_KEY}"
  fi
fi
EOF

  chmod +x "${remote_script_path}"
}

upload_remote_script() {
  local remote_script_path
  remote_script_path="$(make_temp_file "mix-via-ec2-run-${RUN_ID}" ".sh")"
  generate_remote_script "${remote_script_path}"
  "${AWS_CMD[@]}" s3 cp "${remote_script_path}" "s3://${S3_BUCKET}/${REMOTE_SCRIPT_KEY}" --only-show-errors >/dev/null
  rm -f "${remote_script_path}"
}

ssm_object_exists() {
  local key
  key="$1"
  "${AWS_CMD[@]}" s3api head-object --bucket "${S3_BUCKET}" --key "${key}" >/dev/null 2>&1
}

download_project_result_from_s3() {
  local result_archive
  if ! ssm_object_exists "${PROJECT_RESULT_KEY}"; then
    log "resultado do projeto não foi retornado pelo EC2"
    return 0
  fi

  result_archive="$(make_temp_file "mix-via-ec2-project-result-${RUN_ID}" ".tgz")"
  "${AWS_CMD[@]}" s3 cp "s3://${S3_BUCKET}/${PROJECT_RESULT_KEY}" "${result_archive}" --only-show-errors >/dev/null
  tar -xzf "${result_archive}" -C "${LOCAL_PROJECT_PATH}"
  rm -f "${result_archive}"
}

download_home_cache_from_s3() {
  local home_archive
  if ! ssm_object_exists "${HOME_CACHE_KEY}"; then
    log "cache home (~/.mix ~/.hex rebar3) não foi retornado pelo EC2"
    return 0
  fi

  home_archive="$(make_temp_file "mix-via-ec2-home-${RUN_ID}" ".tgz")"
  "${AWS_CMD[@]}" s3 cp "s3://${S3_BUCKET}/${HOME_CACHE_KEY}" "${home_archive}" --only-show-errors >/dev/null
  mkdir -p "${MANAGED_HOME_DIR}"
  tar -xzf "${home_archive}" -C "${MANAGED_HOME_DIR}"
  rm -f "${home_archive}"
}

download_runtime_metadata_from_s3() {
  if ssm_object_exists "${RUNTIME_METADATA_KEY}"; then
    "${AWS_CMD[@]}" s3 cp "s3://${S3_BUCKET}/${RUNTIME_METADATA_KEY}" "${MANAGED_METADATA_DIR}/runtime.txt" --only-show-errors >/dev/null
  fi
}

build_ssm_parameters_file() {
  local parameter_file remote_runner_path remote_run_dir
  parameter_file="$1"
  remote_run_dir="/tmp/mix-via-ec2/runs/${RUN_ID}"
  remote_runner_path="${remote_run_dir}/run.sh"
  require_command python3

  python3 - "${parameter_file}" "${remote_run_dir}" "${S3_BUCKET}" "${REMOTE_SCRIPT_KEY}" "${remote_runner_path}" "${AWS_REGION_NAME}" <<'PY'
import json
import sys

parameter_file, remote_run_dir, s3_bucket, remote_script_key, remote_runner_path, aws_region = sys.argv[1:7]

commands = [
    "set -euo pipefail",
    "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE",
    "unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME",
    "unset AWS_EC2_METADATA_DISABLED",
    "export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null",
]

if aws_region:
    commands.append(f'export AWS_REGION="{aws_region}" AWS_DEFAULT_REGION="{aws_region}"')

payload = {
    "commands": commands + [
        f'mkdir -p "{remote_run_dir}"',
        f'aws s3 cp "s3://{s3_bucket}/{remote_script_key}" "{remote_runner_path}" --only-show-errors >/dev/null',
        f'chmod +x "{remote_runner_path}"',
        f'bash "{remote_runner_path}"',
    ]
}

with open(parameter_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

poll_ssm_command() {
  local command_id status
  command_id="$1"

  while true; do
    status="$("${AWS_CMD[@]}" ssm get-command-invocation \
      --command-id "${command_id}" \
      --instance-id "${INSTANCE_ID}" \
      --query 'Status' \
      --output text 2>/dev/null || true)"

    case "${status}" in
      Pending|InProgress|Delayed)
        sleep 3
        ;;
      Success)
        return 0
        ;;
      Cancelled|TimedOut|Failed|Cancelling)
        return 1
        ;;
      *)
        sleep 2
        ;;
    esac
  done
}

show_ssm_command_output() {
  local command_id stdout_text stderr_text
  command_id="$1"

  stdout_text="$("${AWS_CMD[@]}" ssm get-command-invocation \
    --command-id "${command_id}" \
    --instance-id "${INSTANCE_ID}" \
    --query 'StandardOutputContent' \
    --output text 2>/dev/null || true)"

  stderr_text="$("${AWS_CMD[@]}" ssm get-command-invocation \
    --command-id "${command_id}" \
    --instance-id "${INSTANCE_ID}" \
    --query 'StandardErrorContent' \
    --output text 2>/dev/null || true)"

  if [[ -n "${stdout_text}" && "${stdout_text}" != "None" ]]; then
    printf '%s\n' "${stdout_text}" >&2
  fi

  if [[ -n "${stderr_text}" && "${stderr_text}" != "None" ]]; then
    printf '%s\n' "${stderr_text}" >&2
  fi
}

run_ssm_mix() {
  local parameter_file command_id

  [[ -n "${INSTANCE_ID}" ]] || die "INSTANCE_ID ausente para o modo SSM"
  [[ -n "${AWS_REGION_NAME}" ]] || die "AWS region é obrigatória para o modo SSM"

  configure_aws_cmd
  ensure_s3_bucket
  assert_ssm_managed_instance
  ensure_s3_bucket_policy_for_instance_role
  prepare_run_artifacts

  trap cleanup_s3_run_artifacts EXIT

  if [[ "${COMMAND_TYPE}" == "project" ]]; then
    upload_local_project_archive
  fi

  upload_remote_script

  parameter_file="$(make_temp_file "mix-via-ec2-ssm-params-${RUN_ID}" ".json")"
  build_ssm_parameters_file "${parameter_file}"

  log "executando mix no EC2 via SSM: mix ${MIX_ARGS[*]}"
  command_id="$("${AWS_CMD[@]}" ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name 'AWS-RunShellScript' \
    --comment "mix-via-ec2 ${PROJECT_NAME} ${RUN_ID}" \
    --parameters "file://${parameter_file}" \
    --query 'Command.CommandId' \
    --output text)"

  rm -f "${parameter_file}"

  if ! poll_ssm_command "${command_id}"; then
    show_ssm_command_output "${command_id}"
    die "execução remota via SSM falhou. command-id=${command_id}"
  fi

  show_ssm_command_output "${command_id}"

  if [[ "${COMMAND_TYPE}" == "project" ]]; then
    download_project_result_from_s3
  fi

  if [[ "${SYNC_HOME_CACHE}" == "1" ]]; then
    download_home_cache_from_s3
    write_managed_env_file
  fi

  download_runtime_metadata_from_s3
}

build_ssh_command() {
  SSH_BASE_CMD=(ssh -p "${SSH_PORT}" -o BatchMode=yes)

  if [[ -n "${SSH_IDENTITY}" ]]; then
    SSH_BASE_CMD+=(-i "${SSH_IDENTITY}")
  fi

  if (( ${#SSH_OPTIONS[@]} > 0 )); then
    local ssh_option
    for ssh_option in "${SSH_OPTIONS[@]}"; do
      SSH_BASE_CMD+=(-o "${ssh_option}")
    done
  fi

  SSH_CMD=("${SSH_BASE_CMD[@]}" "${SSH_USER}@${HOST}")
}

sync_project_to_remote_ssh() {
  local remote_parent remote_project quoted_remote_parent quoted_remote_project
  remote_project="$1"
  remote_parent="${remote_project%/*}"
  quoted_remote_parent="$(shell_quote "${remote_parent}")"
  quoted_remote_project="$(shell_quote "${remote_project}")"

  log "sincronizando fontes locais para o EC2 em ${remote_project}"

  tar -czf - \
    --exclude=.git \
    --exclude=deps \
    --exclude=_build \
    --exclude=.elixir_ls \
    --exclude=node_modules \
    -C "${LOCAL_PROJECT_PATH}" . | \
    "${SSH_CMD[@]}" "bash -lc 'set -euo pipefail; mkdir -p ${quoted_remote_parent}; rm -rf ${quoted_remote_project}; mkdir -p ${quoted_remote_project}; tar -xzf - -C ${quoted_remote_project}'"
}

run_remote_mix_ssh() {
  local remote_project remote_mix_args_literal remote_work_dir quoted_remote_work_dir
  remote_project="$1"
  remote_mix_args_literal="$(printf '%q ' "${MIX_ARGS[@]}")"
  remote_work_dir="${remote_project}"

  if [[ "${COMMAND_TYPE}" == "home" ]]; then
    remote_work_dir="\${HOME}"
  fi
  quoted_remote_work_dir="$(shell_quote "${remote_work_dir}")"

  log "executando mix no EC2 via SSH: mix ${MIX_ARGS[*]}"
  "${SSH_CMD[@]}" "bash -lc 'set -euo pipefail; cd ${quoted_remote_work_dir}; mix ${remote_mix_args_literal}'"
}

sync_remote_project_back_ssh() {
  local remote_project include_build quoted_remote_project quoted_include_build
  remote_project="$1"
  include_build="$2"
  quoted_remote_project="$(shell_quote "${remote_project}")"
  quoted_include_build="$(shell_quote "${include_build}")"

  log "trazendo deps e mix.lock do EC2"

  "${SSH_CMD[@]}" "bash -lc '
set -euo pipefail
cd ${quoted_remote_project}
entries=()
if [[ -d deps ]]; then entries+=(deps); fi
if [[ -f mix.lock ]]; then entries+=(mix.lock); fi
if [[ ${quoted_include_build} == 1 ]] && [[ -d _build ]]; then entries+=(_build); fi
if (( \${#entries[@]} == 0 )); then
  printf \"[mix-via-ec2] remoto sem deps/mix.lock/_build\\n\" >&2
  exit 1
fi
tar -czf - \"\${entries[@]}\"
'" | tar -xzf - -C "${LOCAL_PROJECT_PATH}"
}

sync_remote_home_cache_back_ssh() {
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

fetch_remote_metadata_ssh() {
  "${SSH_CMD[@]}" "bash -s" <<'EOF' > "${MANAGED_METADATA_DIR}/runtime.txt"
set -euo pipefail
printf 'uname=%s\n' "$(uname -a 2>/dev/null || true)"
printf 'arch=%s\n' "$(uname -m 2>/dev/null || true)"
printf 'elixir=%s\n' "$(elixir --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]$//')"
printf 'mix=%s\n' "$(mix --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]$//')"
printf 'erlang=%s\n' "$(erl -eval 'io:format(\"~s\", [erlang:system_info(system_architecture)]), halt().' -noshell 2>/dev/null || true)"
EOF
}

run_ssh_mix() {
  [[ -n "${HOST}" ]] || die "host ausente para o modo SSH"
  build_ssh_command
  fetch_remote_metadata_ssh

  if [[ "${COMMAND_TYPE}" == "project" ]]; then
    sync_project_to_remote_ssh "${REMOTE_PROJECT_PATH}"
  fi

  run_remote_mix_ssh "${REMOTE_PROJECT_PATH}"

  if [[ "${COMMAND_TYPE}" == "project" ]]; then
    sync_remote_project_back_ssh "${REMOTE_PROJECT_PATH}" "${SYNC_BUILD}"
  fi

  if [[ "${SYNC_HOME_CACHE}" == "1" ]]; then
    sync_remote_home_cache_back_ssh
    write_managed_env_file
  fi
}

resolve_instance_from_aws
resolve_transport

case "${TRANSPORT}" in
  ssh)
    run_ssh_mix
    ;;
  ssm)
    run_ssm_mix
    ;;
esac

cat <<EOF
Execução remota concluída.

Transporte:
  ${TRANSPORT}

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
