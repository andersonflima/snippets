#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[fetch-url-via-ec2] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "comando obrigatório não encontrado no PATH: $1"
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

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/assets/fetch_url_via_ec2.sh --url <url> --output <arquivo> [opções]

Opções:
  --url <url>                  URL a ser baixada pelo EC2.
  --output <arquivo>           Arquivo local de destino.
  --create-dirs                Cria diretórios do destino local automaticamente.
  --header <valor>             Header HTTP adicional. Repetível.
  --user-agent <valor>         User-Agent do download remoto.
  --proxy <url>                Proxy HTTP/HTTPS a ser usado no EC2.
  --insecure                   Desabilita validação TLS no curl remoto.
  --connect-timeout <seg>      Timeout de conexão do curl remoto. Padrão: 20
  --max-time <seg>             Timeout total do curl remoto. Padrão: 300
  --instance-name <nome>       Instância EC2. Padrão: env compartilhada
  --aws-profile <profile>      Profile AWS.
  --aws-region <region>        Region AWS. Padrão: env compartilhada
  --s3-bucket <bucket>         Bucket intermediário. Padrão: env compartilhada
  --s3-prefix <prefixo>        Prefixo do bucket. Padrão: wrappers-via-ec2
  -h, --help                   Mostra esta ajuda.
USAGE
}

URL=""
OUTPUT_PATH=""
CREATE_DIRS="0"
HEADERS=()
USER_AGENT=""
PROXY_URL=""
INSECURE="0"
CONNECT_TIMEOUT="20"
MAX_TIME="300"
INSTANCE_ID=""
INSTANCE_NAME="${WRAPPERS_VIA_EC2_INSTANCE_NAME:-${MIX_VIA_EC2_INSTANCE_NAME:-Dander}}"
AWS_PROFILE_NAME="${WRAPPERS_VIA_EC2_AWS_PROFILE:-${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}}"
AWS_REGION_NAME="${WRAPPERS_VIA_EC2_AWS_REGION:-${MIX_VIA_EC2_AWS_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-sa-east-1}}}}"
S3_BUCKET="${WRAPPERS_VIA_EC2_S3_BUCKET:-${MIX_VIA_EC2_S3_BUCKET:-}}"
S3_PREFIX="${WRAPPERS_VIA_EC2_S3_PREFIX:-${MIX_VIA_EC2_S3_PREFIX:-wrappers-via-ec2}}"
AWS_CMD=(aws)
AWS_CMD_CONFIGURED="0"
RUN_ID=""
RUN_S3_PREFIX=""
OUTPUT_KEY=""
REMOTE_METADATA_KEY=""
REMOTE_PRINCIPAL_ARN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      URL="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    --create-dirs)
      CREATE_DIRS="1"
      shift
      ;;
    --header)
      HEADERS+=("${2:-}")
      shift 2
      ;;
    --user-agent)
      USER_AGENT="${2:-}"
      shift 2
      ;;
    --proxy)
      PROXY_URL="${2:-}"
      shift 2
      ;;
    --insecure)
      INSECURE="1"
      shift
      ;;
    --connect-timeout)
      CONNECT_TIMEOUT="${2:-}"
      shift 2
      ;;
    --max-time)
      MAX_TIME="${2:-}"
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
    --s3-bucket)
      S3_BUCKET="${2:-}"
      shift 2
      ;;
    --s3-prefix)
      S3_PREFIX="${2:-}"
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

[[ -n "${URL}" ]] || die "--url é obrigatório"
[[ -n "${OUTPUT_PATH}" ]] || die "--output é obrigatório"
[[ -n "${INSTANCE_NAME}" ]] || die "--instance-name é obrigatório"
[[ -n "${AWS_REGION_NAME}" ]] || die "--aws-region é obrigatório"

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
  local output line_count selected_line
  require_command aws
  configure_aws_cmd

  output="$("${AWS_CMD[@]}" ec2 describe-instances \
    --filters "Name=tag:Name,Values=${INSTANCE_NAME}" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].[InstanceId]' \
    --output text)"

  [[ -n "${output}" ]] || die "nenhuma instância running encontrada com tag Name=${INSTANCE_NAME}"

  line_count="$(printf '%s\n' "${output}" | awk 'NF {count++} END {print count+0}')"
  if [[ "${line_count}" -gt 1 ]]; then
    printf '[fetch-url-via-ec2] múltiplas instâncias encontradas para Name=%s:\n%s\n' "${INSTANCE_NAME}" "${output}" >&2
    die "refine a busca da instância"
  fi

  selected_line="$(printf '%s\n' "${output}" | awk 'NF {print; exit}')"
  INSTANCE_ID="$(printf '%s\n' "${selected_line}" | awk '{print $1}')"
  [[ -n "${INSTANCE_ID}" && "${INSTANCE_ID}" != "None" ]] || die "não foi possível resolver o instance id para ${INSTANCE_NAME}"
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

ensure_s3_bucket() {
  local account_id bucket_region
  require_command aws

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
  parameter_file="$(make_temp_file "fetch-url-via-ec2-identity" ".json")"

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
commands.append("aws sts get-caller-identity --query Arn --output text")
with open(parameter_file, "w", encoding="utf-8") as handle:
    json.dump({"commands": commands}, handle, indent=2)
    handle.write("\n")
PY

  command_id="$("${AWS_CMD[@]}" ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name 'AWS-RunShellScript' \
    --comment "fetch-url-via-ec2 identity probe ${INSTANCE_ID}" \
    --parameters "file://${parameter_file}" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "${parameter_file}"

  poll_ssm_command "${command_id}" || {
    show_ssm_command_output "${command_id}"
    die "não foi possível resolver o principal AWS remoto via SSM"
  }

  principal_arn="$("${AWS_CMD[@]}" ssm get-command-invocation \
    --command-id "${command_id}" \
    --instance-id "${INSTANCE_ID}" \
    --query 'StandardOutputContent' \
    --output text 2>/dev/null || true)"
  principal_arn="$(printf '%s' "${principal_arn}" | tr -d '\r' | awk 'NF {print; exit}')"
  [[ -n "${principal_arn}" && "${principal_arn}" != "None" ]] || die "o host remoto não retornou um principal AWS válido"

  REMOTE_PRINCIPAL_ARN="${principal_arn}"
}

ensure_s3_bucket_policy_for_remote_principal() {
  local current_policy_file merged_policy_file current_policy prefix_root object_resource bucket_resource
  local sid_bucket sid_object normalized_principal

  resolve_remote_principal_arn_via_ssm
  normalized_principal="$(normalize_principal_arn "${REMOTE_PRINCIPAL_ARN}")"

  current_policy_file="$(make_temp_file "fetch-url-via-ec2-policy-current" ".json")"
  merged_policy_file="$(make_temp_file "fetch-url-via-ec2-policy-merged" ".json")"

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

  sid_bucket="FetchUrlViaEc2ListBucket${INSTANCE_ID//-/}"
  sid_object="FetchUrlViaEc2ObjectAccess${INSTANCE_ID//-/}"

  python3 - "${current_policy_file}" "${merged_policy_file}" "${bucket_resource}" "${object_resource}" "${sid_bucket}" "${sid_object}" "${normalized_principal}" "${REMOTE_PRINCIPAL_ARN}" <<'PY'
import json
import sys

current_path, merged_path, bucket_resource, object_resource, sid_bucket, sid_object = sys.argv[1:7]
principal_patterns = [p for p in sys.argv[7:] if p]

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
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
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

  "${AWS_CMD[@]}" s3api put-bucket-policy --bucket "${S3_BUCKET}" --policy "file://${merged_policy_file}" >/dev/null
  rm -f "${current_policy_file}" "${merged_policy_file}"
}

prepare_run_artifacts() {
  local output_name
  RUN_ID="$(date +%Y%m%d%H%M%S)-$(random_suffix)"
  output_name="$(basename "${OUTPUT_PATH}")"
  RUN_S3_PREFIX="${S3_PREFIX%/}/${INSTANCE_NAME}/${RUN_ID}"
  OUTPUT_KEY="${RUN_S3_PREFIX}/output/${output_name}"
  REMOTE_METADATA_KEY="${RUN_S3_PREFIX}/runtime.txt"
}

cleanup_s3_run_artifacts() {
  "${AWS_CMD[@]}" s3 rm "s3://${S3_BUCKET}/${RUN_S3_PREFIX}/" --recursive >/dev/null 2>&1 || true
}

build_ssm_parameters_file() {
  local parameter_file
  local python_args
  parameter_file="$1"
  require_command python3
  python_args=("${parameter_file}" "${AWS_REGION_NAME}" "${URL}" "${OUTPUT_KEY}" "${S3_BUCKET}" "${CONNECT_TIMEOUT}" "${MAX_TIME}" "${USER_AGENT}" "${PROXY_URL}" "${INSECURE}")
  if [[ ${#HEADERS[@]} -gt 0 ]]; then
    python_args+=("${HEADERS[@]}")
  fi

  python3 - "${python_args[@]}" <<'PY'
import json
import shlex
import sys

parameter_file = sys.argv[1]
aws_region = sys.argv[2]
url = sys.argv[3]
output_key = sys.argv[4]
s3_bucket = sys.argv[5]
connect_timeout = sys.argv[6]
max_time = sys.argv[7]
user_agent = sys.argv[8]
proxy_url = sys.argv[9]
insecure = sys.argv[10] == "1"
headers = [header for header in sys.argv[11:] if header]

remote_dir = "/tmp/fetch-url-via-ec2"
remote_output = f"{remote_dir}/download.bin"

commands = [
    "set -euo pipefail",
    "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE",
    "unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME",
    "unset AWS_EC2_METADATA_DISABLED",
    "export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null",
]
if aws_region:
    commands.append(f'export AWS_REGION="{aws_region}" AWS_DEFAULT_REGION="{aws_region}"')
commands.append(f'mkdir -p "{remote_dir}"')

curl_cmd = [
    "curl",
    "-fsSL",
    "--connect-timeout", connect_timeout,
    "--max-time", max_time,
    "--retry", "3",
    "--retry-delay", "2",
    "--retry-all-errors",
    "--tlsv1.2",
]
if insecure:
    curl_cmd.append("-k")
if user_agent:
    curl_cmd.extend(["-A", user_agent])
if proxy_url:
    curl_cmd.extend(["--proxy", proxy_url])
for header in headers:
    curl_cmd.extend(["-H", header])
curl_cmd.extend(["--url", url, "-o", remote_output])

commands.append(" ".join(shlex.quote(part) for part in curl_cmd))
commands.append(f'aws s3 cp "{remote_output}" "s3://{s3_bucket}/{output_key}" --only-show-errors >/dev/null')

payload = {"commands": commands}
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

  [[ -n "${stdout_text}" && "${stdout_text}" != "None" ]] && printf '%s\n' "${stdout_text}" >&2
  [[ -n "${stderr_text}" && "${stderr_text}" != "None" ]] && printf '%s\n' "${stderr_text}" >&2
}

s3_object_exists() {
  "${AWS_CMD[@]}" s3api head-object --bucket "${S3_BUCKET}" --key "$1" >/dev/null 2>&1
}

download_result_from_s3() {
  [[ "${CREATE_DIRS}" == "1" ]] && mkdir -p "$(dirname "${OUTPUT_PATH}")"
  "${AWS_CMD[@]}" s3 cp "s3://${S3_BUCKET}/${OUTPUT_KEY}" "${OUTPUT_PATH}" --only-show-errors >/dev/null
}

run_remote_fetch() {
  local parameter_file command_id
  parameter_file="$(make_temp_file "fetch-url-via-ec2-params-${RUN_ID}" ".json")"
  build_ssm_parameters_file "${parameter_file}"

  command_id="$("${AWS_CMD[@]}" ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name 'AWS-RunShellScript' \
    --comment "fetch-url-via-ec2 ${RUN_ID}" \
    --parameters "file://${parameter_file}" \
    --query 'Command.CommandId' \
    --output text)"

  rm -f "${parameter_file}"

  if ! poll_ssm_command "${command_id}"; then
    show_ssm_command_output "${command_id}"
    die "execução remota via SSM falhou. command-id=${command_id}"
  fi

  show_ssm_command_output "${command_id}"
  s3_object_exists "${OUTPUT_KEY}" || die "o EC2 não retornou o artefato esperado em s3://${S3_BUCKET}/${OUTPUT_KEY}"
  download_result_from_s3
}

configure_aws_cmd
resolve_instance_from_aws
assert_ssm_managed_instance
ensure_s3_bucket
ensure_s3_bucket_policy_for_remote_principal
prepare_run_artifacts
trap cleanup_s3_run_artifacts EXIT
run_remote_fetch

cat <<EOF
Download remoto concluído.

Instância:
  ${INSTANCE_NAME} (${INSTANCE_ID})

Bucket:
  ${S3_BUCKET}

Arquivo:
  ${OUTPUT_PATH}
EOF
