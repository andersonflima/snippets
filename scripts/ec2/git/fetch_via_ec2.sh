#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[git-fetch-via-ec2] %s\n' "$*" >&2
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

make_temp_dir() {
  local prefix temp_dir
  prefix="$1"

  if temp_dir="$(mktemp -d "/tmp/${prefix}.XXXXXX" 2>/dev/null)"; then
    :
  elif temp_dir="$(mktemp -d -t "${prefix}" 2>/dev/null)"; then
    :
  else
    die "não foi possível criar diretório temporário para ${prefix}"
  fi

  printf '%s\n' "${temp_dir}"
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/ec2/git/fetch_via_ec2.sh --git-dir <dir> [opções]

Opções:
  --git-dir <dir>              Diretório .git local a ser atualizado remotamente.
  --git-arg <valor>            Argumento adicional repassado para git fetch no EC2. Repetível.
  --proxy <url>                Proxy HTTP/HTTPS a ser usado no EC2.
  --insecure                   Desabilita validação TLS no git remoto.
  --instance-name <nome>       Instância EC2. Padrão: env compartilhada.
  --aws-profile <profile>      Profile AWS.
  --aws-region <region>        Region AWS. Padrão: env compartilhada.
  --s3-bucket <bucket>         Bucket intermediário. Padrão: env compartilhada.
  --s3-prefix <prefixo>        Prefixo do bucket. Padrão: wrappers-via-ec2.
  -h, --help                   Mostra esta ajuda.
USAGE
}

LOCAL_GIT_DIR=""
FETCH_ARGS=()
PROXY_URL=""
INSECURE="0"
INSTANCE_ID=""
INSTANCE_NAME="${WRAPPERS_VIA_EC2_INSTANCE_NAME:-${MIX_VIA_EC2_INSTANCE_NAME:-Dander}}"
AWS_PROFILE_NAME="${WRAPPERS_VIA_EC2_AWS_PROFILE:-${MIX_VIA_EC2_AWS_PROFILE:-${AWS_PROFILE:-}}}"
AWS_REGION_NAME="${WRAPPERS_VIA_EC2_AWS_REGION:-${MIX_VIA_EC2_AWS_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-sa-east-1}}}}"
S3_BUCKET="${WRAPPERS_VIA_EC2_S3_BUCKET:-${MIX_VIA_EC2_S3_BUCKET:-}}"
S3_PREFIX="${WRAPPERS_VIA_EC2_S3_PREFIX:-${MIX_VIA_EC2_S3_PREFIX:-wrappers-via-ec2}}"
AWS_CMD=(aws)
AWS_CMD_CONFIGURED="0"
S3_AWS_CMD=(aws)
RUN_ID=""
RUN_S3_PREFIX=""
INPUT_KEY=""
OUTPUT_KEY=""
REMOTE_PRINCIPAL_ARN=""
LOCAL_TEMP_DIR=""
INPUT_ARCHIVE_PATH=""
OUTPUT_ARCHIVE_PATH=""
LOCAL_GIT_BASENAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --git-dir)
      LOCAL_GIT_DIR="${2:-}"
      shift 2
      ;;
    --git-arg)
      FETCH_ARGS+=("${2:-}")
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

[[ -n "${LOCAL_GIT_DIR}" ]] || die "--git-dir é obrigatório"
[[ -d "${LOCAL_GIT_DIR}" ]] || die "diretório git não encontrado: ${LOCAL_GIT_DIR}"
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
  configure_s3_aws_cmd "${AWS_REGION_NAME}"
}

configure_s3_aws_cmd() {
  local region="$1"
  S3_AWS_CMD=(aws)
  if [[ -n "${AWS_PROFILE_NAME}" ]]; then
    S3_AWS_CMD+=(--profile "${AWS_PROFILE_NAME}")
  fi
  if [[ -n "${region}" ]]; then
    S3_AWS_CMD+=(--region "${region}")
  fi
}

resolve_s3_bucket_region() {
  local bucket_region
  local -a region_probe_cmd=(aws)

  if [[ -n "${AWS_PROFILE_NAME}" ]]; then
    region_probe_cmd+=(--profile "${AWS_PROFILE_NAME}")
  fi

  if ! bucket_region="$("${region_probe_cmd[@]}" s3api get-bucket-location --bucket "${S3_BUCKET}" --query 'LocationConstraint' --output text 2>/dev/null)"; then
    return 1
  fi

  if [[ -z "${bucket_region}" || "${bucket_region}" == "None" || "${bucket_region}" == "null" ]]; then
    bucket_region="us-east-1"
  fi

  configure_s3_aws_cmd "${bucket_region}"
  return 0
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
    printf '[git-fetch-via-ec2] múltiplas instâncias encontradas para Name=%s:\n%s\n' "${INSTANCE_NAME}" "${output}" >&2
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

  if "${S3_AWS_CMD[@]}" s3api head-bucket --bucket "${S3_BUCKET}" >/dev/null 2>&1; then
    resolve_s3_bucket_region || true
    return 0
  fi

  if resolve_s3_bucket_region; then
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

resolve_remote_principal_arn_via_ssm() {
  local parameter_file command_id principal_arn
  parameter_file="$(make_temp_file "git-fetch-via-ec2-identity" ".json")"

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
    --comment "git-fetch-via-ec2 identity probe ${INSTANCE_ID}" \
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

  current_policy_file="$(make_temp_file "git-fetch-via-ec2-policy-current" ".json")"
  merged_policy_file="$(make_temp_file "git-fetch-via-ec2-policy-merged" ".json")"

  current_policy="$("${S3_AWS_CMD[@]}" s3api get-bucket-policy \
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

  sid_bucket="GitFetchViaEc2ListBucket${INSTANCE_ID//-/}"
  sid_object="GitFetchViaEc2ObjectAccess${INSTANCE_ID//-/}"

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

  "${S3_AWS_CMD[@]}" s3api put-bucket-policy --bucket "${S3_BUCKET}" --policy "file://${merged_policy_file}" >/dev/null
  rm -f "${current_policy_file}" "${merged_policy_file}"
}

prepare_run_artifacts() {
  RUN_ID="$(date +%Y%m%d%H%M%S)-$(random_suffix)"
  RUN_S3_PREFIX="${S3_PREFIX%/}/${INSTANCE_NAME}/${RUN_ID}"
  INPUT_KEY="${RUN_S3_PREFIX}/input/repo-git.tar.gz"
  OUTPUT_KEY="${RUN_S3_PREFIX}/output/repo-git.tar.gz"
}

cleanup_s3_run_artifacts() {
  if [[ -n "${RUN_S3_PREFIX}" ]]; then
    "${S3_AWS_CMD[@]}" s3 rm "s3://${S3_BUCKET}/${RUN_S3_PREFIX}/" --recursive >/dev/null 2>&1 || true
  fi
}

cleanup_local_temp_dir() {
  if [[ -n "${LOCAL_TEMP_DIR}" && -d "${LOCAL_TEMP_DIR}" ]]; then
    rm -rf "${LOCAL_TEMP_DIR}"
  fi
}

cleanup_all() {
  cleanup_s3_run_artifacts
  cleanup_local_temp_dir
}

prepare_local_archives() {
  LOCAL_TEMP_DIR="$(make_temp_dir "git-fetch-via-ec2")"
  INPUT_ARCHIVE_PATH="${LOCAL_TEMP_DIR}/repo-git-input.tar.gz"
  OUTPUT_ARCHIVE_PATH="${LOCAL_TEMP_DIR}/repo-git-output.tar.gz"
  LOCAL_GIT_BASENAME="$(basename "${LOCAL_GIT_DIR}")"

  tar -czf "${INPUT_ARCHIVE_PATH}" -C "$(dirname "${LOCAL_GIT_DIR}")" "${LOCAL_GIT_BASENAME}"
  [[ -s "${INPUT_ARCHIVE_PATH}" ]] || die "não foi possível empacotar ${LOCAL_GIT_DIR}"
}

upload_input_archive_to_s3() {
  "${S3_AWS_CMD[@]}" s3 cp "${INPUT_ARCHIVE_PATH}" "s3://${S3_BUCKET}/${INPUT_KEY}" --only-show-errors >/dev/null
}

build_ssm_parameters_file() {
  local parameter_file
  parameter_file="$1"

  require_command python3
  python3 - "${parameter_file}" "${AWS_REGION_NAME}" "${INPUT_KEY}" "${OUTPUT_KEY}" "${S3_BUCKET}" "${PROXY_URL}" "${INSECURE}" "${RUN_ID}" "${LOCAL_GIT_BASENAME}" "${FETCH_ARGS[@]}" <<'PY'
import json
import shlex
import sys

parameter_file = sys.argv[1]
aws_region = sys.argv[2]
input_key = sys.argv[3]
output_key = sys.argv[4]
s3_bucket = sys.argv[5]
proxy_url = sys.argv[6]
insecure = sys.argv[7] == "1"
run_id = sys.argv[8]
git_basename = sys.argv[9]
fetch_args = [arg for arg in sys.argv[10:] if arg]

remote_dir = f"/tmp/git-fetch-via-ec2/{run_id}"
remote_input = f"{remote_dir}/input.tar.gz"
remote_output = f"{remote_dir}/output.tar.gz"
remote_extract_dir = f"{remote_dir}/extract"
remote_git_dir = f"{remote_extract_dir}/{git_basename}"
remote_home_dir = f"{remote_dir}/home"
remote_url_sh = (
    "remote_url=\"$(git --git-dir "
    + shlex.quote(remote_git_dir)
    + " config --get remote.origin.url || true)\"; "
    + "case \"$remote_url\" in "
    + "git@github.com:*) remote_url=\"https://github.com/${remote_url#git@github.com:}\" ;; "
    + "ssh://git@github.com/*) remote_url=\"https://github.com/${remote_url#ssh://git@github.com/}\" ;; "
    + "git://github.com/*) remote_url=\"https://github.com/${remote_url#git://github.com/}\" ;; "
    + "esac; "
    + "[ -n \"$remote_url\" ] || { echo \"remote.origin.url não encontrado\" >&2; exit 1; }; "
    + "git --git-dir "
    + shlex.quote(remote_git_dir)
    + " remote set-url origin \"$remote_url\" >/dev/null 2>&1 || true"
)
cleanup_extraheaders_sh = (
    "git --git-dir "
    + shlex.quote(remote_git_dir)
    + " config --local --name-only --get-regexp '^http\\..*\\.extraheader$' 2>/dev/null | "
    + "while IFS= read -r key; do git --git-dir "
    + shlex.quote(remote_git_dir)
    + " config --local --unset-all \"$key\" >/dev/null 2>&1 || true; done"
)

commands = [
    "set -euo pipefail",
    "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE",
    "unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME",
    "unset AWS_EC2_METADATA_DISABLED",
    "unset HTTPS_PROXY HTTP_PROXY ALL_PROXY https_proxy http_proxy all_proxy",
    "export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null",
    'export GIT_TERMINAL_PROMPT=0',
    "export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1",
]
if aws_region:
    commands.append(f'export AWS_REGION="{aws_region}" AWS_DEFAULT_REGION="{aws_region}"')
if proxy_url:
    commands.append(f'export HTTPS_PROXY="{proxy_url}" HTTP_PROXY="{proxy_url}" ALL_PROXY="{proxy_url}"')
    commands.append(f'export https_proxy="{proxy_url}" http_proxy="{proxy_url}" all_proxy="{proxy_url}"')
commands.append('command -v git >/dev/null 2>&1 || { echo "git não encontrado no EC2" >&2; exit 1; }')
commands.append(f'rm -rf {shlex.quote(remote_dir)}')
commands.append(f'mkdir -p {shlex.quote(remote_extract_dir)} {shlex.quote(remote_home_dir)}')
commands.append(f'export HOME={shlex.quote(remote_home_dir)}')
commands.append(f'aws s3 cp {shlex.quote(f"s3://{s3_bucket}/{input_key}")} {shlex.quote(remote_input)} --only-show-errors >/dev/null')
commands.append(f'tar -xzf {shlex.quote(remote_input)} -C {shlex.quote(remote_extract_dir)}')
commands.append(f'test -d {shlex.quote(remote_git_dir)}')
commands.append(remote_url_sh)
commands.append(f'git --git-dir {shlex.quote(remote_git_dir)} config --local --unset-all http.proxy >/dev/null 2>&1 || true')
commands.append(f'git --git-dir {shlex.quote(remote_git_dir)} config --local --unset-all https.proxy >/dev/null 2>&1 || true')
commands.append(f'git --git-dir {shlex.quote(remote_git_dir)} config --local --unset-all http.extraheader >/dev/null 2>&1 || true')
commands.append(cleanup_extraheaders_sh)
commands.append(f'git --git-dir {shlex.quote(remote_git_dir)} config --local credential.helper \"\" >/dev/null 2>&1 || true')

fetch_cmd = [
    "git",
    "-c",
    "http.version=HTTP/1.1",
    "-c",
    "credential.helper=",
]
if insecure:
    fetch_cmd.extend(["-c", "http.sslVerify=false"])
fetch_cmd.extend(["--git-dir", remote_git_dir, "fetch"])
fetch_cmd.extend(fetch_args)
commands.append(" ".join(shlex.quote(part) for part in fetch_cmd))
commands.append(f'tar -czf {shlex.quote(remote_output)} -C {shlex.quote(remote_extract_dir)} {shlex.quote(git_basename)}')
commands.append(f'test -s {shlex.quote(remote_output)}')
commands.append(f'aws s3 cp {shlex.quote(remote_output)} {shlex.quote(f"s3://{s3_bucket}/{output_key}")} --only-show-errors >/dev/null')

with open(parameter_file, "w", encoding="utf-8") as handle:
    json.dump({"commands": commands}, handle, indent=2)
    handle.write("\n")
PY
}

s3_object_exists() {
  "${S3_AWS_CMD[@]}" s3api head-object --bucket "${S3_BUCKET}" --key "$1" >/dev/null 2>&1
}

download_result_from_s3() {
  "${S3_AWS_CMD[@]}" s3 cp "s3://${S3_BUCKET}/${OUTPUT_KEY}" "${OUTPUT_ARCHIVE_PATH}" --only-show-errors >/dev/null
}

apply_updated_git_dir() {
  local extract_dir updated_git_dir
  extract_dir="$(make_temp_dir "git-fetch-via-ec2-apply")"
  tar -xzf "${OUTPUT_ARCHIVE_PATH}" -C "${extract_dir}"
  updated_git_dir="${extract_dir}/${LOCAL_GIT_BASENAME}"
  [[ -d "${updated_git_dir}" ]] || die "artefato remoto não contém diretório git esperado: ${LOCAL_GIT_BASENAME}"
  cp -a "${updated_git_dir}/." "${LOCAL_GIT_DIR}/"
  rm -rf "${extract_dir}"
}

run_remote_fetch() {
  local parameter_file command_id
  parameter_file="$(make_temp_file "git-fetch-via-ec2-params-${RUN_ID}" ".json")"
  build_ssm_parameters_file "${parameter_file}"

  command_id="$("${AWS_CMD[@]}" ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name 'AWS-RunShellScript' \
    --comment "git-fetch-via-ec2 ${RUN_ID}" \
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
  apply_updated_git_dir
}

configure_aws_cmd
resolve_instance_from_aws
assert_ssm_managed_instance
ensure_s3_bucket
ensure_s3_bucket_policy_for_remote_principal
prepare_run_artifacts
prepare_local_archives
trap cleanup_all EXIT
upload_input_archive_to_s3
run_remote_fetch

cat <<EOF
Fetch remoto concluído.

Instância:
  ${INSTANCE_NAME} (${INSTANCE_ID})

Bucket:
  ${S3_BUCKET}

Diretório git:
  ${LOCAL_GIT_DIR}
EOF
