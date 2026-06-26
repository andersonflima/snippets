#!/usr/bin/env bash

# -----------------------------
# SAFE SHELL CONFIG
# -----------------------------
set -eu
if (set -o pipefail) 2>/dev/null; then
  set -o pipefail
fi

# fallback para bash
if [ -z "${BASH_VERSION:-}" ]; then
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi
fi

echo "[kms.sh] shell=$SHELL bash_version=${BASH_VERSION:-not_bash}" >&2

# -----------------------------
# HELPERS
# -----------------------------
log() { echo "[kms.sh] $*" >&2; }
fail() {
  log "ERROR: $*"
  exit 1
}

normalize() {
  echo "${1:-}" | tr -d '\r\n\t '
}

is_empty() {
  local v
  v=$(normalize "${1:-}")
  [ -z "$v" ] || [ "$v" = "None" ] || [ "$v" = "null" ]
}

# -----------------------------
# INPUT
# -----------------------------
if [ "$#" -lt 2 ]; then
  fail "Uso: kms.sh <cluster_identifier> <region> [policy_name]"
fi

cluster_id="$1"
aws_region="$2"
policy_name="${3:-$cluster_id}"

export AWS_REGION="${AWS_REGION:-$aws_region}"

log "cluster_id=$cluster_id"
log "region=$AWS_REGION"

# -----------------------------
# VALIDAR AWS
# -----------------------------
aws sts get-caller-identity >/dev/null || fail "AWS credentials inválidas"

# -----------------------------
# RESOLVERS (com jq)
# -----------------------------
resolve_rds_instance() {
  aws rds describe-db-instances \
    --db-instance-identifier "$cluster_id" \
    --output json 2>/dev/null | jq -r '.DBInstances[0].KmsKeyId // empty'
}

resolve_rds_cluster() {
  aws rds describe-db-clusters \
    --db-cluster-identifier "$cluster_id" \
    --output json 2>/dev/null | jq -r '.DBClusters[0].KmsKeyId // empty'
}

resolve_elasticache_rg() {
  aws elasticache describe-replication-groups \
    --replication-group-id "$cluster_id" \
    --output json 2>/dev/null | jq -r '.ReplicationGroups[0].KmsKeyId // empty'
}

resolve_elasticache_cluster() {
  aws elasticache describe-cache-clusters \
    --cache-cluster-id "$cluster_id" \
    --output json 2>/dev/null | jq -r '.CacheClusters[0].KmsKeyId // empty'
}

resolve_alias() {
  aws kms list-aliases \
    --output json 2>/dev/null | jq -r ".Aliases[] | select(.AliasName | contains(\"$cluster_id\")) | .TargetKeyId" | head -n1
}

resolve_secret() {
  aws secretsmanager list-secrets \
    --output json 2>/dev/null | jq -r ".SecretList[] | select(.Name | contains(\"$cluster_id\")) | .KmsKeyId" | head -n1
}

# -----------------------------
# RESOLUTION FLOW
# -----------------------------
kms_key_id=""

try_resolve() {
  local name="$1"
  local value
  value=$(normalize "$($name || true)")
  log "$name => '$value'"

  if ! is_empty "$value"; then
    kms_key_id="$value"
    return 0
  fi

  return 1
}

try_resolve resolve_rds_instance ||
  try_resolve resolve_rds_cluster ||
  try_resolve resolve_elasticache_rg ||
  try_resolve resolve_elasticache_cluster ||
  try_resolve resolve_alias ||
  try_resolve resolve_secret ||
  fail "Não foi possível resolver KMS para '${cluster_id}'"

log "KMS resolvido: $kms_key_id"

# -----------------------------
# POLICY BUILD
# -----------------------------
account_id=$(aws sts get-caller-identity --query Account --output text)

policy_id="Resource-Kms-${policy_name//[^A-Za-z0-9_-]/-}"

policy=$(
  cat <<EOF
{
  "Version": "2012-10-17",
  "Id": "${policy_id}",
  "Statement": [
    {
      "Sid": "AllowRoot",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${account_id}:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "AllowServiceUsage",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "rds.amazonaws.com",
          "elasticache.amazonaws.com"
        ]
      },
      "Action": [
        "kms:DescribeKey",
        "kms:CreateGrant",
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*"
      ],
      "Resource": "*",
      "Condition": {
        "StringLike": {
          "aws:SourceArn": [
            "arn:aws:rds:${AWS_REGION}:${account_id}:db:${cluster_id}",
            "arn:aws:rds:${AWS_REGION}:${account_id}:cluster:${cluster_id}",
            "arn:aws:elasticache:${AWS_REGION}:${account_id}:cluster:${cluster_id}",
            "arn:aws:elasticache:${AWS_REGION}:${account_id}:replicationgroup:${cluster_id}"
          ]
        }
      }
    }
  ]
}
EOF
)

# -----------------------------
# APPLY POLICY
# -----------------------------
log "Aplicando policy..."

aws kms put-key-policy \
  --key-id "$kms_key_id" \
  --policy-name default \
  --policy "$policy"

log "Policy aplicada com sucesso"
