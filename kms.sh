#!/usr/bin/env bash

# -----------------------------
# SAFE SHELL CONFIG (portable)
# -----------------------------
set -eu
if (set -o pipefail) 2>/dev/null; then
  set -o pipefail
fi

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

is_empty() {
  [ -z "${1:-}" ] || [ "$1" = "None" ]
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
# RESOLVERS
# -----------------------------
resolve_kms_from_alias() {
  aws kms list-aliases \
    --query "Aliases[?contains(AliasName, \`${cluster_id}\`)].TargetKeyId | [0]" \
    --output text 2>/dev/null || echo ""
}

resolve_kms_from_rds_instance() {
  aws rds describe-db-instances \
    --db-instance-identifier "$cluster_id" \
    --query 'DBInstances[0].KmsKeyId' \
    --output text 2>/dev/null || echo ""
}

resolve_kms_from_rds_cluster() {
  aws rds describe-db-clusters \
    --db-cluster-identifier "$cluster_id" \
    --query 'DBClusters[0].KmsKeyId' \
    --output text 2>/dev/null || echo ""
}

resolve_kms_from_elasticache_rg() {
  aws elasticache describe-replication-groups \
    --replication-group-id "$cluster_id" \
    --query 'ReplicationGroups[0].KmsKeyId' \
    --output text 2>/dev/null || echo ""
}

resolve_kms_from_elasticache_cluster() {
  aws elasticache describe-cache-clusters \
    --cache-cluster-id "$cluster_id" \
    --query 'CacheClusters[0].KmsKeyId' \
    --output text 2>/dev/null || echo ""
}

resolve_kms_from_secret() {
  aws secretsmanager list-secrets \
    --filters Key=name,Values="${cluster_id}-aws" \
    --query 'SecretList[0].KmsKeyId' \
    --output text 2>/dev/null || echo ""
}

# -----------------------------
# RESOLUTION FLOW
# -----------------------------
kms_key_id=""

log "Step 1: RDS instance"
kms_key_id=$(resolve_kms_from_rds_instance)

if is_empty "$kms_key_id"; then
  log "Step 2: RDS cluster"
  kms_key_id=$(resolve_kms_from_rds_cluster)
fi

if is_empty "$kms_key_id"; then
  log "Step 3: ElastiCache replication group"
  kms_key_id=$(resolve_kms_from_elasticache_rg)
fi

if is_empty "$kms_key_id"; then
  log "Step 4: ElastiCache cluster"
  kms_key_id=$(resolve_kms_from_elasticache_cluster)
fi

if is_empty "$kms_key_id"; then
  log "Step 5: alias (fallback)"
  kms_key_id=$(resolve_kms_from_alias)
fi

if is_empty "$kms_key_id"; then
  log "Step 6: Secrets Manager"
  kms_key_id=$(resolve_kms_from_secret)
fi

if is_empty "$kms_key_id"; then
  fail "Não foi possível resolver KMS para cluster '${cluster_id}'"
fi

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
log "Aplicando policy no KMS..."

aws kms put-key-policy \
  --key-id "$kms_key_id" \
  --policy-name default \
  --policy "$policy"

log "Policy aplicada com sucesso"
