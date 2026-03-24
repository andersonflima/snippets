#!/bin/sh
set -eu

LOCALSTACK_CONTAINER_NAME="${LOCALSTACK_CONTAINER_NAME:-localstack-main}"
ROLE_NAME="PlatformAssumeRole"
INLINE_POLICY_NAME="PlatformAssumeRoleAccess"
PERMISSION_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}'
ROOT_OUTPUT_FILE="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)/apps/backend/.localstack-organization-accounts.json"

aws_local() {
  docker exec "$LOCALSTACK_CONTAINER_NAME" awslocal "$@"
}

aws_local_for_account() {
  account_id="$1"
  shift

  docker exec \
    -e AWS_ACCESS_KEY_ID="$account_id" \
    -e AWS_SECRET_ACCESS_KEY="$account_id" \
    "$LOCALSTACK_CONTAINER_NAME" \
    awslocal "$@"
}

normalize_text_output() {
  value="$1"

  if [ "$value" = "None" ] || [ "$value" = "null" ]; then
    printf '%s' ""
    return
  fi

  printf '%s' "$value"
}

ensure_organization() {
  if aws_local organizations describe-organization >/dev/null 2>&1; then
    return
  fi

  aws_local organizations create-organization --feature-set ALL >/dev/null
}

root_id() {
  aws_local organizations list-roots --query 'Roots[0].Id' --output text
}

organization_id() {
  aws_local organizations describe-organization --query 'Organization.Id' --output text
}

find_ou_id() {
  parent_id="$1"
  ou_name="$2"

  normalize_text_output "$(
    aws_local organizations list-organizational-units-for-parent \
      --parent-id "$parent_id" \
      --query "OrganizationalUnits[?Name=='$ou_name'].Id | [0]" \
      --output text
  )"
}

ensure_ou() {
  parent_id="$1"
  ou_name="$2"

  existing_ou_id="$(find_ou_id "$parent_id" "$ou_name")"
  if [ -n "$existing_ou_id" ]; then
    printf '%s' "$existing_ou_id"
    return
  fi

  aws_local organizations create-organizational-unit \
    --parent-id "$parent_id" \
    --name "$ou_name" \
    --query 'OrganizationalUnit.Id' \
    --output text
}

find_account_id_by_email() {
  email="$1"

  normalize_text_output "$(
    aws_local organizations list-accounts \
      --query "Accounts[?Email=='$email'].Id | [0]" \
      --output text
  )"
}

ensure_account() {
  email="$1"
  account_name="$2"

  existing_account_id="$(find_account_id_by_email "$email")"
  if [ -n "$existing_account_id" ]; then
    printf '%s' "$existing_account_id"
    return
  fi

  aws_local organizations create-account \
    --email "$email" \
    --account-name "$account_name" \
    --query 'CreateAccountStatus.AccountId' \
    --output text
}

current_parent_id() {
  account_id="$1"

  aws_local organizations list-parents \
    --child-id "$account_id" \
    --query 'Parents[0].Id' \
    --output text
}

ensure_account_parent() {
  account_id="$1"
  target_parent_id="$2"

  parent_id="$(current_parent_id "$account_id")"
  if [ "$parent_id" = "$target_parent_id" ]; then
    return
  fi

  aws_local organizations move-account \
    --account-id "$account_id" \
    --source-parent-id "$parent_id" \
    --destination-parent-id "$target_parent_id" >/dev/null
}

ensure_platform_role() {
  account_id="$1"
  trust_policy=$(printf '%s' "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${account_id}:root\"},\"Action\":\"sts:AssumeRole\"}]}")

  if aws_local_for_account "$account_id" iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    aws_local_for_account "$account_id" iam update-assume-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-document "$trust_policy" >/dev/null
  else
    aws_local_for_account "$account_id" iam create-role \
      --role-name "$ROLE_NAME" \
      --assume-role-policy-document "$trust_policy" >/dev/null
  fi

  aws_local_for_account "$account_id" iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "$INLINE_POLICY_NAME" \
    --policy-document "$PERMISSION_POLICY" >/dev/null
}

write_inventory_file() {
  org_id="$1"
  root_id_value="$2"
  workloads_ou_id="$3"
  data_ou_id="$4"
  platform_ou_id="$5"
  production_account_id="$6"
  sandbox_account_id="$7"
  data_account_id="$8"
  qa_account_id="$9"
  shared_account_id="${10}"

  mkdir -p "$(dirname "$ROOT_OUTPUT_FILE")"

  cat >"$ROOT_OUTPUT_FILE" <<EOF
{
  "organization": {
    "id": "$org_id",
    "rootId": "$root_id_value"
  },
  "organizationalUnits": {
    "workloads": "$workloads_ou_id",
    "data": "$data_ou_id",
    "platform": "$platform_ou_id"
  },
  "seedAccounts": {
    "production": {
      "accountId": "$production_account_id",
      "name": "Production",
      "allowedRegions": ["us-east-1", "us-west-2", "sa-east-1"]
    },
    "sandbox": {
      "accountId": "$sandbox_account_id",
      "name": "Sandbox",
      "allowedRegions": ["us-east-1", "eu-west-1", "sa-east-1"]
    },
    "data": {
      "accountId": "$data_account_id",
      "name": "Data",
      "allowedRegions": ["us-east-1", "us-east-2"]
    },
    "qa": {
      "accountId": "$qa_account_id",
      "name": "QA",
      "allowedRegions": ["us-east-1", "eu-central-1"]
    },
    "shared": {
      "accountId": "$shared_account_id",
      "name": "Shared Services",
      "allowedRegions": ["us-east-1", "sa-east-1"]
    }
  }
}
EOF
}

ensure_organization

root_id_value="$(root_id)"
org_id="$(organization_id)"

workloads_ou_id="$(ensure_ou "$root_id_value" "Workloads")"
data_ou_id="$(ensure_ou "$root_id_value" "Data")"
platform_ou_id="$(ensure_ou "$root_id_value" "Platform")"

production_account_id="$(ensure_account "production@platform.local" "Production")"
sandbox_account_id="$(ensure_account "sandbox@platform.local" "Sandbox")"
data_account_id="$(ensure_account "data@platform.local" "Data")"
qa_account_id="$(ensure_account "qa@platform.local" "QA")"
shared_account_id="$(ensure_account "shared-services@platform.local" "Shared Services")"

ensure_account_parent "$production_account_id" "$workloads_ou_id"
ensure_account_parent "$sandbox_account_id" "$workloads_ou_id"
ensure_account_parent "$qa_account_id" "$workloads_ou_id"
ensure_account_parent "$data_account_id" "$data_ou_id"
ensure_account_parent "$shared_account_id" "$platform_ou_id"

ensure_platform_role "$production_account_id"
ensure_platform_role "$sandbox_account_id"
ensure_platform_role "$data_account_id"
ensure_platform_role "$qa_account_id"
ensure_platform_role "$shared_account_id"

write_inventory_file \
  "$org_id" \
  "$root_id_value" \
  "$workloads_ou_id" \
  "$data_ou_id" \
  "$platform_ou_id" \
  "$production_account_id" \
  "$sandbox_account_id" \
  "$data_account_id" \
  "$qa_account_id" \
  "$shared_account_id"

printf 'Organization %s pronta no container %s\n' "$org_id" "$LOCALSTACK_CONTAINER_NAME"
printf 'Inventory gerado em %s\n' "$ROOT_OUTPUT_FILE"
printf 'Production=%s Sandbox=%s Data=%s QA=%s Shared=%s\n' \
  "$production_account_id" \
  "$sandbox_account_id" \
  "$data_account_id" \
  "$qa_account_id" \
  "$shared_account_id"
