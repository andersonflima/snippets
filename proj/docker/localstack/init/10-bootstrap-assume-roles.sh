#!/bin/sh
set -eu

ROLE_NAME="PlatformAssumeRole"
INLINE_POLICY_NAME="PlatformAssumeRoleAccess"
ACCOUNT_IDS="111111111111 222222222222 333333333333 444444444444 555555555555"
PERMISSION_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}'

create_or_update_role() {
  account_id="$1"
  trust_policy=$(printf '%s' "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${account_id}:root\"},\"Action\":\"sts:AssumeRole\"}]}")

  export AWS_ACCESS_KEY_ID="$account_id"
  export AWS_SECRET_ACCESS_KEY="$account_id"

  if awslocal iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    awslocal iam update-assume-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-document "$trust_policy" >/dev/null
  else
    awslocal iam create-role \
      --role-name "$ROLE_NAME" \
      --assume-role-policy-document "$trust_policy" >/dev/null
  fi

  awslocal iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "$INLINE_POLICY_NAME" \
    --policy-document "$PERMISSION_POLICY" >/dev/null

  printf 'Role %s pronta em %s\n' "$ROLE_NAME" "$account_id"
}

for account_id in $ACCOUNT_IDS; do
  create_or_update_role "$account_id"
done
