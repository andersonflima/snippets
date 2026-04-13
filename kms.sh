echo "$1"
echo "$2"
echo "$3"

# Determina o KMS key ID por alias direto ou resgata via Secrets Manager para manter o vínculo da chave.
kms_identifier="$1"
if [[ "$kms_identifier" == alias/* ]]; then
  resultado=$(aws kms describe-key --key-id "$kms_identifier" --query 'KeyMetadata.KeyId' --output text)
else
  resultado=$(aws secretsmanager list-secrets --filters Key=name,Values="${kms_identifier}-aws" --query 'SecretList[0].KmsKeyId' --output text)
fi

account_id=$(aws sts get-caller-identity --query Account --output text)
aws_region="${AWS_REGION:-${AWS_DEFAULT_REGION:-$2}}"
instance_name="${3:-rds-instance}"
policy_id="Rds-Kms-${instance_name//[^A-Za-z0-9_-]/-}"

policy_raw=$(
  cat <<-EOF
  {
    "Version": "2012-10-17",
    "Id": "${policy_id}",
    "Statement": [
      {
        "Sid": "Allows admin of the key",
        "Effect": "Allow",
        "Principal": {
          "AWS": [
            "arn:aws:iam::${account_id}:root"
          ]
        },
        "Action": "kms:*",
        "Resource": "*"
      },
      {
        "Sid": "Allow RDS to use key for ${instance_name}",
        "Effect": "Allow",
        "Principal": {
          "Service": "rds.amazonaws.com"
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
              "arn:aws:rds:${aws_region}:${account_id}:db:${instance_name}",
              "arn:aws:rds:${aws_region}:${account_id}:cluster:${instance_name}"
            ]
          }
        }
      }
    ]
  }
EOF
)

