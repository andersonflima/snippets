# Role assumida pelos microserviços action-driven na conta-alvo (cliente).
# Deploy NA CONTA DO CLIENTE. É assumida (sts:AssumeRole) pela role de plataforma
# informada em `platform_role_arn`. As permissões vêm de permissions-policy.json
# (fonte única). Trust é parametrizado abaixo.
#
# Uso:
#   terraform init
#   terraform apply \
#     -var 'platform_role_arn=arn:aws:iam::<PLATFORM_ACCOUNT_ID>:role/<PLATFORM_ASSUME_ROLE>'
#     # opcional: -var 'external_id=<id>'

variable "role_name" {
  description = "Nome da role criada na conta-alvo."
  type        = string
  default     = "microservicos-actions-role"
}

variable "platform_role_arn" {
  description = "ARN da role de plataforma autorizada a assumir esta role."
  type        = string
}

variable "external_id" {
  description = "ExternalId opcional exigido no AssumeRole (null = sem ExternalId)."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags aplicadas à role."
  type        = map(string)
  default     = {}
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AllowPlatformAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.platform_role_arn]
    }

    dynamic "condition" {
      for_each = var.external_id == null ? [] : [var.external_id]
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [condition.value]
      }
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = var.role_name
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
  tags                 = var.tags
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.role_name}-policy"
  role   = aws_iam_role.this.id
  policy = file("${path.module}/permissions-policy.json")
}

output "role_arn" {
  description = "ARN da role a ser usada no assume-role pelos microserviços."
  value       = aws_iam_role.this.arn
}
