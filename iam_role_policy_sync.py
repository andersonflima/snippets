"""CLI Python para criar e atualizar roles e policies IAM para Lambda e outros serviços.

Uso
===

```bash
python3 iam_role_policy_sync.py --role-name analytics-lambda-role --need athena
python3 iam_role_policy_sync.py --role-name analytics-job-role --service ecs-task --need athena
python3 iam_role_policy_sync.py --config-file iam-config.json
python3 iam_role_policy_sync.py --config-file iam-config.json --profile sandbox --dry-run
```

Formato do arquivo JSON
=======================

Modo simples
============

```json
{
  "simple_roles": [
    {
      "name": "analytics-lambda-role",
      "service": "lambda",
      "needs": ["athena"]
    }
  ]
}
```

Nesse modo o script resolve o minimo viavel automaticamente.

```json
{
  "roles": [
    {
      "name": "orders-lambda-role",
      "description": "Role de execucao da Lambda Orders",
      "service_principals": ["lambda.amazonaws.com"],
      "managed_policy_arns": [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
      ],
      "managed_policy_refs": ["shared-dynamodb-read"],
      "inline_policies": [
        {
          "name": "allow-sqs-send",
          "document": {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Action": ["sqs:SendMessage"],
                "Resource": ["arn:aws:sqs:sa-east-1:123456789012:orders"]
              }
            ]
          }
        }
      ]
    }
  ],
  "managed_policies": [
    {
      "name": "shared-dynamodb-read",
      "description": "Acesso compartilhado de leitura no DynamoDB",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
            "Resource": ["arn:aws:dynamodb:sa-east-1:123456789012:table/orders"]
          }
        ]
      },
      "attach_to_roles": ["orders-lambda-role"]
    }
  ]
}
```

Observações
===========

- `service_principals` aceita Lambda e qualquer outro principal AWS, como:
  - `lambda.amazonaws.com`
  - `ecs-tasks.amazonaws.com`
  - `events.amazonaws.com`
  - `states.amazonaws.com`
- Para trusts mais complexos, use `trust_policy_document`.
- O script é idempotente:
  - cria role/policy quando não existe
  - atualiza trust policy quando mudou
  - cria nova versão da customer managed policy quando o documento mudou
  - anexa managed policies ausentes
  - reaplica inline policies
- O script não remove anexos nem deleta roles/policies.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.parse
from typing import Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError


LOGGER = logging.getLogger(__name__)
DEFAULT_TRUST_ACTIONS = ["sts:AssumeRole"]
AWS_LAMBDA_BASIC_EXECUTION_ROLE_ARN = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
SERVICE_PRESETS = {
    "lambda": {
        "service_principals": ["lambda.amazonaws.com"],
        "managed_policy_arns": [AWS_LAMBDA_BASIC_EXECUTION_ROLE_ARN],
    },
    "ecs-task": {
        "service_principals": ["ecs-tasks.amazonaws.com"],
        "managed_policy_arns": [],
    },
    "events": {
        "service_principals": ["events.amazonaws.com"],
        "managed_policy_arns": [],
    },
    "states": {
        "service_principals": ["states.amazonaws.com"],
        "managed_policy_arns": [],
    },
    "apigateway": {
        "service_principals": ["apigateway.amazonaws.com"],
        "managed_policy_arns": [],
    },
}
SERVICE_ALIASES = {
    "lambda": "lambda",
    "lambda.amazonaws.com": "lambda",
    "ecs": "ecs-task",
    "ecs-task": "ecs-task",
    "ecs-tasks.amazonaws.com": "ecs-task",
    "eventbridge": "events",
    "events": "events",
    "events.amazonaws.com": "events",
    "step-functions": "states",
    "states": "states",
    "states.amazonaws.com": "states",
    "apigateway": "apigateway",
    "apigateway.amazonaws.com": "apigateway",
}
NEED_PRESETS = {
    "athena": {
        "inline_policies": [
            {
                "name": "preset-athena-access",
                "document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AthenaQueryExecution",
                            "Effect": "Allow",
                            "Action": [
                                "athena:GetQueryExecution",
                                "athena:GetQueryResults",
                                "athena:GetWorkGroup",
                                "athena:StartQueryExecution",
                                "athena:StopQueryExecution",
                            ],
                            "Resource": "*",
                        },
                        {
                            "Sid": "AthenaCatalogRead",
                            "Effect": "Allow",
                            "Action": [
                                "glue:BatchGetPartition",
                                "glue:GetDatabase",
                                "glue:GetDatabases",
                                "glue:GetPartition",
                                "glue:GetPartitions",
                                "glue:GetTable",
                                "glue:GetTables",
                            ],
                            "Resource": "*",
                        },
                        {
                            "Sid": "AthenaResultBucketAccess",
                            "Effect": "Allow",
                            "Action": [
                                "s3:AbortMultipartUpload",
                                "s3:GetBucketLocation",
                                "s3:GetObject",
                                "s3:ListBucket",
                                "s3:ListBucketMultipartUploads",
                                "s3:ListMultipartUploadParts",
                                "s3:PutObject",
                            ],
                            "Resource": "*",
                        },
                    ],
                },
            }
        ]
    }
}


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))
    LOGGER.setLevel(getattr(logging, log_level.upper(), logging.INFO))


def _build_aws_session(profile_name: str) -> Any:
    session_kwargs = {}
    if profile_name:
        session_kwargs["profile_name"] = profile_name
    return boto3.session.Session(**session_kwargs)


def _parse_cli_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cria ou atualiza roles e policies IAM para Lambda e outros serviços.",
    )
    parser.add_argument("--role-name", help="Modo simples: nome da role para criar ou atualizar.")
    parser.add_argument(
        "--service",
        default="lambda",
        help="Modo simples: servico que assumira a role. Ex.: lambda, ecs-task, events, states. Padrao: lambda.",
    )
    parser.add_argument(
        "--need",
        dest="needs",
        action="append",
        help="Modo simples: necessidade funcional. Ex.: athena. Pode repetir.",
    )
    parser.add_argument("--description", help="Modo simples: descricao da role.")
    parser.add_argument("--config-file", help="Arquivo JSON com a definicao das roles e policies.")
    parser.add_argument("--config-json", help="Payload JSON inline.")
    parser.add_argument("--profile", help="AWS profile para execucao local.")
    parser.add_argument("--dry-run", action="store_true", help="Nao aplica mudancas; apenas mostra o plano.")
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Nivel de log. Padrao: INFO.",
    )
    return parser.parse_args(argv)


def _json_dumps_canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _parse_json_object(raw_value: str, *, field_name: str) -> dict[str, Any]:
    try:
        parsed_value = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(f"{field_name} nao contem JSON valido") from error

    if not isinstance(parsed_value, dict):
        raise ValueError(f"{field_name} deve representar um objeto JSON")
    return parsed_value


def _load_config_from_cli(args: argparse.Namespace) -> dict[str, Any]:
    if args.config_file and args.config_json:
        raise ValueError("use apenas um entre --config-file e --config-json")

    if args.config_file:
        with open(args.config_file, "r", encoding="utf-8") as config_file:
            return _parse_json_object(config_file.read(), field_name="--config-file")

    if args.config_json:
        return _parse_json_object(args.config_json, field_name="--config-json")

    if not sys.stdin.isatty():
        return _parse_json_object(sys.stdin.read(), field_name="stdin")

    raise ValueError("informe --config-file, --config-json ou envie o JSON via stdin")


def _normalize_text(value: Any, *, field_name: str, required: bool = False, fallback: str = "") -> str:
    if isinstance(value, str):
        normalized_value = value.strip()
        if normalized_value:
            return normalized_value
    if required:
        raise ValueError(f"{field_name} e obrigatorio")
    return fallback


def _normalize_text_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        normalized_value = value.strip()
        return [normalized_value] if normalized_value else []
    if isinstance(value, list):
        normalized_values = [
            item.strip()
            for item in value
            if isinstance(item, str) and item.strip()
        ]
        return normalized_values
    raise ValueError(f"{field_name} deve ser string ou lista de strings")


def _deduplicate_text_list(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _normalize_bool(value: Any, *, field_name: str, fallback: bool = False) -> bool:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized_value = value.strip().lower()
        if normalized_value in {"1", "true", "yes", "on"}:
            return True
        if normalized_value in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} deve ser booleano")


def _normalize_policy_document(value: Any, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded_value = urllib.parse.unquote(value).strip()
        if not decoded_value:
            raise ValueError(f"{field_name} nao pode ser vazio")
        parsed_value = _parse_json_object(decoded_value, field_name=field_name)
        return parsed_value
    raise ValueError(f"{field_name} deve ser objeto JSON")


def _normalize_tags(value: Any, *, field_name: str) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [
            {"Key": str(key), "Value": str(item_value)}
            for key, item_value in sorted(value.items())
        ]
    if isinstance(value, list):
        normalized_tags = []
        for item in value:
            if not isinstance(item, dict):
                raise ValueError(f"{field_name} deve conter objetos")
            key = _normalize_text(
                item.get("Key", item.get("key")),
                field_name=f"{field_name}.Key",
                required=True,
            )
            tag_value = _normalize_text(
                item.get("Value", item.get("value")),
                field_name=f"{field_name}.Value",
                required=True,
            )
            normalized_tags.append({"Key": key, "Value": tag_value})
        return sorted(normalized_tags, key=lambda item: item["Key"])
    raise ValueError(f"{field_name} deve ser objeto ou lista de objetos")


def _normalize_optional_int(
    value: Any,
    *,
    field_name: str,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} deve ser inteiro")
    if isinstance(value, int):
        parsed_value = value
    elif isinstance(value, str) and value.strip():
        parsed_value = int(value.strip())
    else:
        raise ValueError(f"{field_name} deve ser inteiro")

    if minimum is not None and parsed_value < minimum:
        raise ValueError(f"{field_name} deve ser >= {minimum}")
    if maximum is not None and parsed_value > maximum:
        raise ValueError(f"{field_name} deve ser <= {maximum}")
    return parsed_value


def _build_trust_policy_document(role_spec: dict[str, Any]) -> dict[str, Any]:
    if role_spec["trust_policy_document"]:
        return role_spec["trust_policy_document"]

    principal: dict[str, Any] = {}
    if role_spec["service_principals"]:
        principal["Service"] = _to_singleton_or_list(role_spec["service_principals"])
    if role_spec["aws_principals"]:
        principal["AWS"] = _to_singleton_or_list(role_spec["aws_principals"])

    if not principal:
        raise ValueError(
            f"role {role_spec['name']} precisa de service_principals, aws_principals ou trust_policy_document"
        )

    statement: dict[str, Any] = {
        "Effect": "Allow",
        "Principal": principal,
        "Action": _to_singleton_or_list(role_spec["trust_actions"] or DEFAULT_TRUST_ACTIONS),
    }
    if role_spec["trust_conditions"]:
        statement["Condition"] = role_spec["trust_conditions"]

    return {
        "Version": "2012-10-17",
        "Statement": [statement],
    }


def _to_singleton_or_list(values: list[str]) -> Any:
    return values[0] if len(values) == 1 else values


def _resolve_service_preset(service_name: Any) -> dict[str, Any]:
    normalized_service_name = _normalize_text(service_name, field_name="service", required=True).lower()
    resolved_service_name = SERVICE_ALIASES.get(normalized_service_name)
    if not resolved_service_name:
        raise ValueError(f"service nao suportado: {normalized_service_name}")
    service_preset = SERVICE_PRESETS[resolved_service_name]
    return {
        "name": resolved_service_name,
        **service_preset,
    }


def _resolve_need_inline_policies(need_names: list[str]) -> list[dict[str, Any]]:
    resolved_policies = []
    for need_name in need_names:
        normalized_need_name = _normalize_text(need_name, field_name="need", required=True).lower()
        need_preset = NEED_PRESETS.get(normalized_need_name)
        if not need_preset:
            raise ValueError(f"need nao suportado: {normalized_need_name}")
        resolved_policies.extend(need_preset.get("inline_policies", []))
    return [
        {
            "name": _normalize_text(policy["name"], field_name=f"preset[{index}].name", required=True),
            "document": _normalize_policy_document(
                policy["document"],
                field_name=f"preset[{index}].document",
            ),
        }
        for index, policy in enumerate(resolved_policies)
    ]


def _build_simple_role_description(*, role_name: str, service_name: str, need_names: list[str], raw_description: str) -> str:
    if raw_description:
        return raw_description
    if not need_names:
        return f"Auto-managed role for {service_name}"
    return f"Auto-managed role for {service_name} with {', '.join(need_names)} access"


def _build_inline_policy_spec(raw_policy: Any, *, role_name: str, index: int) -> dict[str, Any]:
    if not isinstance(raw_policy, dict):
        raise ValueError(f"roles[{role_name}].inline_policies[{index}] deve ser objeto")
    return {
        "name": _normalize_text(raw_policy.get("name"), field_name=f"inline_policy[{index}].name", required=True),
        "document": _normalize_policy_document(
            raw_policy.get("document"),
            field_name=f"inline_policy[{index}].document",
        ),
    }


def _build_role_spec(raw_role: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw_role, dict):
        raise ValueError(f"roles[{index}] deve ser objeto")

    role_name = _normalize_text(raw_role.get("name"), field_name=f"roles[{index}].name", required=True)
    inline_policies = [
        _build_inline_policy_spec(raw_policy, role_name=role_name, index=policy_index)
        for policy_index, raw_policy in enumerate(raw_role.get("inline_policies", []))
    ]

    return {
        "name": role_name,
        "path": _normalize_text(raw_role.get("path"), field_name=f"roles[{index}].path", fallback="/"),
        "description": _normalize_text(raw_role.get("description"), field_name=f"roles[{index}].description"),
        "max_session_duration": _normalize_optional_int(
            raw_role.get("max_session_duration"),
            field_name=f"roles[{index}].max_session_duration",
            minimum=3600,
            maximum=43200,
        ),
        "service_principals": _normalize_text_list(
            raw_role.get("service_principals", raw_role.get("service_principal")),
            field_name=f"roles[{index}].service_principals",
        ),
        "aws_principals": _normalize_text_list(
            raw_role.get("aws_principals", raw_role.get("aws_principal")),
            field_name=f"roles[{index}].aws_principals",
        ),
        "trust_actions": _normalize_text_list(
            raw_role.get("trust_actions"),
            field_name=f"roles[{index}].trust_actions",
        ),
        "trust_conditions": _normalize_policy_document(
            raw_role.get("trust_conditions") or {},
            field_name=f"roles[{index}].trust_conditions",
        ) if raw_role.get("trust_conditions") is not None else {},
        "trust_policy_document": _normalize_policy_document(
            raw_role.get("trust_policy_document"),
            field_name=f"roles[{index}].trust_policy_document",
        ) if raw_role.get("trust_policy_document") is not None else {},
        "managed_policy_arns": _normalize_text_list(
            raw_role.get("managed_policy_arns"),
            field_name=f"roles[{index}].managed_policy_arns",
        ),
        "managed_policy_refs": _normalize_text_list(
            raw_role.get("managed_policy_refs"),
            field_name=f"roles[{index}].managed_policy_refs",
        ),
        "inline_policies": inline_policies,
        "merge_trust_principals": _normalize_bool(
            raw_role.get("merge_trust_principals"),
            field_name=f"roles[{index}].merge_trust_principals",
            fallback=False,
        ),
        "tags": _normalize_tags(raw_role.get("tags"), field_name=f"roles[{index}].tags"),
    }


def _build_simple_role_spec(raw_role: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw_role, dict):
        raise ValueError(f"simple_roles[{index}] deve ser objeto")

    role_name = _normalize_text(raw_role.get("name"), field_name=f"simple_roles[{index}].name", required=True)
    service_preset = _resolve_service_preset(raw_role.get("service", "lambda"))
    need_names = _normalize_text_list(raw_role.get("needs"), field_name=f"simple_roles[{index}].needs")
    inline_policies = _resolve_need_inline_policies(need_names)

    return {
        "name": role_name,
        "path": _normalize_text(raw_role.get("path"), field_name=f"simple_roles[{index}].path", fallback="/"),
        "description": _build_simple_role_description(
            role_name=role_name,
            service_name=service_preset["name"],
            need_names=need_names,
            raw_description=_normalize_text(
                raw_role.get("description"),
                field_name=f"simple_roles[{index}].description",
            ),
        ),
        "max_session_duration": _normalize_optional_int(
            raw_role.get("max_session_duration"),
            field_name=f"simple_roles[{index}].max_session_duration",
            minimum=3600,
            maximum=43200,
        ),
        "service_principals": service_preset["service_principals"],
        "aws_principals": [],
        "trust_actions": DEFAULT_TRUST_ACTIONS,
        "trust_conditions": {},
        "trust_policy_document": {},
        "managed_policy_arns": _deduplicate_text_list(service_preset["managed_policy_arns"]),
        "managed_policy_refs": [],
        "inline_policies": inline_policies,
        "merge_trust_principals": True,
        "tags": _normalize_tags(raw_role.get("tags"), field_name=f"simple_roles[{index}].tags"),
    }


def _build_managed_policy_spec(raw_policy: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw_policy, dict):
        raise ValueError(f"managed_policies[{index}] deve ser objeto")

    policy_arn = _normalize_text(raw_policy.get("policy_arn"), field_name=f"managed_policies[{index}].policy_arn")
    policy_name = _normalize_text(raw_policy.get("name"), field_name=f"managed_policies[{index}].name")
    if not policy_arn and not policy_name:
        raise ValueError(f"managed_policies[{index}] precisa de name ou policy_arn")

    if policy_arn.startswith("arn:aws:iam::aws:policy/") and raw_policy.get("document") is not None:
        raise ValueError(
            f"managed_policies[{index}] nao pode atualizar policy gerenciada pela AWS: {policy_arn}"
        )

    return {
        "name": policy_name,
        "policy_arn": policy_arn,
        "path": _normalize_text(raw_policy.get("path"), field_name=f"managed_policies[{index}].path", fallback="/"),
        "description": _normalize_text(
            raw_policy.get("description"),
            field_name=f"managed_policies[{index}].description",
        ),
        "document": _normalize_policy_document(
            raw_policy.get("document"),
            field_name=f"managed_policies[{index}].document",
        ) if raw_policy.get("document") is not None else {},
        "attach_to_roles": _normalize_text_list(
            raw_policy.get("attach_to_roles"),
            field_name=f"managed_policies[{index}].attach_to_roles",
        ),
        "tags": _normalize_tags(raw_policy.get("tags"), field_name=f"managed_policies[{index}].tags"),
    }


def build_sync_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    simple_roles = [
        _build_simple_role_spec(raw_role, index=index)
        for index, raw_role in enumerate(raw_config.get("simple_roles", []))
    ]
    explicit_roles = [
        _build_role_spec(raw_role, index=index)
        for index, raw_role in enumerate(raw_config.get("roles", []))
    ]
    roles = [*explicit_roles, *simple_roles]
    managed_policies = [
        _build_managed_policy_spec(raw_policy, index=index)
        for index, raw_policy in enumerate(raw_config.get("managed_policies", []))
    ]

    if not roles and not managed_policies:
        raise ValueError("config precisa conter ao menos uma role ou managed policy")

    role_names = [role["name"] for role in roles]
    policy_names = [policy["name"] for policy in managed_policies if policy["name"]]

    if len(role_names) != len(set(role_names)):
        raise ValueError("config contem roles duplicadas")
    if len(policy_names) != len(set(policy_names)):
        raise ValueError("config contem managed_policies duplicadas por nome")

    for role in roles:
        _build_trust_policy_document(role)
        for policy_ref in role["managed_policy_refs"]:
            if not policy_ref.startswith("arn:") and policy_ref not in policy_names:
                raise ValueError(
                    f"role {role['name']} referencia managed policy inexistente: {policy_ref}"
                )

    for policy in managed_policies:
        for role_name in policy["attach_to_roles"]:
            if role_name not in role_names:
                raise ValueError(
                    f"managed policy {policy['name'] or policy['policy_arn']} referencia role inexistente: {role_name}"
                )

    return {
        "roles": roles,
        "managed_policies": managed_policies,
    }


def _is_not_found_error(error: ClientError) -> bool:
    error_code = error.response.get("Error", {}).get("Code", "")
    return error_code in {"NoSuchEntity", "NoSuchEntityException", "ResourceNotFoundException"}


def _get_role_if_exists(iam_client: Any, *, role_name: str) -> Optional[dict[str, Any]]:
    try:
        response = iam_client.get_role(RoleName=role_name)
    except ClientError as error:
        if _is_not_found_error(error):
            return None
        raise
    role = response.get("Role") if isinstance(response, dict) else None
    return role if isinstance(role, dict) else None


def _get_policy_if_exists(iam_client: Any, *, policy_arn: str) -> Optional[dict[str, Any]]:
    try:
        response = iam_client.get_policy(PolicyArn=policy_arn)
    except ClientError as error:
        if _is_not_found_error(error):
            return None
        raise
    policy = response.get("Policy") if isinstance(response, dict) else None
    return policy if isinstance(policy, dict) else None


def _list_local_policies(iam_client: Any, *, path_prefix: str) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    marker: Optional[str] = None

    while True:
        request: dict[str, Any] = {"Scope": "Local", "PathPrefix": path_prefix}
        if marker:
            request["Marker"] = marker
        response = iam_client.list_policies(**request)
        page_policies = response.get("Policies", []) if isinstance(response, dict) else []
        policies.extend(policy for policy in page_policies if isinstance(policy, dict))
        if not response.get("IsTruncated"):
            return policies
        marker = response.get("Marker")


def _find_local_policy_by_name(iam_client: Any, *, policy_name: str, path: str) -> Optional[dict[str, Any]]:
    candidate_policies = _list_local_policies(iam_client, path_prefix=path)
    for policy in candidate_policies:
        if policy.get("PolicyName") == policy_name and policy.get("Path") == path:
            return policy
    return None


def _get_default_policy_document(iam_client: Any, *, policy_arn: str, default_version_id: str) -> dict[str, Any]:
    response = iam_client.get_policy_version(
        PolicyArn=policy_arn,
        VersionId=default_version_id,
    )
    policy_version = response.get("PolicyVersion") if isinstance(response, dict) else None
    if not isinstance(policy_version, dict):
        raise RuntimeError(f"nao foi possivel obter a versao default da policy {policy_arn}")
    return _normalize_policy_document(
        policy_version.get("Document"),
        field_name=f"PolicyVersion[{policy_arn}]",
    )


def _list_policy_versions(iam_client: Any, *, policy_arn: str) -> list[dict[str, Any]]:
    response = iam_client.list_policy_versions(PolicyArn=policy_arn)
    versions = response.get("Versions", []) if isinstance(response, dict) else []
    return [version for version in versions if isinstance(version, dict)]


def _list_attached_role_policy_arns(iam_client: Any, *, role_name: str) -> list[str]:
    attached_policy_arns: list[str] = []
    marker: Optional[str] = None

    while True:
        request: dict[str, Any] = {"RoleName": role_name}
        if marker:
            request["Marker"] = marker
        response = iam_client.list_attached_role_policies(**request)
        attached_policies = response.get("AttachedPolicies", []) if isinstance(response, dict) else []
        attached_policy_arns.extend(
            policy.get("PolicyArn", "")
            for policy in attached_policies
            if isinstance(policy, dict) and policy.get("PolicyArn")
        )
        if not response.get("IsTruncated"):
            return attached_policy_arns
        marker = response.get("Marker")


def _record_action(actions: list[dict[str, Any]], *, action: str, resource_type: str, resource_name: str, **details: Any) -> None:
    actions.append(
        {
            "action": action,
            "resource_type": resource_type,
            "resource_name": resource_name,
            **details,
        }
    )


def _normalize_statement_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def _merge_principal_values(existing_value: Any, desired_value: Any) -> Any:
    merged_values = _deduplicate_text_list(
        [
            *_normalize_statement_values(existing_value),
            *_normalize_statement_values(desired_value),
        ]
    )
    return _to_singleton_or_list(merged_values) if merged_values else existing_value


def _trust_statement_merge_key(statement: dict[str, Any]) -> tuple[str, str, str]:
    effect = _normalize_text(statement.get("Effect"), field_name="Effect", fallback="")
    actions = sorted(_normalize_statement_values(statement.get("Action")))
    condition = statement.get("Condition", {})
    return (
        effect,
        _json_dumps_canonical(actions),
        _json_dumps_canonical(condition),
    )


def _merge_trust_policy_documents(current_document: dict[str, Any], desired_document: dict[str, Any]) -> dict[str, Any]:
    current_statements = [
        statement
        for statement in current_document.get("Statement", [])
        if isinstance(statement, dict)
    ]
    desired_statements = [
        statement
        for statement in desired_document.get("Statement", [])
        if isinstance(statement, dict)
    ]

    merged_statements = [json.loads(json.dumps(statement)) for statement in current_statements]
    for desired_statement in desired_statements:
        desired_key = _trust_statement_merge_key(desired_statement)
        matching_index = next(
            (
                index
                for index, current_statement in enumerate(merged_statements)
                if _trust_statement_merge_key(current_statement) == desired_key
            ),
            None,
        )

        if matching_index is None:
            merged_statements.append(json.loads(json.dumps(desired_statement)))
            continue

        merged_statement = merged_statements[matching_index]
        merged_principal = dict(merged_statement.get("Principal", {}))
        desired_principal = desired_statement.get("Principal", {})
        if not isinstance(desired_principal, dict):
            continue
        if "Service" in desired_principal:
            merged_principal["Service"] = _merge_principal_values(
                merged_principal.get("Service"),
                desired_principal.get("Service"),
            )
        if "AWS" in desired_principal:
            merged_principal["AWS"] = _merge_principal_values(
                merged_principal.get("AWS"),
                desired_principal.get("AWS"),
            )
        merged_statement["Principal"] = merged_principal

    return {
        "Version": _normalize_text(
            current_document.get("Version"),
            field_name="Version",
            fallback=_normalize_text(desired_document.get("Version"), field_name="Version", fallback="2012-10-17"),
        ),
        "Statement": merged_statements,
    }


def _ensure_role(iam_client: Any, *, role_spec: dict[str, Any], dry_run: bool, actions: list[dict[str, Any]]) -> None:
    role_name = role_spec["name"]
    expected_trust_policy = _build_trust_policy_document(role_spec)
    existing_role = _get_role_if_exists(iam_client, role_name=role_name)

    if existing_role is None:
        if dry_run:
            _record_action(actions, action="would_create_role", resource_type="role", resource_name=role_name)
            return

        create_request: dict[str, Any] = {
            "RoleName": role_name,
            "AssumeRolePolicyDocument": json.dumps(expected_trust_policy),
            "Path": role_spec["path"] or "/",
        }
        if role_spec["description"]:
            create_request["Description"] = role_spec["description"]
        if role_spec["max_session_duration"] is not None:
            create_request["MaxSessionDuration"] = role_spec["max_session_duration"]
        if role_spec["tags"]:
            create_request["Tags"] = role_spec["tags"]
        iam_client.create_role(**create_request)
        _record_action(actions, action="create_role", resource_type="role", resource_name=role_name)
        return

    current_trust_policy = _normalize_policy_document(
        existing_role.get("AssumeRolePolicyDocument"),
        field_name=f"role[{role_name}].AssumeRolePolicyDocument",
    )
    target_trust_policy = (
        _merge_trust_policy_documents(current_trust_policy, expected_trust_policy)
        if role_spec["merge_trust_principals"]
        else expected_trust_policy
    )

    if _json_dumps_canonical(current_trust_policy) != _json_dumps_canonical(target_trust_policy):
        if dry_run:
            _record_action(actions, action="would_update_trust_policy", resource_type="role", resource_name=role_name)
        else:
            iam_client.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(target_trust_policy),
            )
            _record_action(actions, action="update_trust_policy", resource_type="role", resource_name=role_name)

    if role_spec["description"] and role_spec["description"] != existing_role.get("Description", ""):
        if dry_run:
            _record_action(actions, action="would_update_role_description", resource_type="role", resource_name=role_name)
        else:
            iam_client.update_role_description(
                RoleName=role_name,
                Description=role_spec["description"],
            )
            _record_action(actions, action="update_role_description", resource_type="role", resource_name=role_name)

    if role_spec["tags"]:
        if dry_run:
            _record_action(
                actions,
                action="would_tag_role",
                resource_type="role",
                resource_name=role_name,
                tag_count=len(role_spec["tags"]),
            )
        else:
            iam_client.tag_role(RoleName=role_name, Tags=role_spec["tags"])
            _record_action(
                actions,
                action="tag_role",
                resource_type="role",
                resource_name=role_name,
                tag_count=len(role_spec["tags"]),
            )


def _ensure_managed_policy(
    iam_client: Any,
    *,
    policy_spec: dict[str, Any],
    dry_run: bool,
    actions: list[dict[str, Any]],
) -> dict[str, str]:
    policy_name = policy_spec["name"] or policy_spec["policy_arn"]
    policy_arn = policy_spec["policy_arn"]
    existing_policy = _get_policy_if_exists(iam_client, policy_arn=policy_arn) if policy_arn else None

    if existing_policy is None and not policy_arn and policy_spec["name"]:
        existing_policy = _find_local_policy_by_name(
            iam_client,
            policy_name=policy_spec["name"],
            path=policy_spec["path"] or "/",
        )

    if existing_policy is None and not policy_spec["document"]:
        if dry_run:
            _record_action(
                actions,
                action="would_reference_external_policy",
                resource_type="managed_policy",
                resource_name=policy_name,
            )
            return {"policy_arn": policy_spec["policy_arn"], "policy_ref": policy_name}
        raise ValueError(f"managed policy {policy_name} nao existe e nao possui document para criacao")

    if existing_policy is None:
        if dry_run:
            _record_action(actions, action="would_create_managed_policy", resource_type="managed_policy", resource_name=policy_name)
            return {"policy_arn": "", "policy_ref": policy_name}

        response = iam_client.create_policy(
            PolicyName=policy_spec["name"],
            PolicyDocument=json.dumps(policy_spec["document"]),
            Path=policy_spec["path"] or "/",
            Description=policy_spec["description"],
            Tags=policy_spec["tags"],
        )
        created_policy = response.get("Policy") if isinstance(response, dict) else None
        if not isinstance(created_policy, dict):
            raise RuntimeError(f"nao foi possivel criar a managed policy {policy_name}")
        _record_action(actions, action="create_managed_policy", resource_type="managed_policy", resource_name=policy_name)
        return {
            "policy_arn": _normalize_text(created_policy.get("Arn"), field_name=f"managed_policy[{policy_name}].Arn", required=True),
            "policy_ref": policy_name,
        }

    resolved_policy_arn = _normalize_text(existing_policy.get("Arn"), field_name=f"managed_policy[{policy_name}].Arn", required=True)
    if not policy_spec["document"]:
        return {"policy_arn": resolved_policy_arn, "policy_ref": policy_name}

    default_version_id = _normalize_text(
        existing_policy.get("DefaultVersionId"),
        field_name=f"managed_policy[{policy_name}].DefaultVersionId",
        required=True,
    )
    current_document = _get_default_policy_document(
        iam_client,
        policy_arn=resolved_policy_arn,
        default_version_id=default_version_id,
    )

    if _json_dumps_canonical(current_document) == _json_dumps_canonical(policy_spec["document"]):
        return {"policy_arn": resolved_policy_arn, "policy_ref": policy_name}

    if dry_run:
        _record_action(
            actions,
            action="would_update_managed_policy_document",
            resource_type="managed_policy",
            resource_name=policy_name,
        )
        return {"policy_arn": resolved_policy_arn, "policy_ref": policy_name}

    policy_versions = _list_policy_versions(iam_client, policy_arn=resolved_policy_arn)
    non_default_versions = [
        version
        for version in policy_versions
        if not version.get("IsDefaultVersion")
    ]
    if len(policy_versions) >= 5 and non_default_versions:
        oldest_non_default_version = sorted(
            non_default_versions,
            key=lambda version: version.get("CreateDate", ""),
        )[0]
        iam_client.delete_policy_version(
            PolicyArn=resolved_policy_arn,
            VersionId=oldest_non_default_version["VersionId"],
        )
        _record_action(
            actions,
            action="delete_old_policy_version",
            resource_type="managed_policy",
            resource_name=policy_name,
            version_id=oldest_non_default_version["VersionId"],
        )

    iam_client.create_policy_version(
        PolicyArn=resolved_policy_arn,
        PolicyDocument=json.dumps(policy_spec["document"]),
        SetAsDefault=True,
    )
    _record_action(
        actions,
        action="update_managed_policy_document",
        resource_type="managed_policy",
        resource_name=policy_name,
    )
    return {"policy_arn": resolved_policy_arn, "policy_ref": policy_name}


def _resolve_role_policy_targets(
    *,
    role_spec: dict[str, Any],
    managed_policy_results: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    direct_targets = [
        {"policy_arn": policy_arn, "policy_ref": policy_arn}
        for policy_arn in role_spec["managed_policy_arns"]
    ]

    referenced_targets = []
    for policy_ref in role_spec["managed_policy_refs"]:
        if policy_ref.startswith("arn:"):
            referenced_targets.append({"policy_arn": policy_ref, "policy_ref": policy_ref})
            continue
        referenced_target = managed_policy_results[policy_ref]
        referenced_targets.append(referenced_target)

    merged_targets = {
        (target["policy_arn"] or target["policy_ref"]): target
        for target in [*direct_targets, *referenced_targets]
    }
    return list(merged_targets.values())


def _attach_managed_policies_to_role(
    iam_client: Any,
    *,
    role_spec: dict[str, Any],
    policy_targets: list[dict[str, str]],
    dry_run: bool,
    actions: list[dict[str, Any]],
) -> None:
    if not policy_targets:
        return

    existing_role = _get_role_if_exists(iam_client, role_name=role_spec["name"])
    existing_attached_policy_arns = (
        set(_list_attached_role_policy_arns(iam_client, role_name=role_spec["name"]))
        if existing_role is not None
        else set()
    )

    for policy_target in policy_targets:
        policy_arn = policy_target["policy_arn"]
        policy_ref = policy_target["policy_ref"]

        if policy_arn and policy_arn in existing_attached_policy_arns:
            continue

        if dry_run:
            _record_action(
                actions,
                action="would_attach_managed_policy",
                resource_type="role_policy_attachment",
                resource_name=role_spec["name"],
                policy_arn=policy_arn,
                policy_ref=policy_ref,
            )
            continue

        if not policy_arn:
            raise RuntimeError(
                f"nao foi possivel resolver o ARN da managed policy {policy_ref} para anexar na role {role_spec['name']}"
            )

        iam_client.attach_role_policy(RoleName=role_spec["name"], PolicyArn=policy_arn)
        _record_action(
            actions,
            action="attach_managed_policy",
            resource_type="role_policy_attachment",
            resource_name=role_spec["name"],
            policy_arn=policy_arn,
        )


def _put_inline_policies(
    iam_client: Any,
    *,
    role_spec: dict[str, Any],
    dry_run: bool,
    actions: list[dict[str, Any]],
) -> None:
    for inline_policy in role_spec["inline_policies"]:
        if dry_run:
            _record_action(
                actions,
                action="would_put_inline_policy",
                resource_type="inline_policy",
                resource_name=f"{role_spec['name']}:{inline_policy['name']}",
            )
            continue

        iam_client.put_role_policy(
            RoleName=role_spec["name"],
            PolicyName=inline_policy["name"],
            PolicyDocument=json.dumps(inline_policy["document"]),
        )
        _record_action(
            actions,
            action="put_inline_policy",
            resource_type="inline_policy",
            resource_name=f"{role_spec['name']}:{inline_policy['name']}",
        )


def sync_iam_resources(iam_client: Any, config: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    managed_policy_results: dict[str, dict[str, str]] = {}

    for policy_spec in config["managed_policies"]:
        policy_result = _ensure_managed_policy(
            iam_client,
            policy_spec=policy_spec,
            dry_run=dry_run,
            actions=actions,
        )
        if policy_spec["name"]:
            managed_policy_results[policy_spec["name"]] = policy_result

    for role_spec in config["roles"]:
        _ensure_role(iam_client, role_spec=role_spec, dry_run=dry_run, actions=actions)

    policy_targets_by_role = {
        role_spec["name"]: _resolve_role_policy_targets(
            role_spec=role_spec,
            managed_policy_results=managed_policy_results,
        )
        for role_spec in config["roles"]
    }

    for policy_spec in config["managed_policies"]:
        for role_name in policy_spec["attach_to_roles"]:
            if policy_spec["name"]:
                policy_targets_by_role[role_name].append(managed_policy_results[policy_spec["name"]])
            elif policy_spec["policy_arn"]:
                policy_targets_by_role[role_name].append(
                    {"policy_arn": policy_spec["policy_arn"], "policy_ref": policy_spec["policy_arn"]}
                )

    for role_spec in config["roles"]:
        role_targets = {
            (target["policy_arn"] or target["policy_ref"]): target
            for target in policy_targets_by_role[role_spec["name"]]
        }
        _attach_managed_policies_to_role(
            iam_client,
            role_spec=role_spec,
            policy_targets=list(role_targets.values()),
            dry_run=dry_run,
            actions=actions,
        )
        _put_inline_policies(iam_client, role_spec=role_spec, dry_run=dry_run, actions=actions)

    summary: dict[str, int] = {}
    for action in actions:
        summary[action["action"]] = summary.get(action["action"], 0) + 1

    return {
        "ok": True,
        "status": "ok",
        "dry_run": dry_run,
        "role_count": len(config["roles"]),
        "managed_policy_count": len(config["managed_policies"]),
        "summary": summary,
        "actions": actions,
    }


def _build_simple_cli_raw_config(args: argparse.Namespace) -> dict[str, Any]:
    if not args.role_name:
        raise ValueError("--role-name e obrigatorio no modo simples")
    return {
        "simple_roles": [
            {
                "name": args.role_name,
                "service": args.service,
                "needs": args.needs or [],
                "description": args.description,
            }
        ]
    }


def run_cli(argv: Optional[list[str]] = None) -> int:
    try:
        args = _parse_cli_args(argv)
        _configure_logging(args.log_level)
        raw_config = _build_simple_cli_raw_config(args) if args.role_name else _load_config_from_cli(args)
        config = build_sync_config(raw_config)
        session = _build_aws_session(args.profile or "")
        iam_client = session.client("iam")
        response = sync_iam_resources(iam_client, config, dry_run=args.dry_run)
    except ValueError as error:
        response = {
            "ok": False,
            "status": "error",
            "error_type": "config",
            "error": str(error),
        }
    except (BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError) as error:
        response = {
            "ok": False,
            "status": "error",
            "error_type": "aws",
            "error": str(error),
        }
    except Exception as error:
        LOGGER.exception("run_cli.runtime_error")
        response = {
            "ok": False,
            "status": "error",
            "error_type": "runtime",
            "error": str(error),
        }

    json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0 if response.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(run_cli())
