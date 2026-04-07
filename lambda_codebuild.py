"""AWS Lambda para orquestrar CodeBuild em contas alvo via AssumeRole.

Handler
=======

- `lambda_codebuild.lambda_handler`

Visão geral
===========

Esta Lambda foi desenhada para rodar em uma conta central e operar builds em
outras contas AWS. O fluxo principal é:

1. receber a lista de alvos
2. resolver qual role deve ser assumida em cada conta
3. ler um `buildspec.yml` de um bucket S3 central
4. opcionalmente garantir que o projeto CodeBuild exista na conta target
5. ler o buildspec YAML do bucket central
6. chamar `codebuild.start_build`

Ela suporta dois modos.

Modo 1: projetos já existentes
------------------------------

Nesse modo a Lambda só assume a role alvo e chama `start_build`.

Campos mínimos:
- `target_role_arns`
- `codebuild_project_name`
- `codebuild_region`
- `codebuild_buildspec_s3_uri` ou `bucket + key`

Modo 2: contas alvo + criação do projeto
----------------------------------------

Nesse modo a Lambda recebe os IDs das contas alvo, monta o ARN da role via
template, carrega o buildspec de um bucket S3 central e cria o projeto
CodeBuild caso ele não exista.

Campos mínimos:
- `target_account_ids`
- `assume_role_arn_template`
- `codebuild_project_name`
- `codebuild_region`
- `codebuild_project_definition`
- `codebuild_buildspec_s3_uri` ou `bucket + key`

Quando usar cada modo
=====================

Use `target_role_arns` quando:
- as roles alvo não seguem um padrão simples
- você já tem os ARNs prontos
- você só quer disparar builds em projetos que já existem

Use `target_account_ids` + `assume_role_arn_template` quando:
- a Lambda roda numa conta central
- as contas target compartilham a mesma convenção de role
- você quer reduzir repetição de configuração
- você quer permitir criação automática do projeto CodeBuild por conta

Placeholders suportados
=======================

Quando a Lambda está no modo `target_account_ids`, alguns campos aceitam o
placeholder `{account_id}`. Esse placeholder é substituído pelo ID da conta
target antes do `AssumeRole`, do `create_project` e do `start_build`.

Campos com suporte prático a `{account_id}`:
- `assume_role_arn_template`
- `codebuild_project_name`
- `codebuild_source_version`
- `codebuild_environment_variables[*].value`
- `codebuild_project_definition`
- `assume_role_external_id`

Exemplo:
- `arn:aws:iam::{account_id}:role/codebuild-trigger`
- `deploy-{account_id}`

Para a conta `111111111111`, isso vira:
- `arn:aws:iam::111111111111:role/codebuild-trigger`
- `deploy-111111111111`

Onde informar o bucket do buildspec
===================================

O buildspec YAML sempre vem de um bucket S3 central. Há dois formatos aceitos:

Formato recomendado:
- `codebuild_buildspec_s3_uri = "s3://meu-bucket/buildspecs/deploy.yml"`

Formato expandido:
- `codebuild_buildspec_s3_bucket = "meu-bucket"`
- `codebuild_buildspec_s3_key = "buildspecs/deploy.yml"`
- `codebuild_buildspec_s3_object_version = "..."` quando precisar de uma
  versão específica do objeto

Importante:
- esse bucket é lido pela Lambda central
- esse bucket não é o bucket de artifacts do CodeBuild
- a role da Lambda central precisa de `s3:GetObject` nesse objeto
- a Lambda não aceita mais buildspec inline como contrato principal

Parâmetros de entrada
=====================

Payload suportado
-----------------

- `target_role_arns` ou `targetRoleArns`
  Lista de ARNs completos das roles alvo.
- `target_account_ids` ou `targetAccountIds`
  Lista de account IDs das contas alvo.
- `assume_role_arn_template` ou `assumeRoleArnTemplate` ou `assume_role`
  Template do ARN da role alvo. Deve conter `{account_id}`.
- `codebuild_project_name` ou `codebuildProjectName`
  Nome do projeto CodeBuild.
- `codebuild_region` ou `codebuildRegion`
  Região do CodeBuild nas contas target.
- `codebuild_project_definition` ou `codebuildProjectDefinition`
  Definição base para `create_project`.
- `codebuild_buildspec_s3_uri` ou `codebuildBuildspecS3Uri`
  URI `s3://bucket/key` do buildspec central.
- `codebuild_buildspec_s3_bucket`
  Bucket do buildspec quando não for usada URI completa.
- `codebuild_buildspec_s3_key`
  Chave do objeto do buildspec no bucket.
- `codebuild_buildspec_s3_object_version`
  Versão específica do objeto no S3, quando aplicável.
- `codebuild_source_version` ou `codebuildSourceVersion` ou `sourceVersion`
  Branch, tag ou commit enviado ao `start_build`.
- `codebuild_environment_variables` ou `environmentVariables`
  Lista no formato:
  `[{ "name": "ENV_NAME", "value": "production", "type": "PLAINTEXT" }]`
- `assume_role_external_id` ou `assumeRoleExternalId`
  External ID opcional usado no `AssumeRole`.
- `assume_role_session_name_prefix` ou `assumeRoleSessionNamePrefix`
  Prefixo da sessão STS.
- `assume_role_duration_seconds` ou `assumeRoleDurationSeconds`
  Duração da sessão STS.
- `max_workers` ou `maxWorkers`
  Quantidade máxima de execuções paralelas.

Variáveis de ambiente
---------------------

Todos os campos principais também podem ser fornecidos por ambiente:

- `TARGET_ROLE_ARNS`
- `TARGET_ACCOUNT_IDS`
- `ASSUME_ROLE_ARN_TEMPLATE`
- `ASSUME_ROLE`
- `CODEBUILD_PROJECT_NAME`
- `CODEBUILD_REGION`
- `CODEBUILD_PROJECT_DEFINITION`
- `CODEBUILD_BUILDSPEC_S3_URI`
- `CODEBUILD_BUILDSPEC_S3_BUCKET`
- `CODEBUILD_BUILDSPEC_S3_KEY`
- `CODEBUILD_BUILDSPEC_S3_OBJECT_VERSION`
- `CODEBUILD_SOURCE_VERSION`
- `CODEBUILD_ENVIRONMENT_VARIABLES`
- `ASSUME_ROLE_EXTERNAL_ID`
- `ASSUME_ROLE_SESSION_NAME_PREFIX`
- `ASSUME_ROLE_DURATION_SECONDS`
- `MAX_WORKERS`
- `LOG_LEVEL`

Precedência
-----------

Variáveis de ambiente têm precedência sobre o payload do evento.

Exemplos práticos
=================

Exemplo 1: usar projetos já existentes
--------------------------------------

Esse é o caso mais simples. A Lambda não cria projeto, apenas assume as roles
 informadas e dispara o build.

```json
{
  "target_role_arns": [
    "arn:aws:iam::111111111111:role/codebuild-trigger",
    "arn:aws:iam::222222222222:role/codebuild-trigger"
  ],
  "codebuild_project_name": "deploy-project",
  "codebuild_region": "sa-east-1",
  "codebuild_buildspec_s3_uri": "s3://central-artifacts/buildspecs/deploy.yml",
  "codebuild_source_version": "refs/heads/main",
  "codebuild_environment_variables": [
    {
      "name": "ENV_NAME",
      "value": "production",
      "type": "PLAINTEXT"
    }
  ]
}
```

Exemplo 2: conta central criando projeto nas contas alvo
--------------------------------------------------------

Esse é o modo recomendado quando a Lambda central deve preparar tudo nas contas
target.

```json
{
  "target_account_ids": ["111111111111", "222222222222"],
  "assume_role_arn_template": "arn:aws:iam::{account_id}:role/codebuild-trigger",
  "codebuild_project_name": "deploy-{account_id}",
  "codebuild_region": "sa-east-1",
  "codebuild_project_definition": {
    "serviceRole": "arn:aws:iam::{account_id}:role/codebuild-service-role",
    "environment": {
      "type": "LINUX_CONTAINER",
      "image": "aws/codebuild/standard:7.0",
      "computeType": "BUILD_GENERAL1_SMALL"
    },
    "source": {
      "type": "NO_SOURCE"
    },
    "artifacts": {
      "type": "NO_ARTIFACTS"
    }
  },
  "codebuild_buildspec_s3_uri": "s3://central-artifacts/buildspecs/deploy.yml",
  "codebuild_source_version": "refs/heads/main",
  "max_workers": 4
}
```

Explicação do exemplo acima:
- a Lambda vai assumir `codebuild-trigger` em cada conta target
- vai ler o arquivo `deploy.yml` do bucket central
- vai garantir o projeto `deploy-111111111111`, `deploy-222222222222`, etc.
- vai iniciar um build em cada projeto

Exemplo 3: configuração por variáveis de ambiente
-------------------------------------------------

Útil quando a Lambda deve ficar "fixa" e o evento só dispara a execução.

```bash
export TARGET_ACCOUNT_IDS="111111111111,222222222222"
export ASSUME_ROLE_ARN_TEMPLATE="arn:aws:iam::{account_id}:role/codebuild-trigger"
export CODEBUILD_PROJECT_NAME="deploy-{account_id}"
export CODEBUILD_REGION="sa-east-1"
export CODEBUILD_PROJECT_DEFINITION='{"serviceRole":"arn:aws:iam::{account_id}:role/codebuild-service-role","environment":{"type":"LINUX_CONTAINER","image":"aws/codebuild/standard:7.0","computeType":"BUILD_GENERAL1_SMALL"},"source":{"type":"NO_SOURCE"},"artifacts":{"type":"NO_ARTIFACTS"}}'
export CODEBUILD_BUILDSPEC_S3_URI="s3://central-artifacts/buildspecs/deploy.yml"
export MAX_WORKERS="4"
```

Papéis IAM Envolvidos
=====================

Esta parte costuma gerar confusão. Existem dois papéis diferentes no fluxo.

`assume_role_arn_template`
--------------------------

Exemplo:
- `arn:aws:iam::{account_id}:role/codebuild-trigger`

Esse é o papel que a Lambda central assume na conta target via STS.

Responsabilidade:
- consultar projeto CodeBuild
- criar projeto CodeBuild
- iniciar o build
- passar a `serviceRole` para o projeto quando necessário

Em outras palavras:
- esse papel é de orquestração
- ele é usado antes do build rodar

Trust policy típica:
- confia na conta central ou na role da Lambda central

Permissões típicas:
- `codebuild:BatchGetProjects`
- `codebuild:CreateProject`
- `codebuild:StartBuild`
- `iam:PassRole` para a `serviceRole` do projeto

`serviceRole`
-------------

Exemplo:
- `arn:aws:iam::{account_id}:role/codebuild-service-role`

Esse é o papel usado pelo próprio serviço CodeBuild durante a execução do job.

Responsabilidade:
- escrever logs
- ler secrets/parâmetros
- acessar S3/ECR/SSM/Secrets Manager
- executar deploys ou chamadas AWS que o build precise

Em outras palavras:
- esse papel é de execução do build
- ele é usado depois que o CodeBuild começa a rodar

Trust policy típica:
- confia em `codebuild.amazonaws.com`

Permissões típicas:
- variam conforme o que o build faz em runtime
- normalmente incluem CloudWatch Logs, S3, ECR, SSM, Secrets Manager, etc.

Resumo da diferença
-------------------

- `assume_role_arn_template`: papel que a Lambda assume para controlar a conta target
- `serviceRole`: papel que o CodeBuild usa para executar o job dentro da conta target

Fluxo prático:
1. a Lambda central assume `codebuild-trigger`
2. a Lambda cria/consulta/dispara o projeto
3. o CodeBuild inicia e usa `codebuild-service-role`

Permissões necessárias
======================

A role da Lambda central precisa de:
- `sts:AssumeRole` nas roles alvo
- `s3:GetObject` no `buildspec` central, quando usar S3

A role assumida na conta target precisa de:
- `codebuild:BatchGetProjects`
- `codebuild:CreateProject`
- `codebuild:StartBuild`
- `iam:PassRole` para a `serviceRole` configurada no projeto

A trust policy da role target deve permitir a conta central assumir a role.

Pontos de atenção operacionais
==============================

`codebuild_project_definition`
------------------------------

Esse bloco é usado apenas para `create_project`. Hoje a Lambda garante
existência do projeto, mas não sincroniza diferenças. Em outras palavras:

- se o projeto não existir, ela cria
- se o projeto existir, ela usa o que já está lá
- ela não chama `update_project`

`source.type = NO_SOURCE`
-------------------------

Esse valor é válido quando o próprio build consegue se virar sem um source fixo
configurado no projeto. Nesse cenário:

- o buildspec pode baixar código manualmente
- o buildspec pode chamar outras automações
- o projeto pode ser puramente operacional

Mas existe um cuidado importante:

- `codebuild_source_version` só faz sentido real quando existe um source que o
  CodeBuild consegue interpretar
- se o projeto estiver em `NO_SOURCE`, o campo pode não ter efeito prático
  sozinho

Se o seu caso depende de branch/tag/commit do repositório, normalmente vale
mais a pena configurar `source` de verdade no projeto, ou então baixar o código
explicitamente dentro do `buildspec`.

Observações sobre execução
==========================

- cada execução gera um `run_id` para rastreabilidade
- o `start_build` usa `idempotencyToken`
- a Lambda apenas inicia o build
- ela não aguarda término do CodeBuild
- a resposta indica sucesso do disparo, não sucesso do deploy final

Estrutura resumida da resposta
==============================

- `ok=true` e `status=ok`
  Todos os builds foram iniciados.
- `ok=false` e `status=partial_ok`
  Alguns alvos falharam.
- `ok=false` e `status=error`
  Houve erro de configuração ou erro global.

Cada item de `results` normalmente contém:
- `target_role_arn`
- `target_account_id`
- `codebuild_project_name`
- `codebuild_region`
- `status`
- `build_id`
- `build_arn`
- `build_number`
- `build_status`
- `codebuild_project_action`
  Valores possíveis: `SKIPPED`, `EXISTING`, `CREATED`
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
    ProxyConnectionError,
    ReadTimeoutError,
)

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

AWS_ACCOUNT_ID_PATTERN = re.compile(r"^\d{12}$")
ROLE_ARN_PATTERN = re.compile(r"^arn:aws[a-zA-Z-]*:iam::\d{12}:role/.+$")
CODEBUILD_ENV_VAR_TYPES = frozenset({"PLAINTEXT", "PARAMETER_STORE", "SECRETS_MANAGER"})
S3_URI_PATTERN = re.compile(r"^s3://([^/]+)/(.+)$")


def _resolve_optional_text(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _resolve_optional_value(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        if isinstance(value, (list, dict, tuple, set)):
            if value:
                return value
            continue
        return value
    return None


def _safe_str_field(value: Any, *, field_name: str, required: bool = True) -> str:
    if value is None:
        if required:
            raise ValueError(f"{field_name} é obrigatório")
        return ""
    text = str(value).strip()
    if not text and required:
        raise ValueError(f"{field_name} é obrigatório")
    return text


def _safe_text_content(value: Any, *, field_name: str, required: bool = True) -> str:
    if value is None:
        if required:
            raise ValueError(f"{field_name} é obrigatório")
        return ""
    text = value if isinstance(value, str) else str(value)
    if required and text == "":
        raise ValueError(f"{field_name} é obrigatório")
    return text


def _safe_dict_field(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} deve ser um objeto JSON")
    return value


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso_with_milliseconds(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _dt_to_iso_with_milliseconds(value)
    return value


def _log_event(action: str, *, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    payload = {
        "action": action,
        "eventTime": _dt_to_iso_with_milliseconds(_now_utc()),
        **fields,
    }
    logger.log(level, "%s", json.dumps(_to_json_safe(payload), ensure_ascii=False, default=str))


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value).split(",")
    normalized: List[str] = []
    for item in raw_items:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _dedupe_values(values: List[str]) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _safe_str_field(value, field_name="value")
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _extract_account_id_from_role_arn(role_arn: str) -> str:
    value = _safe_str_field(role_arn, field_name="target_role_arn")
    if not ROLE_ARN_PATTERN.fullmatch(value):
        raise ValueError(f"target_role_arn inválido: {value}")
    return value.split(":")[4]


def _normalize_environment_variables(raw_value: Any) -> List[Dict[str, str]]:
    if raw_value is None:
        return []
    parsed_value = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed_value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("codebuild_environment_variables deve ser JSON válido") from exc
    if not isinstance(parsed_value, list):
        raise ValueError("codebuild_environment_variables deve ser uma lista")

    normalized: List[Dict[str, str]] = []
    for index, item in enumerate(parsed_value):
        item_dict = _safe_dict_field(item, f"codebuild_environment_variables[{index}]")
        name = _safe_str_field(item_dict.get("name"), field_name=f"codebuild_environment_variables[{index}].name")
        value = _safe_str_field(item_dict.get("value"), field_name=f"codebuild_environment_variables[{index}].value")
        var_type = _safe_str_field(
            item_dict.get("type"),
            field_name=f"codebuild_environment_variables[{index}].type",
            required=False,
        ).upper() or "PLAINTEXT"
        if var_type not in CODEBUILD_ENV_VAR_TYPES:
            allowed_types = ", ".join(sorted(CODEBUILD_ENV_VAR_TYPES))
            raise ValueError(
                f"codebuild_environment_variables[{index}].type inválido: {var_type}. Valores permitidos: {allowed_types}"
            )
        normalized.append({"name": name, "value": value, "type": var_type})
    return normalized


def _normalize_json_object(raw_value: Any, *, field_name: str) -> Optional[Dict[str, Any]]:
    if raw_value is None:
        return None
    parsed_value = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return None
        try:
            parsed_value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field_name} deve ser JSON válido") from exc
    if not isinstance(parsed_value, dict):
        raise ValueError(f"{field_name} deve ser um objeto JSON")
    return parsed_value


def _normalize_target_account_ids(raw_value: Any) -> List[str]:
    account_ids = _dedupe_values(_normalize_list(raw_value))
    normalized: List[str] = []
    for index, account_id in enumerate(account_ids):
        value = _safe_str_field(account_id, field_name=f"target_account_ids[{index}]")
        if not AWS_ACCOUNT_ID_PATTERN.fullmatch(value):
            raise ValueError(f"target_account_ids[{index}] inválido: {value}")
        normalized.append(value)
    return normalized


def _normalize_target_role_arns(raw_value: Any) -> List[str]:
    role_arns = _dedupe_values(_normalize_list(raw_value))
    normalized: List[str] = []
    for index, role_arn in enumerate(role_arns):
        value = _safe_str_field(role_arn, field_name=f"target_role_arns[{index}]")
        _extract_account_id_from_role_arn(value)
        normalized.append(value)
    return normalized


def _parse_s3_uri(value: str, *, field_name: str) -> Dict[str, str]:
    text = _safe_str_field(value, field_name=field_name)
    match = S3_URI_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"{field_name} inválido: {text}")
    return {"bucket": match.group(1), "key": match.group(2)}


def _resolve_buildspec_s3_location(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    s3_uri = _resolve_optional_text(
        os.getenv("CODEBUILD_BUILDSPEC_S3_URI", ""),
        payload.get("codebuild_buildspec_s3_uri"),
        payload.get("codebuildBuildspecS3Uri"),
        payload.get("buildspec_s3_uri"),
        payload.get("buildspecS3Uri"),
    )
    if s3_uri:
        location = _parse_s3_uri(s3_uri, field_name="codebuild_buildspec_s3_uri")
    else:
        bucket = _resolve_optional_text(
            os.getenv("CODEBUILD_BUILDSPEC_S3_BUCKET", ""),
            payload.get("codebuild_buildspec_s3_bucket"),
            payload.get("codebuildBuildspecS3Bucket"),
            payload.get("buildspec_s3_bucket"),
            payload.get("buildspecBucket"),
        )
        key = _resolve_optional_text(
            os.getenv("CODEBUILD_BUILDSPEC_S3_KEY", ""),
            payload.get("codebuild_buildspec_s3_key"),
            payload.get("codebuildBuildspecS3Key"),
            payload.get("buildspec_s3_key"),
            payload.get("buildspecKey"),
        )
        if not bucket and not key:
            return None
        location = {
            "bucket": _safe_str_field(bucket, field_name="codebuild_buildspec_s3_bucket"),
            "key": _safe_str_field(key, field_name="codebuild_buildspec_s3_key"),
        }
    version_id = _resolve_optional_text(
        os.getenv("CODEBUILD_BUILDSPEC_S3_OBJECT_VERSION", ""),
        payload.get("codebuild_buildspec_s3_object_version"),
        payload.get("codebuildBuildspecS3ObjectVersion"),
        payload.get("buildspec_s3_object_version"),
        payload.get("buildspecObjectVersion"),
    )
    if version_id:
        location["version_id"] = version_id
    return location


def _build_role_arn_from_template(*, account_id: str, template: str) -> str:
    template_value = _safe_str_field(template, field_name="assume_role_arn_template")
    if "{account_id}" not in template_value:
        raise ValueError("assume_role_arn_template deve conter o placeholder {account_id}")
    role_arn = template_value.replace("{account_id}", account_id)
    _extract_account_id_from_role_arn(role_arn)
    return role_arn


def _replace_account_id_placeholder(value: Any, *, account_id: str) -> Any:
    if isinstance(value, str):
        return value.replace("{account_id}", account_id)
    if isinstance(value, list):
        return [_replace_account_id_placeholder(item, account_id=account_id) for item in value]
    if isinstance(value, dict):
        return {
            key: _replace_account_id_placeholder(inner, account_id=account_id)
            for key, inner in value.items()
        }
    return value


def _build_error_response_fields(exc: BaseException) -> Dict[str, str]:
    message = str(exc)
    message_lower = message.lower()
    if "target_role_arns" in message_lower or "target_role_arn" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "Nenhuma role alvo válida foi informada para assumir a conta de destino.",
            "resolution": "Informe target_role_arns no payload ou TARGET_ROLE_ARNS no ambiente com ARNs IAM role válidos.",
        }
    if "target_account_ids" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "Nenhuma conta alvo válida foi informada.",
            "resolution": "Informe target_account_ids no payload ou TARGET_ACCOUNT_IDS no ambiente com account IDs de 12 dígitos.",
        }
    if "assume_role_arn_template" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "O template da role de destino não foi informado corretamente.",
            "resolution": "Defina assume_role_arn_template/ASSUME_ROLE_ARN_TEMPLATE com o placeholder {account_id}.",
        }
    if "codebuild_project_name" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "O nome do projeto do CodeBuild não foi informado.",
            "resolution": "Defina codebuild_project_name no payload ou CODEBUILD_PROJECT_NAME no ambiente.",
        }
    if "codebuild_region" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A região do CodeBuild não foi informada.",
            "resolution": "Defina codebuild_region no payload ou AWS_REGION/AWS_DEFAULT_REGION/CODEBUILD_REGION no ambiente.",
        }
    if "codebuild_project_definition" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A definição base do projeto CodeBuild está ausente ou inválida.",
            "resolution": "Defina codebuild_project_definition/ CODEBUILD_PROJECT_DEFINITION com serviceRole e environment válidos.",
        }
    if "codebuild_buildspec_s3" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A localização do buildspec no S3 está ausente ou inválida.",
            "resolution": "Defina codebuild_buildspec_s3_uri ou codebuild_buildspec_s3_bucket + codebuild_buildspec_s3_key.",
        }
    if "accessdenied" in message_lower or "acesso" in message_lower:
        return {
            "error": message,
            "error_detail": message,
            "user_message": "A execução não tem permissão para assumir a role ou iniciar o CodeBuild na conta alvo.",
            "resolution": "Revise a trust policy da role alvo e as permissões para sts:AssumeRole e codebuild:StartBuild.",
        }
    return {
        "error": message,
        "error_detail": message,
        "user_message": "A execução falhou por erro interno.",
        "resolution": "Revise os logs da Lambda e os parâmetros enviados no payload ou nas variáveis de ambiente.",
    }


def _client_error_code(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return exc.__class__.__name__
    return _safe_str_field(error.get("Code"), field_name="Error.Code", required=False) or exc.__class__.__name__


def _client_error_message(exc: ClientError) -> str:
    response = exc.response if isinstance(getattr(exc, "response", None), dict) else {}
    error = response.get("Error")
    if not isinstance(error, dict):
        return str(exc)
    return _safe_str_field(error.get("Message"), field_name="Error.Message", required=False) or str(exc)


def _build_aws_session() -> Any:
    return boto3.session.Session()


def _get_session_client(session: Any, service_name: str, *, region_name: Optional[str] = None) -> Any:
    return session.client(service_name, region_name=region_name)


def _build_assume_role_session_name(*, run_id: str, role_arn: str, prefix: str) -> str:
    digest = hashlib.sha1(role_arn.encode("utf-8")).hexdigest()[:12]
    sanitized_prefix = re.sub(r"[^a-zA-Z0-9+=,.@-]", "-", _safe_str_field(prefix, field_name="assume_role_session_name_prefix"))
    return f"{sanitized_prefix}-{run_id}-{digest}"[:64]


def _assume_role_session(
    *,
    base_session: Any,
    role_arn: str,
    external_id: Optional[str],
    session_name: str,
    duration_seconds: int,
) -> Any:
    sts_client = _get_session_client(base_session, "sts")
    request: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": session_name,
        "DurationSeconds": duration_seconds,
    }
    if external_id:
        request["ExternalId"] = external_id

    response = sts_client.assume_role(**request)
    credentials = _safe_dict_field(response.get("Credentials"), "assume_role.Credentials")
    return boto3.session.Session(
        aws_access_key_id=_safe_str_field(credentials.get("AccessKeyId"), field_name="Credentials.AccessKeyId"),
        aws_secret_access_key=_safe_str_field(credentials.get("SecretAccessKey"), field_name="Credentials.SecretAccessKey"),
        aws_session_token=_safe_str_field(credentials.get("SessionToken"), field_name="Credentials.SessionToken", required=False),
        region_name=None,
    )


def build_codebuild_config(event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Resolve e valida a configuração final da execução.

    Esta função faz a composição entre:
    - payload do evento
    - variáveis de ambiente
    - aliases de nomes aceitos pelo contrato

    Também aplica as regras de negócio mais importantes:
    - `target_role_arns` e `target_account_ids` são mutuamente equivalentes
    - `target_account_ids` exige `assume_role_arn_template`
    - modo de criação de projeto exige `codebuild_project_definition`
    - buildspec em S3 é obrigatório quando o modo novo é usado

    O retorno desta função é o contrato interno consumido pelo restante da
    Lambda.
    """
    payload = event if isinstance(event, dict) else {}
    raw_target_role_arns = _resolve_optional_value(
        os.getenv("TARGET_ROLE_ARNS", ""),
        payload.get("target_role_arns"),
        payload.get("targetRoleArns"),
        payload.get("role_arns"),
        payload.get("roleArns"),
    )
    raw_target_account_ids = _resolve_optional_value(
        os.getenv("TARGET_ACCOUNT_IDS", ""),
        payload.get("target_account_ids"),
        payload.get("targetAccountIds"),
        payload.get("target_accounts"),
        payload.get("targetAccounts"),
        payload.get("account_ids"),
        payload.get("accountIds"),
    )
    assume_role_arn_template = _safe_str_field(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_ARN_TEMPLATE", ""),
            os.getenv("ASSUME_ROLE", ""),
            payload.get("assume_role_arn_template"),
            payload.get("assumeRoleArnTemplate"),
            payload.get("assume_role"),
            payload.get("assumeRole"),
        ),
        field_name="assume_role_arn_template",
        required=False,
    )
    target_account_ids = _normalize_target_account_ids(raw_target_account_ids)
    if raw_target_role_arns is not None:
        target_role_arns = _normalize_target_role_arns(raw_target_role_arns)
    elif target_account_ids:
        if not assume_role_arn_template:
            raise ValueError("assume_role_arn_template é obrigatório quando target_account_ids for informado")
        target_role_arns = [
            _build_role_arn_from_template(account_id=account_id, template=assume_role_arn_template)
            for account_id in target_account_ids
        ]
    else:
        target_role_arns = []
    if not target_role_arns:
        raise ValueError("target_role_arns ou target_account_ids é obrigatório")
    resolved_target_account_ids = [_extract_account_id_from_role_arn(role_arn) for role_arn in target_role_arns]

    codebuild_project_name = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_PROJECT_NAME", ""),
            payload.get("codebuild_project_name"),
            payload.get("codebuildProjectName"),
            payload.get("project_name"),
            payload.get("projectName"),
        ),
        field_name="codebuild_project_name",
    )
    codebuild_region = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_REGION", ""),
            payload.get("codebuild_region"),
            payload.get("codebuildRegion"),
            os.getenv("AWS_REGION", ""),
            os.getenv("AWS_DEFAULT_REGION", ""),
        ),
        field_name="codebuild_region",
    )
    codebuild_source_version = _safe_str_field(
        _resolve_optional_text(
            os.getenv("CODEBUILD_SOURCE_VERSION", ""),
            payload.get("codebuild_source_version"),
            payload.get("codebuildSourceVersion"),
            payload.get("source_version"),
            payload.get("sourceVersion"),
        ),
        field_name="codebuild_source_version",
        required=False,
    )
    environment_variables = _normalize_environment_variables(
        _resolve_optional_value(
            os.getenv("CODEBUILD_ENVIRONMENT_VARIABLES", ""),
            payload.get("codebuild_environment_variables"),
            payload.get("codebuildEnvironmentVariables"),
            payload.get("environment_variables"),
            payload.get("environmentVariables"),
        )
    )
    buildspec_s3_location = _resolve_buildspec_s3_location(payload)
    external_id = _safe_str_field(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_EXTERNAL_ID", ""),
            payload.get("assume_role_external_id"),
            payload.get("assumeRoleExternalId"),
        ),
        field_name="assume_role_external_id",
        required=False,
    )
    session_name_prefix = _safe_str_field(
        _resolve_optional_text(
            os.getenv("ASSUME_ROLE_SESSION_NAME_PREFIX", ""),
            payload.get("assume_role_session_name_prefix"),
            payload.get("assumeRoleSessionNamePrefix"),
            "codebuild-trigger",
        ),
        field_name="assume_role_session_name_prefix",
    )
    try:
        assume_role_duration_seconds = int(
            _resolve_optional_text(
                os.getenv("ASSUME_ROLE_DURATION_SECONDS", ""),
                payload.get("assume_role_duration_seconds"),
                payload.get("assumeRoleDurationSeconds"),
                "3600",
            )
            or "3600"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("assume_role_duration_seconds deve ser um inteiro válido") from exc
    if assume_role_duration_seconds < 900 or assume_role_duration_seconds > 43200:
        raise ValueError("assume_role_duration_seconds deve estar entre 900 e 43200")
    try:
        max_workers = int(
            _resolve_optional_text(
                os.getenv("MAX_WORKERS", ""),
                payload.get("max_workers"),
                payload.get("maxWorkers"),
                "4",
            )
            or "4"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("max_workers deve ser um inteiro válido") from exc
    max_workers = max(1, max_workers)
    codebuild_project_definition = _normalize_json_object(
        _resolve_optional_value(
            os.getenv("CODEBUILD_PROJECT_DEFINITION", ""),
            payload.get("codebuild_project_definition"),
            payload.get("codebuildProjectDefinition"),
        ),
        field_name="codebuild_project_definition",
    )
    if buildspec_s3_location is None:
        raise ValueError("codebuild_buildspec_s3_uri é obrigatório")
    if target_account_ids and codebuild_project_definition is None:
        raise ValueError(
            "codebuild_project_definition é obrigatório quando target_account_ids for informado"
        )

    run_time = _now_utc()
    run_id = run_time.strftime("%Y%m%dT%H%M%SZ")
    config = {
        "run_id": run_id,
        "run_time": run_time,
        "target_role_arns": target_role_arns,
        "target_account_ids": resolved_target_account_ids,
        "assume_role_arn_template": assume_role_arn_template,
        "codebuild_project_name": codebuild_project_name,
        "codebuild_region": codebuild_region,
        "codebuild_buildspec_s3_location": buildspec_s3_location,
        "codebuild_source_version": codebuild_source_version,
        "codebuild_environment_variables": environment_variables,
        "codebuild_project_definition": codebuild_project_definition,
        "assume_role_external_id": external_id,
        "assume_role_session_name_prefix": session_name_prefix,
        "assume_role_duration_seconds": assume_role_duration_seconds,
        "max_workers": max_workers,
    }
    _log_event(
        "config.codebuild.resolved",
        run_id=run_id,
        target_count=len(target_role_arns),
        target_account_count=len(resolved_target_account_ids),
        codebuild_project_name=codebuild_project_name,
        codebuild_region=codebuild_region,
        buildspec_override_enabled=True,
        buildspec_source="s3",
        source_version=codebuild_source_version or None,
        environment_variable_count=len(environment_variables),
        project_definition_enabled=bool(codebuild_project_definition),
        max_workers=max_workers,
    )
    return config


def _resolve_target_execution_config(config: Dict[str, Any], *, role_arn: str) -> Dict[str, Any]:
    """Materializa a configuração para uma conta target específica.

    Tudo que aceita `{account_id}` é resolvido aqui. Isso permite que o payload
    de entrada continue enxuto e que cada execução em paralelo receba seus
    valores já expandidos.
    """
    account_id = _extract_account_id_from_role_arn(role_arn)
    resolved = {
        **config,
        "target_role_arn": role_arn,
        "target_account_id": account_id,
        "codebuild_project_name": _replace_account_id_placeholder(
            config.get("codebuild_project_name"),
            account_id=account_id,
        ),
        "codebuild_buildspec_s3_location": _replace_account_id_placeholder(
            config.get("codebuild_buildspec_s3_location"),
            account_id=account_id,
        ),
        "codebuild_source_version": _replace_account_id_placeholder(
            config.get("codebuild_source_version"),
            account_id=account_id,
        ),
        "codebuild_environment_variables": _replace_account_id_placeholder(
            config.get("codebuild_environment_variables"),
            account_id=account_id,
        ),
        "codebuild_project_definition": _replace_account_id_placeholder(
            config.get("codebuild_project_definition"),
            account_id=account_id,
        ),
        "assume_role_external_id": _replace_account_id_placeholder(
            config.get("assume_role_external_id"),
            account_id=account_id,
        ),
    }
    return resolved


def _load_buildspec_content_for_target(base_session: Any, config: Dict[str, Any]) -> str:
    """Carrega o buildspec final que será usado no target.

    Comportamento:
    - lê o objeto em `codebuild_buildspec_s3_location`

    O conteúdo é preservado como texto, sem normalização destrutiva, para não
    alterar o YAML original vindo do S3.
    """
    s3_location = config.get("codebuild_buildspec_s3_location")
    if isinstance(s3_location, dict):
        s3_client = _get_session_client(base_session, "s3")
        request = {
            "Bucket": _safe_str_field(s3_location.get("bucket"), field_name="codebuild_buildspec_s3_bucket"),
            "Key": _safe_str_field(s3_location.get("key"), field_name="codebuild_buildspec_s3_key"),
        }
        version_id = _safe_str_field(
            s3_location.get("version_id"),
            field_name="codebuild_buildspec_s3_object_version",
            required=False,
        )
        if version_id:
            request["VersionId"] = version_id
        response = s3_client.get_object(**request)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("get_object não retornou um body válido para o buildspec")
        content = body.read()
        buildspec_text = content.decode("utf-8") if isinstance(content, bytes) else str(content)
        return _safe_text_content(buildspec_text, field_name="codebuild_buildspec")
    raise ValueError("codebuild_buildspec_s3_uri é obrigatório")


def _build_codebuild_project_request(config: Dict[str, Any], *, buildspec_content: Optional[str]) -> Dict[str, Any]:
    project_definition = _normalize_json_object(
        config.get("codebuild_project_definition"),
        field_name="codebuild_project_definition",
    )
    if project_definition is None:
        raise ValueError("codebuild_project_definition é obrigatório para criar projeto CodeBuild")
    request = deepcopy(project_definition)
    request["name"] = _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name")
    request["serviceRole"] = _safe_str_field(
        request.get("serviceRole"),
        field_name="codebuild_project_definition.serviceRole",
    )
    environment = _safe_dict_field(
        request.get("environment"),
        "codebuild_project_definition.environment",
    )
    _safe_str_field(environment.get("type"), field_name="codebuild_project_definition.environment.type")
    _safe_str_field(environment.get("image"), field_name="codebuild_project_definition.environment.image")
    _safe_str_field(
        environment.get("computeType"),
        field_name="codebuild_project_definition.environment.computeType",
    )
    request["environment"] = environment
    source = request.get("source")
    if source is None:
        source = {"type": "NO_SOURCE"}
    source = _safe_dict_field(source, "codebuild_project_definition.source")
    source_type = _safe_str_field(
        source.get("type"),
        field_name="codebuild_project_definition.source.type",
        required=False,
    ) or "NO_SOURCE"
    source["type"] = source_type
    if buildspec_content:
        source["buildspec"] = buildspec_content
    request["source"] = source
    artifacts = request.get("artifacts")
    if artifacts is None:
        artifacts = {"type": "NO_ARTIFACTS"}
    artifacts = _safe_dict_field(artifacts, "codebuild_project_definition.artifacts")
    artifacts["type"] = _safe_str_field(
        artifacts.get("type"),
        field_name="codebuild_project_definition.artifacts.type",
        required=False,
    ) or "NO_ARTIFACTS"
    request["artifacts"] = artifacts
    return request


def _ensure_codebuild_project(
    codebuild_client: Any,
    config: Dict[str, Any],
    *,
    buildspec_content: Optional[str],
) -> str:
    """Garante a existência do projeto CodeBuild na conta target.

    Fluxo:
    - se não houver `codebuild_project_definition`, não cria nada
    - consulta o projeto por nome via `batch_get_projects`
    - se existir, retorna `EXISTING`
    - se não existir, chama `create_project` e retorna `CREATED`

    Importante:
    - hoje o comportamento é "ensure exists"
    - esta função não faz `update_project`
    """
    project_name = _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name")
    if config.get("codebuild_project_definition") is None:
        return "SKIPPED"
    response = codebuild_client.batch_get_projects(names=[project_name])
    projects = response.get("projects")
    if isinstance(projects, list) and projects:
        return "EXISTING"
    request = _build_codebuild_project_request(config, buildspec_content=buildspec_content)
    codebuild_client.create_project(**request)
    _log_event(
        "codebuild.project.created",
        target_role_arn=config.get("target_role_arn"),
        target_account_id=config.get("target_account_id"),
        codebuild_project_name=project_name,
        codebuild_region=config.get("codebuild_region"),
    )
    return "CREATED"


def _build_start_build_request(
    config: Dict[str, Any],
    *,
    role_arn: str,
    buildspec_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Monta o payload de `codebuild.start_build`.

    O `buildspec_override` já deve conter o YAML carregado do S3 central para o
    target atual.
    """
    request: Dict[str, Any] = {
        "projectName": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
        "idempotencyToken": hashlib.sha1(
            "|".join(
                [
                    _safe_str_field(config.get("run_id"), field_name="run_id"),
                    _safe_str_field(role_arn, field_name="target_role_arn"),
                    _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
                    _safe_str_field(config.get("codebuild_source_version"), field_name="codebuild_source_version", required=False),
                ]
            ).encode("utf-8")
        ).hexdigest()[:32],
    }
    resolved_buildspec_override = _safe_text_content(
        buildspec_override,
        field_name="codebuild_buildspec",
    )
    request["buildspecOverride"] = resolved_buildspec_override
    source_version = _safe_str_field(
        config.get("codebuild_source_version"),
        field_name="codebuild_source_version",
        required=False,
    )
    if source_version:
        request["sourceVersion"] = source_version
    environment_variables = config.get("codebuild_environment_variables")
    if isinstance(environment_variables, list) and environment_variables:
        request["environmentVariablesOverride"] = [
            {
                "name": _safe_str_field(item.get("name"), field_name="codebuild_environment_variable.name"),
                "value": _safe_str_field(item.get("value"), field_name="codebuild_environment_variable.value"),
                "type": _safe_str_field(item.get("type"), field_name="codebuild_environment_variable.type"),
            }
            for item in environment_variables
            if isinstance(item, dict)
        ]
    return request


def _start_codebuild_for_role(base_session: Any, config: Dict[str, Any], role_arn: str) -> Dict[str, Any]:
    account_id = _extract_account_id_from_role_arn(role_arn)
    target_config = _resolve_target_execution_config(config, role_arn=role_arn)
    session_name = _build_assume_role_session_name(
        run_id=_safe_str_field(config.get("run_id"), field_name="run_id"),
        role_arn=role_arn,
        prefix=_safe_str_field(config.get("assume_role_session_name_prefix"), field_name="assume_role_session_name_prefix"),
    )
    _log_event(
        "codebuild.target.start",
        target_role_arn=role_arn,
        target_account_id=account_id,
        codebuild_project_name=target_config.get("codebuild_project_name"),
        codebuild_region=target_config.get("codebuild_region"),
        session_name=session_name,
    )
    assumed_session = _assume_role_session(
        base_session=base_session,
        role_arn=role_arn,
        external_id=_safe_str_field(
            target_config.get("assume_role_external_id"),
            field_name="assume_role_external_id",
            required=False,
        ),
        session_name=session_name,
        duration_seconds=int(config.get("assume_role_duration_seconds", 3600)),
    )
    buildspec_content = _load_buildspec_content_for_target(base_session, target_config)
    codebuild_client = _get_session_client(
        assumed_session,
        "codebuild",
        region_name=_safe_str_field(target_config.get("codebuild_region"), field_name="codebuild_region"),
    )
    project_action = _ensure_codebuild_project(
        codebuild_client,
        target_config,
        buildspec_content=buildspec_content,
    )
    request = _build_start_build_request(
        target_config,
        role_arn=role_arn,
        buildspec_override=buildspec_content,
    )
    response = codebuild_client.start_build(**request)
    build = _safe_dict_field(response.get("build"), "start_build.build")
    build_id = _safe_str_field(build.get("id"), field_name="build.id")
    build_arn = _safe_str_field(build.get("arn"), field_name="build.arn", required=False)
    build_number = build.get("buildNumber")
    build_status = _safe_str_field(build.get("buildStatus"), field_name="build.buildStatus", required=False) or "IN_PROGRESS"
    result = {
        "target_role_arn": role_arn,
        "target_account_id": account_id,
        "codebuild_project_name": _safe_str_field(
            target_config.get("codebuild_project_name"),
            field_name="codebuild_project_name",
        ),
        "codebuild_region": _safe_str_field(target_config.get("codebuild_region"), field_name="codebuild_region"),
        "build_id": build_id,
        "build_arn": build_arn,
        "build_number": build_number,
        "build_status": build_status,
        "codebuild_project_action": project_action,
        "status": "STARTED",
        "source_version": _safe_str_field(
            target_config.get("codebuild_source_version"),
            field_name="codebuild_source_version",
            required=False,
        ),
        "buildspec_override_applied": bool(buildspec_content),
    }
    _log_event(
        "codebuild.target.started",
        target_role_arn=role_arn,
        target_account_id=account_id,
        build_id=build_id,
        build_arn=build_arn,
        build_status=build_status,
        codebuild_project_name=result["codebuild_project_name"],
        codebuild_region=result["codebuild_region"],
    )
    return result


def _build_target_error_result(config: Dict[str, Any], *, role_arn: str, error: BaseException) -> Dict[str, Any]:
    fields = _build_error_response_fields(error)
    account_id = ""
    try:
        account_id = _extract_account_id_from_role_arn(role_arn)
    except ValueError:
        account_id = ""
    project_name = _safe_str_field(
        config.get("codebuild_project_name"),
        field_name="codebuild_project_name",
        required=False,
    )
    if account_id:
        project_name = _replace_account_id_placeholder(project_name, account_id=account_id)
    result = {
        "target_role_arn": role_arn,
        "target_account_id": account_id,
        "codebuild_project_name": project_name,
        "codebuild_region": _safe_str_field(config.get("codebuild_region"), field_name="codebuild_region", required=False),
        "status": "FAILED",
        **fields,
    }
    return result


def _run_codebuild(config: Dict[str, Any]) -> Dict[str, Any]:
    base_session = _build_aws_session()
    role_arns = list(config.get("target_role_arns") or [])
    results: List[Dict[str, Any]] = []

    def execute_role(role_arn: str) -> Dict[str, Any]:
        try:
            return _start_codebuild_for_role(base_session, config, role_arn)
        except Exception as exc:
            _log_event(
                "codebuild.target.failed",
                target_role_arn=role_arn,
                codebuild_project_name=config.get("codebuild_project_name"),
                codebuild_region=config.get("codebuild_region"),
                error=str(exc),
                level=logging.WARNING,
            )
            return _build_target_error_result(config, role_arn=role_arn, error=exc)

    worker_count = min(max(1, int(config.get("max_workers", 1))), len(role_arns))
    if worker_count <= 1:
        results = [execute_role(role_arn) for role_arn in role_arns]
    else:
        indexed_results: List[Optional[Dict[str, Any]]] = [None] * len(role_arns)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(execute_role, role_arn): index
                for index, role_arn in enumerate(role_arns)
            }
            for future in as_completed(futures):
                indexed_results[futures[future]] = future.result()
        results = [result for result in indexed_results if isinstance(result, dict)]

    failed_count = sum(
        _safe_str_field(result.get("status"), field_name="result.status", required=False).upper() == "FAILED"
        for result in results
        if isinstance(result, dict)
    )
    response = {
        "status": "partial_ok" if failed_count else "ok",
        "run_id": _safe_str_field(config.get("run_id"), field_name="run_id"),
        "codebuild_project_name": _safe_str_field(config.get("codebuild_project_name"), field_name="codebuild_project_name"),
        "codebuild_region": _safe_str_field(config.get("codebuild_region"), field_name="codebuild_region"),
        "target_count": len(role_arns),
        "results": results,
    }
    _log_event(
        "codebuild.run.completed",
        run_id=response["run_id"],
        status=response["status"],
        target_count=response["target_count"],
        failed_count=failed_count,
        codebuild_project_name=response["codebuild_project_name"],
        codebuild_region=response["codebuild_region"],
    )
    return response


def lambda_handler(event: Optional[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    """Ponto de entrada público da Lambda.

    Resumo do fluxo:
    - valida e resolve a configuração
    - executa os alvos em paralelo conforme `max_workers`
    - devolve uma resposta consolidada por conta/role target

    Este handler reporta:
    - `status=ok` quando todos os alvos iniciam com sucesso
    - `status=partial_ok` quando parte deles falha
    - `status=error` quando a própria execução não consegue ser iniciada
    """
    event_keys = sorted(event.keys()) if isinstance(event, dict) else []
    _log_event(
        "handler.start",
        has_event=event is not None,
        event_keys=event_keys,
        aws_request_id=getattr(context, "aws_request_id", None),
    )
    try:
        config = build_codebuild_config(event)
        run_result = _run_codebuild(config)
        response = {
            "ok": _safe_str_field(run_result.get("status"), field_name="status", required=False).lower() == "ok",
            **run_result,
        }
        _log_event(
            "handler.success" if response["ok"] else "handler.partial_failure",
            run_id=response.get("run_id"),
            status=response.get("status"),
            target_count=response.get("target_count"),
            codebuild_project_name=response.get("codebuild_project_name"),
            codebuild_region=response.get("codebuild_region"),
            level=logging.INFO if response["ok"] else logging.WARNING,
        )
        return response
    except ValueError as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.config_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "config",
            **fields,
        }
    except (BotoCoreError, ClientError, ConnectTimeoutError, EndpointConnectionError, NoCredentialsError, NoRegionError,
            PartialCredentialsError, ProxyConnectionError, ReadTimeoutError, TimeoutError) as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.aws_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "aws",
            **fields,
        }
    except Exception as exc:
        fields = _build_error_response_fields(exc)
        _log_event("handler.runtime_error", error=str(exc), level=logging.ERROR)
        return {
            "ok": False,
            "status": "error",
            "error_type": "runtime",
            **fields,
        }
