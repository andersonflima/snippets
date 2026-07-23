#!/usr/bin/env python3
"""Gera os contratos OpenAPI 3.0 do API Gateway (REST API) para os microserviços.

Topologia: API Gateway (edge, IAM SigV4) -> VPC Link -> NLB interno -> EKS -> pod.
Cada ação é um recurso/path. connectionId (VPC Link) e DNS do NLB vêm de stage
variables, então o mesmo contrato serve para qualquer ambiente.

Saídas:
  ../<service>/contract/openapi.yaml       # contrato por microserviço (autocontido)

O contrato consolidado (api-gateway/openapi.yaml) é montado por gen_gateway.py,
que agrega todos os */contract/openapi.yaml.
"""
from __future__ import annotations

import os

# --- mini serializer YAML (block style) — sem dependência externa --------------
def _scalar(v) -> str:
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None:
        return "null"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    special = any(c in s for c in ":#{}[],&*!|>'\"%@`") or s.strip() != s
    if special or s in ("", "true", "false", "null", "~") or s[:1] in "-?:":
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


def _key(k) -> str:
    if isinstance(k, str):
        numeric = k.lstrip("-").replace(".", "", 1).isdigit()
        if k in ("true", "false", "null", "~", "") or numeric or any(c in k for c in ":#{}[],&*!|>'\"%@`"):
            return _scalar(k)
        return k
    return _scalar(k)


def to_yaml(data, indent=0) -> list[str]:
    pad = "  " * indent
    lines: list[str] = []
    if isinstance(data, dict):
        for k, v in data.items():
            key = _key(k)
            if isinstance(v, dict) and v:
                lines.append(f"{pad}{key}:")
                lines += to_yaml(v, indent + 1)
            elif isinstance(v, list) and v:
                lines.append(f"{pad}{key}:")
                for item in v:
                    if isinstance(item, (dict, list)) and item:
                        sub = to_yaml(item, indent + 1)
                        first = sub[0].lstrip()
                        lines.append(f"{pad}  - {first}")
                        lines += [("  " + ln) for ln in sub[1:]]
                    else:
                        lines.append(f"{pad}  - {_scalar(item)}")
            elif isinstance(v, dict):
                lines.append(f"{pad}{key}: {{}}")
            elif isinstance(v, list):
                lines.append(f"{pad}{key}: []")
            else:
                lines.append(f"{pad}{key}: {_scalar(v)}")
    return lines


def dump_yaml(data) -> str:
    return "\n".join(to_yaml(data)) + "\n"


# --- schemas reutilizados ------------------------------------------------------
def s_str(desc, **extra):
    return {"type": "string", "description": desc, **extra}


ENVELOPE_PROPS = {
    "account": s_str("Conta AWS alvo (12 dígitos).", pattern=r"^\d{12}$"),
    "resource": s_str("Nome ou ARN do recurso alvo da ação."),
    "roleArn": s_str(
        "Role para STS:AssumeRole na conta alvo.",
        pattern=r"^arn:aws:iam::\d{12}:role/.+$",
    ),
    "region": s_str("Região AWS.", pattern=r"^[a-z]{2}-[a-z]+-\d$"),
    "environment": {
        "type": "string",
        "enum": ["dev", "homol", "staging", "prod"],
        "description": "Ambiente target da ação. 'prod' exige GMUD aprovada (ServiceNow).",
    },
    "changeNumber": s_str("Número da GMUD/change no ServiceNow (obrigatório p/ produção)."),
    "requestId": s_str("Idempotency key opcional (correlação do pipeline)."),
    "dryRun": {"type": "boolean", "default": False, "description": "Valida sem executar."},
}
ENVELOPE_REQUIRED = ["account", "resource", "roleArn", "region", "environment"]
PRODUCTIVE_ENVIRONMENTS = ["prod"]


# --- definição das ações (action-driven) ---------------------------------------
SERVICES = [
    {
        "name": "db-password",
        "summary": "Conecta no banco e troca a senha do usuário informado.",
        "params": {
            "dbIdentifier": s_str("Identificador/endpoint da instância RDS."),
            "username": s_str("Usuário cuja senha será trocada."),
            "newPasswordSecretArn": s_str(
                "ARN do segredo (Secrets Manager) com a nova senha — nunca plaintext.",
                pattern=r"^arn:aws:secretsmanager:.+$",
            ),
            "engine": {"type": "string", "enum": ["postgres", "mysql", "mariadb", "aurora-postgresql", "aurora-mysql"]},
        },
        "required": ["dbIdentifier", "username", "newPasswordSecretArn"],
    },
    {
        "name": "kms",
        "summary": "Cria Custom KMS Key e vincula/re-encripta, substituindo a default/herdada.",
        "params": {
            "keyAlias": s_str("Alias da custom key (alias/...)."),
            "description": s_str("Descrição da key."),
            "targetResourceType": {"type": "string", "enum": ["db-instance", "db-snapshot"]},
            "targetResourceId": s_str("Recurso que receberá a key."),
            "replaceInherited": {"type": "boolean", "default": True, "description": "Substitui a key default/herdada."},
            "keyPolicyJson": s_str("Política da key (JSON), opcional."),
        },
        "required": ["keyAlias", "targetResourceType", "targetResourceId"],
    },
    {
        "name": "vpc-link",
        "summary": "Cria acesso privado (PrivateLink) da conta do time ao banco.",
        "params": {
            "dbIdentifier": s_str("Banco a expor de forma privada."),
            "consumerAccount": s_str("Conta consumidora (time).", pattern=r"^\d{12}$"),
            "allowedPrincipals": {"type": "array", "items": {"type": "string"}, "description": "Principals autorizados no endpoint service."},
            "endpointServiceName": s_str("Nome do VPC Endpoint Service (PrivateLink)."),
            "ports": {"type": "array", "items": {"type": "integer"}, "description": "Portas expostas."},
        },
        "required": ["dbIdentifier", "consumerAccount"],
    },
    {
        "name": "servicenow",
        "summary": "Integra com o ServiceNow para acompanhamento de GMUD e autorização de execução produtiva.",
        "params": {
            "operation": {"type": "string", "enum": ["validate", "register", "status"]},
            "action": s_str("Ação/microserviço sendo gateada (ex.: destroy)."),
            "changeNumber": s_str("Número da GMUD/change a validar/registrar."),
            "operationId": s_str("Correlação com a operação em andamento."),
            "workNote": s_str("Nota de trabalho a registrar no change (operation=register)."),
            "state": s_str("Novo estado/anotação de progresso (operation=register)."),
        },
        "required": ["operation"],
    },
    {
        "name": "rds-data",
        "summary": "Wrapper seguro do RDS Data API: avalia o SQL contra regras (S3) antes de executar.",
        "params": {
            "sql": s_str("SQL a executar (validado contra as regras de negócio)."),
            "secretArn": s_str("ARN do segredo (Secrets Manager) com as credenciais do banco."),
            "resourceArn": s_str("ARN do cluster Aurora (default: campo resource)."),
            "database": s_str("Banco/Database alvo."),
            "schema": s_str("Schema alvo (opcional)."),
            "parameters": {
                "type": "object",
                "description": "Parâmetros nomeados (name -> value) p/ SQL parametrizado.",
                "additionalProperties": True,
            },
            "includeResultMetadata": {"type": "boolean", "default": False},
            "rulesBucket": s_str("Bucket S3 com as regras (default: env RULES_BUCKET)."),
            "rulesKey": s_str("Chave do .json de regras (default: env RULES_KEY)."),
        },
        "required": ["sql", "secretArn"],
    },
    {
        "name": "finops",
        "summary": "Varredura read-only de desperdício e recomendações de economia (RDS/EC2/EBS/EIP/ELB/snapshots).",
        "params": {
            "scope": {
                "type": "string",
                "enum": ["all", "rds", "ec2", "ebs", "eip", "elb", "snapshots"],
                "default": "all",
                "description": "Escopo da varredura de desperdício.",
            },
            "lookbackDays": {
                "type": "integer",
                "default": 14,
                "description": "Janela (dias) de métricas do CloudWatch para detectar ociosidade.",
            },
        },
        "required": [],
    },
    {
        "name": "insights",
        "summary": (
            "Analytics read-only por produto: inventário de recursos, métricas, "
            "logs, metadados (perf. de BD) e rightsizing/FinOps. Sem GMUD."
        ),
        # Read-only: não exige 'resource' no envelope (o alvo vai em params.resourceId).
        "envelope_required": ["account", "roleArn", "region", "environment"],
        "success_status": "200",
        "success_description": "Resultado síncrono de analytics.",
        "success_schema": "ActionResult",
        "params": {
            "action": {
                "type": "string",
                "enum": ["resources", "metrics", "logs", "metadata", "finops"],
                "description": "Tipo de análise solicitada.",
            },
            "product": {
                "type": "string",
                "enum": ["rds", "ec2", "ebs", "elb", "eip", "snapshot", "kms", "vpc-endpoint", "all"],
                "default": "all",
                "description": "Produto AWS alvo.",
            },
            "resourceId": s_str("Recurso alvo (obrigatório p/ metrics/logs/metadata)."),
            "filters": {
                "type": "object",
                "description": "Filtros da listagem (search/status/env/type).",
                "additionalProperties": True,
            },
            "metric": s_str("Métrica (cpu|memory|connections|iops|storageUsed|latency)."),
            "lookback": {
                "type": "integer",
                "description": "Janela: minutos (metrics/logs) ou dias (finops).",
            },
            "level": s_str("Nível de log (error|warn|info)."),
            "limit": {"type": "integer", "description": "Máximo de itens retornados."},
        },
        "required": ["action"],
    },
]


def integration(svc_name: str) -> dict:
    return {
        "x-amazon-apigateway-integration": {
            "type": "http_proxy",
            "httpMethod": "POST",
            "connectionType": "VPC_LINK",
            "connectionId": "${stageVariables.vpcLinkId}",
            "uri": "http://${stageVariables.nlbDns}/" + svc_name + "/execute",
            "passthroughBehavior": "when_no_match",
            "timeoutInMillis": 29000,
            "requestParameters": {
                "integration.request.header.X-Request-Id": "context.requestId",
            },
            "responses": {"default": {"statusCode": "200"}},
        }
    }


def request_schema(svc: dict) -> dict:
    props = dict(ENVELOPE_PROPS)
    props["params"] = {
        "type": "object",
        "description": f"Parâmetros específicos da ação {svc['name']}.",
        "properties": svc["params"],
        "required": svc.get("required", []),
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "additionalProperties": False,
        # Serviços read-only (ex.: insights) podem exigir um envelope menor.
        "required": svc.get("envelope_required", ENVELOPE_REQUIRED) + ["params"],
        "properties": props,
    }


RESPONSES = {
    "202": {
        "description": "Ação aceita (assíncrona).",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ActionAccepted"}}},
    },
    "400": {"description": "Requisição inválida.", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "403": {"description": "AssumeRole negado / sem permissão.", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "404": {"description": "Recurso não encontrado.", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "409": {"description": "Conflito / estado inválido do recurso.", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "422": {"description": "Payload inválido (validação do schema).", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "500": {"description": "Erro interno inesperado.", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
    "502": {"description": "Falha no upstream (EKS/NLB).", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
}


def success_response(svc: dict) -> dict:
    status = svc.get("success_status", "202")
    schema = svc.get("success_schema", "ActionAccepted")
    description = svc.get("success_description", "Ação aceita (assíncrona).")
    return {
        status: {
            "description": description,
            "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{schema}"}}},
        }
    }


def responses_for(svc: dict) -> dict:
    errors = {code: spec for code, spec in RESPONSES.items() if code != "202"}
    return {**success_response(svc), **errors}

COMMON_SCHEMAS = {
    "ActionAccepted": {
        "type": "object",
        "required": ["operationId", "status"],
        "properties": {
            "operationId": s_str("ID da operação assíncrona."),
            "status": {"type": "string", "enum": ["accepted", "in-progress"]},
            "resource": s_str("Recurso alvo."),
            "account": s_str("Conta alvo."),
            "links": {"type": "object", "properties": {"status": s_str("URL de consulta de status.")}},
        },
    },
    "ActionResult": {
        "type": "object",
        "required": ["operationId", "status", "product", "action", "detail"],
        "properties": {
            "operationId": s_str("ID da operação."),
            "status": {"type": "string", "enum": ["ok"]},
            "product": s_str("Produto AWS analisado."),
            "action": s_str("Tipo de análise executada."),
            "detail": {"type": "object", "additionalProperties": True},
        },
    },
    "Error": {
        "type": "object",
        "required": ["code", "message"],
        "properties": {
            "code": {"type": "string", "enum": ["validation_error", "assume_role_denied", "not_found", "conflict", "upstream_error"]},
            "message": s_str("Descrição do erro."),
            "requestId": s_str("Correlação."),
        },
    },
}

# Authorizer Cognito (JWT). providerARNs é resolvido no deploy/import — o
# pipeline substitui ${COGNITO_USER_POOL_ARN} pelo ARN do User Pool.
SECURITY_SCHEME = {
    "CognitoJWT": {
        "type": "apiKey",
        "name": "Authorization",
        "in": "header",
        "x-amazon-apigateway-authtype": "cognito_user_pools",
        "x-amazon-apigateway-authorizer": {
            "type": "cognito_user_pools",
            "providerARNs": ["${COGNITO_USER_POOL_ARN}"],
        },
    }
}


def operation(svc: dict) -> dict:
    return {
        "post": {
            "summary": svc["summary"],
            "operationId": svc["name"].replace("-", "_") + "_execute",
            "tags": [svc["name"]],
            "security": [{"CognitoJWT": []}],
            "x-amazon-apigateway-request-validator": "all",
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{schema_name(svc)}"}}},
            },
            "responses": responses_for(svc),
            **integration(svc["name"]),
        }
    }


def schema_name(svc: dict) -> str:
    return "".join(p.capitalize() for p in svc["name"].split("-")) + "Request"


def base_doc(title: str, description: str) -> dict:
    return {
        "openapi": "3.0.3",
        "info": {"title": title, "version": "1.0.0", "description": description},
        "servers": [{"url": "https://${stageVariables.apiDomain}/${stageVariables.basePath}"}],
    }


def gateway_extensions() -> dict:
    return {
        "x-amazon-apigateway-request-validators": {
            "all": {"validateRequestBody": True, "validateRequestParameters": True}
        },
    }


HERE = os.path.dirname(os.path.abspath(__file__))
MS_ROOT = os.path.dirname(HERE)


def write(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    print("wrote", os.path.relpath(path, MS_ROOT))


def build_service(svc: dict) -> dict:
    doc = base_doc(
        f"{svc['name']} — contrato (API Gateway path)",
        svc["summary"] + " Exposto via API Gateway -> VPC Link -> NLB interno -> EKS.",
    )
    doc.update(gateway_extensions())
    doc["paths"] = {f"/{svc['name']}": operation(svc)}
    doc["components"] = {
        "securitySchemes": SECURITY_SCHEME,
        "schemas": {schema_name(svc): request_schema(svc), **COMMON_SCHEMAS},
    }
    return doc


def main() -> None:
    # O contrato consolidado (api-gateway/openapi.yaml) agora é montado por
    # gen_gateway.py, que agrega todos os */contract/openapi.yaml. Aqui geramos
    # apenas o contrato por microserviço (serviços especiais deste gerador).
    for svc in SERVICES:
        folder = svc["name"]
        write(os.path.join(MS_ROOT, folder, "contract", "openapi.yaml"), dump_yaml(build_service(svc)))


if __name__ == "__main__":
    main()
