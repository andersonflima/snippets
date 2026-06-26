#!/usr/bin/env python3
"""Gera os contratos OpenAPI 3.0 do API Gateway (REST API) para os microserviços.

Topologia: API Gateway (edge, IAM SigV4) -> VPC Link -> NLB interno -> EKS -> pod.
Cada ação é um recurso/path. connectionId (VPC Link) e DNS do NLB vêm de stage
variables, então o mesmo contrato serve para qualquer ambiente.

Saídas:
  api-gateway/openapi.yaml                 # contrato consolidado do gateway
  ../<service>/contract/openapi.yaml       # contrato por microserviço (autocontido)
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
        "name": "restore",
        "summary": "Restaura snapshot → instância ou cria snapshot de uma instância.",
        "params": {
            "operation": {"type": "string", "enum": ["create-snapshot", "restore-snapshot"]},
            "snapshotIdentifier": s_str("Snapshot de origem (restore) ou destino (create)."),
            "targetInstanceIdentifier": s_str("Identificador da instância resultante."),
            "dbInstanceClass": s_str("Classe da instância restaurada."),
            "dbSubnetGroupName": s_str("Subnet group de destino."),
            "kmsKeyId": s_str("KMS key para a instância/snapshot resultante."),
        },
        "required": ["operation"],
    },
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
        "name": "replicate",
        "summary": "Copia qualquer recurso cross-account ou recria em outra region.",
        "params": {
            "sourceAccount": s_str("Conta de origem (default: account).", pattern=r"^\d{12}$"),
            "sourceRegion": s_str("Região de origem."),
            "destinationAccount": s_str("Conta de destino.", pattern=r"^\d{12}$"),
            "destinationRegion": s_str("Região de destino."),
            "resourceType": {"type": "string", "enum": ["db-snapshot", "db-instance", "ami", "kms-key", "parameter-group"]},
            "resourceId": s_str("Recurso a replicar."),
            "kmsKeyId": s_str("KMS key de destino p/ re-encriptar."),
            "shareThenCopy": {"type": "boolean", "default": True, "description": "Compartilha cross-account e copia re-encriptando."},
        },
        "required": ["destinationAccount", "destinationRegion", "resourceType", "resourceId"],
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
        "name": "modify",
        "summary": "Modify genérico — inclui instance class e engine version.",
        "params": {
            "resourceType": {"type": "string", "enum": ["db-instance", "db-cluster", "ec2-instance"]},
            "modifications": {
                "type": "object",
                "description": "Campos a modificar (contrato por tipo de recurso).",
                "properties": {
                    "dbInstanceClass": s_str("Nova instance class."),
                    "engineVersion": s_str("Nova engine version."),
                    "parameterGroupName": s_str("Parameter group."),
                    "backupRetentionPeriod": {"type": "integer"},
                },
            },
            "applyImmediately": {"type": "boolean", "default": False},
        },
        "required": ["resourceType", "modifications"],
    },
    {
        "name": "create",
        "summary": "Provisiona recursos (contrato por tipo).",
        "params": {
            "resourceType": {"type": "string", "enum": ["db-instance", "db-subnet-group", "security-group", "parameter-group"]},
            "spec": {"type": "object", "description": "Especificação do recurso (contrato por tipo)."},
            "waitUntilAvailable": {"type": "boolean", "default": True},
        },
        "required": ["resourceType", "spec"],
    },
    {
        "name": "destroy",
        "summary": "Remove recursos (cleanup pós-fluxo).",
        "params": {
            "resourceType": {"type": "string", "enum": ["db-instance", "db-snapshot", "vpc-endpoint", "kms-grant", "security-group"]},
            "skipFinalSnapshot": {"type": "boolean", "default": True},
            "finalSnapshotIdentifier": s_str("Snapshot final, se skipFinalSnapshot=false."),
        },
        "required": ["resourceType"],
    },
    {
        "name": "start-stop",
        "summary": "Liga/desliga recursos que suportam power.",
        "params": {
            "operation": {"type": "string", "enum": ["start", "stop"]},
            "resourceType": {"type": "string", "enum": ["db-instance", "db-cluster", "ec2-instance"]},
        },
        "required": ["operation", "resourceType"],
    },
    {
        "name": "storage",
        "summary": "Altera storage — tipo (gp3/io1/…) e aumento de tamanho.",
        "params": {
            "resourceType": {"type": "string", "enum": ["db-instance", "ec2-volume"]},
            "storageType": {"type": "string", "enum": ["gp2", "gp3", "io1", "io2"]},
            "allocatedStorage": {"type": "integer", "description": "Novo tamanho (GiB) — só aumento."},
            "iops": {"type": "integer"},
            "storageThroughput": {"type": "integer"},
            "applyImmediately": {"type": "boolean", "default": False},
        },
        "required": ["resourceType"],
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
        "required": ENVELOPE_REQUIRED + ["params"],
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
    "502": {"description": "Falha no upstream (EKS/NLB).", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
}

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
            "responses": RESPONSES,
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


def build_gateway() -> dict:
    paths = {f"/{svc['name']}": operation(svc) for svc in SERVICES}
    schemas = dict(COMMON_SCHEMAS)
    for svc in SERVICES:
        schemas[schema_name(svc)] = request_schema(svc)
    doc = base_doc(
        "microservicos-actions — API Gateway (REST)",
        "Front-door dos microserviços action-driven no EKS. API Gateway (edge, "
        "Cognito JWT) -> VPC Link -> NLB interno -> EKS. Disparado pela ação do "
        "cliente no frontend. Um path por ação.",
    )
    doc.update(gateway_extensions())
    doc["paths"] = paths
    doc["components"] = {"securitySchemes": SECURITY_SCHEME, "schemas": schemas}
    return doc


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
    write(os.path.join(HERE, "openapi.yaml"), dump_yaml(build_gateway()))
    for svc in SERVICES:
        folder = svc["name"]
        write(os.path.join(MS_ROOT, folder, "contract", "openapi.yaml"), dump_yaml(build_service(svc)))


if __name__ == "__main__":
    main()
