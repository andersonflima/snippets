"""Ação dbca: descobre o recurso, resolve a query de metadados e executa.

Fluxo: valida a query (admin) -> assume-role na conta -> descobre tipo/VPC/secret
do recurso -> checa se a query se aplica ao engine -> executa (Aurora via Data API
ou DynamoDB via API) -> normaliza {columns, rows} para exibir ao usuário.
"""
from __future__ import annotations

import os
import uuid

from .aws import ActionError, assumed_session, resolve_role_arn
from .discovery import classify
from .engines import aurora
from .engines import dynamodb as dynamodb_engine
from .models import ActionAccepted, DbcaRequest
from .queries import get_query, load_catalog, secret_from_map

DEFAULT_REGION = os.getenv("DEFAULT_TARGET_REGION") or os.getenv("AWS_REGION") or "sa-east-1"


def execute(req: DbcaRequest) -> ActionAccepted:
    query = get_query(req.params.queryId)
    if not query:
        raise ActionError("validation_error", f"query desconhecida: {req.params.queryId}", 400)

    region = req.region or DEFAULT_REGION
    role_arn = resolve_role_arn(req.account, req.roleArn)
    session = assumed_session(req.account, role_arn, region)

    disc = classify(session, req.resource, region)
    engines = query.get("engines") or {}
    if disc.engine not in engines:
        raise ActionError(
            "validation_error",
            f"a query '{query['id']}' não se aplica a {disc.engine} (suporta: {sorted(engines.keys())})",
            400,
        )

    base = {
        "query": query["id"],
        "label": query.get("label"),
        "resourceType": disc.resource_type,
        "engine": disc.engine,
        "vpcId": disc.vpc_id,
        "endpoint": disc.endpoint,
        "region": region,
    }

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()),
            resource=req.resource,
            account=req.account,
            detail={**base, "dryRun": True},
        )

    if disc.resource_type == "aurora":
        catalog = load_catalog()
        result = aurora.run(
            session, disc, query, req.params.parameters, req.params.database, secret_from_map(catalog, req.resource)
        )
    else:
        result = dynamodb_engine.run(session, disc, query, req.params.parameters)

    return ActionAccepted(
        operationId=str(uuid.uuid4()),
        resource=req.resource,
        account=req.account,
        detail={**base, **result},
    )
