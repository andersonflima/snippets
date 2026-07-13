"""Ação dynamodb: executa qualquer operação DynamoDB permitida pelas regras.

O caller informa `params.operation` (ex.: CreateTable, PutItem, Query...) e
`params.args` (os kwargs do client boto3 `dynamodb`, no formato low-level da AWS).
As regras externas — segregadas por ambiente e com exceções por conta/recurso —
decidem o que pode e o que não pode antes de qualquer chamada AWS.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from .aws import ActionError, assumed_session
from .gmud import ensure_change_authorized
from .models import ActionAccepted, DynamoRequest
from .operations import resolve
from .policy import evaluate
from .rules import load_rules


def _jsonable(value: Any) -> Any:
    """Torna a resposta do boto3 serializável (Decimal/bytes/set/aninhados)."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return [_jsonable(v) for v in value]
    return value


def execute(req: DynamoRequest) -> ActionAccepted:
    p = req.params
    op = resolve(p.operation)
    if op is None:
        raise ActionError(
            "validation_error",
            f"operação DynamoDB não suportada: {p.operation}",
            400,
        )

    args = dict(p.args or {})
    rules = load_rules({})
    decision = evaluate(rules, req, op, args)

    # dryRun é preview puro: reporta a decisão (inclui se exigiria GMUD) sem gate
    # de ServiceNow nem chamada AWS.
    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()),
            resource=decision.resource or req.resource,
            account=req.account,
            detail={
                "dryRun": True,
                "operation": op.name,
                "category": op.category,
                "mutating": op.mutating,
                "method": op.method,
                "environment": req.environment,
                "exceptionApplied": decision.exception_id,
                "gmudRequired": decision.gmud_required,
                "args": _jsonable(args),
            },
        )

    # GMUD só quando a política exige (mutação em prod sem exceção que a libere).
    ensure_change_authorized(op.name, req, decision.gmud_required)

    session = assumed_session(req.account, req.roleArn, req.region)
    client = session.client("dynamodb")
    result = getattr(client, op.method)(**args)
    result.pop("ResponseMetadata", None)

    return ActionAccepted(
        operationId=str(uuid.uuid4()),
        resource=decision.resource or req.resource,
        account=req.account,
        detail={
            "operation": op.name,
            "exceptionApplied": decision.exception_id,
            "result": _jsonable(result),
        },
    )
