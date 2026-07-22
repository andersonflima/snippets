"""Dispatch genérico: resolve op -> regra externa -> assume role -> boto3."""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from .aws import ActionError, assumed_session
from .models import ActionAccepted, StartStopRequest
from .operations import resolve
from .policy import evaluate
from .rules import load_rules
from .validate import validate_args
from .gmud import ensure_change_authorized


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def execute(req: StartStopRequest) -> ActionAccepted:
    p = req.params
    op = resolve(p.operation)
    if op is None:
        raise ActionError("validation_error", f"operação não suportada por este serviço: {p.operation}", 400)

    args = dict(p.args or {})
    validate_args(op, args)
    rules = load_rules({})
    decision = evaluate(rules, req, op, args)

    if req.dryRun:
        return ActionAccepted(
            operationId=str(uuid.uuid4()), resource=decision.resource or req.resource, account=req.account,
            detail={
                "dryRun": True, "operation": op.name, "client": op.client, "method": op.method,
                "category": op.category, "mutating": op.mutating, "resourceType": decision.resource_type,
                "gmudRequired": decision.gmud_required, "exceptionApplied": decision.exception_id,
                "args": _jsonable(args),
            },
        )

    ensure_change_authorized(op.name, req, decision.gmud_required)

    session = assumed_session(req.account, req.roleArn, req.region)
    client = session.client(op.client)
    result = getattr(client, op.method)(**args)
    if isinstance(result, dict):
        result.pop("ResponseMetadata", None)

    return ActionAccepted(
        operationId=str(uuid.uuid4()), resource=decision.resource or req.resource, account=req.account,
        detail={"operation": op.name, "exceptionApplied": decision.exception_id, "result": _jsonable(result)},
    )
