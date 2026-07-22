"""Enforcement genérico da regra externa (opt-in por chave; ausência = liberado).

Ordem: região -> exceção -> allow/deny de operação/categoria/resourceType por
ambiente -> GMUD. Tudo baseado no documento carregado por rules.load_rules.
"""
from __future__ import annotations

from dataclasses import dataclass

from .aws import ActionError
from .operations import Operation, resource_of


@dataclass(frozen=True)
class Decision:
    resource: str | None
    resource_type: str | None
    exception_id: str | None
    gmud_required: bool


def _deny(message: str) -> None:
    raise ActionError("rule_violation", message, 403)


def _op_in(names: list | None, op: Operation) -> bool:
    """Casa a operação por chave '<client>:<Op>' (precisa) OU nome curto (qualquer client)."""
    names = names or []
    return op.key in names or op.name in names


def _exception_active(exc: dict) -> bool:
    return True if not exc.get("expiresAt") else str(exc.get("expiresAt")) >= ""


def _matching_exception(rules: dict, req, op: Operation, resource: str | None) -> dict | None:
    for exc in rules.get("exceptions", []) or []:
        if exc.get("account") and exc["account"] != req.account:
            continue
        if exc.get("environment") not in (None, req.environment):
            continue
        if exc.get("resource") and exc["resource"] != resource:
            continue
        if not _exception_active(exc):
            continue
        ops = exc.get("allowOperations", []) or []
        cats = exc.get("allowCategories", []) or []
        if "*" in ops or _op_in(ops, op) or "*" in cats or op.category in cats:
            return exc
    return None


def _enforce_environment(rules: dict, req, op: Operation, resource_type: str | None) -> None:
    env = (rules.get("environments") or {}).get(req.environment) or {}
    if _op_in(env.get("deniedOperations"), op):
        _deny(f"operação bloqueada em {req.environment}: {op.key}")
    allowed_ops = env.get("allowedOperations")
    if allowed_ops and not _op_in(allowed_ops, op):
        _deny(f"operação não permitida em {req.environment}: {op.key}")
    if op.category in (env.get("deniedCategories") or []):
        _deny(f"categoria bloqueada em {req.environment}: {op.category}")
    allowed_cats = env.get("allowedCategories")
    if allowed_cats and op.category not in allowed_cats:
        _deny(f"categoria não permitida em {req.environment}: {op.category}")
    if resource_type:
        if resource_type in (env.get("deniedResourceTypes") or []):
            _deny(f"tipo de recurso bloqueado em {req.environment}: {resource_type}")
        allowed_rt = env.get("allowedResourceTypes")
        if allowed_rt and resource_type not in allowed_rt:
            _deny(f"tipo de recurso não permitido em {req.environment}: {resource_type}")


def _gmud_required(rules: dict, req, op: Operation) -> bool:
    env = (rules.get("environments") or {}).get(req.environment) or {}
    cats = env.get("gmudForCategories")
    if isinstance(cats, list):
        return op.category in cats
    if env.get("requireGmudForMutations", req.environment == "prod"):
        return op.mutating
    return False


def evaluate(rules: dict, req, op: Operation, args: dict) -> Decision:
    allowed_regions = rules.get("allowedRegions")
    if allowed_regions and req.region not in allowed_regions:
        _deny(f"região não permitida: {req.region} (permitidas: {allowed_regions})")

    resource = resource_of(op, args) or (req.resource if req.resource != "*" else None)
    resource_type = op.resource_type

    exc = _matching_exception(rules, req, op, resource)
    if exc is not None:
        return Decision(resource, resource_type, exc.get("reason") or "exception", False)

    _enforce_environment(rules, req, op, resource_type)
    return Decision(resource, resource_type, None, _gmud_required(rules, req, op))
