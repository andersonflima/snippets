"""Gate de GMUD: em ambiente produtivo, exige change autorizada no ServiceNow."""
from __future__ import annotations

import os

import httpx

from .aws import ActionError

PRODUCTIVE_ENVIRONMENTS = {"prod"}
SERVICENOW_SERVICE_URL = os.getenv("SERVICENOW_SERVICE_URL", "http://servicenow/servicenow/execute")


def ensure_change_authorized(action: str, req) -> None:
    """Bloqueia a execução se o ambiente é produtivo e não há GMUD autorizada.

    A change é sempre criada no ServiceNow (nunca por nós): em produção o código
    da GMUD (changeNumber) é obrigatório para ser buscada/validada/acompanhada.
    """
    if req.environment not in PRODUCTIVE_ENVIRONMENTS:
        return
    if not req.changeNumber:
        raise ActionError("validation_error", "changeNumber (GMUD) é obrigatório em produção", 400)
    payload = {
        "account": req.account,
        "resource": req.resource,
        "roleArn": req.roleArn,
        "region": req.region,
        "environment": req.environment,
        "changeNumber": req.changeNumber,
        "requestId": req.requestId,
        "params": {"operation": "validate", "action": action, "changeNumber": req.changeNumber},
    }
    try:
        resp = httpx.post(SERVICENOW_SERVICE_URL, json=payload, timeout=10.0)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"falha ao validar GMUD: {exc}", 502) from exc
    if resp.status_code >= 400:
        raise ActionError("gmud_required", f"GMUD não autorizada ({resp.status_code})", 403)
    allowed = bool((resp.json().get("detail") or {}).get("allowed"))
    if not allowed:
        raise ActionError("gmud_required", "execução produtiva requer GMUD aprovada na janela", 403)
