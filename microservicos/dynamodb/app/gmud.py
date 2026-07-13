"""Gate de GMUD: exige change autorizada no ServiceNow quando a política pedir.

Diferente dos serviços de RDS (que gateiam qualquer ação em produção), aqui o
gate só vale quando a regra do ambiente marca `requireGmudForMutations` e a
operação é mutante — e é dispensado quando uma exceção liberou a ação (a própria
exceção é a autorização explícita). Assim, leituras em produção não pedem GMUD.
"""
from __future__ import annotations

import os

import httpx

from .aws import ActionError

SERVICENOW_SERVICE_URL = os.getenv("SERVICENOW_SERVICE_URL", "http://servicenow/servicenow/execute")


def ensure_change_authorized(action: str, req, required: bool) -> None:
    if not required:
        return
    if not req.changeNumber:
        raise ActionError("validation_error", "changeNumber (GMUD) é obrigatório para esta mutação em produção", 400)
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
