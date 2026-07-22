"""Gate de GMUD (ServiceNow) para ambiente produtivo."""
from __future__ import annotations

import os

import httpx

from .aws import ActionError

SERVICENOW_URL = os.getenv("SERVICENOW_SERVICE_URL", "http://servicenow/servicenow/execute")


def ensure_change_authorized(action: str, req, required: bool) -> None:
    if not required:
        return
    if not req.changeNumber:
        raise ActionError("gmud_required", "changeNumber (GMUD) obrigatório para esta ação", 400)
    payload = {
        "account": req.account, "resource": req.resource, "roleArn": req.roleArn,
        "region": req.region, "environment": req.environment,
        "changeNumber": req.changeNumber, "params": {"operation": "validate", "action": action},
    }
    try:
        resp = httpx.post(SERVICENOW_URL, json=payload, timeout=10.0)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"falha ao validar GMUD: {exc}", 502) from exc
    if resp.status_code >= 300:
        raise ActionError("gmud_required", f"GMUD não autorizada ({resp.status_code})", 403)
    detail = resp.json().get("detail") or {}
    if detail.get("allowed") is not True:
        raise ActionError("gmud_required", "mudança não está autorizada/na janela", 403)
