"""Ação servicenow: GMUD via ServiceNow Table API (validate/register/status)."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import httpx

from .aws import ActionError
from .models import ActionAccepted, ServicenowRequest

# ServiceNow Change state: -1 = Implement (liberado p/ execução). Configurável.
ALLOWED_STATES = {s.strip() for s in os.getenv("SERVICENOW_ALLOWED_STATES", "-1,implement").split(",") if s.strip()}
CHANGE_TABLE = os.getenv("SERVICENOW_CHANGE_TABLE", "change_request")


def _client() -> httpx.Client:
    base = os.getenv("SERVICENOW_INSTANCE_URL")
    if not base:
        raise ActionError("validation_error", "SERVICENOW_INSTANCE_URL não configurado", 400)
    headers = {"Accept": "application/json"}
    auth = None
    token = os.getenv("SERVICENOW_TOKEN")
    user = os.getenv("SERVICENOW_USER")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif user:
        auth = (user, os.getenv("SERVICENOW_PASSWORD", ""))
    return httpx.Client(base_url=base.rstrip("/"), headers=headers, auth=auth, timeout=15.0)


def _get_change(client: httpx.Client, number: str) -> dict:
    resp = client.get(
        f"/api/now/table/{CHANGE_TABLE}",
        params={"sysparm_query": f"number={number}", "sysparm_limit": 1},
    )
    if resp.status_code in (401, 403):
        raise ActionError("assume_role_denied", "credencial ServiceNow inválida", 403)
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if not results:
        raise ActionError("not_found", f"change {number} não encontrado", 404)
    return results[0]


def _within_window(change: dict) -> bool:
    start = change.get("start_date") or change.get("work_start")
    end = change.get("end_date") or change.get("work_end")
    if not start or not end:
        return False
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        begin = datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
        finish = datetime.strptime(end, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return begin <= datetime.now(timezone.utc) <= finish


def execute(req: ServicenowRequest) -> ActionAccepted:
    p = req.params
    number = p.changeNumber or req.changeNumber
    operation = p.operation

    if operation in ("validate", "status") and not number:
        raise ActionError("validation_error", "changeNumber é obrigatório", 400)

    try:
        with _client() as client:
            if operation == "validate":
                change = _get_change(client, number)
                state = str(change.get("state", ""))
                in_window = _within_window(change)
                allowed = state in ALLOWED_STATES and in_window
                detail = {
                    "operation": "validate", "change": number, "state": state,
                    "withinWindow": in_window, "allowed": allowed,
                }
            elif operation == "status":
                change = _get_change(client, number)
                detail = {"operation": "status", "change": number, "state": str(change.get("state", ""))}
            elif operation == "register":
                if req.dryRun:
                    detail = {"operation": "register", "change": number, "dryRun": True}
                else:
                    change = _get_change(client, number)
                    note = p.workNote or f"action={p.action} operationId={p.operationId or req.requestId}"
                    patch = client.patch(
                        f"/api/now/table/{CHANGE_TABLE}/{change['sys_id']}",
                        json={"work_notes": note},
                    )
                    patch.raise_for_status()
                    detail = {"operation": "register", "change": number, "registered": True}
            else:
                raise ActionError("validation_error", f"operation inválida: {operation}", 400)
    except httpx.HTTPError as exc:
        raise ActionError("upstream_error", f"ServiceNow: {exc}", 502) from exc

    return ActionAccepted(operationId=str(uuid.uuid4()), resource=req.resource, account=req.account, detail=detail)
