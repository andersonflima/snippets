"""Ação servicenow: GMUD via ServiceNow Table API (validate/register/status).

A validação de uma GMUD para execução produtiva exige, de forma configurável:
  1. status de implementação liberado  (state ∈ SERVICENOW_ALLOWED_STATES)
  2. dentro da janela planejada         (start_date ≤ agora ≤ end_date)
  3. aprovações corretas                (approval ∈ SERVICENOW_APPROVED_STATES)
  4. sem conflitos                      (conflict_status ∈ SERVICENOW_CONFLICT_OK)
  5. tarefas registradas                (change_task ≥ SERVICENOW_MIN_TASKS)

Cada checagem é opcional via flag de ambiente; ausência de flag mantém o default
seguro. O `detail` retorna o resultado por checagem (para monitoramento) e o
booleano `allowed` que o gate de GMUD (`ensure_change_authorized`) consome.
"""
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
TASK_TABLE = os.getenv("SERVICENOW_TASK_TABLE", "change_task")

# Checagens adicionais (opt-out por flag). Valores comparados em lower-case.
REQUIRE_APPROVAL = os.getenv("SERVICENOW_REQUIRE_APPROVAL", "true").strip().lower() != "false"
APPROVED_STATES = {s.strip().lower() for s in os.getenv("SERVICENOW_APPROVED_STATES", "approved").split(",") if s.strip()}
REQUIRE_NO_CONFLICT = os.getenv("SERVICENOW_REQUIRE_NO_CONFLICT", "true").strip().lower() != "false"
CONFLICT_OK = {s.strip().lower() for s in os.getenv("SERVICENOW_CONFLICT_OK", "no conflict").split(",") if s.strip()}
REQUIRE_TASKS = os.getenv("SERVICENOW_REQUIRE_TASKS", "true").strip().lower() != "false"


def _min_tasks() -> int:
    try:
        return max(0, int(os.getenv("SERVICENOW_MIN_TASKS", "1")))
    except ValueError:
        return 1


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


def _list_tasks(client: httpx.Client, change_sys_id: str) -> list[dict]:
    """Change tasks registradas para a GMUD (para a checagem de tarefas)."""
    if not change_sys_id:
        return []
    resp = client.get(
        f"/api/now/table/{TASK_TABLE}",
        params={
            "sysparm_query": f"change_request={change_sys_id}",
            "sysparm_fields": "number,short_description,state",
            "sysparm_limit": 200,
        },
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def _window(change: dict) -> dict:
    """Janela planejada e se 'agora' está dentro dela."""
    start = change.get("start_date") or change.get("work_start")
    end = change.get("end_date") or change.get("work_end")
    info = {"ok": False, "value": {"start": start or None, "end": end or None}, "required": True}
    if not start or not end:
        return info
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        begin = datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
        finish = datetime.strptime(end, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return info
    info["ok"] = begin <= datetime.now(timezone.utc) <= finish
    return info


def _assess(client: httpx.Client, change: dict) -> dict:
    """Avalia todas as regras da GMUD e devolve um retrato completo + `allowed`."""
    sys_id = str(change.get("sys_id", ""))
    state = str(change.get("state", ""))
    approval = str(change.get("approval", "")).strip().lower()
    conflict = str(change.get("conflict_status", "")).strip().lower()

    tasks = _list_tasks(client, sys_id) if REQUIRE_TASKS else []
    task_count = len(tasks)
    min_tasks = _min_tasks()

    checks = {
        "state": {"ok": state in ALLOWED_STATES, "value": state, "required": True},
        "window": _window(change),
        "approval": {
            "ok": (not REQUIRE_APPROVAL) or approval in APPROVED_STATES,
            "value": approval or "unknown",
            "required": REQUIRE_APPROVAL,
        },
        "conflict": {
            "ok": (not REQUIRE_NO_CONFLICT) or conflict in CONFLICT_OK,
            "value": conflict or "unknown",
            "required": REQUIRE_NO_CONFLICT,
        },
        "tasks": {
            "ok": (not REQUIRE_TASKS) or task_count >= min_tasks,
            "value": task_count,
            "required": REQUIRE_TASKS,
        },
    }
    reasons = [name for name, c in checks.items() if c["required"] and not c["ok"]]
    return {
        "state": state,
        "withinWindow": checks["window"]["ok"],
        "approval": approval or "unknown",
        "conflictStatus": conflict or "unknown",
        "taskCount": task_count,
        "checks": checks,
        "reasons": reasons,
        "allowed": not reasons,
    }


def execute(req: ServicenowRequest) -> ActionAccepted:
    p = req.params
    number = p.changeNumber or req.changeNumber
    operation = p.operation

    # Toda operação (validate/status/register) opera sobre uma change existente.
    if not number:
        raise ActionError("validation_error", "changeNumber é obrigatório", 400)

    try:
        with _client() as client:
            if operation == "validate":
                change = _get_change(client, number)
                detail = {"operation": "validate", "change": number, **_assess(client, change)}
            elif operation == "status":
                # Retrato completo da GMUD para monitoramento (sem gatear).
                change = _get_change(client, number)
                detail = {"operation": "status", "change": number, **_assess(client, change)}
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
