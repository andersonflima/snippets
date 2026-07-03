"""Rotas de proxy: o frontend chama o BFF, que repassa a ação ao microserviço."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from ..auth.dependencies import current_user
from ..domain.errors import BffError
from ..domain.models import UserPublic

router = APIRouter(prefix="/api", tags=["actions"])

# Ações suportadas (espelha os microserviços action-driven gerados).
SUPPORTED_SERVICES = frozenset({
    "create", "destroy", "modify", "start-stop", "storage", "restore",
    "replicate", "kms", "vpc-link", "db-password", "rds-data", "servicenow",
    "finops",
})


@router.post("/{service}/execute")
def execute(service: str, payload: dict[str, Any], request: Request,
            user: UserPublic = Depends(current_user)) -> JSONResponse:
    if service not in SUPPORTED_SERVICES:
        raise BffError("not_found", f"ação desconhecida: {service}", 404)
    client = request.app.state.gateway
    request_id = request.headers.get("x-request-id") or payload.get("requestId")
    upstream = client.execute(service, payload, actor=user, request_id=request_id)
    # repassa status e corpo do microserviço de forma transparente (preserva ErrorResponse).
    media = upstream.headers.get("content-type", "")
    if media.startswith("application/json"):
        return JSONResponse(status_code=upstream.status_code, content=upstream.json())
    return JSONResponse(
        status_code=upstream.status_code,
        content={"code": "upstream_non_json", "message": upstream.text[:500], "requestId": request_id},
    )
