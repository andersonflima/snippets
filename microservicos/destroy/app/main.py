"""API do microserviço destroy — exposto via API Gateway -> VPC Link -> NLB -> EKS."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
from .gmud import ensure_change_authorized
from .models import ActionAccepted, ErrorResponse, DestroyRequest

app = FastAPI(title="destroy action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


def _client_error_to_http(exc: ClientError) -> tuple[int, str]:
    code = exc.response.get("Error", {}).get("Code", "")
    if "NotFound" in code or code.endswith("NotFoundFault"):
        return 404, "not_found"
    if "AccessDenied" in code or "Forbidden" in code or "Unauthorized" in code:
        return 403, "assume_role_denied"
    return 409, "conflict"


@app.post(
    "/destroy/execute",
    response_model=ActionAccepted,
    status_code=202,
    responses={
        400: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
def run(req: DestroyRequest):
    try:
        ensure_change_authorized("destroy", req)
        return execute(req)
    except ActionError as exc:
        return JSONResponse(
            status_code=exc.http,
            content=ErrorResponse(code=exc.code, message=exc.message, requestId=req.requestId).model_dump(),
        )
    except ClientError as exc:
        http, code = _client_error_to_http(exc)
        return JSONResponse(
            status_code=http,
            content=ErrorResponse(code=code, message=str(exc), requestId=req.requestId).model_dump(),
        )
