"""API do microserviço dynamodb — exposto via API Gateway -> VPC Link -> NLB -> EKS."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
from .models import ActionAccepted, DynamoRequest, ErrorResponse

app = FastAPI(title="dynamodb action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


@app.exception_handler(RequestValidationError)
async def on_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Padroniza erros de validacao (Pydantic) no envelope ErrorResponse."""
    errors = exc.errors()
    message = "payload invalido"
    if errors:
        loc = ".".join(str(part) for part in errors[0].get("loc", []) if part != "body")
        message = f"payload invalido: {loc}: {errors[0].get('msg', '')}".strip(": ")
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            code="validation_error",
            message=message,
            requestId=request.headers.get("x-request-id"),
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def on_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    """Captura qualquer erro inesperado, garantindo 500 no envelope ErrorResponse."""
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            code="internal_error",
            message="erro interno inesperado",
            requestId=request.headers.get("x-request-id"),
        ).model_dump(),
    )


def _client_error_to_http(exc: ClientError) -> tuple[int, str]:
    code = exc.response.get("Error", {}).get("Code", "")
    if "NotFound" in code or code.endswith("NotFoundException"):
        return 404, "not_found"
    if "AccessDenied" in code or "Forbidden" in code or "Unauthorized" in code:
        return 403, "assume_role_denied"
    return 409, "conflict"


@app.post(
    "/dynamodb/execute",
    response_model=ActionAccepted,
    status_code=202,
    responses={
        400: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
def run(req: DynamoRequest):
    try:
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
