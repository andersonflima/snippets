"""FastAPI app do serviço `restore` (POST /restore/execute)."""
from __future__ import annotations

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .aws import ActionError
from .handler import execute
from .models import ActionAccepted, ErrorResponse, RestoreRequest

app = FastAPI(title="restore action microservice", version="1.0.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    return {"status": "ready"}


def _err(http: int, code: str, message: str, request_id: str | None) -> JSONResponse:
    return JSONResponse(status_code=http, content=ErrorResponse(code=code, message=message, requestId=request_id).model_dump())


def _client_error_to_http(exc: ClientError) -> tuple[int, str]:
    code = exc.response.get("Error", {}).get("Code", "")
    if "NotFound" in code or code.endswith("NotFoundFault"):
        return 404, "not_found"
    if code in ("AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "Forbidden"):
        return 403, "assume_role_denied"
    return 409, "conflict"


@app.exception_handler(RequestValidationError)
async def _on_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
    loc = "; ".join(".".join(str(p) for p in e["loc"]) + ": " + e["msg"] for e in exc.errors())
    return _err(422, "validation_error", loc, request.headers.get("x-request-id"))


@app.exception_handler(Exception)
async def _on_error(request: Request, exc: Exception) -> JSONResponse:
    return _err(500, "internal_error", str(exc), request.headers.get("x-request-id"))


@app.post("/restore/execute", response_model=ActionAccepted, status_code=202,
          responses={c: {"model": ErrorResponse} for c in (400, 403, 404, 409, 422, 500, 502)})
def run(req: RestoreRequest) -> ActionAccepted | JSONResponse:
    try:
        return execute(req)
    except ActionError as exc:
        return _err(exc.http, exc.code, exc.message, req.requestId)
    except ClientError as exc:
        http, code = _client_error_to_http(exc)
        return _err(http, code, str(exc), req.requestId)
