"""BFF do console de ações — única fronteira entre o frontend Angular e os microserviços.

Responsável por: login/sessão (JWT próprio em cookie httpOnly), e proxy autenticado
aos microserviços via API Gateway usando credencial de serviço (M2M client_credentials).
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .auth.routes import router as auth_router
from .auth.users import EnvUserRepository
from .config import load_settings
from .domain.errors import BffError
from .domain.models import ErrorResponse
from .gateway.client import GatewayClient
from .gateway.m2m import M2MTokenProvider
from .gateway.routes import router as actions_router


def create_app() -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="actions console BFF", version="1.0.0")

    # composição (wiring) — guardada no estado da app, sem singletons globais.
    app.state.settings = settings
    app.state.users = EnvUserRepository(settings.users_json)
    app.state.gateway = GatewayClient(settings, M2MTokenProvider(settings))

    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,  # necessário p/ o cookie httpOnly
            allow_methods=["GET", "POST"],
            allow_headers=["Content-Type", "X-Request-Id"],
        )

    _register_error_handlers(app)

    @app.get("/healthz", tags=["health"])
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", tags=["health"])
    def readyz() -> dict:
        return {"status": "ready"}

    app.include_router(auth_router)
    app.include_router(actions_router)
    return app


def _register_error_handlers(app: FastAPI) -> None:
    def _envelope(status: int, code: str, message: str, request: Request) -> JSONResponse:
        return JSONResponse(
            status_code=status,
            content=ErrorResponse(
                code=code, message=message, requestId=request.headers.get("x-request-id")
            ).model_dump(),
        )

    @app.exception_handler(BffError)
    async def on_bff_error(request: Request, exc: BffError) -> JSONResponse:
        return _envelope(exc.http, exc.code, exc.message, request)

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        message = "payload invalido"
        if errors:
            loc = ".".join(str(part) for part in errors[0].get("loc", []) if part != "body")
            message = f"payload invalido: {loc}: {errors[0].get('msg', '')}".strip(": ")
        return _envelope(422, "validation_error", message, request)

    @app.exception_handler(Exception)
    async def on_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        return _envelope(500, "internal_error", "erro interno inesperado", request)


app = create_app()
