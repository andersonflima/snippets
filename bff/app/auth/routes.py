"""Rotas de autenticação: login (emite JWT + cookie), logout e sessão atual."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

from ..domain.models import LoginRequest, LoginResponse, MeResponse, UserPublic
from ..security.cookies import clear_session_cookie, set_session_cookie
from ..security.jwt import issue_token
from .dependencies import current_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, request: Request, response: Response) -> LoginResponse:
    settings = request.app.state.settings
    repo = request.app.state.users
    user = repo.authenticate(payload.username, payload.password)
    token = issue_token(settings, subject=user.username, roles=user.roles)
    set_session_cookie(response, settings, token)
    return LoginResponse(user=user)


@router.post("/logout", status_code=204)
def logout(request: Request, response: Response) -> Response:
    clear_session_cookie(response, request.app.state.settings)
    response.status_code = 204
    return response


@router.get("/me", response_model=MeResponse)
def me(user: UserPublic = Depends(current_user)) -> MeResponse:
    return MeResponse(user=user)
