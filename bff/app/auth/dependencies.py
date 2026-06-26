"""Dependências de autenticação: recupera o usuário a partir do cookie de sessão."""
from __future__ import annotations

from fastapi import Request

from ..config import Settings
from ..domain.errors import AuthError
from ..domain.models import UserPublic
from ..security.jwt import verify_token


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def current_user(request: Request) -> UserPublic:
    """Lê o cookie httpOnly, valida o JWT próprio e devolve o usuário da sessão."""
    settings: Settings = request.app.state.settings
    token = request.cookies.get(settings.cookie_name)
    if not token:
        raise AuthError("autenticação requerida", code="missing_session")
    claims = verify_token(settings, token)
    return UserPublic(username=_subject(claims), roles=claims.get("roles") or [])


def _subject(claims: dict) -> str:
    username = claims.get("sub")
    if not username:
        raise AuthError("sessão inválida", code="invalid_session")
    return username
