"""Emissão e verificação do JWT próprio do BFF (HS256)."""
from __future__ import annotations

import time
from typing import Any

import jwt as pyjwt

from ..config import Settings
from ..domain.errors import AuthError


def issue_token(settings: Settings, *, subject: str, roles: list[str]) -> str:
    """Cria um JWT assinado para a sessão do usuário."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "sub": subject,
        "roles": roles,
        "iss": settings.jwt_issuer,
        "iat": now,
        "exp": now + settings.jwt_ttl_seconds,
    }
    return pyjwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def verify_token(settings: Settings, token: str) -> dict[str, Any]:
    """Valida assinatura/expiração/emissor e devolve as claims. Levanta AuthError se inválido."""
    try:
        return pyjwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"],
            issuer=settings.jwt_issuer,
            options={"require": ["exp", "iat", "sub"]},
        )
    except pyjwt.ExpiredSignatureError as exc:
        raise AuthError("sessão expirada", code="session_expired") from exc
    except pyjwt.InvalidTokenError as exc:
        raise AuthError("sessão inválida", code="invalid_session") from exc
