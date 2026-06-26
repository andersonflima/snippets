"""Modelos do contrato do BFF (auth + envelope de erro)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, description="Identificador do usuário.")
    password: str = Field(min_length=1, description="Senha em texto (validada via bcrypt).")


class UserPublic(BaseModel):
    username: str
    roles: list[str] = Field(default_factory=list)


class LoginResponse(BaseModel):
    user: UserPublic


class MeResponse(BaseModel):
    user: UserPublic


class ErrorResponse(BaseModel):
    """Mesmo envelope de erro dos microserviços, para consistência ponta a ponta."""

    code: str
    message: str
    requestId: Optional[str] = None
