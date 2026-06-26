"""Repositório de usuários do BFF.

Abstração plugável: a implementação padrão lê os usuários de um JSON em
ambiente/secret (`BFF_USERS`), com senhas em bcrypt — sem texto puro, sem DB.
Trocar por um provedor externo (LDAP/AD, banco) é só implementar `UserRepository`.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from ..domain.errors import AuthError
from ..domain.models import UserPublic
from ..security.passwords import verify_password


@dataclass(frozen=True)
class UserRecord:
    username: str
    password_hash: str
    roles: tuple[str, ...]

    def to_public(self) -> UserPublic:
        return UserPublic(username=self.username, roles=list(self.roles))


class UserRepository(Protocol):
    def authenticate(self, username: str, password: str) -> UserPublic: ...

    def find(self, username: str) -> UserPublic | None: ...


class EnvUserRepository:
    """Implementação default baseada em `BFF_USERS` (lista JSON)."""

    def __init__(self, users_json: str) -> None:
        self._users = _parse_users(users_json)

    def authenticate(self, username: str, password: str) -> UserPublic:
        record = self._users.get(username)
        # bcrypt mesmo quando o usuário não existe evita timing/enumeração grosseira.
        reference = record.password_hash if record else _DUMMY_HASH
        ok = verify_password(password, reference)
        if not record or not ok:
            raise AuthError("credenciais inválidas", code="invalid_credentials")
        return record.to_public()

    def find(self, username: str) -> UserPublic | None:
        record = self._users.get(username)
        return record.to_public() if record else None


# hash bcrypt fixo de uma senha aleatória — só para o caminho de comparação dummy.
_DUMMY_HASH = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO0jJjK8.Gnj1q2X6m9o0p1q2r3s4t5u6"


def _parse_users(users_json: str) -> dict[str, UserRecord]:
    try:
        raw = json.loads(users_json or "[]")
    except json.JSONDecodeError:
        return {}
    out: dict[str, UserRecord] = {}
    for item in raw if isinstance(raw, list) else []:
        username = item.get("username")
        password_hash = item.get("passwordHash") or item.get("password_hash")
        if not username or not password_hash:
            continue
        roles = tuple(item.get("roles") or [])
        out[username] = UserRecord(username=username, password_hash=password_hash, roles=roles)
    return out
