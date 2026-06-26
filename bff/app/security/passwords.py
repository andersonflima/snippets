"""Verificação de senha com bcrypt (sem armazenar/transitar texto puro)."""
from __future__ import annotations

import bcrypt


def verify_password(plain: str, hashed: str) -> bool:
    """Compara a senha em texto com o hash bcrypt. Nunca levanta em hash inválido."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def hash_password(plain: str) -> str:
    """Gera um hash bcrypt — utilitário para seed/rotação de credenciais."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
