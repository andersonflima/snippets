"""Erros de domínio do BFF, mapeáveis para HTTP (mesmo padrão dos microserviços)."""
from __future__ import annotations


class BffError(Exception):
    """Erro de aplicação do BFF com código semântico e status HTTP."""

    def __init__(self, code: str, message: str, http: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http = http


class AuthError(BffError):
    """Falha de autenticação/autorização (credencial, sessão, token)."""

    def __init__(self, message: str, http: int = 401, code: str = "unauthorized") -> None:
        super().__init__(code, message, http)


class UpstreamError(BffError):
    """Falha ao falar com um upstream (API Gateway / IdP M2M)."""

    def __init__(self, message: str, http: int = 502, code: str = "upstream_error") -> None:
        super().__init__(code, message, http)
