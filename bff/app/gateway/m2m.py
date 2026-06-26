"""Token M2M (OAuth2 client_credentials) para o BFF falar com o API Gateway.

O token é cacheado em memória e renovado pouco antes de expirar. A identidade do
usuário NÃO vai aqui (isso é máquina-a-máquina); ela viaja como contexto na request.
"""
from __future__ import annotations

import threading
import time

import httpx

from ..config import Settings
from ..domain.errors import UpstreamError

_EXPIRY_SKEW = 30  # renova 30s antes de expirar


class M2MTokenProvider:
    """Fornece um access token de serviço, com cache thread-safe."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._expires_at - _EXPIRY_SKEW:
                return self._token
            return self._refresh()

    def _refresh(self) -> str:
        s = self._settings
        if not (s.m2m_token_url and s.m2m_client_id and s.m2m_client_secret):
            raise UpstreamError("credenciais M2M do BFF não configuradas", code="m2m_not_configured")
        data = {"grant_type": "client_credentials"}
        if s.m2m_scope:
            data["scope"] = s.m2m_scope
        try:
            resp = httpx.post(
                s.m2m_token_url,
                data=data,
                auth=(s.m2m_client_id, s.m2m_client_secret),
                timeout=s.upstream_timeout,
            )
        except httpx.HTTPError as exc:
            raise UpstreamError(f"falha ao obter token M2M: {exc}") from exc
        if resp.status_code >= 400:
            raise UpstreamError(f"IdP M2M recusou ({resp.status_code})", code="m2m_denied")
        body = resp.json()
        token = body.get("access_token")
        if not token:
            raise UpstreamError("resposta M2M sem access_token")
        self._token = token
        self._expires_at = time.time() + int(body.get("expires_in", 3600))
        return token
