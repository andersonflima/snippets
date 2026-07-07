"""Cliente do API Gateway: encaminha a ação ao microserviço com token M2M + ator."""
from __future__ import annotations

import httpx

from ..config import Settings
from ..domain.errors import UpstreamError
from ..domain.models import UserPublic
from .m2m import M2MTokenProvider


class GatewayClient:
    """Repassa `POST /<service>/execute` ao gateway, anexando identidade do ator."""

    def __init__(self, settings: Settings, tokens: M2MTokenProvider) -> None:
        self._settings = settings
        self._tokens = tokens

    def execute(self, service: str, payload: dict, actor: UserPublic, request_id: str | None) -> httpx.Response:
        base = self._settings.gateway_base_url
        if not base:
            raise UpstreamError("API_GATEWAY_BASE_URL não configurado", code="gateway_not_configured")
        headers = {
            "X-Actor": actor.username,
            "X-Actor-Roles": ",".join(actor.roles),
        }
        # Bearer M2M só quando habilitado (produção). Em dev/local sem Cognito,
        # encaminha apenas com a identidade do ator, sem token de serviço.
        if self._settings.m2m_enabled:
            headers["Authorization"] = f"Bearer {self._tokens.token()}"
        if request_id:
            headers["X-Request-Id"] = request_id
        try:
            return httpx.post(
                f"{base}/{service}/execute",
                json=payload,
                headers=headers,
                timeout=self._settings.upstream_timeout,
            )
        except httpx.HTTPError as exc:
            raise UpstreamError(f"falha ao chamar {service}: {exc}") from exc
