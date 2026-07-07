"""Testes do GatewayClient: Bearer M2M presente só quando habilitado (produção)."""
from __future__ import annotations

import dataclasses

import httpx

from app.config import Settings
from app.domain.models import UserPublic
from app.gateway.client import GatewayClient


class _RecordingTokens:
    """Provider M2M fake que registra se o token chegou a ser solicitado."""

    def __init__(self) -> None:
        self.calls = 0

    def token(self) -> str:
        self.calls += 1
        return "m2m-token"


def _settings(**over) -> Settings:
    base = Settings(jwt_secret="x", gateway_base_url="https://gw.example.com")
    return dataclasses.replace(base, **over)


def _capture_headers(monkeypatch) -> dict:
    captured: dict = {}

    def fake_post(url, json, headers, timeout):  # noqa: A002 - assinatura do httpx.post
        captured.update(headers)
        captured["_url"] = url
        return httpx.Response(202, request=httpx.Request("POST", url))

    monkeypatch.setattr("app.gateway.client.httpx.post", fake_post)
    return captured


def _actor() -> UserPublic:
    return UserPublic(username="alice", roles=["operator"])


def test_m2m_enabled_attaches_bearer(monkeypatch):
    headers = _capture_headers(monkeypatch)
    tokens = _RecordingTokens()
    client = GatewayClient(_settings(m2m_enabled=True), tokens)  # type: ignore[arg-type]

    client.execute("create", {"account": "1"}, actor=_actor(), request_id="rq-1")

    assert headers["Authorization"] == "Bearer m2m-token"
    assert headers["X-Actor"] == "alice"
    assert headers["_url"] == "https://gw.example.com/create/execute"
    assert tokens.calls == 1


def test_m2m_disabled_forwards_without_bearer(monkeypatch):
    headers = _capture_headers(monkeypatch)
    tokens = _RecordingTokens()
    client = GatewayClient(_settings(m2m_enabled=False), tokens)  # type: ignore[arg-type]

    client.execute("create", {"account": "1"}, actor=_actor(), request_id=None)

    assert "Authorization" not in headers      # sem token de serviço no local
    assert headers["X-Actor"] == "alice"        # identidade do ator preservada
    assert tokens.calls == 0                     # provider M2M nem é chamado
