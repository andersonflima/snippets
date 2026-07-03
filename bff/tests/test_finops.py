"""Testes do proxy do BFF para o microserviço finops (action-driven)."""
from __future__ import annotations

import json

import httpx
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    from app.security.passwords import hash_password

    users = [{"username": "alice", "passwordHash": hash_password("s3nha!"), "roles": ["operator"]}]
    monkeypatch.setenv("BFF_JWT_SECRET", "test-secret")
    monkeypatch.setenv("BFF_USERS", json.dumps(users))
    monkeypatch.setenv("BFF_COOKIE_SECURE", "false")  # TestClient roda em http
    monkeypatch.setenv("BFF_COOKIE_SAMESITE", "lax")
    monkeypatch.setenv("API_GATEWAY_BASE_URL", "https://gw.example.com")

    from app.main import create_app

    app = create_app()

    class FakeGateway:
        last: dict = {}

        def execute(self, service, payload, actor, request_id):
            FakeGateway.last = {"service": service, "payload": payload,
                                "actor": actor.username, "roles": list(actor.roles)}
            req = httpx.Request("POST", "https://gw.example.com")
            return httpx.Response(
                202,
                json={"operationId": "op-1", "status": "accepted", "detail": {"dryRun": True}},
                request=req,
            )

    app.state.gateway = FakeGateway()
    return TestClient(app, raise_server_exceptions=False)


def test_finops_requires_auth(client):
    r = client.post("/api/finops/execute", json={"account": "123456789012"})
    assert r.status_code == 401
    assert r.json()["code"] == "missing_session"


def test_finops_forwards_envelope_with_actor(client):
    client.post("/auth/login", json={"username": "alice", "password": "s3nha!"})
    payload = {
        "account": "123456789012",
        "resource": "all",
        "roleArn": "arn:aws:iam::123456789012:role/microservicos-actions-role",
        "region": "sa-east-1",
        "environment": "prod",
        "params": {"scope": "all", "lookbackDays": 14},
    }
    r = client.post("/api/finops/execute", json=payload, headers={"x-request-id": "rq-1"})
    assert r.status_code == 202
    assert r.json()["operationId"] == "op-1"
    forwarded = client.app.state.gateway.last
    assert forwarded["service"] == "finops"
    assert forwarded["payload"] == payload
    assert forwarded["actor"] == "alice"
    assert forwarded["roles"] == ["operator"]
