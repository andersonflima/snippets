"""Testes do BFF: login/sessão (cookie httpOnly + JWT próprio) e proxy autenticado."""
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


def test_login_sets_cookie_and_me_roundtrip(client):
    r = client.post("/auth/login", json={"username": "alice", "password": "s3nha!"})
    assert r.status_code == 200
    assert r.json()["user"] == {"username": "alice", "roles": ["operator"]}
    assert client.cookies.get("bff_session")  # cookie de sessão emitido

    me = client.get("/auth/me")
    assert me.status_code == 200
    assert me.json()["user"]["username"] == "alice"


def test_login_wrong_password_is_401_envelope(client):
    r = client.post("/auth/login", json={"username": "alice", "password": "errada"})
    assert r.status_code == 401
    body = r.json()
    assert body["code"] == "invalid_credentials"
    assert set(body) == {"code", "message", "requestId"}


def test_me_without_session_is_401(client):
    r = client.get("/auth/me")
    assert r.status_code == 401
    assert r.json()["code"] == "missing_session"


def test_proxy_requires_auth(client):
    r = client.post("/api/rds-data/execute", json={"foo": "bar"})
    assert r.status_code == 401
    assert r.json()["code"] == "missing_session"


def test_proxy_forwards_with_actor(client):
    client.post("/auth/login", json={"username": "alice", "password": "s3nha!"})
    payload = {"account": "123456789012", "resource": "c", "params": {"sql": "SELECT 1"}}
    r = client.post("/api/rds-data/execute", json=payload, headers={"x-request-id": "rq-1"})
    assert r.status_code == 202
    assert r.json()["operationId"] == "op-1"
    forwarded = client.app.state.gateway.last
    assert forwarded["service"] == "rds-data"
    assert forwarded["actor"] == "alice"
    assert forwarded["roles"] == ["operator"]


def test_proxy_unknown_service_is_404(client):
    client.post("/auth/login", json={"username": "alice", "password": "s3nha!"})
    r = client.post("/api/nope/execute", json={})
    assert r.status_code == 404
    assert r.json()["code"] == "not_found"


def test_logout_clears_session(client):
    client.post("/auth/login", json={"username": "alice", "password": "s3nha!"})
    assert client.get("/auth/me").status_code == 200
    out = client.post("/auth/logout")
    assert out.status_code == 204
    client.cookies.clear()
    assert client.get("/auth/me").status_code == 401
