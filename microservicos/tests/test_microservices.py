from __future__ import annotations

import importlib
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
SERVICE_DIRS = tuple(sorted(p for p in ROOT.iterdir() if (p / "app" / "main.py").exists()))
SERVICE_NAMES = tuple(p.name for p in SERVICE_DIRS)
ACTION_SERVICES = (
    "create",
    "data",
    "describe",
    "destroy",
    "modify",
    "replicate",
    "restore",
    "start-stop",
)


@dataclass(frozen=True)
class LoadedService:
    name: str
    root: Path
    main: Any
    handler: Any
    models: Any


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(autouse=True)
def isolated_imports() -> None:
    yield
    for key in tuple(sys.modules):
        if key == "app" or key.startswith("app."):
            sys.modules.pop(key, None)


def load_service(name: str) -> LoadedService:
    root = ROOT / name
    for key in tuple(sys.modules):
        if key == "app" or key.startswith("app."):
            sys.modules.pop(key, None)
    sys.path.insert(0, str(root))
    try:
        main = importlib.import_module("app.main")
        handler = importlib.import_module("app.handler")
        models = importlib.import_module("app.models")
        return LoadedService(name=name, root=root, main=main, handler=handler, models=models)
    finally:
        sys.path.remove(str(root))


@pytest.fixture(params=SERVICE_NAMES, ids=SERVICE_NAMES)
def service(request: pytest.FixtureRequest) -> LoadedService:
    return load_service(str(request.param))


def base_payload(name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "account": "123456789012",
        "resource": "resource-1",
        "roleArn": "arn:aws:iam::123456789012:role/platform-executor",
        "region": "us-east-1",
        "environment": "dev",
        "dryRun": True,
        "params": params or {},
    }
    if name in ACTION_SERVICES:
        payload["params"] = {"operation": "unsupported:Operation", "args": {}}
    elif name == "dbca":
        payload["params"] = {"queryId": "db-overview"}
    elif name == "db-password":
        payload["params"] = {
            "dbIdentifier": "db-1",
            "username": "app_user",
            "newPasswordSecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:new",
        }
    elif name == "finops":
        payload["params"] = {"scope": "all", "lookbackDays": 7}
    elif name == "insights":
        payload["params"] = {"action": "resources", "product": "rds", "limit": 2}
    elif name == "kms":
        payload["params"] = {
            "keyAlias": "alias/app",
            "targetResourceType": "db-instance",
            "targetResourceId": "db-1",
        }
    elif name == "rds-data":
        payload["params"] = {
            "sql": "SELECT * FROM public.orders WHERE id = :id",
            "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db",
            "rulesBucket": "rules-bucket",
            "parameters": {"id": 7, "active": True, "score": 2.5, "note": None},
        }
    elif name == "servicenow":
        payload["params"] = {"operation": "validate", "changeNumber": "CHG0001", "action": "destroy"}
    elif name == "vpc-link":
        payload["params"] = {"dbIdentifier": "db-1", "consumerAccount": "210987654321"}
    return payload


def execute_path(name: str) -> str:
    return f"/{name}/execute"


def test_service_inventory_is_explicit() -> None:
    assert SERVICE_NAMES == (
        "create",
        "data",
        "db-password",
        "dbca",
        "describe",
        "destroy",
        "finops",
        "insights",
        "kms",
        "modify",
        "rds-data",
        "replicate",
        "restore",
        "servicenow",
        "start-stop",
        "vpc-link",
    )


def test_health_and_ready_endpoints(service: LoadedService) -> None:
    client = TestClient(service.main.app)
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_execute_endpoint_rejects_invalid_envelope(service: LoadedService) -> None:
    client = TestClient(service.main.app)
    response = client.post(execute_path(service.name), json={"account": "invalid"})
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "validation_error"
    assert "requestId" in body


def test_execute_endpoint_maps_action_error(service: LoadedService, monkeypatch: pytest.MonkeyPatch) -> None:
    error_cls = service.main.ActionError

    def fail(_req: Any) -> Any:
        raise error_cls("conflict", "blocked by policy", 409)

    monkeypatch.setattr(service.main, "execute", fail)
    response = TestClient(service.main.app).post(execute_path(service.name), json=base_payload(service.name))
    assert response.status_code == 409
    assert response.json()["code"] == "conflict"


def test_execute_endpoint_maps_client_error(service: LoadedService, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(_req: Any) -> Any:
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "AssumeRole",
        )

    monkeypatch.setattr(service.main, "execute", fail)
    response = TestClient(service.main.app).post(execute_path(service.name), json=base_payload(service.name))
    assert response.status_code == 403
    assert response.json()["code"] == "assume_role_denied"


def test_service_openapi_contract_is_segregated_and_gateway_ready(service: LoadedService) -> None:
    contract_path = service.root / "contract" / "openapi.yaml"
    assert contract_path.exists()
    contract = yaml.safe_load(contract_path.read_text())
    assert contract["openapi"].startswith("3.")
    assert contract["paths"]
    assert "CognitoJWT" in contract["components"]["securitySchemes"]
    assert contract["x-amazon-apigateway-request-validators"]["all"]["validateRequestBody"] is True
    for path, methods in contract["paths"].items():
        assert path.startswith(f"/{service.name}")
        for method in methods.values():
            integration = method.get("x-amazon-apigateway-integration")
            assert integration["connectionType"] == "VPC_LINK"
            assert integration["connectionId"] == "${stageVariables.vpcLinkId}"
            assert "${stageVariables.nlbDns}" in integration["uri"]


def test_gateway_contract_is_segregated_by_service(service: LoadedService) -> None:
    contract_path = ROOT / "api-gateway" / "contracts" / service.name / "openapi.yaml"
    assert contract_path.exists()
    contract = yaml.safe_load(contract_path.read_text())
    assert set(contract["paths"]).issubset(set(yaml.safe_load((service.root / "contract" / "openapi.yaml").read_text())["paths"]))
    assert all(path.startswith(f"/{service.name}") for path in contract["paths"])


def test_insights_contract_matches_synchronous_runtime() -> None:
    contract = yaml.safe_load((ROOT / "insights" / "contract" / "openapi.yaml").read_text())
    responses = contract["paths"]["/insights"]["post"]["responses"]
    assert 200 in responses
    assert 202 not in responses
    assert responses[200]["content"]["application/json"]["schema"]["$ref"] == "#/components/schemas/ActionResult"
    assert "ActionResult" in contract["components"]["schemas"]


@pytest.mark.parametrize("name", ACTION_SERVICES)
def test_generated_action_services_cover_dry_run_execute_and_jsonable(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service(name)
    op = next(iter(service.handler.resolve.__globals__["CATALOG"].values()))
    payload = base_payload(name)
    payload["params"] = {"operation": op.key, "args": {op.resource_arg or "Name": "target-1", "count": 1}}
    req_model = getattr(service.models, "".join(part.capitalize() for part in name.split("-")) + "Request")
    req = req_model(**payload)

    monkeypatch.setattr(service.handler, "validate_args", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service.handler, "load_rules", lambda _defaults: {})
    monkeypatch.setattr(
        service.handler,
        "evaluate",
        lambda _rules, _req, _op, _args: SimpleNamespace(
            resource="target-1",
            resource_type="db-instance",
            gmud_required=False,
            exception_id=None,
        ),
    )
    if hasattr(service.handler, "ensure_change_authorized"):
        monkeypatch.setattr(service.handler, "ensure_change_authorized", lambda *_args, **_kwargs: None)

    dry = service.handler.execute(req)
    assert dry.status == "accepted"
    assert dry.detail["dryRun"] is True
    assert dry.detail["operation"] == op.name

    class Client:
        def __getattr__(self, method: str) -> Any:
            def call(**kwargs: Any) -> dict[str, Any]:
                assert method == op.method
                assert kwargs
                return {"decimal": Decimal("2.5"), "raw": b"ok", "ResponseMetadata": {"RequestId": "x"}}

            return call

    monkeypatch.setattr(service.handler, "assumed_session", lambda *_args: SimpleNamespace(client=lambda _name: Client()))
    req = req_model(**{**payload, "dryRun": False})
    result = service.handler.execute(req)
    assert result.detail["result"] == {"decimal": 2.5, "raw": "ok"}


def test_action_services_reject_unknown_operation() -> None:
    service = load_service("create")
    req = service.models.CreateRequest(**base_payload("create"))
    with pytest.raises(service.handler.ActionError) as err:
        service.handler.execute(req)
    assert err.value.code == "validation_error"
    assert err.value.http == 400


def test_rds_data_sql_policy_and_parameter_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service("rds-data")
    rules = {
        "allowedStatements": ["SELECT", "UPDATE"],
        "requireWhereOnWrite": True,
        "tables": {"allow": ["public.orders"]},
    }
    assert service.handler.evaluate_sql("SELECT * FROM public.orders WHERE id = :id", rules, "dev") == (True, "permitido")
    allowed, reason = service.handler.evaluate_sql("UPDATE public.orders SET total = 1", rules, "dev")
    assert allowed is False
    assert "sem WHERE" in reason
    assert service.handler._to_sql_parameters({"a": None, "b": True, "c": 1, "d": 2.5, "e": "x"}) == [
        {"name": "a", "value": {"isNull": True}},
        {"name": "b", "value": {"booleanValue": True}},
        {"name": "c", "value": {"longValue": 1}},
        {"name": "d", "value": {"doubleValue": 2.5}},
        {"name": "e", "value": {"stringValue": "x"}},
    ]

    monkeypatch.setattr(service.handler, "_load_rules", lambda *_args: rules)
    req = service.models.RdsDataRequest(**base_payload("rds-data"))
    dry = service.handler.execute(req)
    assert dry.detail == {"dryRun": True, "allowed": True, "reason": "permitido"}


def test_dbca_dry_run_uses_catalog_discovery_and_role_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service("dbca")
    monkeypatch.setattr(service.handler, "resolve_role_arn", lambda account, role: role)
    monkeypatch.setattr(service.handler, "assumed_session", lambda *_args: object())
    monkeypatch.setattr(
        service.handler,
        "classify",
        lambda *_args: SimpleNamespace(
            engine="aurora-postgresql",
            resource_type="aurora",
            vpc_id="vpc-1",
            endpoint="db.example",
        ),
    )
    result = service.handler.execute(service.models.DbcaRequest(**base_payload("dbca")))
    assert result.detail["query"] == "db-overview"
    assert result.detail["dryRun"] is True
    assert result.detail["engine"] == "aurora-postgresql"


def test_insights_mock_resources_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service("insights")
    monkeypatch.setenv("INSIGHTS_MODE", "mock")
    req = service.models.InsightsRequest(**base_payload("insights"))
    first = service.handler.execute(req)
    second = service.handler.execute(req)
    assert first.status == "ok"
    assert first.detail == second.detail
    assert first.detail["dryRun"] is True
    assert first.detail["products"] == ["rds"]


@pytest.mark.parametrize("name", ("kms", "vpc-link"))
def test_simple_boto_services_cover_dry_run_and_execute(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service(name)
    req_model = getattr(service.models, "".join(part.capitalize() for part in name.split("-")) + "Request")
    monkeypatch.setattr(service.handler, "load_rules", lambda _defaults: {})
    monkeypatch.setattr(service.handler, "enforce_common", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service.handler, "enforce_allowed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        service.handler,
        "assumed_session",
        lambda *_args: SimpleNamespace(client=lambda _name: SimpleNamespace()),
    )
    dry = service.handler.execute(req_model(**base_payload(name)))
    assert dry.detail["dryRun"] is True

    calls: list[tuple[str, dict[str, Any]]] = []

    class Client:
        def __getattr__(self, method: str) -> Any:
            def call(**kwargs: Any) -> dict[str, Any]:
                calls.append((method, kwargs))
                if method == "create_key":
                    return {"KeyMetadata": {"KeyId": "key-1"}}
                return {}

            return call

    monkeypatch.setattr(service.handler, "assumed_session", lambda *_args: SimpleNamespace(client=lambda _name: Client()))
    payload = {**base_payload(name), "dryRun": False}
    if name == "vpc-link":
        payload["params"] = {**payload["params"], "endpointServiceId": "vpce-svc-1"}
    result = service.handler.execute(req_model(**payload))
    assert result.status == "accepted"
    assert calls


def test_finops_validation_and_dry_run() -> None:
    service = load_service("finops")
    payload = base_payload("finops")
    payload["params"] = {"scope": "bad"}
    with pytest.raises(service.handler.ActionError) as err:
        service.handler.execute(service.models.FinopsRequest(**payload))
    assert err.value.http == 400
    result = service.handler.execute(service.models.FinopsRequest(**base_payload("finops")))
    assert result.detail["dryRun"] is True


def test_db_password_secret_parser_and_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service("db-password")
    assert service.handler._secret_password(json.dumps({"password": "pw"})) == "pw"
    assert service.handler._secret_password("plain") == "plain"

    class Rds:
        def describe_db_instances(self, DBInstanceIdentifier: str) -> dict[str, Any]:
            assert DBInstanceIdentifier == "db-1"
            return {"DBInstances": []}

    monkeypatch.setattr(service.handler, "assumed_session", lambda *_args: SimpleNamespace(client=lambda name: Rds()))
    with pytest.raises(service.handler.ActionError) as err:
        service.handler.execute(service.models.DbPasswordRequest(**{**base_payload("db-password"), "dryRun": False}))
    assert err.value.code == "not_found"


def test_servicenow_assessment_and_required_change_number(monkeypatch: pytest.MonkeyPatch) -> None:
    service = load_service("servicenow")
    with pytest.raises(service.handler.ActionError) as err:
        service.handler.execute(service.models.ServicenowRequest(**base_payload("servicenow", {"operation": "validate"})))
    assert err.value.http == 400

    change = {
        "sys_id": "1",
        "number": "CHG0001",
        "state": "implement",
        "approval": "approved",
        "conflict_status": "no conflict",
        "risk": "low",
        "start_date": "2026-01-01 00:00:00",
        "end_date": "2099-01-01 00:00:00",
    }

    class Client:
        def __enter__(self) -> "Client":
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

    monkeypatch.setattr(service.handler, "_client", lambda: Client())
    monkeypatch.setattr(service.handler, "_get_change", lambda _client, _number: change)
    monkeypatch.setattr(service.handler, "_list_tasks", lambda _client, _sys_id: [{"state": "closed"}])
    result = service.handler.execute(service.models.ServicenowRequest(**base_payload("servicenow")))
    assert result.detail["allowed"] is True
