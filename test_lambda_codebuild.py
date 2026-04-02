import os
import sys
import types
import unittest
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch


def install_aws_stubs() -> None:
    boto3_module = types.ModuleType("boto3")
    boto3_session_module = types.ModuleType("boto3.session")
    botocore_module = types.ModuleType("botocore")
    botocore_exceptions_module = types.ModuleType("botocore.exceptions")

    class Session:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

        def client(self, service_name: str, region_name: str | None = None) -> Any:
            raise AssertionError(f"Unexpected boto3 session client request: {service_name} {region_name}")

    class BotoCoreError(Exception):
        pass

    class ClientError(Exception):
        def __init__(self, response: dict | None = None, operation_name: str = "") -> None:
            super().__init__(operation_name)
            self.response = response or {}

    boto3_session_module.Session = Session
    boto3_module.session = boto3_session_module

    botocore_module.exceptions = botocore_exceptions_module
    botocore_exceptions_module.BotoCoreError = BotoCoreError
    botocore_exceptions_module.ClientError = ClientError

    for name in (
        "ConnectTimeoutError",
        "EndpointConnectionError",
        "NoCredentialsError",
        "NoRegionError",
        "PartialCredentialsError",
        "ProxyConnectionError",
        "ReadTimeoutError",
    ):
        setattr(botocore_exceptions_module, name, type(name, (Exception,), {}))

    sys.modules.setdefault("boto3", boto3_module)
    sys.modules.setdefault("boto3.session", boto3_session_module)
    sys.modules.setdefault("botocore", botocore_module)
    sys.modules.setdefault("botocore.exceptions", botocore_exceptions_module)


install_aws_stubs()

import lambda_codebuild as codebuild_lambda


FIXED_NOW = datetime(2026, 4, 2, 18, 15, 16, tzinfo=timezone.utc)
ROLE_ARN_A = "arn:aws:iam::111111111111:role/codebuild-trigger"
ROLE_ARN_B = "arn:aws:iam::222222222222:role/codebuild-trigger"
ROLE_ARN_FAIL = "arn:aws:iam::333333333333:role/codebuild-trigger-fail"


class DummyContext:
    aws_request_id = "req-123"


class FakeCodeBuildClient:
    def __init__(self, *, role_arn: str, calls: list[dict[str, Any]]) -> None:
        self.role_arn = role_arn
        self.calls = calls

    def start_build(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"role_arn": self.role_arn, "request": kwargs})
        account_id = self.role_arn.split(":")[4]
        role_name = self.role_arn.split("/")[-1]
        return {
            "build": {
                "id": f"{role_name}:build/001",
                "arn": f"arn:aws:codebuild:sa-east-1:{account_id}:build/{role_name}:001",
                "buildNumber": 1,
                "buildStatus": "IN_PROGRESS",
            }
        }


class FakeAssumedSession:
    def __init__(self, *, role_arn: str, calls: list[dict[str, Any]]) -> None:
        self.role_arn = role_arn
        self.calls = calls

    def client(self, service_name: str, region_name: str | None = None) -> Any:
        if service_name != "codebuild":
            raise AssertionError(f"Unexpected service requested: {service_name}")
        self.calls.append({"role_arn": self.role_arn, "region_name": region_name, "operation": "client"})
        return FakeCodeBuildClient(role_arn=self.role_arn, calls=self.calls)


class LambdaCodeBuildTests(unittest.TestCase):
    def test_build_codebuild_config_accepts_environment_variables_list_from_payload(self) -> None:
        event = {
            "target_role_arns": [ROLE_ARN_A],
            "codebuild_project_name": "deploy-project",
            "codebuild_region": "sa-east-1",
            "codebuild_environment_variables": [
                {"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"}
            ],
        }

        with patch.object(codebuild_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.dict(os.environ, {}, clear=True):
                config = codebuild_lambda.build_codebuild_config(event)

        self.assertEqual(config["run_id"], "20260402T181516Z")
        self.assertEqual(config["target_role_arns"], [ROLE_ARN_A])
        self.assertEqual(
            config["codebuild_environment_variables"],
            [{"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"}],
        )

    def test_build_start_build_request_includes_buildspec_source_version_and_environment_variables(self) -> None:
        config = {
            "run_id": "20260402T181516Z",
            "codebuild_project_name": "deploy-project",
            "codebuild_buildspec": "buildspecs/deploy.yml",
            "codebuild_source_version": "refs/heads/main",
            "codebuild_environment_variables": [
                {"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"},
                {"name": "PARAM_NAME", "value": "/app/value", "type": "PARAMETER_STORE"},
            ],
        }

        request = codebuild_lambda._build_start_build_request(config, role_arn=ROLE_ARN_A)

        self.assertEqual(request["projectName"], "deploy-project")
        self.assertEqual(request["buildspecOverride"], "buildspecs/deploy.yml")
        self.assertEqual(request["sourceVersion"], "refs/heads/main")
        self.assertEqual(
            request["environmentVariablesOverride"],
            [
                {"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"},
                {"name": "PARAM_NAME", "value": "/app/value", "type": "PARAMETER_STORE"},
            ],
        )
        self.assertIn("idempotencyToken", request)
        self.assertEqual(len(request["idempotencyToken"]), 32)

    def test_lambda_handler_returns_config_error_when_target_roles_are_missing(self) -> None:
        event = {
            "codebuild_project_name": "deploy-project",
            "codebuild_region": "sa-east-1",
        }

        with patch.dict(os.environ, {}, clear=True):
            response = codebuild_lambda.lambda_handler(event, DummyContext())

        self.assertFalse(response["ok"])
        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error_type"], "config")
        self.assertIn("target_role_arns", response["error"])

    def test_lambda_handler_starts_builds_for_target_roles(self) -> None:
        event = {
            "target_role_arns": [ROLE_ARN_A, ROLE_ARN_B],
            "codebuild_project_name": "deploy-project",
            "codebuild_region": "sa-east-1",
            "codebuild_buildspec": "buildspecs/deploy.yml",
            "codebuild_source_version": "refs/heads/main",
            "codebuild_environment_variables": [
                {"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"}
            ],
            "assume_role_external_id": "external-id-123",
            "assume_role_duration_seconds": 1800,
            "assume_role_session_name_prefix": "trigger",
            "max_workers": 2,
        }
        assume_calls: list[dict[str, Any]] = []
        codebuild_calls: list[dict[str, Any]] = []

        def fake_assume_role_session(**kwargs: Any) -> Any:
            assume_calls.append(kwargs)
            return FakeAssumedSession(role_arn=kwargs["role_arn"], calls=codebuild_calls)

        with patch.object(codebuild_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.object(codebuild_lambda, "_build_aws_session", return_value=object()):
                with patch.object(codebuild_lambda, "_assume_role_session", side_effect=fake_assume_role_session):
                    with patch.dict(os.environ, {}, clear=True):
                        response = codebuild_lambda.lambda_handler(event, DummyContext())

        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["target_count"], 2)
        self.assertEqual([result["target_role_arn"] for result in response["results"]], [ROLE_ARN_A, ROLE_ARN_B])
        self.assertEqual([result["status"] for result in response["results"]], ["STARTED", "STARTED"])
        self.assertEqual([call["role_arn"] for call in assume_calls], [ROLE_ARN_A, ROLE_ARN_B])
        self.assertEqual([call["external_id"] for call in assume_calls], ["external-id-123", "external-id-123"])
        self.assertEqual([call["duration_seconds"] for call in assume_calls], [1800, 1800])
        self.assertTrue(all(call["session_name"].startswith("trigger-20260402T181516Z-") for call in assume_calls))

        start_build_requests = [call["request"] for call in codebuild_calls if "request" in call]
        self.assertEqual(len(start_build_requests), 2)
        self.assertTrue(all(request["projectName"] == "deploy-project" for request in start_build_requests))
        self.assertTrue(all(request["buildspecOverride"] == "buildspecs/deploy.yml" for request in start_build_requests))
        self.assertTrue(all(request["sourceVersion"] == "refs/heads/main" for request in start_build_requests))
        self.assertTrue(
            all(
                request["environmentVariablesOverride"] == [
                    {"name": "ENV_NAME", "value": "production", "type": "PLAINTEXT"}
                ]
                for request in start_build_requests
            )
        )

    def test_lambda_handler_returns_partial_ok_when_one_role_fails(self) -> None:
        event = {
            "target_role_arns": [ROLE_ARN_A, ROLE_ARN_FAIL],
            "codebuild_project_name": "deploy-project",
            "codebuild_region": "sa-east-1",
            "max_workers": 1,
        }

        def fake_assume_role_session(**kwargs: Any) -> Any:
            if kwargs["role_arn"] == ROLE_ARN_FAIL:
                raise RuntimeError("assume role failed")
            return FakeAssumedSession(role_arn=kwargs["role_arn"], calls=[])

        with patch.object(codebuild_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.object(codebuild_lambda, "_build_aws_session", return_value=object()):
                with patch.object(codebuild_lambda, "_assume_role_session", side_effect=fake_assume_role_session):
                    with patch.dict(os.environ, {}, clear=True):
                        response = codebuild_lambda.lambda_handler(event, DummyContext())

        self.assertFalse(response["ok"])
        self.assertEqual(response["status"], "partial_ok")
        self.assertEqual([result["target_role_arn"] for result in response["results"]], [ROLE_ARN_A, ROLE_ARN_FAIL])
        self.assertEqual([result["status"] for result in response["results"]], ["STARTED", "FAILED"])
        self.assertEqual(response["results"][1]["error"], "assume role failed")


if __name__ == "__main__":
    unittest.main()
