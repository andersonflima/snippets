import io
import json
import os
import sys
import tempfile
import types
import unittest
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
    botocore_exceptions_module.NoCredentialsError = type("NoCredentialsError", (Exception,), {})
    botocore_exceptions_module.PartialCredentialsError = type("PartialCredentialsError", (Exception,), {})

    sys.modules.setdefault("boto3", boto3_module)
    sys.modules.setdefault("boto3.session", boto3_session_module)
    sys.modules.setdefault("botocore", botocore_module)
    sys.modules.setdefault("botocore.exceptions", botocore_exceptions_module)


install_aws_stubs()

import iam_role_policy_sync as iam_sync


def _client_error(code: str, operation_name: str) -> Exception:
    return iam_sync.ClientError({"Error": {"Code": code, "Message": code}}, operation_name)


def _build_fake_iam_client(*, account_id: str = "123456789012") -> Any:
    state = {
        "account_id": account_id,
        "roles": {},
        "policies": {},
        "policy_version_seq": 1,
        "policy_create_seq": 1,
        "calls": [],
    }

    def make_policy_arn(name: str, path: str) -> str:
        normalized_path = path if path.startswith("/") else f"/{path}"
        normalized_path = normalized_path if normalized_path != "/" else ""
        return f"arn:aws:iam::{state['account_id']}:policy{normalized_path}/{name}".replace("//", "/")

    def get_role(*, RoleName: str) -> dict[str, Any]:
        state["calls"].append({"operation": "get_role", "RoleName": RoleName})
        role = state["roles"].get(RoleName)
        if role is None:
            raise _client_error("NoSuchEntity", "GetRole")
        return {"Role": role}

    def create_role(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "create_role", "request": kwargs})
        role_name = kwargs["RoleName"]
        role = {
            "RoleName": role_name,
            "Arn": f"arn:aws:iam::{state['account_id']}:role/{role_name}",
            "AssumeRolePolicyDocument": json.loads(kwargs["AssumeRolePolicyDocument"]),
            "Description": kwargs.get("Description", ""),
            "Path": kwargs.get("Path", "/"),
            "MaxSessionDuration": kwargs.get("MaxSessionDuration"),
            "Tags": kwargs.get("Tags", []),
            "AttachedManagedPolicies": [],
            "InlinePolicies": {},
        }
        state["roles"][role_name] = role
        return {"Role": role}

    def update_assume_role_policy(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "update_assume_role_policy", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        role["AssumeRolePolicyDocument"] = json.loads(kwargs["PolicyDocument"])
        return {}

    def update_role_description(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "update_role_description", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        role["Description"] = kwargs["Description"]
        return {}

    def tag_role(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "tag_role", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        role["Tags"] = kwargs["Tags"]
        return {}

    def list_policies(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "list_policies", "request": kwargs})
        path_prefix = kwargs.get("PathPrefix", "/")
        policies = [
            {
                "Arn": policy["Arn"],
                "PolicyName": policy["PolicyName"],
                "Path": policy["Path"],
                "DefaultVersionId": policy["DefaultVersionId"],
            }
            for policy in state["policies"].values()
            if policy["Path"] == path_prefix
        ]
        return {"Policies": policies, "IsTruncated": False}

    def get_policy(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "get_policy", "request": kwargs})
        policy = state["policies"].get(kwargs["PolicyArn"])
        if policy is None:
            raise _client_error("NoSuchEntity", "GetPolicy")
        return {
            "Policy": {
                "Arn": policy["Arn"],
                "PolicyName": policy["PolicyName"],
                "Path": policy["Path"],
                "DefaultVersionId": policy["DefaultVersionId"],
            }
        }

    def create_policy(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "create_policy", "request": kwargs})
        policy_arn = make_policy_arn(kwargs["PolicyName"], kwargs.get("Path", "/"))
        state["policy_create_seq"] += 1
        state["policies"][policy_arn] = {
            "Arn": policy_arn,
            "PolicyName": kwargs["PolicyName"],
            "Path": kwargs.get("Path", "/"),
            "Description": kwargs.get("Description", ""),
            "Tags": kwargs.get("Tags", []),
            "DefaultVersionId": "v1",
            "Versions": [
                {
                    "VersionId": "v1",
                    "IsDefaultVersion": True,
                    "Document": json.loads(kwargs["PolicyDocument"]),
                    "CreateDate": 1,
                }
            ],
        }
        return {"Policy": state["policies"][policy_arn]}

    def get_policy_version(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "get_policy_version", "request": kwargs})
        policy = state["policies"][kwargs["PolicyArn"]]
        version = next(
            version
            for version in policy["Versions"]
            if version["VersionId"] == kwargs["VersionId"]
        )
        return {"PolicyVersion": version}

    def list_policy_versions(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "list_policy_versions", "request": kwargs})
        policy = state["policies"][kwargs["PolicyArn"]]
        return {"Versions": policy["Versions"]}

    def create_policy_version(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "create_policy_version", "request": kwargs})
        policy = state["policies"][kwargs["PolicyArn"]]
        for version in policy["Versions"]:
            version["IsDefaultVersion"] = False
        state["policy_version_seq"] += 1
        version_id = f"v{state['policy_version_seq']}"
        new_version = {
            "VersionId": version_id,
            "IsDefaultVersion": bool(kwargs.get("SetAsDefault")),
            "Document": json.loads(kwargs["PolicyDocument"]),
            "CreateDate": state["policy_version_seq"],
        }
        policy["Versions"].append(new_version)
        if kwargs.get("SetAsDefault"):
            policy["DefaultVersionId"] = version_id
        return {"PolicyVersion": new_version}

    def delete_policy_version(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "delete_policy_version", "request": kwargs})
        policy = state["policies"][kwargs["PolicyArn"]]
        policy["Versions"] = [
            version
            for version in policy["Versions"]
            if version["VersionId"] != kwargs["VersionId"]
        ]
        return {}

    def list_attached_role_policies(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "list_attached_role_policies", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        return {
            "AttachedPolicies": [
                {"PolicyArn": policy_arn}
                for policy_arn in role["AttachedManagedPolicies"]
            ],
            "IsTruncated": False,
        }

    def attach_role_policy(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "attach_role_policy", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        if kwargs["PolicyArn"] not in role["AttachedManagedPolicies"]:
            role["AttachedManagedPolicies"].append(kwargs["PolicyArn"])
        return {}

    def put_role_policy(**kwargs: Any) -> dict[str, Any]:
        state["calls"].append({"operation": "put_role_policy", "request": kwargs})
        role = state["roles"][kwargs["RoleName"]]
        role["InlinePolicies"][kwargs["PolicyName"]] = json.loads(kwargs["PolicyDocument"])
        return {}

    return types.SimpleNamespace(
        state=state,
        get_role=get_role,
        create_role=create_role,
        update_assume_role_policy=update_assume_role_policy,
        update_role_description=update_role_description,
        tag_role=tag_role,
        list_policies=list_policies,
        get_policy=get_policy,
        create_policy=create_policy,
        get_policy_version=get_policy_version,
        list_policy_versions=list_policy_versions,
        create_policy_version=create_policy_version,
        delete_policy_version=delete_policy_version,
        list_attached_role_policies=list_attached_role_policies,
        attach_role_policy=attach_role_policy,
        put_role_policy=put_role_policy,
    )


def _build_fake_session(iam_client: Any) -> Any:
    def client(service_name: str, region_name: str | None = None) -> Any:
        if service_name != "iam":
            raise AssertionError(f"Unexpected service requested: {service_name} {region_name}")
        return iam_client

    return types.SimpleNamespace(client=client)


class IamRolePolicySyncTests(unittest.TestCase):
    def test_build_sync_config_accepts_simple_lambda_role_with_athena_need(self) -> None:
        config = iam_sync.build_sync_config(
            {
                "simple_roles": [
                    {
                        "name": "analytics-lambda-role",
                        "service": "lambda",
                        "needs": ["athena"],
                    }
                ]
            }
        )

        role = config["roles"][0]
        self.assertEqual(role["name"], "analytics-lambda-role")
        self.assertEqual(role["service_principals"], ["lambda.amazonaws.com"])
        self.assertEqual(
            role["managed_policy_arns"],
            [iam_sync.AWS_LAMBDA_BASIC_EXECUTION_ROLE_ARN],
        )
        self.assertTrue(role["merge_trust_principals"])
        self.assertEqual(role["inline_policies"][0]["name"], "preset-athena-access")

    def test_build_sync_config_accepts_lambda_role_and_shared_policy(self) -> None:
        raw_config = {
            "roles": [
                {
                    "name": "orders-lambda-role",
                    "service_principals": ["lambda.amazonaws.com"],
                    "managed_policy_refs": ["shared-dynamodb-read"],
                }
            ],
            "managed_policies": [
                {
                    "name": "shared-dynamodb-read",
                    "document": {
                        "Version": "2012-10-17",
                        "Statement": [],
                    },
                    "attach_to_roles": ["orders-lambda-role"],
                }
            ],
        }

        config = iam_sync.build_sync_config(raw_config)

        self.assertEqual(config["roles"][0]["name"], "orders-lambda-role")
        self.assertEqual(config["roles"][0]["service_principals"], ["lambda.amazonaws.com"])
        self.assertEqual(config["managed_policies"][0]["name"], "shared-dynamodb-read")

    def test_sync_iam_resources_adds_lambda_and_athena_to_existing_role_without_losing_existing_trust(self) -> None:
        iam_client = _build_fake_iam_client()
        iam_client.create_role(
            RoleName="analytics-shared-role",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "events.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
            Path="/",
            Description="existing",
            Tags=[],
        )

        config = iam_sync.build_sync_config(
            {
                "simple_roles": [
                    {
                        "name": "analytics-shared-role",
                        "service": "lambda",
                        "needs": ["athena"],
                    }
                ]
            }
        )

        response = iam_sync.sync_iam_resources(iam_client, config, dry_run=False)

        self.assertTrue(response["ok"])
        role = iam_client.state["roles"]["analytics-shared-role"]
        principal_service = role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"]
        self.assertEqual(
            sorted(principal_service if isinstance(principal_service, list) else [principal_service]),
            ["events.amazonaws.com", "lambda.amazonaws.com"],
        )
        self.assertIn(iam_sync.AWS_LAMBDA_BASIC_EXECUTION_ROLE_ARN, role["AttachedManagedPolicies"])
        self.assertIn("preset-athena-access", role["InlinePolicies"])
        self.assertEqual(response["summary"]["update_trust_policy"], 1)
        self.assertEqual(response["summary"]["attach_managed_policy"], 1)
        self.assertEqual(response["summary"]["put_inline_policy"], 1)

    def test_sync_iam_resources_creates_role_policy_and_attachments(self) -> None:
        iam_client = _build_fake_iam_client()
        config = iam_sync.build_sync_config(
            {
                "roles": [
                    {
                        "name": "orders-lambda-role",
                        "description": "Orders Lambda execution role",
                        "service_principals": ["lambda.amazonaws.com"],
                        "managed_policy_arns": [
                            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                        ],
                        "managed_policy_refs": ["shared-dynamodb-read"],
                        "inline_policies": [
                            {
                                "name": "allow-sqs-send",
                                "document": {
                                    "Version": "2012-10-17",
                                    "Statement": [
                                        {
                                            "Effect": "Allow",
                                            "Action": ["sqs:SendMessage"],
                                            "Resource": ["arn:aws:sqs:sa-east-1:123456789012:orders"],
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                ],
                "managed_policies": [
                    {
                        "name": "shared-dynamodb-read",
                        "document": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": ["dynamodb:GetItem"],
                                    "Resource": ["arn:aws:dynamodb:sa-east-1:123456789012:table/orders"],
                                }
                            ],
                        },
                    }
                ],
            }
        )

        response = iam_sync.sync_iam_resources(iam_client, config, dry_run=False)

        self.assertTrue(response["ok"])
        role = iam_client.state["roles"]["orders-lambda-role"]
        self.assertEqual(
            role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"]["Service"],
            "lambda.amazonaws.com",
        )
        self.assertIn(
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            role["AttachedManagedPolicies"],
        )
        self.assertEqual(len(role["AttachedManagedPolicies"]), 2)
        self.assertIn("allow-sqs-send", role["InlinePolicies"])
        self.assertEqual(response["summary"]["create_role"], 1)
        self.assertEqual(response["summary"]["create_managed_policy"], 1)
        self.assertEqual(response["summary"]["attach_managed_policy"], 2)
        self.assertEqual(response["summary"]["put_inline_policy"], 1)

    def test_sync_iam_resources_updates_existing_policy_version_when_document_changes(self) -> None:
        iam_client = _build_fake_iam_client()
        create_response = iam_client.create_policy(
            PolicyName="shared-dynamodb-read",
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["dynamodb:GetItem"],
                            "Resource": ["arn:aws:dynamodb:sa-east-1:123456789012:table/orders"],
                        }
                    ],
                }
            ),
            Path="/",
            Description="old",
            Tags=[],
        )
        created_policy_arn = create_response["Policy"]["Arn"]

        config = iam_sync.build_sync_config(
            {
                "managed_policies": [
                    {
                        "name": "shared-dynamodb-read",
                        "document": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": ["dynamodb:GetItem", "dynamodb:Query"],
                                    "Resource": ["arn:aws:dynamodb:sa-east-1:123456789012:table/orders"],
                                }
                            ],
                        },
                    }
                ]
            }
        )

        response = iam_sync.sync_iam_resources(iam_client, config, dry_run=False)

        self.assertTrue(response["ok"])
        policy = iam_client.state["policies"][created_policy_arn]
        self.assertEqual(policy["DefaultVersionId"], "v2")
        self.assertEqual(response["summary"]["update_managed_policy_document"], 1)

    def test_run_cli_supports_dry_run_with_config_file(self) -> None:
        iam_client = _build_fake_iam_client()
        fake_session = _build_fake_session(iam_client)
        config = {
            "roles": [
                {
                    "name": "orders-lambda-role",
                    "service_principals": ["lambda.amazonaws.com"],
                }
            ]
        }
        stdout = io.StringIO()

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as config_file:
            json.dump(config, config_file)
            config_path = config_file.name

        try:
            with patch.object(iam_sync, "_build_aws_session", return_value=fake_session):
                with patch("sys.stdout", stdout):
                    exit_code = iam_sync.run_cli(["--config-file", config_path, "--dry-run"])
        finally:
            os.unlink(config_path)

        self.assertEqual(exit_code, 0)
        response = json.loads(stdout.getvalue())
        self.assertTrue(response["ok"])
        self.assertTrue(response["dry_run"])
        self.assertEqual(response["summary"]["would_create_role"], 1)

    def test_run_cli_supports_simple_mode(self) -> None:
        iam_client = _build_fake_iam_client()
        fake_session = _build_fake_session(iam_client)
        stdout = io.StringIO()

        with patch.object(iam_sync, "_build_aws_session", return_value=fake_session):
            with patch("sys.stdout", stdout):
                exit_code = iam_sync.run_cli(
                    ["--role-name", "analytics-lambda-role", "--need", "athena", "--dry-run"]
                )

        self.assertEqual(exit_code, 0)
        response = json.loads(stdout.getvalue())
        self.assertTrue(response["ok"])
        self.assertTrue(response["dry_run"])
        self.assertEqual(response["summary"]["would_create_role"], 1)
        self.assertEqual(response["summary"]["would_attach_managed_policy"], 1)
        self.assertEqual(response["summary"]["would_put_inline_policy"], 1)


if __name__ == "__main__":
    unittest.main()
