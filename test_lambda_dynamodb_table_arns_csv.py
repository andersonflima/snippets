import io
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

import lambda_dynamodb_table_arns_csv as inventory_lambda


FIXED_NOW = datetime(2026, 4, 8, 18, 25, 0, tzinfo=timezone.utc)


class DummyContext:
    aws_request_id = "req-456"


class FakeCloudControlClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def list_resources(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if not self.responses:
            raise AssertionError("Unexpected cloudcontrol.list_resources call")
        return self.responses.pop(0)


class FakeDynamoDbClient:
    def __init__(self, describe_table_by_name: dict[str, str]) -> None:
        self.describe_table_by_name = describe_table_by_name
        self.calls: list[dict[str, Any]] = []

    def describe_table(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        table_name = kwargs["TableName"]
        table_arn = self.describe_table_by_name[table_name]
        return {"Table": {"TableArn": table_arn}}


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"ETag": '"etag-001"'}


class FakeAthenaClient:
    def __init__(
        self,
        *,
        start_query_execution_response: dict[str, Any],
        get_query_execution_responses: list[dict[str, Any]],
        get_query_results_responses: list[dict[str, Any]],
    ) -> None:
        self.start_query_execution_response = start_query_execution_response
        self.get_query_execution_responses = get_query_execution_responses
        self.get_query_results_responses = get_query_results_responses
        self.calls: list[dict[str, Any]] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"operation": "start_query_execution", "request": kwargs})
        return self.start_query_execution_response

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"operation": "get_query_execution", "request": kwargs})
        if not self.get_query_execution_responses:
            raise AssertionError("Unexpected athena.get_query_execution call")
        return self.get_query_execution_responses.pop(0)

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"operation": "get_query_results", "request": kwargs})
        if not self.get_query_results_responses:
            raise AssertionError("Unexpected athena.get_query_results call")
        return self.get_query_results_responses.pop(0)


class FakeSession:
    def __init__(self, clients: dict[tuple[str, str | None], Any]) -> None:
        self.clients = clients

    def client(self, service_name: str, region_name: str | None = None) -> Any:
        client = self.clients.get((service_name, region_name))
        if client is None:
            raise AssertionError(f"Unexpected service requested: {service_name} {region_name}")
        return client


class LambdaDynamoDbTableArnsCsvTests(unittest.TestCase):
    def test_build_lambda_config_uses_env_precedence_and_generates_default_key(self) -> None:
        event = {
            "bucket": "bucket-do-event",
            "regions": ["us-east-1"],
        }

        with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.dict(
                os.environ,
                {
                    "OUTPUT_BUCKET": "bucket-do-ambiente",
                    "REGIONS": "sa-east-1,us-east-1",
                    "AWS_REGION": "eu-west-1",
                },
                clear=True,
            ):
                config = inventory_lambda.build_lambda_config(event)

        self.assertEqual(config["bucket"], "bucket-do-ambiente")
        self.assertEqual(config["regions"], ["sa-east-1", "us-east-1"])
        self.assertEqual(
            config["key"],
            "inventories/dynamodb-table-arns/dynamodb-table-arns-20260408T182500Z.csv",
        )

    def test_build_lambda_config_switches_to_athena_mode_when_query_is_present(self) -> None:
        event = {
            "bucket": "inventory-bucket",
            "regions": ["sa-east-1"],
            "query": "select table_arn from metadata",
            "athena_database": "catalog_db",
        }

        with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.dict(os.environ, {}, clear=True):
                config = inventory_lambda.build_lambda_config(event)

        self.assertEqual(config["mode"], "athena")
        self.assertEqual(config["query"], "select table_arn from metadata")
        self.assertEqual(config["athena_database"], "catalog_db")
        self.assertEqual(config["athena_catalog"], "")
        self.assertEqual(config["athena_workgroup"], "primary")
        self.assertEqual(
            config["athena_result_output_location"],
            "s3://inventory-bucket/athena-query-results/dynamodb-table-arns/20260408T182500Z/",
        )

    def test_build_lambda_config_keeps_athena_context_empty_for_complete_query(self) -> None:
        event = {
            "bucket": "inventory-bucket",
            "regions": ["sa-east-1"],
            "query": "select table_arn from meu_catalog.meu_schema.minha_view",
        }

        with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
            with patch.dict(os.environ, {}, clear=True):
                config = inventory_lambda.build_lambda_config(event)

        self.assertEqual(config["mode"], "athena")
        self.assertEqual(config["athena_database"], "")
        self.assertEqual(config["athena_catalog"], "")
        self.assertEqual(config["query"], "select table_arn from meu_catalog.meu_schema.minha_view")

    def test_lambda_handler_lists_tables_generates_csv_and_uploads_to_s3(self) -> None:
        cloudcontrol_client_sa = FakeCloudControlClient(
            [
                {
                    "ResourceDescriptions": [
                        {
                            "Identifier": "orders",
                            "Properties": (
                                '{"TableArn":"arn:aws:dynamodb:sa-east-1:111111111111:table/orders"}'
                            ),
                        }
                    ],
                    "NextToken": "page-2",
                },
                {
                    "ResourceDescriptions": [
                        {
                            "Identifier": "payments",
                            "Properties": "{}",
                        }
                    ]
                },
            ]
        )
        cloudcontrol_client_us = FakeCloudControlClient(
            [
                {
                    "ResourceDescriptions": [
                        {
                            "Identifier": "arn:aws:dynamodb:us-east-1:111111111111:table/customers",
                            "Properties": "",
                        }
                    ]
                }
            ]
        )
        dynamodb_client_sa = FakeDynamoDbClient(
            {
                "payments": "arn:aws:dynamodb:sa-east-1:111111111111:table/payments",
            }
        )
        dynamodb_client_us = FakeDynamoDbClient({})
        s3_client = FakeS3Client()
        session = FakeSession(
            {
                ("cloudcontrol", "sa-east-1"): cloudcontrol_client_sa,
                ("cloudcontrol", "us-east-1"): cloudcontrol_client_us,
                ("dynamodb", "sa-east-1"): dynamodb_client_sa,
                ("dynamodb", "us-east-1"): dynamodb_client_us,
                ("s3", "sa-east-1"): s3_client,
            }
        )

        event = {
            "bucket": "inventory-bucket",
            "prefix": "exports/dynamodb",
            "regions": ["sa-east-1", "us-east-1"],
        }

        with patch.object(inventory_lambda, "_build_aws_session", return_value=session):
            with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
                with patch.dict(os.environ, {}, clear=True):
                    response = inventory_lambda.lambda_handler(event, DummyContext())

        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["table_count"], 3)
        self.assertEqual(response["bucket"], "inventory-bucket")
        self.assertEqual(
            response["key"],
            "exports/dynamodb/dynamodb-table-arns-20260408T182500Z.csv",
        )
        self.assertEqual(
            response["s3_uri"],
            "s3://inventory-bucket/exports/dynamodb/dynamodb-table-arns-20260408T182500Z.csv",
        )
        self.assertEqual(
            response["table_arns_sample"],
            [
                "arn:aws:dynamodb:sa-east-1:111111111111:table/orders",
                "arn:aws:dynamodb:sa-east-1:111111111111:table/payments",
                "arn:aws:dynamodb:us-east-1:111111111111:table/customers",
            ],
        )
        self.assertEqual(
            dynamodb_client_sa.calls,
            [{"TableName": "payments"}],
        )
        self.assertEqual(
            cloudcontrol_client_sa.calls,
            [
                {"TypeName": "AWS::DynamoDB::Table", "MaxResults": 100},
                {"TypeName": "AWS::DynamoDB::Table", "MaxResults": 100, "NextToken": "page-2"},
            ],
        )
        self.assertEqual(
            cloudcontrol_client_us.calls,
            [{"TypeName": "AWS::DynamoDB::Table", "MaxResults": 100}],
        )

        self.assertEqual(len(s3_client.calls), 1)
        uploaded_object = s3_client.calls[0]
        self.assertEqual(
            uploaded_object["Bucket"],
            "inventory-bucket",
        )
        self.assertEqual(
            uploaded_object["Key"],
            "exports/dynamodb/dynamodb-table-arns-20260408T182500Z.csv",
        )
        csv_content = uploaded_object["Body"].decode("utf-8")
        self.assertEqual(
            csv_content,
            "\n".join(
                [
                    "table_arn,table_name,region,account_id",
                    "arn:aws:dynamodb:sa-east-1:111111111111:table/orders,orders,sa-east-1,111111111111",
                    "arn:aws:dynamodb:sa-east-1:111111111111:table/payments,payments,sa-east-1,111111111111",
                    "arn:aws:dynamodb:us-east-1:111111111111:table/customers,customers,us-east-1,111111111111",
                    "",
                ]
            ),
        )

    def test_lambda_handler_executes_athena_query_and_uploads_csv_result(self) -> None:
        athena_client = FakeAthenaClient(
            start_query_execution_response={"QueryExecutionId": "query-123"},
            get_query_execution_responses=[
                {"QueryExecution": {"Status": {"State": "RUNNING"}}},
                {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
            ],
            get_query_results_responses=[
                {
                    "ResultSet": {
                        "ResultSetMetadata": {
                            "ColumnInfo": [
                                {"Name": "table_arn"},
                                {"Name": "table_name"},
                            ]
                        },
                        "Rows": [
                            {"Data": [{"VarCharValue": "table_arn"}, {"VarCharValue": "table_name"}]},
                            {
                                "Data": [
                                    {"VarCharValue": "arn:aws:dynamodb:sa-east-1:111111111111:table/orders"},
                                    {"VarCharValue": "orders"},
                                ]
                            },
                        ],
                    },
                    "NextToken": "page-2",
                },
                {
                    "ResultSet": {
                        "ResultSetMetadata": {
                            "ColumnInfo": [
                                {"Name": "table_arn"},
                                {"Name": "table_name"},
                            ]
                        },
                        "Rows": [
                            {
                                "Data": [
                                    {"VarCharValue": "arn:aws:dynamodb:sa-east-1:111111111111:table/payments"},
                                    {"VarCharValue": "payments"},
                                ]
                            }
                        ],
                    }
                },
            ],
        )
        s3_client = FakeS3Client()
        session = FakeSession(
            {
                ("athena", "sa-east-1"): athena_client,
                ("s3", "sa-east-1"): s3_client,
            }
        )
        event = {
            "bucket": "inventory-bucket",
            "prefix": "exports/dynamodb",
            "regions": ["sa-east-1"],
            "query": "select table_arn, table_name from metadata.dynamodb_tables",
            "athena_database": "metadata",
            "athena_poll_interval_seconds": 0,
        }

        with patch.object(inventory_lambda, "_build_aws_session", return_value=session):
            with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
                with patch.dict(os.environ, {}, clear=True):
                    response = inventory_lambda.lambda_handler(event, DummyContext())

        self.assertTrue(response["ok"])
        self.assertEqual(response["mode"], "athena")
        self.assertEqual(response["query_execution_id"], "query-123")
        self.assertEqual(response["row_count"], 2)
        self.assertEqual(response["columns"], ["table_arn", "table_name"])
        self.assertEqual(
            response["table_arns_sample"],
            [
                "arn:aws:dynamodb:sa-east-1:111111111111:table/orders",
                "arn:aws:dynamodb:sa-east-1:111111111111:table/payments",
            ],
        )
        self.assertEqual(
            athena_client.calls,
            [
                {
                    "operation": "start_query_execution",
                    "request": {
                        "QueryString": "select table_arn, table_name from metadata.dynamodb_tables",
                        "WorkGroup": "primary",
                        "ResultConfiguration": {
                            "OutputLocation": "s3://inventory-bucket/athena-query-results/dynamodb-table-arns/20260408T182500Z/"
                        },
                        "QueryExecutionContext": {"Database": "metadata"},
                    },
                },
                {
                    "operation": "get_query_execution",
                    "request": {"QueryExecutionId": "query-123"},
                },
                {
                    "operation": "get_query_execution",
                    "request": {"QueryExecutionId": "query-123"},
                },
                {
                    "operation": "get_query_results",
                    "request": {"QueryExecutionId": "query-123"},
                },
                {
                    "operation": "get_query_results",
                    "request": {"QueryExecutionId": "query-123", "NextToken": "page-2"},
                },
            ],
        )
        self.assertEqual(len(s3_client.calls), 1)
        uploaded_csv = s3_client.calls[0]["Body"].decode("utf-8")
        self.assertEqual(
            uploaded_csv,
            "\n".join(
                [
                    "table_arn,table_name",
                    "arn:aws:dynamodb:sa-east-1:111111111111:table/orders,orders",
                    "arn:aws:dynamodb:sa-east-1:111111111111:table/payments,payments",
                    "",
                ]
            ),
        )

    def test_lambda_handler_returns_data_integrity_error_when_athena_result_is_inconsistent(self) -> None:
        athena_client = FakeAthenaClient(
            start_query_execution_response={"QueryExecutionId": "query-999"},
            get_query_execution_responses=[
                {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
            ],
            get_query_results_responses=[
                {
                    "ResultSet": {
                        "ResultSetMetadata": {
                            "ColumnInfo": [
                                {"Name": "table_arn"},
                                {"Name": "table_name"},
                            ]
                        },
                        "Rows": [
                            {"Data": [{"VarCharValue": "table_arn"}, {"VarCharValue": "table_name"}]},
                            {
                                "Data": [
                                    {"VarCharValue": "arn:aws:dynamodb:sa-east-1:111111111111:table/orders"},
                                    {"VarCharValue": "payments"},
                                ]
                            },
                        ],
                    }
                }
            ],
        )
        s3_client = FakeS3Client()
        session = FakeSession(
            {
                ("athena", "sa-east-1"): athena_client,
                ("s3", "sa-east-1"): s3_client,
            }
        )
        event = {
            "bucket": "inventory-bucket",
            "regions": ["sa-east-1"],
            "query": "select table_arn, table_name from metadata.dynamodb_tables",
            "athena_database": "metadata",
            "athena_poll_interval_seconds": 0,
        }

        with patch.object(inventory_lambda, "_build_aws_session", return_value=session):
            with patch.object(inventory_lambda, "_now_utc", return_value=FIXED_NOW):
                with patch.dict(os.environ, {}, clear=True):
                    response = inventory_lambda.lambda_handler(event, DummyContext())

        self.assertFalse(response["ok"])
        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error_type"], "data_integrity")
        self.assertIn("table_name inconsistente", response["error"])
        self.assertEqual(s3_client.calls, [])

    def test_lambda_handler_returns_config_error_when_bucket_is_missing(self) -> None:
        event = {
            "regions": ["sa-east-1"],
        }

        with patch.dict(os.environ, {"AWS_REGION": "sa-east-1"}, clear=True):
            response = inventory_lambda.lambda_handler(event, DummyContext())

        self.assertFalse(response["ok"])
        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error_type"], "config")
        self.assertIn("bucket", response["error"])

    def test_run_local_cli_executes_lambda_handler_and_prints_json(self) -> None:
        stdout = io.StringIO()

        with patch.object(
            inventory_lambda,
            "lambda_handler",
            return_value={"ok": True, "status": "ok", "mode": "athena"},
        ) as lambda_handler_mock:
            with patch("sys.stdout", stdout):
                with patch.dict(os.environ, {}, clear=True):
                    exit_code = inventory_lambda.run_local_cli(
                        [
                            "--bucket",
                            "inventory-bucket",
                            "--region",
                            "sa-east-1",
                            "--query",
                            "select table_arn from meu_catalog.meu_schema.minha_view",
                            "--aws-profile",
                            "sandbox",
                            "--aws-default-region",
                            "sa-east-1",
                        ]
                    )
                    self.assertEqual(os.environ["AWS_PROFILE"], "sandbox")
                    self.assertEqual(os.environ["AWS_REGION"], "sa-east-1")
                    self.assertEqual(os.environ["AWS_DEFAULT_REGION"], "sa-east-1")

        self.assertEqual(exit_code, 0)
        lambda_handler_mock.assert_called_once()
        event, context = lambda_handler_mock.call_args.args
        self.assertEqual(event["bucket"], "inventory-bucket")
        self.assertEqual(event["regions"], ["sa-east-1"])
        self.assertEqual(
            event["query"],
            "select table_arn from meu_catalog.meu_schema.minha_view",
        )
        self.assertEqual(context.aws_request_id, "local-cli")
        self.assertEqual(
            stdout.getvalue(),
            '{\n  "ok": true,\n  "status": "ok",\n  "mode": "athena"\n}\n',
        )


if __name__ == "__main__":
    unittest.main()
