import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
import json
import sys
import threading
import time
import types
from typing import Any
from unittest.mock import patch


def install_aws_stubs() -> None:
    boto3_module = types.ModuleType("boto3")
    dynamodb_module = types.ModuleType("boto3.dynamodb")
    dynamodb_types_module = types.ModuleType("boto3.dynamodb.types")
    botocore_module = types.ModuleType("botocore")
    botocore_exceptions_module = types.ModuleType("botocore.exceptions")

    class TypeDeserializer:
        def deserialize(self, value: dict) -> dict:
            return value

    class TypeSerializer:
        def serialize(self, value: Any) -> dict:
            if value is None:
                return {"NULL": True}
            if isinstance(value, bool):
                return {"BOOL": value}
            if isinstance(value, str):
                return {"S": value}
            if isinstance(value, (int, Decimal)):
                return {"N": str(value)}
            if isinstance(value, list):
                return {"L": [self.serialize(item) for item in value]}
            if isinstance(value, dict):
                return {"M": {key: self.serialize(item) for key, item in value.items()}}
            raise TypeError(f"Unsupported test serializer type: {type(value)!r}")

    class BotoCoreError(Exception):
        pass

    class ClientError(Exception):
        def __init__(self, response: dict | None = None, operation_name: str = "") -> None:
            super().__init__(operation_name)
            self.response = response or {}

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

    boto3_module.dynamodb = dynamodb_module
    dynamodb_module.types = dynamodb_types_module
    dynamodb_types_module.TypeDeserializer = TypeDeserializer
    dynamodb_types_module.TypeSerializer = TypeSerializer
    botocore_module.exceptions = botocore_exceptions_module
    botocore_exceptions_module.BotoCoreError = BotoCoreError
    botocore_exceptions_module.ClientError = ClientError

    sys.modules.setdefault("boto3", boto3_module)
    sys.modules.setdefault("boto3.dynamodb", dynamodb_module)
    sys.modules.setdefault("boto3.dynamodb.types", dynamodb_types_module)
    sys.modules.setdefault("botocore", botocore_module)
    sys.modules.setdefault("botocore.exceptions", botocore_exceptions_module)


install_aws_stubs()

import dynamodb_snapshot_lambda as snapshot_lambda


TABLE_ARN = "arn:aws:dynamodb:us-east-1:111111111111:table/orders"


def build_manager() -> dict:
    return {
        "config": {
            "run_id": "20260309T000000Z",
            "mode": "full",
            "dry_run": False,
            "wait_for_completion": False,
            "max_workers": 1,
            "checkpoint_key": "snapshots/_checkpoint.json",
            "permission_precheck_enabled": False,
        },
        "checkpoint_store": {"bucket": "checkpoint-bucket", "key": "snapshots/_checkpoint.json"},
    }


def build_result(status: str) -> dict:
    return {
        "table_name": "orders",
        "table_arn": TABLE_ARN,
        "mode": "FULL",
        "status": status,
        "source": "native",
        "checkpoint_to": "2026-03-09T00:00:00Z",
    }


def build_export_manager(
    mode: str = "full",
    *,
    bucket_owner: str | None = None,
    run_time: datetime | None = None,
    s3_prefix: str = "dynamodb-snapshots",
) -> dict:
    resolved_run_time = run_time or datetime(2026, 3, 9, tzinfo=timezone.utc)
    return {
        "config": {
            "run_id": "20260309T000000Z",
            "mode": mode,
            "dry_run": False,
            "wait_for_completion": False,
            "bucket": "snapshot-bucket",
            "bucket_owner": bucket_owner,
            "s3_prefix": s3_prefix,
            "run_time": resolved_run_time,
        }
    }


class FakeDynamoDBClient:
    def __init__(self, pitr_statuses: list[str]) -> None:
        self.pitr_statuses = pitr_statuses
        self.describe_calls: list[str] = []
        self.update_calls: list[dict] = []
        self.export_calls: list[dict] = []
        self.operations: list[str] = []
        self._describe_index = 0

    def describe_continuous_backups(self, TableName: str) -> dict:
        self.describe_calls.append(TableName)
        self.operations.append("describe")
        status = self.pitr_statuses[min(self._describe_index, len(self.pitr_statuses) - 1)]
        self._describe_index += 1
        continuous_backups_status = "ENABLED" if status != "DISABLED" else "DISABLED"
        return {
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": continuous_backups_status,
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": status,
                },
            }
        }

    def update_continuous_backups(
        self,
        *,
        TableName: str,
        PointInTimeRecoverySpecification: dict,
    ) -> dict:
        self.update_calls.append(
            {
                "TableName": TableName,
                "PointInTimeRecoverySpecification": PointInTimeRecoverySpecification,
            }
        )
        self.operations.append("update")
        return {
            "ContinuousBackupsDescription": {
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": "ENABLING",
                }
            }
        }

    def export_table_to_point_in_time(self, **params: dict) -> dict:
        self.export_calls.append(params)
        self.operations.append("export")
        return {
            "ExportDescription": {
                "ExportArn": f"{TABLE_ARN}/export/016",
            }
        }


class FakeSTSClient:
    def __init__(self, arn: str) -> None:
        self.arn = arn
        self.calls = 0

    def get_caller_identity(self) -> dict:
        self.calls += 1
        return {"Arn": self.arn}


class FakeSession:
    def __init__(self, sts_client: FakeSTSClient) -> None:
        self.sts_client = sts_client

    def client(self, service_name: str, region_name: str | None = None) -> Any:
        if service_name != "sts":
            raise AssertionError(f"Unexpected service requested: {service_name}")
        return self.sts_client


class SnapshotCheckpointPersistenceTests(unittest.TestCase):
    def run_snapshot(self, status: str) -> tuple[dict, list[dict]]:
        checkpoint_payloads: list[dict] = []

        def save_checkpoint(_store: dict, payload: dict) -> None:
            checkpoint_payloads.append(payload)

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_prime_assumed_session_from_direct_targets", lambda manager: None)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_resolve_targets", lambda manager: [TABLE_ARN])
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_resolve_ignore", lambda manager: [])
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_prime_assumed_session_from_targets", lambda manager, entries: None)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "checkpoint_load", lambda store: {"version": 1, "tables": {}})
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "checkpoint_save", save_checkpoint)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_snapshot_table",
                    lambda manager, entry, previous_state: build_result(status),
                )
            )

            result = snapshot_lambda.snapshot_manager_run(build_manager(), {})

        return result, checkpoint_payloads

    def test_started_export_does_not_advance_checkpoint(self) -> None:
        result, checkpoint_payloads = self.run_snapshot("STARTED")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(checkpoint_payloads), 1)
        self.assertEqual(checkpoint_payloads[0]["tables"], {})

    def test_completed_export_advances_checkpoint(self) -> None:
        result, checkpoint_payloads = self.run_snapshot("COMPLETED")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(checkpoint_payloads), 1)
        self.assertEqual(
            checkpoint_payloads[0]["tables"][TABLE_ARN],
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-09T00:00:00Z",
                "last_mode": "FULL",
                "source": "native",
            },
        )

    def test_checkpoint_state_payload_is_persisted_when_returned(self) -> None:
        checkpoint_payloads: list[dict] = []

        def save_checkpoint(_store: dict, payload: dict) -> None:
            checkpoint_payloads.append(payload)

        result_payload = {
            "table_name": "orders",
            "table_arn": TABLE_ARN,
            "mode": "INCREMENTAL",
            "status": "PENDING",
            "source": "pending_export_tracking",
            "checkpoint_state": {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-09T00:00:00Z",
                "last_mode": "FULL",
                "source": "native",
                "pending_exports": [
                    {
                        "export_arn": f"{TABLE_ARN}/export/016",
                        "checkpoint_to": "2026-03-10T00:00:00Z",
                        "mode": "INCREMENTAL",
                        "source": "native",
                    }
                ],
            },
        }

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_prime_assumed_session_from_direct_targets", lambda manager: None)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_resolve_targets", lambda manager: [TABLE_ARN])
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_resolve_ignore", lambda manager: [])
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_prime_assumed_session_from_targets", lambda manager, entries: None)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "checkpoint_load", lambda store: {"version": 1, "tables": {}})
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "checkpoint_save", save_checkpoint)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_snapshot_table",
                    lambda manager, entry, previous_state: result_payload,
                )
            )

            result = snapshot_lambda.snapshot_manager_run(build_manager(), {})

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(checkpoint_payloads), 1)
        self.assertEqual(
            checkpoint_payloads[0]["tables"][TABLE_ARN],
            result_payload["checkpoint_state"],
        )


class SnapshotPendingExportTrackingTests(unittest.TestCase):
    def test_started_incremental_is_tracked_as_pending_export(self) -> None:
        checkpoint_state = snapshot_lambda.snapshot_manager_build_checkpoint_state(
            {"table_name": "orders", "table_arn": TABLE_ARN},
            {},
        )
        result = {
            "table_name": "orders",
            "table_arn": TABLE_ARN,
            "mode": "INCREMENTAL",
            "status": "STARTED",
            "source": "native",
            "export_arn": f"{TABLE_ARN}/export/016",
            "checkpoint_from": "2026-03-09T00:00:00Z",
            "checkpoint_to": "2026-03-10T00:00:00Z",
        }

        updated = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager("incremental"),
            checkpoint_state,
            result,
        )

        self.assertNotIn("last_to", updated)
        self.assertEqual(len(updated["pending_exports"]), 1)
        self.assertEqual(updated["pending_exports"][0]["export_arn"], f"{TABLE_ARN}/export/016")
        self.assertEqual(updated["pending_exports"][0]["checkpoint_to"], "2026-03-10T00:00:00Z")

    def test_reconcile_completed_pending_promotes_checkpoint(self) -> None:
        class FakeDescribeExportClient:
            def describe_export(self, *, ExportArn: str) -> dict:
                return {
                    "ExportDescription": {
                        "ExportArn": ExportArn,
                        "ExportStatus": "COMPLETED",
                    }
                }

        previous_state = {
            "table_name": "orders",
            "table_arn": TABLE_ARN,
            "pending_exports": [
                {
                    "export_arn": f"{TABLE_ARN}/export/016",
                    "checkpoint_to": "2026-03-10T00:00:00Z",
                    "mode": "INCREMENTAL",
                    "source": "native",
                }
            ],
        }

        reconciled = snapshot_lambda.snapshot_manager_reconcile_pending_exports(
            previous_state,
            "orders",
            TABLE_ARN,
            ddb_client=FakeDescribeExportClient(),
        )

        self.assertEqual(reconciled["last_to"], "2026-03-10T00:00:00Z")
        self.assertEqual(reconciled["last_mode"], "INCREMENTAL")
        self.assertEqual(reconciled["source"], "native")
        self.assertNotIn("pending_exports", reconciled)

    def test_snapshot_table_blocks_new_export_when_pending_is_running(self) -> None:
        class FakeDescribeExportClient:
            def __init__(self) -> None:
                self.describe_calls: list[str] = []

            def describe_export(self, *, ExportArn: str) -> dict:
                self.describe_calls.append(ExportArn)
                return {
                    "ExportDescription": {
                        "ExportArn": ExportArn,
                        "ExportStatus": "IN_PROGRESS",
                    }
                }

        ddb_client = FakeDescribeExportClient()
        manager = build_export_manager("incremental")
        previous_state = {
            "table_name": "orders",
            "table_arn": TABLE_ARN,
            "pending_exports": [
                {
                    "export_arn": f"{TABLE_ARN}/export/016",
                    "checkpoint_to": "2026-03-10T00:00:00Z",
                    "mode": "INCREMENTAL",
                    "source": "native",
                }
            ],
        }

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_resolve_execution_context", lambda manager_obj, table_name, table_arn: {
                    "ddb": ddb_client,
                    "s3": object(),
                    "session_mode": "shared_session_by_table_region",
                    "assume_role_arn": None,
                    "table_account_id": "111111111111",
                    "table_region": "us-east-1",
                })
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "snapshot_manager_validate_export_session_account", lambda manager_obj, table_name, table_arn, context: None)
            )

            result = snapshot_lambda.snapshot_manager_snapshot_table(
                manager,
                {"table_name": "orders", "table_arn": TABLE_ARN},
                previous_state,
            )

        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["source"], "pending_export_tracking")
        self.assertEqual(len(result["pending_exports"]), 1)
        self.assertEqual(ddb_client.describe_calls, [f"{TABLE_ARN}/export/016"])


class SnapshotErrorGuidanceTests(unittest.TestCase):
    def test_lambda_handler_returns_friendly_config_error(self) -> None:
        result = snapshot_lambda.lambda_handler({}, None)

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_type"], "config")
        self.assertEqual(
            result["user_message"],
            "Nenhuma tabela alvo foi informada para a execução.",
        )
        self.assertIn("TARGET_TABLE_ARNS", result["resolution"])

    def test_table_error_result_exposes_account_mismatch_guidance(self) -> None:
        error = RuntimeError(
            "TableArn arn:aws:dynamodb:us-east-1:111111111111:table/orders pertence à conta 111111111111, "
            "mas caller=222222222222. A operação ExportTableToPointInTime exige sessão da conta dona da tabela."
        )

        result = snapshot_lambda._build_table_error_result(
            table_name="orders",
            table_arn=TABLE_ARN,
            mode="FULL",
            error=error,
            dry_run=False,
        )

        self.assertEqual(
            result["user_message"],
            "A sessão AWS atual não pertence à mesma conta da tabela alvo.",
        )
        self.assertIn("ASSUME_ROLE", result["resolution"])
        self.assertEqual(result["error_detail"], str(error))

    def test_timeout_error_guidance_mentions_wait_configuration(self) -> None:
        result = snapshot_lambda._build_error_response_fields(
            TimeoutError("Timeout aguardando export arn:aws:dynamodb:us-east-1:111111111111:table/orders/export/016"),
        )

        self.assertEqual(
            result["user_message"],
            "O export demorou mais do que o limite de espera configurado nesta execução.",
        )
        self.assertIn("WAIT_FOR_COMPLETION=false", result["resolution"])


class SnapshotOutputTests(unittest.TestCase):
    def test_build_snapshot_config_reads_output_cloudwatch_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "OUTPUT_CLOUDWATCH_ENABLED": "true",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertTrue(config["output_cloudwatch_enabled"])

    def test_build_snapshot_config_reads_output_dynamodb_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "OUTPUT_DYNAMODB_ENABLED": "true",
                "OUTPUT_DYNAMODB_TABLE": "snapshot-output",
                "OUTPUT_DYNAMODB_REGION": "sa-east-1",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertTrue(config["output_dynamodb_enabled"])
        self.assertEqual(config["output_dynamodb_table"], "snapshot-output")
        self.assertEqual(config["output_dynamodb_region"], "sa-east-1")

    def test_build_snapshot_config_ignores_legacy_local_cloudwatch_destination_settings(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "OUTPUT_CLOUDWATCH_ENABLED": "true",
                "OUTPUT_CLOUDWATCH_LOG_GROUP": "/custom/dynamodb-snapshot/legacy",
                "OUTPUT_CLOUDWATCH_LOG_STREAM_PREFIX": "terminal",
                "OUTPUT_CLOUDWATCH_REGION": "sa-east-1",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertNotIn("output_cloudwatch_log_group", config)
        self.assertNotIn("output_cloudwatch_log_stream_prefix", config)
        self.assertNotIn("output_cloudwatch_region", config)

    def test_build_snapshot_config_prioritizes_runtime_fields_from_env_over_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "S3_PREFIX": "env-prefix",
                "CHECKPOINT_BUCKET": "env-checkpoint-bucket",
                "CHECKPOINT_KEY": "env-prefix/_checkpoint.json",
                "OUTPUT_CLOUDWATCH_ENABLED": "false",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {
                    "s3_prefix": "payload-prefix",
                    "checkpoint_bucket": "payload-checkpoint-bucket",
                    "checkpoint_key": "payload-prefix/checkpoint.json",
                    "output_cloudwatch_enabled": True,
                }
            )

        self.assertEqual(config["s3_prefix"], "env-prefix")
        self.assertEqual(config["checkpoint_bucket"], "env-checkpoint-bucket")
        self.assertEqual(config["checkpoint_key"], "env-prefix/_checkpoint.json")
        self.assertFalse(config["output_cloudwatch_enabled"])

    def test_build_snapshot_config_prioritizes_output_dynamodb_settings_from_env_over_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "OUTPUT_DYNAMODB_ENABLED": "true",
                "OUTPUT_DYNAMODB_TABLE": "env-output-table",
                "OUTPUT_DYNAMODB_REGION": "sa-east-1",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {
                    "output_dynamodb_enabled": False,
                    "output_dynamodb_table": "event-output-table",
                    "output_dynamodb_region": "us-east-1",
                }
            )

        self.assertTrue(config["output_dynamodb_enabled"])
        self.assertEqual(config["output_dynamodb_table"], "env-output-table")
        self.assertEqual(config["output_dynamodb_region"], "sa-east-1")

    def test_build_snapshot_config_uses_runtime_fields_from_event_when_env_is_absent(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {
                    "s3_prefix": "payload-prefix",
                    "checkpoint_bucket": "payload-checkpoint-bucket",
                    "checkpoint_key": "payload-prefix/checkpoint.json",
                    "output_cloudwatch_enabled": True,
                }
            )

        self.assertEqual(config["s3_prefix"], "payload-prefix")
        self.assertEqual(config["checkpoint_bucket"], "payload-checkpoint-bucket")
        self.assertEqual(config["checkpoint_key"], "payload-prefix/checkpoint.json")
        self.assertTrue(config["output_cloudwatch_enabled"])

    def test_build_snapshot_config_uses_output_dynamodb_settings_from_event_when_env_is_absent(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {
                    "output_dynamodb_enabled": True,
                    "output_dynamodb_table": "payload-output-table",
                    "output_dynamodb_region": "us-east-1",
                }
            )

        self.assertTrue(config["output_dynamodb_enabled"])
        self.assertEqual(config["output_dynamodb_table"], "payload-output-table")
        self.assertEqual(config["output_dynamodb_region"], "us-east-1")

    def test_build_snapshot_config_rejects_output_dynamodb_without_table(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "OUTPUT_DYNAMODB_TABLE deve ser informado quando OUTPUT_DYNAMODB_ENABLED=true",
            ):
                snapshot_lambda.build_snapshot_config({"output_dynamodb_enabled": True})

    def test_build_snapshot_config_reads_checkpoint_dynamodb_table_arn_from_env(self) -> None:
        checkpoint_table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints"

        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "CHECKPOINT_DYNAMODB_TABLE_ARN": checkpoint_table_arn,
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertEqual(
            config["checkpoint_dynamodb_table_arn"],
            checkpoint_table_arn,
        )

    def test_build_snapshot_config_uses_checkpoint_dynamodb_table_arn_from_event_when_env_is_absent(self) -> None:
        checkpoint_table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints"

        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {
                    "checkpoint_dynamodb_table_arn": checkpoint_table_arn,
                }
            )

        self.assertEqual(
            config["checkpoint_dynamodb_table_arn"],
            checkpoint_table_arn,
        )

    def test_build_checkpoint_store_for_session_creates_checkpoint_dynamodb_table_when_missing(self) -> None:
        checkpoint_table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints"
        create_calls: list[dict[str, Any]] = []
        fake_session = object()

        class FakeDynamoCheckpointClient:
            def __init__(self) -> None:
                self.describe_attempts = 0

            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                self.describe_attempts += 1
                if self.describe_attempts == 1:
                    raise snapshot_lambda.ClientError(
                        {
                            "Error": {
                                "Code": "ResourceNotFoundException",
                                "Message": "Requested resource not found",
                            }
                        },
                        "DescribeTable",
                    )
                if self.describe_attempts == 2:
                    return {"Table": {"TableStatus": "CREATING"}}
                return {
                    "Table": {
                        "TableStatus": "ACTIVE",
                        "AttributeDefinitions": [
                            {"AttributeName": "TableName", "AttributeType": "S"},
                            {"AttributeName": "RecordType", "AttributeType": "S"},
                        ],
                        "KeySchema": [
                            {"AttributeName": "TableName", "KeyType": "HASH"},
                            {"AttributeName": "RecordType", "KeyType": "RANGE"},
                        ],
                    }
                }

            def create_table(self, **kwargs: Any) -> None:
                create_calls.append(kwargs)

        checkpoint_client = FakeDynamoCheckpointClient()

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return checkpoint_client

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "_get_session_client", resolve_client)
            )
            stack.enter_context(
                patch.object(snapshot_lambda.time, "sleep", lambda _seconds: None)
            )
            store = snapshot_lambda.build_checkpoint_store_for_session(
                fake_session,
                {
                    "checkpoint_key": "snapshots/_checkpoint.json",
                    "checkpoint_dynamodb_table_arn": checkpoint_table_arn,
                },
            )

        self.assertEqual(store["backend"], "dynamodb")
        self.assertEqual(store["table_name"], "snapshot-checkpoints")
        self.assertEqual(len(create_calls), 1)
        self.assertEqual(create_calls[0]["TableName"], "snapshot-checkpoints")
        self.assertEqual(create_calls[0]["BillingMode"], "PAY_PER_REQUEST")
        self.assertEqual(
            create_calls[0]["AttributeDefinitions"],
            [
                {"AttributeName": "TableName", "AttributeType": "S"},
                {"AttributeName": "RecordType", "AttributeType": "S"},
            ],
        )
        self.assertEqual(
            create_calls[0]["KeySchema"],
            [
                {"AttributeName": "TableName", "KeyType": "HASH"},
                {"AttributeName": "RecordType", "KeyType": "RANGE"},
            ],
        )
        self.assertGreaterEqual(checkpoint_client.describe_attempts, 3)

    def test_build_checkpoint_store_for_session_rejects_invalid_checkpoint_dynamodb_schema(self) -> None:
        checkpoint_table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints"
        fake_session = object()

        class FakeDynamoCheckpointClient:
            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                return {
                    "Table": {
                        "TableStatus": "ACTIVE",
                        "AttributeDefinitions": [
                            {"AttributeName": "TableName", "AttributeType": "S"},
                        ],
                        "KeySchema": [
                            {"AttributeName": "TableName", "KeyType": "HASH"},
                        ],
                    }
                }

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return FakeDynamoCheckpointClient()

        with patch.object(snapshot_lambda, "_get_session_client", resolve_client):
            with self.assertRaisesRegex(
                RuntimeError,
                "PK=TableName e SK=RecordType",
            ):
                snapshot_lambda.build_checkpoint_store_for_session(
                    fake_session,
                    {
                        "checkpoint_key": "snapshots/_checkpoint.json",
                        "checkpoint_dynamodb_table_arn": checkpoint_table_arn,
                    },
                )

    def test_build_snapshot_config_accepts_assume_role_as_official_event_key(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {"assume_role": "arn:aws:iam::111111111111:role/snapshot"}
            )

        self.assertEqual(config["assume_role"], "arn:aws:iam::111111111111:role/snapshot")
        self.assertEqual(config["assume_role_arn"], "arn:aws:iam::111111111111:role/snapshot")

    def test_build_snapshot_config_prioritizes_assume_role_from_env_over_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "ASSUME_ROLE": "arn:aws:iam::222222222222:role/from-env",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config(
                {"assume_role": "arn:aws:iam::111111111111:role/from-event"}
            )

        self.assertEqual(config["assume_role"], "arn:aws:iam::222222222222:role/from-env")
        self.assertEqual(config["assume_role_arn"], "arn:aws:iam::222222222222:role/from-env")

    def test_build_snapshot_config_accepts_snapshot_bucket_from_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"snapshot_bucket": "snapshot-bucket"})

        self.assertEqual(config["bucket"], "snapshot-bucket")

    def test_build_snapshot_config_prioritizes_snapshot_bucket_from_env_over_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket-from-env",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"snapshot_bucket": "snapshot-bucket-from-event"})

        self.assertEqual(config["bucket"], "snapshot-bucket-from-env")

    def test_build_snapshot_config_prioritizes_targets_from_env_over_event(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders-from-env",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"targets": ["orders-from-event"]})

        self.assertEqual(config["targets"], ["orders-from-env"])

    def test_build_snapshot_config_reads_bucket_owner_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "S3_BUCKET_OWNER": "222222222222",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertEqual(config["bucket_owner"], "222222222222")

    def test_build_snapshot_config_uses_payload_bucket_owner_when_env_is_absent(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"bucket_owner": "333333333333"})

        self.assertEqual(config["bucket_owner"], "333333333333")

    def test_build_snapshot_config_prioritizes_env_bucket_owner_over_payload(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "S3_BUCKET_OWNER": "222222222222",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"bucket_owner": "333333333333"})

        self.assertEqual(config["bucket_owner"], "222222222222")

    def test_build_snapshot_config_rejects_invalid_bucket_owner(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "S3_BUCKET_OWNER": "invalid-owner",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "S3_BUCKET_OWNER deve ser um account id AWS de 12 dígitos"):
                snapshot_lambda.build_snapshot_config({})

    def test_build_run_response_adds_snapshot_bucket_and_normalizes_assume_role(self) -> None:
        manager = {
            "config": {
                "run_id": "20260309T000000Z",
                "mode": "incremental",
                "checkpoint_key": "snapshots/_checkpoint.json",
                "bucket": "snapshot-bucket",
            }
        }

        response = snapshot_lambda.snapshot_manager_build_run_response(
            manager,
            [
                {
                    "table_name": "orders",
                    "table_arn": TABLE_ARN,
                    "mode": "INCREMENTAL",
                    "status": "STARTED",
                    "export_arn": f"{TABLE_ARN}/export/016",
                    "assume_role_arn": "arn:aws:iam::111111111111:role/snapshot",
                }
            ],
            None,
            {},
        )

        self.assertEqual(response["results"][0]["snapshot_bucket"], "snapshot-bucket")
        self.assertEqual(
            response["results"][0]["assume_role"],
            "arn:aws:iam::111111111111:role/snapshot",
        )
        self.assertEqual(
            response["results"][0]["export_arn"],
            f"{TABLE_ARN}/export/016",
        )
        self.assertEqual(response["results"][0]["export_job_id"], "016")
        self.assertNotIn("assume_role_arn", response["results"][0])

    def test_lambda_handler_adds_snapshot_bucket_to_success_payload(self) -> None:
        config = {"bucket": "snapshot-bucket", "output_cloudwatch_enabled": False}

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "create_snapshot_manager", lambda resolved_config: {"config": resolved_config})
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "results": [],
                    },
                )
            )

            result = snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(result["snapshot_bucket"], "snapshot-bucket")

    def test_lambda_handler_emits_output_to_cloudwatch_when_enabled(self) -> None:
        config = {"bucket": "snapshot-bucket", "output_cloudwatch_enabled": True}
        logged_events: list[dict] = []

        def capture_log_event(action: str, *, level: int = 20, **fields: Any) -> None:
            logged_events.append({"action": action, "level": level, "fields": fields})

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "create_snapshot_manager", lambda resolved_config: {"config": resolved_config})
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "results": [],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_log_event", capture_log_event)
            )

            snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        output_events = [item for item in logged_events if item["action"] == "output.cloudwatch"]
        self.assertEqual(len(output_events), 1)
        self.assertEqual(output_events[0]["fields"]["source"], "lambda_handler.success")
        self.assertEqual(output_events[0]["fields"]["snapshot_bucket"], "snapshot-bucket")
        self.assertEqual(
            output_events[0]["fields"]["output"]["snapshot_bucket"],
            "snapshot-bucket",
        )

    def test_lambda_handler_emits_output_to_dynamodb_when_enabled(self) -> None:
        config = {
            "bucket": "snapshot-bucket",
            "output_cloudwatch_enabled": False,
            "output_dynamodb_enabled": True,
            "output_dynamodb_table": "snapshot-output",
            "output_dynamodb_region": "sa-east-1",
        }
        put_calls: list[dict[str, Any]] = []
        create_calls: list[dict[str, Any]] = []
        fake_session = object()

        class FakeDynamoOutputClient:
            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                return {"Table": {"TableStatus": "ACTIVE"}}

            def create_table(self, **kwargs: Any) -> None:
                create_calls.append(kwargs)

            def put_item(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        output_client = FakeDynamoOutputClient()

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return output_client

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "create_snapshot_manager",
                    lambda resolved_config: {
                        "config": resolved_config,
                        "session": fake_session,
                        "_assume_session": fake_session,
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "COMPLETED",
                                "source": "native",
                                "export_arn": f"{TABLE_ARN}/export/016",
                                "s3_prefix": "DDB/20260309/111111111111/orders/INCR",
                                "checkpoint_to": "2026-03-09T00:00:00Z",
                                "export_job_id": "016",
                                "started_at": "2026-03-09T00:00:00Z",
                                "assume_role_arn": "arn:aws:iam::111111111111:role/snapshot",
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_get_session_client", resolve_client)
            )

            result = snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(create_calls, [])
        self.assertEqual(len(put_calls), 1)
        self.assertEqual(put_calls[0]["TableName"], "snapshot-output")
        item = put_calls[0]["Item"]
        self.assertEqual(
            item["Export ARN"]["S"],
            f"{TABLE_ARN}/export/016",
        )
        self.assertEqual(
            item["Table name"]["S"],
            "orders",
        )
        self.assertEqual(
            item["Destination S3 Bucket"]["S"],
            "s3://snapshot-bucket/DDB/20260309/111111111111/orders/INCR",
        )
        self.assertEqual(
            item["Status"]["S"],
            "Completed",
        )
        self.assertEqual(
            item["Export job start time (utc-03:00)"]["S"],
            "2026-03-08T21:00:00-03:00",
        )
        self.assertEqual(
            item["Export Type"]["S"],
            "Incremental export",
        )
        self.assertEqual(
            set(item.keys()),
            {
                "Export ARN",
                "Table name",
                "Destination S3 Bucket",
                "Status",
                "Export job start time (utc-03:00)",
                "Export Type",
            },
        )

    def test_lambda_handler_uses_output_session_to_emit_dynamodb_output(self) -> None:
        config = {
            "bucket": "snapshot-bucket",
            "output_cloudwatch_enabled": False,
            "output_dynamodb_enabled": True,
            "output_dynamodb_table": "snapshot-output",
            "output_dynamodb_region": "sa-east-1",
        }
        put_calls: list[dict[str, Any]] = []
        fake_output_session = object()
        fake_assume_session = object()

        class FakeDynamoOutputClient:
            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                return {"Table": {"TableStatus": "ACTIVE"}}

            def create_table(self, **kwargs: Any) -> None:
                raise AssertionError("create_table não deveria ser chamado")

            def put_item(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        output_client = FakeDynamoOutputClient()

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_output_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return output_client

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "create_snapshot_manager",
                    lambda resolved_config: {
                        "config": resolved_config,
                        "session": fake_output_session,
                        "_assume_session": fake_assume_session,
                        "_output_session": fake_output_session,
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "COMPLETED",
                                "source": "native",
                                "export_arn": f"{TABLE_ARN}/export/016",
                                "s3_prefix": "DDB/20260309/111111111111/orders/INCR",
                                "checkpoint_to": "2026-03-09T00:00:00Z",
                                "export_job_id": "016",
                                "started_at": "2026-03-09T00:00:00Z",
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_get_session_client", resolve_client)
            )

            result = snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(len(put_calls), 1)
        self.assertEqual(put_calls[0]["TableName"], "snapshot-output")

    def test_lambda_handler_ignores_non_whitelisted_table_fields_in_dynamodb_item(self) -> None:
        config = {
            "bucket": "snapshot-bucket",
            "output_cloudwatch_enabled": False,
            "output_dynamodb_enabled": True,
            "output_dynamodb_table": "snapshot-output",
            "output_dynamodb_region": "sa-east-1",
        }
        put_calls: list[dict[str, Any]] = []
        fake_session = object()

        class FakeDynamoOutputClient:
            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                return {"Table": {"TableStatus": "ACTIVE"}}

            def create_table(self, **kwargs: Any) -> None:
                raise AssertionError("create_table não deveria ser chamado")

            def put_item(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        output_client = FakeDynamoOutputClient()

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return output_client

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "create_snapshot_manager",
                    lambda resolved_config: {
                        "config": resolved_config,
                        "session": fake_session,
                        "_assume_session": fake_session,
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "STARTED",
                                "source": "pending_export_tracking",
                                "s3_prefix": "DDB/20260309/111111111111/orders/INCR1",
                                "checkpoint_from": "2026-03-09T00:00:00Z",
                                "checkpoint_to": "2026-03-10T00:00:00Z",
                                "pending_exports": [
                                    {
                                        "export_arn": f"{TABLE_ARN}/export/016",
                                        "checkpoint_from": "2026-03-09T00:00:00Z",
                                        "checkpoint_to": "2026-03-10T00:00:00Z",
                                        "mode": "INCREMENTAL",
                                        "source": "native",
                                        "started_at": "2026-03-10T00:00:00Z",
                                    }
                                ],
                                "checkpoint_state": {
                                    "table_name": "orders",
                                    "last_to": "2026-03-10T00:00:00Z",
                                    "pending_exports": [
                                        {
                                            "export_arn": f"{TABLE_ARN}/export/016",
                                            "checkpoint_to": "2026-03-10T00:00:00Z",
                                            "mode": "INCREMENTAL",
                                            "source": "native",
                                            "started_at": "2026-03-10T00:00:00Z",
                                        }
                                    ],
                                },
                                "manifest": {
                                    "files": ["part-0001.jsonl.gz"],
                                    "total_items": 10,
                                    "total_parts": 1,
                                },
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_get_session_client", resolve_client)
            )

            result = snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(len(put_calls), 1)
        table_item = put_calls[0]["Item"]
        self.assertEqual(
            table_item["Export ARN"]["S"],
            f"{TABLE_ARN}/export/016",
        )
        self.assertEqual(
            table_item["Table name"]["S"],
            "orders",
        )
        self.assertEqual(
            table_item["Destination S3 Bucket"]["S"],
            "s3://snapshot-bucket/DDB/20260309/111111111111/orders/INCR1",
        )
        self.assertEqual(
            table_item["Status"]["S"],
            "In progress",
        )
        self.assertEqual(
            table_item["Export job start time (utc-03:00)"]["S"],
            "2026-03-09T21:00:00-03:00",
        )
        self.assertEqual(
            table_item["Export Type"]["S"],
            "Incremental export",
        )
        self.assertEqual(
            set(table_item.keys()),
            {
                "Export ARN",
                "Table name",
                "Destination S3 Bucket",
                "Status",
                "Export job start time (utc-03:00)",
                "Export Type",
            },
        )

    def test_lambda_handler_creates_output_dynamodb_table_when_missing(self) -> None:
        config = {
            "bucket": "snapshot-bucket",
            "output_cloudwatch_enabled": False,
            "output_dynamodb_enabled": True,
            "output_dynamodb_table": "snapshot-output",
            "output_dynamodb_region": "sa-east-1",
        }
        put_calls: list[dict[str, Any]] = []
        create_calls: list[dict[str, Any]] = []
        fake_session = object()

        class FakeDynamoOutputClient:
            def __init__(self) -> None:
                self.describe_attempts = 0

            def describe_table(self, **kwargs: Any) -> dict[str, Any]:
                self.describe_attempts += 1
                if self.describe_attempts == 1:
                    raise snapshot_lambda.ClientError(
                        {
                            "Error": {
                                "Code": "ResourceNotFoundException",
                                "Message": "Requested resource not found",
                            }
                        },
                        "DescribeTable",
                    )
                if self.describe_attempts == 2:
                    return {"Table": {"TableStatus": "CREATING"}}
                return {"Table": {"TableStatus": "ACTIVE"}}

            def create_table(self, **kwargs: Any) -> None:
                create_calls.append(kwargs)

            def put_item(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        output_client = FakeDynamoOutputClient()

        def resolve_client(session: Any, service_name: str, *, region_name: str | None = None) -> Any:
            self.assertIs(session, fake_session)
            self.assertEqual(service_name, "dynamodb")
            self.assertEqual(region_name, "sa-east-1")
            return output_client

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "create_snapshot_manager",
                    lambda resolved_config: {
                        "config": resolved_config,
                        "session": fake_session,
                        "_assume_session": fake_session,
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "COMPLETED",
                                "source": "native",
                                "export_arn": f"{TABLE_ARN}/export/016",
                                "started_at": "2026-03-09T00:00:00Z",
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_get_session_client", resolve_client)
            )
            stack.enter_context(
                patch.object(snapshot_lambda.time, "sleep", lambda _seconds: None)
            )

            result = snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        self.assertTrue(result["ok"])
        self.assertEqual(len(create_calls), 1)
        self.assertEqual(create_calls[0]["TableName"], "snapshot-output")
        self.assertEqual(create_calls[0]["BillingMode"], "PAY_PER_REQUEST")
        self.assertEqual(
            create_calls[0]["AttributeDefinitions"],
            [
                {"AttributeName": "Export ARN", "AttributeType": "S"},
            ],
        )
        self.assertEqual(
            create_calls[0]["KeySchema"],
            [
                {"AttributeName": "Export ARN", "KeyType": "HASH"},
            ],
        )
        self.assertEqual(len(put_calls), 1)
        self.assertGreaterEqual(output_client.describe_attempts, 3)

    def test_resolve_execution_context_without_arn_reuses_manager_clients(self) -> None:
        ddb_client = object()
        s3_client = object()
        manager = {
            "session": object(),
            "default_region": "us-east-1",
            "ddb": ddb_client,
            "s3": s3_client,
            "config": {
                "assume_role": None,
                "assume_role_arn": None,
            },
        }

        _, context = snapshot_lambda.snapshot_manager_resolve_execution_context(
            manager,
            "orders",
            "orders",
        )

        self.assertIs(context["ddb"], ddb_client)
        self.assertIs(context["s3"], s3_client)
        self.assertEqual(context["session_mode"], "shared_session_without_arn")
        self.assertEqual(context["table_region"], "us-east-1")

    def test_snapshot_manager_load_csv_source_uses_manager_s3_client(self) -> None:
        expected_payload = "orders\npayments\n"

        class FakeBody:
            def read(self) -> bytes:
                return expected_payload.encode("utf-8")

        class FakeS3Client:
            def __init__(self) -> None:
                self.calls: list[dict[str, str]] = []

            def get_object(self, *, Bucket: str, Key: str) -> dict:
                self.calls.append({"Bucket": Bucket, "Key": Key})
                return {"Body": FakeBody()}

        s3_client = FakeS3Client()
        manager = {
            "session": object(),
            "s3": s3_client,
        }

        payload = snapshot_lambda.snapshot_manager_load_csv_source(
            manager,
            "s3://snapshot-bucket/targets.csv",
            source_name="targets_csv",
        )

        self.assertEqual(payload, expected_payload)
        self.assertEqual(
            s3_client.calls,
            [{"Bucket": "snapshot-bucket", "Key": "targets.csv"}],
        )

    def test_lambda_handler_emits_one_cloudwatch_event_per_table(self) -> None:
        config = {"bucket": "snapshot-bucket", "output_cloudwatch_enabled": True}
        logged_events: list[dict] = []

        def capture_log_event(action: str, *, level: int = 20, **fields: Any) -> None:
            logged_events.append({"action": action, "level": level, "fields": fields})

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "create_snapshot_manager", lambda resolved_config: {"config": resolved_config})
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "STARTED",
                                "source": "native",
                                "export_arn": f"{TABLE_ARN}/export/016",
                                "export_job_id": "016",
                                "checkpoint_from": "2026-03-09T00:00:00Z",
                                "checkpoint_to": "2026-03-10T00:00:00Z",
                                "s3_prefix": "DDB/20260309/111111111111/orders/INCR1",
                                "assume_role_arn": "arn:aws:iam::111111111111:role/snapshot",
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_log_event", capture_log_event)
            )

            snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        table_events = [item for item in logged_events if item["action"] == "output.cloudwatch.table"]
        self.assertEqual(len(table_events), 1)
        self.assertEqual(table_events[0]["fields"]["snapshot_bucket"], "snapshot-bucket")
        self.assertEqual(table_events[0]["fields"]["table_name"], "orders")
        self.assertEqual(table_events[0]["fields"]["table_status"], "STARTED")
        self.assertEqual(table_events[0]["fields"]["checkpoint_to"], "2026-03-10T00:00:00Z")
        self.assertEqual(table_events[0]["fields"]["export_job_id"], "016")
        self.assertEqual(
            table_events[0]["fields"]["assume_role"],
            "arn:aws:iam::111111111111:role/snapshot",
        )
        self.assertEqual(
            table_events[0]["fields"]["tracking_export_job_ids"],
            ["016"],
        )
        self.assertNotIn("export_arn", table_events[0]["fields"])

    def test_lambda_handler_emits_pending_tracking_metadata_in_table_event(self) -> None:
        config = {"bucket": "snapshot-bucket", "output_cloudwatch_enabled": True}
        logged_events: list[dict] = []

        def capture_log_event(action: str, *, level: int = 20, **fields: Any) -> None:
            logged_events.append({"action": action, "level": level, "fields": fields})

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "create_snapshot_manager", lambda resolved_config: {"config": resolved_config})
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "mode": "incremental",
                        "dry_run": False,
                        "results": [
                            {
                                "table_name": "orders",
                                "table_arn": TABLE_ARN,
                                "mode": "INCREMENTAL",
                                "status": "PENDING",
                                "source": "pending_export_tracking",
                                "pending_exports": [
                                    {
                                        "export_arn": f"{TABLE_ARN}/export/016",
                                        "checkpoint_from": "2026-03-09T00:00:00Z",
                                        "checkpoint_to": "2026-03-10T00:00:00Z",
                                        "mode": "INCREMENTAL",
                                        "source": "native",
                                        "started_at": "2026-03-10T00:00:00Z",
                                    }
                                ],
                            }
                        ],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_log_event", capture_log_event)
            )

            snapshot_lambda.lambda_handler({"targets": [TABLE_ARN]}, None)

        table_events = [item for item in logged_events if item["action"] == "output.cloudwatch.table"]
        self.assertEqual(len(table_events), 1)
        self.assertEqual(table_events[0]["fields"]["table_status"], "PENDING")
        self.assertEqual(table_events[0]["fields"]["export_job_id"], "016")
        self.assertEqual(table_events[0]["fields"]["checkpoint_from"], "2026-03-09T00:00:00Z")
        self.assertEqual(table_events[0]["fields"]["checkpoint_to"], "2026-03-10T00:00:00Z")
        self.assertEqual(table_events[0]["fields"]["pending_export_count"], 1)
        self.assertEqual(
            table_events[0]["fields"]["tracking_export_job_ids"],
            ["016"],
        )
        self.assertNotIn("export_arn", table_events[0]["fields"])

    def test_build_output_table_events_skips_result_without_table_identity(self) -> None:
        events = snapshot_lambda._build_output_table_events(
            "lambda_handler.success",
            {
                "status": "ok",
                "run_id": "20260309T000000Z",
                "results": [
                    {
                        "status": "STARTED",
                        "mode": "INCREMENTAL",
                    }
                ],
            },
            aws_request_id=None,
            snapshot_bucket="snapshot-bucket",
        )

        self.assertEqual(events, [])

    def test_lambda_handler_skips_cloudwatch_output_when_disabled_explicitly(self) -> None:
        config = {"bucket": "snapshot-bucket", "output_cloudwatch_enabled": True}
        logged_events: list[dict] = []

        def capture_log_event(action: str, *, level: int = 20, **fields: Any) -> None:
            logged_events.append({"action": action, "level": level, "fields": fields})

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(snapshot_lambda, "build_snapshot_config", lambda _event: config)
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "create_snapshot_manager", lambda resolved_config: {"config": resolved_config})
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "snapshot_manager_run",
                    lambda _manager, _event: {
                        "status": "ok",
                        "run_id": "20260309T000000Z",
                        "results": [],
                    },
                )
            )
            stack.enter_context(
                patch.object(snapshot_lambda, "_log_event", capture_log_event)
            )

            snapshot_lambda.lambda_handler(
                {"targets": [TABLE_ARN]},
                None,
                emit_cloudwatch_output=False,
            )

        output_events = [item for item in logged_events if item["action"] == "output.cloudwatch"]
        self.assertEqual(output_events, [])


class SnapshotFunctionalRefactorTests(unittest.TestCase):
    def test_scan_fallback_sends_consistent_filter_expression_params(self) -> None:
        paginate_calls: list[dict[str, Any]] = []
        put_calls: list[dict[str, Any]] = []
        export_from = datetime(2026, 3, 8, tzinfo=timezone.utc)
        export_to = datetime(2026, 3, 9, tzinfo=timezone.utc)

        class FakeScanPaginator:
            def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
                paginate_calls.append(kwargs)
                return [{"Items": []}]

        class FakeDynamoScanClient:
            def get_paginator(self, operation_name: str) -> FakeScanPaginator:
                self.operation_name = operation_name
                return FakeScanPaginator()

        class FakeS3Client:
            def put_object(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        manager = build_export_manager("incremental")
        manager["config"] = {
            **manager["config"],
            "fallback_updated_attr": "_updated_at",
            "fallback_updated_attr_type": "string",
            "fallback_partition_size": 100,
            "fallback_compress": False,
        }

        result = snapshot_lambda.snapshot_manager_scan_to_s3_partitioned(
            manager,
            "orders",
            TABLE_ARN,
            export_from,
            export_to,
            "DDB/20260309/111111111111/orders/INCR1",
            ddb_client=FakeDynamoScanClient(),
            s3_client=FakeS3Client(),
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
        )

        self.assertEqual(
            paginate_calls,
            [
                {
                    "TableName": "orders",
                    "FilterExpression": "#u BETWEEN :from AND :to",
                    "ExpressionAttributeNames": {"#u": "_updated_at"},
                    "ExpressionAttributeValues": {
                        ":from": {"S": "2026-03-08T00:00:00Z"},
                        ":to": {"S": "2026-03-09T00:00:00Z"},
                    },
                }
            ],
        )
        self.assertEqual(result["items_written"], 0)
        self.assertEqual(result["files_written"], 0)
        self.assertEqual(result["pages_scanned"], 1)
        self.assertEqual(len(put_calls), 1)
        self.assertEqual(put_calls[0]["Bucket"], "snapshot-bucket-us-east-1")
        self.assertEqual(
            put_calls[0]["Key"],
            "DDB/20260309/111111111111/orders/INCR1/manifest.json",
        )

    def test_checkpoint_save_does_not_mutate_input_payload(self) -> None:
        class FakePaginator:
            def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
                return [{"Contents": []}]

        class FakeS3Client:
            def __init__(self) -> None:
                self.saved_payload: dict[str, Any] | None = None

            def put_object(self, **kwargs: Any) -> None:
                self.saved_payload = json.loads(kwargs["Body"].decode("utf-8"))

            def get_object(self, **kwargs: Any) -> dict[str, Any]:
                raise snapshot_lambda.ClientError(
                    {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                    "GetObject",
                )

            def get_paginator(self, operation_name: str) -> FakePaginator:
                self.operation_name = operation_name
                return FakePaginator()

        s3_client = FakeS3Client()
        store = {"s3": s3_client, "bucket": "checkpoint-bucket", "key": "snapshots/_checkpoint.json"}
        payload = {
            "version": 1,
            "tables": {
                TABLE_ARN: {
                    "table_name": "orders",
                    "table_arn": TABLE_ARN,
                    "last_to": "2026-03-09T00:00:00Z",
                }
            },
        }
        original_payload = json.loads(json.dumps(payload))

        snapshot_lambda.checkpoint_save(store, payload)

        self.assertEqual(payload, original_payload)
        self.assertIsNotNone(s3_client.saved_payload)
        self.assertIn("updated_at", s3_client.saved_payload)

    def test_checkpoint_save_uses_dynamodb_when_configured(self) -> None:
        put_calls: list[dict[str, Any]] = []

        class FakeDynamoCheckpointClient:
            def get_item(self, **kwargs: Any) -> dict[str, Any]:
                return {}

            def put_item(self, **kwargs: Any) -> None:
                put_calls.append(kwargs)

        payload = {
            "version": 1,
            "tables": {
                TABLE_ARN: {
                    "table_name": "orders",
                    "table_arn": TABLE_ARN,
                    "last_to": "2026-03-09T00:00:00Z",
                }
            },
        }

        snapshot_lambda.checkpoint_save(
            {
                "backend": "dynamodb",
                "ddb": FakeDynamoCheckpointClient(),
                "table_name": "snapshot-checkpoints",
                "key": "snapshots/_checkpoint.json",
            },
            payload,
        )

        self.assertEqual(len(put_calls), 2)
        self.assertEqual({call["TableName"] for call in put_calls}, {"snapshot-checkpoints"})
        current_item = next(
            call["Item"]
            for call in put_calls
            if call["Item"]["RecordType"]["S"] == "CURRENT"
        )
        snapshot_item = next(
            call["Item"]
            for call in put_calls
            if call["Item"]["RecordType"]["S"].startswith("SNAPSHOT#")
        )
        self.assertEqual(
            current_item["TableName"]["S"],
            "orders",
        )
        self.assertEqual(
            current_item["StateKey"]["S"],
            TABLE_ARN,
        )
        self.assertEqual(
            current_item["LastTo"]["S"],
            "2026-03-09T00:00:00Z",
        )
        self.assertEqual(snapshot_item["TableName"]["S"], "orders")
        self.assertTrue(snapshot_item["RecordType"]["S"].startswith("SNAPSHOT#"))
        current_put_call = next(
            call
            for call in put_calls
            if call["Item"]["RecordType"]["S"] == "CURRENT"
        )
        self.assertEqual(
            current_put_call["ExpressionAttributeNames"],
            {
                "#pk": snapshot_lambda.CHECKPOINT_DYNAMODB_PARTITION_KEY,
                "#sk": snapshot_lambda.CHECKPOINT_DYNAMODB_SORT_KEY,
            },
        )
        self.assertNotIn("ExpressionAttributeValues", current_put_call)

    def test_checkpoint_load_reads_dynamodb_payload_when_configured(self) -> None:
        payload = {
            "version": 1,
            "tables": {
                TABLE_ARN: {
                    "table_name": "orders",
                    "table_arn": TABLE_ARN,
                    "last_to": "2026-03-09T00:00:00Z",
                    "last_mode": "FULL",
                    "source": "native",
                }
            },
        }

        class FakeDynamoCheckpointClient:
            def scan(self, **kwargs: Any) -> dict[str, Any]:
                return {
                    "Items": [
                        snapshot_lambda._marshal_dynamodb_item(
                            {
                                "TableName": "orders",
                                "RecordType": "CURRENT",
                                "StateKey": TABLE_ARN,
                                "Revision": 1,
                                "TableArn": TABLE_ARN,
                                "LastTo": "2026-03-09T00:00:00Z",
                                "LastMode": "FULL",
                                "Source": "native",
                            }
                        ),
                        snapshot_lambda._marshal_dynamodb_item(
                            {
                                "TableName": "orders",
                                "RecordType": "SNAPSHOT#2026-03-09T00:00:00Z#abc123",
                                "StateKey": TABLE_ARN,
                                "EventId": "abc123",
                                "ObservedAt": "2026-03-09T00:00:00Z",
                                "TableArn": TABLE_ARN,
                                "LastTo": "2026-03-09T00:00:00Z",
                            }
                        ),
                    ]
                }

        loaded_payload = snapshot_lambda.checkpoint_load(
            {
                "backend": "dynamodb",
                "ddb": FakeDynamoCheckpointClient(),
                "table_name": "snapshot-checkpoints",
                "key": "snapshots/_checkpoint.json",
            }
        )

        self.assertEqual(
            loaded_payload["tables"][TABLE_ARN],
            payload["tables"][TABLE_ARN],
        )
        self.assertIn("updated_at", loaded_payload)

    def test_checkpoint_save_s3_preserves_concurrent_pending_exports_via_history_merge(self) -> None:
        class MemoryBody:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

        class FakePaginator:
            def __init__(self, client: "FakeS3Client") -> None:
                self.client = client

            def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
                contents = [
                    {"Key": key}
                    for (bucket_name, key), _body in sorted(self.client.objects.items())
                    if bucket_name == Bucket and key.startswith(Prefix)
                ]
                return [{"Contents": contents}]

        class FakeS3Client:
            def __init__(self) -> None:
                self.objects: dict[tuple[str, str], bytes] = {}
                self.put_calls: list[dict[str, Any]] = []

            def put_object(self, **kwargs: Any) -> None:
                body = kwargs["Body"]
                self.objects[(kwargs["Bucket"], kwargs["Key"])] = (
                    body if isinstance(body, bytes) else body.encode("utf-8")
                )
                self.put_calls.append(kwargs)

            def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
                return {
                    "Body": MemoryBody(self.objects[(Bucket, Key)]),
                }

            def get_paginator(self, operation_name: str) -> FakePaginator:
                self.operation_name = operation_name
                return FakePaginator(self)

        initial_state = snapshot_lambda.snapshot_manager_build_checkpoint_state(
            {"table_name": "orders", "table_arn": TABLE_ARN},
            {},
        )
        current_state = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager(
                "incremental",
                run_time=datetime(2026, 3, 10, tzinfo=timezone.utc),
            ),
            initial_state,
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/016",
                "checkpoint_to": "2026-03-10T00:00:00Z",
            },
        )
        candidate_state = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager(
                "incremental",
                run_time=datetime(2026, 3, 11, tzinfo=timezone.utc),
            ),
            initial_state,
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/017",
                "checkpoint_to": "2026-03-11T00:00:00Z",
            },
        )

        s3_client = FakeS3Client()
        store = {
            "s3": s3_client,
            "bucket": "checkpoint-bucket",
            "key": "snapshots/_checkpoint.json",
        }
        s3_client.objects[("checkpoint-bucket", "snapshots/_checkpoint.json")] = json.dumps(
            {
                "version": 2,
                "updated_at": "2026-03-10T00:00:00Z",
                "tables": {
                    TABLE_ARN: current_state,
                },
            },
            ensure_ascii=False,
        ).encode("utf-8")

        snapshot_lambda.checkpoint_save(
            store,
            {
                "version": 2,
                "tables": {
                    TABLE_ARN: candidate_state,
                },
            },
        )

        merged_payload = snapshot_lambda.checkpoint_load(store)
        merged_state = merged_payload["tables"][TABLE_ARN]
        pending_export_arns = {
            item["export_arn"]
            for item in merged_state["pending_exports"]
        }

        self.assertEqual(
            pending_export_arns,
            {
                f"{TABLE_ARN}/export/016",
                f"{TABLE_ARN}/export/017",
            },
        )
        self.assertEqual(len(merged_state["history"]), 2)
        history_put_calls = [
            call
            for call in s3_client.put_calls
            if "_checkpoint.history/" in call["Key"]
        ]
        self.assertEqual(len(history_put_calls), 1)

    def test_checkpoint_save_dynamodb_retries_with_history_merge_under_concurrency(self) -> None:
        initial_state = snapshot_lambda.snapshot_manager_build_checkpoint_state(
            {"table_name": "orders", "table_arn": TABLE_ARN},
            {},
        )
        current_state = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager(
                "incremental",
                run_time=datetime(2026, 3, 10, tzinfo=timezone.utc),
            ),
            initial_state,
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/016",
                "checkpoint_to": "2026-03-10T00:00:00Z",
            },
        )
        concurrent_state = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager(
                "incremental",
                run_time=datetime(2026, 3, 12, tzinfo=timezone.utc),
            ),
            current_state,
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/018",
                "checkpoint_to": "2026-03-12T00:00:00Z",
            },
        )
        candidate_state = snapshot_lambda.snapshot_manager_apply_result_to_checkpoint_state(
            build_export_manager(
                "incremental",
                run_time=datetime(2026, 3, 11, tzinfo=timezone.utc),
            ),
            initial_state,
            {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/017",
                "checkpoint_to": "2026-03-11T00:00:00Z",
            },
        )

        class FakeDynamoCheckpointClient:
            def __init__(self) -> None:
                self.put_calls: list[dict[str, Any]] = []
                self.snapshot_items: dict[tuple[str, str], dict[str, Any]] = {}
                self.current_item = self._build_current_item(current_state, revision=1)
                self.current_put_attempts = 0

            def _build_current_item(self, state: dict[str, Any], *, revision: int) -> dict[str, Any]:
                return snapshot_lambda._marshal_dynamodb_item(
                    {
                        "TableName": "orders",
                        "RecordType": "CURRENT",
                        "StateKey": TABLE_ARN,
                        "Revision": revision,
                        "TableArn": TABLE_ARN,
                        "LastTo": state.get("last_to"),
                        "LastMode": state.get("last_mode"),
                        "Source": state.get("source"),
                        "PendingExports": state.get("pending_exports"),
                    }
                )

            def get_item(self, **kwargs: Any) -> dict[str, Any]:
                return {"Item": json.loads(json.dumps(self.current_item))}

            def put_item(self, **kwargs: Any) -> None:
                self.put_calls.append(kwargs)
                item = kwargs["Item"]
                record_type = item["RecordType"]["S"]
                if record_type.startswith("SNAPSHOT#"):
                    snapshot_key = (item["TableName"]["S"], record_type)
                    if snapshot_key in self.snapshot_items:
                        raise snapshot_lambda.ClientError(
                            {
                                "Error": {
                                    "Code": "ConditionalCheckFailedException",
                                    "Message": "duplicate snapshot",
                                }
                            },
                            "PutItem",
                        )
                    self.snapshot_items[snapshot_key] = json.loads(json.dumps(item))
                    return

                self.current_put_attempts += 1
                if self.current_put_attempts == 1:
                    self.current_item = self._build_current_item(concurrent_state, revision=2)
                    raise snapshot_lambda.ClientError(
                        {
                            "Error": {
                                "Code": "ConditionalCheckFailedException",
                                "Message": "stale revision",
                            }
                        },
                        "PutItem",
                    )
                self.current_item = json.loads(json.dumps(item))

        ddb_client = FakeDynamoCheckpointClient()

        snapshot_lambda.checkpoint_save(
            {
                "backend": "dynamodb",
                "ddb": ddb_client,
                "table_name": "snapshot-checkpoints",
                "key": "snapshots/_checkpoint.json",
            },
            {
                "version": 2,
                "tables": {
                    TABLE_ARN: candidate_state,
                },
            },
        )

        saved_payload = snapshot_lambda._deserialize_dynamodb_item(ddb_client.current_item)
        pending_export_arns = {
            item["export_arn"]
            for item in saved_payload["PendingExports"]
        }

        self.assertEqual(len(ddb_client.put_calls), 4)
        self.assertEqual(
            pending_export_arns,
            {
                f"{TABLE_ARN}/export/016",
                f"{TABLE_ARN}/export/017",
                f"{TABLE_ARN}/export/018",
            },
        )
        self.assertEqual(len(ddb_client.snapshot_items), 1)
        self.assertEqual(saved_payload["Revision"], 3)
        current_put_calls = [
            call
            for call in ddb_client.put_calls
            if call["Item"]["RecordType"]["S"] == "CURRENT"
        ]
        self.assertEqual(len(current_put_calls), 2)
        self.assertTrue(
            all(
                call["ExpressionAttributeNames"]
                == {
                    "#revision": snapshot_lambda.CHECKPOINT_DYNAMODB_REVISION_ATTR,
                }
                for call in current_put_calls
            )
        )
        self.assertTrue(
            all(
                call["ExpressionAttributeValues"]
                == {
                    ":expected_revision": {"N": str(index + 1)},
                }
                for index, call in enumerate(current_put_calls)
            )
        )

    def test_set_active_session_returns_new_manager_without_mutating_original(self) -> None:
        class FakeSession:
            def __init__(self, region_name: str) -> None:
                self.region_name = region_name

            def client(self, service_name: str, region_name: str | None = None) -> dict[str, str]:
                return {
                    "service_name": service_name,
                    "region_name": region_name or self.region_name,
                }

        manager = {
            "config": {
                "run_id": "20260309T000000Z",
                "checkpoint_bucket": "checkpoint-bucket",
                "checkpoint_key": "snapshots/_checkpoint.json",
            },
            "session": "old-session",
            "_assume_session": "old-session",
            "default_region": "sa-east-1",
            "_active_assume_role_arn": None,
            "_table_client_cache": {"cached": {"session_mode": "shared"}},
            "_table_client_lock": object(),
            "ddb": "old-ddb",
            "s3": "old-s3",
            "checkpoint_store": {"bucket": "checkpoint-bucket", "key": "snapshots/_checkpoint.json"},
        }

        next_manager = snapshot_lambda.snapshot_manager_set_active_session(
            manager,
            FakeSession("us-east-1"),
            source="test",
            assumed_role_arn="arn:aws:iam::111111111111:role/snapshot",
        )

        self.assertIsNot(next_manager, manager)
        self.assertEqual(manager["session"], "old-session")
        self.assertEqual(manager["_table_client_cache"], {"cached": {"session_mode": "shared"}})
        self.assertEqual(next_manager["default_region"], "us-east-1")
        self.assertEqual(next_manager["_active_assume_role_arn"], "arn:aws:iam::111111111111:role/snapshot")
        self.assertEqual(next_manager["_table_client_cache"], {})
        self.assertEqual(next_manager["checkpoint_store"]["bucket"], "checkpoint-bucket")
        self.assertEqual(next_manager["checkpoint_store"]["key"], "snapshots/_checkpoint.json")

    def test_parse_new_layout_export_key_accepts_compact_and_legacy_dates(self) -> None:
        compact = snapshot_lambda._parse_new_layout_export_key(
            "DDB/20260313/111111111111/orders/FULL/run_id=20260313T000000Z/manifest-summary.json"
        )
        legacy = snapshot_lambda._parse_new_layout_export_key(
            "DDB/2026-03-13/111111111111/orders/FULL/run_id=20260313T000000Z/manifest-summary.json"
        )

        self.assertEqual(
            compact,
            {
                "export_date": "20260313",
                "account_id": "111111111111",
                "table_name": "orders",
                "export_type": "FULL",
            },
        )
        self.assertEqual(
            legacy,
            {
                "export_date": "2026-03-13",
                "account_id": "111111111111",
                "table_name": "orders",
                "export_type": "FULL",
            },
        )


class SnapshotPointInTimeRecoveryTests(unittest.TestCase):
    def test_full_export_enables_pitr_before_export_when_disabled(self) -> None:
        ddb_client = FakeDynamoDBClient(["DISABLED", "ENABLING", "ENABLED"])

        with patch.object(snapshot_lambda.time, "sleep", lambda _seconds: None):
            result = snapshot_lambda.snapshot_manager_start_full_export(
                build_export_manager(),
                "orders",
                TABLE_ARN,
                ddb_client=ddb_client,
                execution_context={
                    "session_mode": "shared_session_by_table_region",
                    "assume_role_arn": None,
                    "table_account_id": "111111111111",
                    "table_region": "us-east-1",
                },
            )

        self.assertEqual(result["status"], "STARTED")
        self.assertEqual(result["started_at"], "2026-03-09T00:00:00Z")
        self.assertEqual(ddb_client.describe_calls, ["orders", "orders", "orders"])
        self.assertEqual(
            ddb_client.update_calls,
            [
                {
                    "TableName": "orders",
                    "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True},
                }
            ],
        )
        self.assertEqual(ddb_client.operations, ["describe", "update", "describe", "describe", "export"])
        self.assertEqual(ddb_client.export_calls[0]["TableArn"], TABLE_ARN)

    def test_full_export_skips_pitr_update_when_already_enabled(self) -> None:
        ddb_client = FakeDynamoDBClient(["ENABLED"])

        result = snapshot_lambda.snapshot_manager_start_full_export(
            build_export_manager(),
            "orders",
            TABLE_ARN,
            ddb_client=ddb_client,
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
        )

        self.assertEqual(result["status"], "STARTED")
        self.assertEqual(result["started_at"], "2026-03-09T00:00:00Z")
        self.assertEqual(ddb_client.update_calls, [])

    def test_full_export_sends_s3_bucket_owner_when_configured(self) -> None:
        ddb_client = FakeDynamoDBClient(["ENABLED"])

        snapshot_lambda.snapshot_manager_start_full_export(
            build_export_manager(bucket_owner="222222222222"),
            "orders",
            TABLE_ARN,
            ddb_client=ddb_client,
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
        )

        self.assertEqual(ddb_client.export_calls[0]["S3BucketOwner"], "222222222222")

    def test_full_export_uses_bucket_with_table_region_suffix(self) -> None:
        ddb_client = FakeDynamoDBClient(["ENABLED"])
        table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/orders"

        result = snapshot_lambda.snapshot_manager_start_full_export(
            build_export_manager(),
            "orders",
            table_arn,
            ddb_client=ddb_client,
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "sa-east-1",
            },
        )

        self.assertEqual(
            ddb_client.export_calls[0]["S3Bucket"],
            "snapshot-bucket-sa-east-1",
        )
        self.assertEqual(
            result["snapshot_bucket"],
            "snapshot-bucket-sa-east-1",
        )

    def test_incremental_export_sends_s3_bucket_owner_when_configured(self) -> None:
        ddb_client = FakeDynamoDBClient(["ENABLED"])

        snapshot_lambda.snapshot_manager_start_incremental_export(
            build_export_manager("incremental", bucket_owner="222222222222"),
            "orders",
            TABLE_ARN,
            datetime(2026, 3, 8, tzinfo=timezone.utc),
            datetime(2026, 3, 9, tzinfo=timezone.utc),
            ddb_client=ddb_client,
            s3_client=object(),
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
            incremental_reference={
                "checkpoint_from": "2026-03-08T00:00:00Z",
                "checkpoint_source": "checkpoint",
            },
        )

        self.assertEqual(ddb_client.export_calls[0]["S3BucketOwner"], "222222222222")
        self.assertEqual(ddb_client.operations, ["describe", "export"])

    def test_find_latest_full_export_checkpoint_uses_bucket_with_table_region_suffix(self) -> None:
        paginate_calls: list[dict[str, Any]] = []
        table_arn = "arn:aws:dynamodb:sa-east-1:111111111111:table/orders"

        class FakePaginator:
            def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
                paginate_calls.append(kwargs)
                return [{"Contents": []}]

        class FakeS3Client:
            def get_paginator(self, operation_name: str) -> FakePaginator:
                self.operation_name = operation_name
                return FakePaginator()

        result = snapshot_lambda.snapshot_manager_find_latest_full_export_checkpoint(
            build_export_manager("incremental"),
            "orders",
            table_arn,
            s3_client=FakeS3Client(),
            execution_context={
                "table_account_id": "111111111111",
                "table_region": "sa-east-1",
            },
        )

        self.assertIsNone(result)
        self.assertEqual(
            [call["Bucket"] for call in paginate_calls],
            ["snapshot-bucket-sa-east-1"] * len(paginate_calls),
        )
        self.assertGreaterEqual(len(paginate_calls), 1)

    def test_full_export_client_token_changes_when_prefix_changes(self) -> None:
        first_client = FakeDynamoDBClient(["ENABLED"])
        second_client = FakeDynamoDBClient(["ENABLED"])

        snapshot_lambda.snapshot_manager_start_full_export(
            build_export_manager(
                run_time=datetime(2026, 3, 9, tzinfo=timezone.utc),
            ),
            "orders",
            TABLE_ARN,
            ddb_client=first_client,
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
        )
        snapshot_lambda.snapshot_manager_start_full_export(
            build_export_manager(
                run_time=datetime(2026, 3, 10, tzinfo=timezone.utc),
            ),
            "orders",
            TABLE_ARN,
            ddb_client=second_client,
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
        )

        self.assertNotEqual(
            first_client.export_calls[0]["ClientToken"],
            second_client.export_calls[0]["ClientToken"],
        )

    def test_incremental_export_client_token_changes_when_destination_changes(self) -> None:
        first_client = FakeDynamoDBClient(["ENABLED"])
        second_client = FakeDynamoDBClient(["ENABLED"])
        export_from = datetime(2026, 3, 8, tzinfo=timezone.utc)
        export_to = datetime(2026, 3, 9, tzinfo=timezone.utc)

        snapshot_lambda.snapshot_manager_start_incremental_export(
            build_export_manager("incremental"),
            "orders",
            TABLE_ARN,
            export_from,
            export_to,
            ddb_client=first_client,
            s3_client=object(),
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
            incremental_reference={
                "checkpoint_from": "2026-03-08T00:00:00Z",
                "checkpoint_source": "checkpoint",
            },
        )
        snapshot_lambda.snapshot_manager_start_incremental_export(
            build_export_manager("incremental", bucket_owner="222222222222"),
            "orders",
            TABLE_ARN,
            export_from,
            export_to,
            ddb_client=second_client,
            s3_client=object(),
            execution_context={
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
            },
            incremental_reference={
                "checkpoint_from": "2026-03-08T00:00:00Z",
                "checkpoint_source": "checkpoint",
            },
        )

        self.assertNotEqual(
            first_client.export_calls[0]["ClientToken"],
            second_client.export_calls[0]["ClientToken"],
        )

    def test_snapshot_table_caps_incremental_window_to_24_hours(self) -> None:
        captured_window: dict[str, datetime] = {}

        def fake_start_incremental_export(
            manager: dict,
            table_name: str,
            table_arn: str,
            export_from: datetime,
            export_to: datetime,
            **kwargs: Any,
        ) -> dict:
            captured_window["export_from"] = export_from
            captured_window["export_to"] = export_to
            return {
                "table_name": table_name,
                "table_arn": table_arn,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "checkpoint_from": kwargs["incremental_reference"]["checkpoint_from"],
                "checkpoint_source": kwargs["incremental_reference"]["checkpoint_source"],
                "checkpoint_to": snapshot_lambda._dt_to_iso(export_to),
            }

        manager = {
            "config": {
                "mode": "incremental",
                "dry_run": False,
                "run_time": datetime(2026, 3, 10, 6, tzinfo=timezone.utc),
            }
        }

        with patch.object(
            snapshot_lambda,
            "snapshot_manager_validate_export_session_account",
            lambda *args, **kwargs: None,
        ), patch.object(
            snapshot_lambda,
            "snapshot_manager_start_incremental_export",
            fake_start_incremental_export,
        ):
            result = snapshot_lambda.snapshot_manager_snapshot_table(
                manager,
                {"table_name": "orders", "table_arn": TABLE_ARN},
                {"last_to": "2026-03-09T00:00:00Z"},
                execution_context={
                    "ddb": object(),
                    "s3": object(),
                    "session_mode": "shared_session_by_table_region",
                    "assume_role_arn": None,
                    "table_account_id": "111111111111",
                    "table_region": "us-east-1",
                },
            )

        self.assertEqual(
            captured_window["export_from"],
            datetime(2026, 3, 9, tzinfo=timezone.utc),
        )
        self.assertEqual(
            captured_window["export_to"],
            datetime(2026, 3, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(result["status"], "STARTED")
        self.assertEqual(result["checkpoint_to"], "2026-03-10T00:00:00Z")

    def test_snapshot_table_skips_incremental_window_shorter_than_15_minutes(self) -> None:
        manager = {
            "config": {
                "mode": "incremental",
                "dry_run": False,
                "run_time": datetime(2026, 3, 9, 0, 10, tzinfo=timezone.utc),
            }
        }

        with patch.object(
            snapshot_lambda,
            "snapshot_manager_validate_export_session_account",
            lambda *args, **kwargs: None,
        ), patch.object(
            snapshot_lambda,
            "snapshot_manager_start_incremental_export",
            side_effect=AssertionError("incremental export não deveria iniciar"),
        ):
            result = snapshot_lambda.snapshot_manager_snapshot_table(
                manager,
                {"table_name": "orders", "table_arn": TABLE_ARN},
                {"last_to": "2026-03-09T00:00:00Z"},
                execution_context={
                    "ddb": object(),
                    "s3": object(),
                    "session_mode": "shared_session_by_table_region",
                    "assume_role_arn": None,
                    "table_account_id": "111111111111",
                    "table_region": "us-east-1",
                },
            )

        self.assertEqual(result["status"], "SKIPPED")
        self.assertEqual(result["source"], "incremental_window_guard")
        self.assertIn("15 minutos", result["message"])
        self.assertEqual(result["checkpoint_state"]["last_to"], "2026-03-09T00:00:00Z")


class SnapshotPerformanceTests(unittest.TestCase):
    def test_extract_session_identity_uses_cache_per_session(self) -> None:
        sts_client = FakeSTSClient("arn:aws:sts::111111111111:assumed-role/snapshot/session")
        session = FakeSession(sts_client)
        manager = {
            "_session_identity_cache": {},
            "_session_identity_lock": threading.Lock(),
        }

        first_identity = snapshot_lambda.snapshot_manager_extract_session_identity(manager, session)
        second_identity = snapshot_lambda.snapshot_manager_extract_session_identity(manager, session)

        self.assertEqual(
            first_identity,
            "arn:aws:sts::111111111111:assumed-role/snapshot/session",
        )
        self.assertEqual(second_identity, first_identity)
        self.assertEqual(sts_client.calls, 1)

    def test_extract_session_identity_shares_single_sts_lookup_under_concurrency(self) -> None:
        class SlowFakeSTSClient(FakeSTSClient):
            def get_caller_identity(self) -> dict:
                time.sleep(0.01)
                return super().get_caller_identity()

        sts_client = SlowFakeSTSClient(
            "arn:aws:sts::111111111111:assumed-role/snapshot/session"
        )
        session = FakeSession(sts_client)
        manager = {
            "_session_identity_cache": {},
            "_session_identity_lock": threading.Lock(),
        }
        results: list[str] = []
        results_lock = threading.Lock()

        def resolve_identity() -> None:
            identity = snapshot_lambda.snapshot_manager_extract_session_identity(
                manager,
                session,
            )
            with results_lock:
                results.append(identity)

        workers = [threading.Thread(target=resolve_identity) for _ in range(5)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        self.assertEqual(
            results,
            ["arn:aws:sts::111111111111:assumed-role/snapshot/session"] * 5,
        )
        self.assertEqual(sts_client.calls, 1)

    def test_prepare_execution_entries_reuses_context_cached_in_preflight(self) -> None:
        sts_client = FakeSTSClient("arn:aws:sts::111111111111:assumed-role/snapshot/session")
        session = FakeSession(sts_client)
        entry = {"table_name": "orders", "table_arn": TABLE_ARN}
        manager = {
            "config": {
                "mode": "full",
                "dry_run": False,
                "permission_precheck_enabled": True,
            },
            "_execution_context_cache": {},
            "_execution_context_lock": threading.Lock(),
            "_session_identity_cache": {},
            "_session_identity_lock": threading.Lock(),
        }
        resolve_calls: list[tuple[str, str]] = []

        def fake_resolve_execution_context(
            current_manager: dict,
            table_name: str,
            table_arn: str,
        ) -> tuple[dict, dict]:
            resolve_calls.append((table_name, table_arn))
            return current_manager, {
                "session": session,
                "ddb": object(),
                "s3": object(),
                "session_mode": "shared_session_by_table_region",
                "assume_role_arn": None,
                "table_account_id": "111111111111",
                "table_region": "us-east-1",
                "table_name": table_name,
                "table_arn": table_arn,
            }

        with patch.object(
            snapshot_lambda,
            "snapshot_manager_resolve_execution_context",
            side_effect=fake_resolve_execution_context,
        ):
            next_manager, allowed_entries, failures = snapshot_lambda.snapshot_manager_partition_by_permission_precheck(
                manager,
                [entry],
            )
            _, prepared_entries = snapshot_lambda.snapshot_manager_prepare_execution_entries(
                next_manager,
                allowed_entries,
                {},
            )

        self.assertEqual(failures, [])
        self.assertEqual(allowed_entries, [entry])
        self.assertEqual(resolve_calls, [("orders", TABLE_ARN)])
        self.assertEqual(sts_client.calls, 1)
        self.assertEqual(len(prepared_entries), 1)
        self.assertEqual(
            prepared_entries[0]["execution_context"]["caller_arn"],
            "arn:aws:sts::111111111111:assumed-role/snapshot/session",
        )


if __name__ == "__main__":
    unittest.main()
