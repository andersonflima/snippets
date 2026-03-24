import sys
import types
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
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
            if isinstance(value, int):
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


class FakeDescribeExportClient:
    def __init__(self, *, item_count_by_arn: dict[str, int | None] | None = None) -> None:
        self.item_count_by_arn = item_count_by_arn or {}
        self.describe_calls: list[str] = []

    def describe_export(self, *, ExportArn: str) -> dict:
        self.describe_calls.append(ExportArn)
        description: dict[str, Any] = {
            "ExportArn": ExportArn,
            "ExportStatus": "COMPLETED",
        }
        item_count = self.item_count_by_arn.get(ExportArn)
        if item_count is not None:
            description["ItemCount"] = item_count
        return {"ExportDescription": description}


class FakeIncrementalPitrClient(FakeDescribeExportClient):
    def __init__(
        self,
        *,
        earliest_restorable: datetime,
        latest_restorable: datetime,
        item_count_by_arn: dict[str, int | None] | None = None,
    ) -> None:
        super().__init__(item_count_by_arn=item_count_by_arn)
        self.earliest_restorable = earliest_restorable
        self.latest_restorable = latest_restorable
        self.describe_continuous_backups_calls: list[str] = []
        self.export_calls: list[dict[str, Any]] = []

    def describe_continuous_backups(self, TableName: str) -> dict:
        self.describe_continuous_backups_calls.append(TableName)
        return {
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": "ENABLED",
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": "ENABLED",
                    "EarliestRestorableDateTime": self.earliest_restorable,
                    "LatestRestorableDateTime": self.latest_restorable,
                },
            }
        }

    def export_table_to_point_in_time(self, **params: Any) -> dict:
        self.export_calls.append(params)
        return {
            "ExportDescription": {
                "ExportArn": f"{TABLE_ARN}/export/099",
                "ExportStatus": "IN_PROGRESS",
            },
            "ResponseMetadata": {"RequestId": "req-099"},
        }


class SnapshotAutomaticModeTests(unittest.TestCase):
    def build_config(self) -> dict:
        return {
            "run_id": "20260323T000000Z",
            "run_time": datetime(2026, 3, 23, tzinfo=timezone.utc),
            "bucket": "snapshot-bucket",
            "bucket_owner": None,
            "dry_run": False,
            "wait_for_completion": False,
            "snapshot_bucket_exact": False,
            "scan_fallback_enabled": False,
            "incremental_export_view_type": "NEW_IMAGE",
        }

    def build_target(self) -> snapshot_lambda.TableTarget:
        return snapshot_lambda.TableTarget(
            raw_ref="orders",
            table_name="orders",
            table_arn=TABLE_ARN,
            account_id="111111111111",
            region="us-east-1",
        )

    def test_build_export_prefix_is_unique_for_multiple_runs_same_day(self) -> None:
        target = self.build_target()
        first_run = datetime(2026, 3, 23, 10, 0, 0, tzinfo=timezone.utc)
        second_run = datetime(2026, 3, 23, 11, 0, 0, tzinfo=timezone.utc)

        first_prefix = snapshot_lambda._build_export_prefix(
            first_run,
            target,
            "INCREMENTAL_EXPORT",
            incremental_index=1,
        )
        second_prefix = snapshot_lambda._build_export_prefix(
            second_run,
            target,
            "INCREMENTAL_EXPORT",
            incremental_index=1,
        )

        self.assertNotEqual(first_prefix, second_prefix)
        self.assertTrue(first_prefix.endswith("/INCR/run_id=20260323T100000Z"))
        self.assertTrue(second_prefix.endswith("/INCR/run_id=20260323T110000Z"))

    def run_process_table(
        self,
        *,
        previous_state: dict,
        ddb_client: Any,
        start_incremental_export: Any | None = None,
        start_full_export: Any | None = None,
        config_override: dict | None = None,
    ) -> dict:
        base_session = types.SimpleNamespace(region_name="us-east-1")
        target = self.build_target()
        fake_s3_client = object()
        config = self.build_config()
        if isinstance(config_override, dict):
            config = {**config, **config_override}

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "_resolve_table_target",
                    lambda raw_target_ref, session, runtime_region: target,
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "_resolve_assumed_session_for_target",
                    lambda **kwargs: (base_session, None),
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "_get_session_client",
                    lambda session, service_name, region_name=None: ddb_client if service_name == "dynamodb" else fake_s3_client,
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "checkpoint_load_table_state",
                    lambda store, target_table_name, target_table_arn=None: previous_state,
                )
            )
            stack.enter_context(
                patch.object(
                    snapshot_lambda,
                    "_read_table_created_at_iso",
                    lambda ddb, table_name, table_arn: "2026-03-01T00:00:00Z",
                )
            )
            if start_incremental_export is not None:
                stack.enter_context(
                    patch.object(snapshot_lambda, "_start_incremental_export", start_incremental_export)
                )
            if start_full_export is not None:
                stack.enter_context(
                    patch.object(snapshot_lambda, "_start_full_export", start_full_export)
                )

            return snapshot_lambda._process_table(
                config=config,
                base_session=base_session,
                checkpoint_store={},
                raw_target_ref="orders",
                ignore_set=set(),
                assume_session_cache={},
            )

    def test_build_snapshot_config_always_uses_automatic_mode(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "CHECKPOINT_DYNAMODB_TABLE_ARN": "arn:aws:dynamodb:us-east-1:111111111111:table/checkpoints",
                "SNAPSHOT_MODE": "full",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"mode": "incremental"})

        self.assertEqual(config["mode"], "automatic")

    def test_build_snapshot_config_does_not_expose_catch_up(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "CHECKPOINT_DYNAMODB_TABLE_ARN": "arn:aws:dynamodb:us-east-1:111111111111:table/checkpoints",
                "CATCH_UP": "true",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({"catch_up": True})

        self.assertNotIn("catch_up", config)

    def test_build_snapshot_config_reads_incremental_cycle_limit_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "CHECKPOINT_DYNAMODB_TABLE_ARN": "arn:aws:dynamodb:us-east-1:111111111111:table/checkpoints",
                "MAX_INCREMENTAL_EXPORTS_PER_CYCLE": "8",
            },
            clear=True,
        ):
            config = snapshot_lambda.build_snapshot_config({})

        self.assertEqual(config["max_incremental_exports_per_cycle"], 8)

    def test_build_snapshot_config_rejects_invalid_incremental_cycle_limit(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SNAPSHOT_BUCKET": "snapshot-bucket",
                "TARGET_TABLES": "orders",
                "CHECKPOINT_DYNAMODB_TABLE_ARN": "arn:aws:dynamodb:us-east-1:111111111111:table/checkpoints",
                "MAX_INCREMENTAL_EXPORTS_PER_CYCLE": "0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "MAX_INCREMENTAL_EXPORTS_PER_CYCLE deve ser maior que zero",
            ):
                snapshot_lambda.build_snapshot_config({})

    def test_reconcile_pending_exports_persists_item_count_from_completed_export(self) -> None:
        reconciled = snapshot_lambda._reconcile_pending_exports(
            ddb_client=FakeDescribeExportClient(item_count_by_arn={f"{TABLE_ARN}/export/001": 7}),
            state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "pending_exports": [
                    {
                        "export_arn": f"{TABLE_ARN}/export/001",
                        "checkpoint_to": "2026-03-22T00:00:00Z",
                        "mode": "INCREMENTAL",
                        "source": "native",
                    }
                ],
                "incremental_seq": 1,
            },
            table_name="orders",
            table_arn=TABLE_ARN,
        )

        self.assertEqual(reconciled["last_export_arn"], f"{TABLE_ARN}/export/001")
        self.assertEqual(reconciled["last_export_item_count"], 7)
        self.assertEqual(reconciled["last_mode"], "INCREMENTAL")
        self.assertEqual(reconciled["last_to"], "2026-03-22T00:00:00Z")

    def test_first_incremental_after_full_uses_index_one(self) -> None:
        captured: dict[str, Any] = {}

        def fake_start_incremental_export(**kwargs: Any) -> dict:
            captured["incremental_index"] = kwargs["incremental_index"]
            return {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/002",
                "checkpoint_from": "2026-03-22T00:00:00Z",
                "checkpoint_to": "2026-03-23T00:00:00Z",
            }

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T00:00:00Z",
                "last_mode": "FULL",
                "incremental_seq": 0,
            },
            ddb_client=FakeDescribeExportClient(),
            start_incremental_export=fake_start_incremental_export,
        )

        self.assertEqual(captured["incremental_index"], 1)
        self.assertEqual(result["mode_selection_reason"], "initial_incremental_after_full")
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 1)

    def test_previous_incremental_without_items_reuses_same_incremental_index(self) -> None:
        captured: dict[str, Any] = {}
        previous_export_arn = f"{TABLE_ARN}/export/010"

        def fake_start_incremental_export(**kwargs: Any) -> dict:
            captured["incremental_index"] = kwargs["incremental_index"]
            return {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/011",
                "checkpoint_from": "2026-03-22T00:00:00Z",
                "checkpoint_to": "2026-03-23T00:00:00Z",
            }

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T00:00:00Z",
                "last_mode": "INCREMENTAL",
                "last_export_arn": previous_export_arn,
                "incremental_seq": 2,
            },
            ddb_client=FakeDescribeExportClient(item_count_by_arn={previous_export_arn: 0}),
            start_incremental_export=fake_start_incremental_export,
        )

        self.assertEqual(captured["incremental_index"], 2)
        self.assertEqual(result["mode_selection_reason"], "previous_incremental_without_items")
        self.assertEqual(result["checkpoint_state"]["last_export_item_count"], 0)
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 2)

    def test_previous_incremental_with_items_advances_incremental_index(self) -> None:
        captured: dict[str, Any] = {}
        previous_export_arn = f"{TABLE_ARN}/export/020"

        def fake_start_incremental_export(**kwargs: Any) -> dict:
            captured["incremental_index"] = kwargs["incremental_index"]
            return {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "INCREMENTAL",
                "status": "STARTED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/021",
                "checkpoint_from": "2026-03-22T00:00:00Z",
                "checkpoint_to": "2026-03-23T00:00:00Z",
            }

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T00:00:00Z",
                "last_mode": "INCREMENTAL",
                "last_export_arn": previous_export_arn,
                "incremental_seq": 2,
            },
            ddb_client=FakeDescribeExportClient(item_count_by_arn={previous_export_arn: 5}),
            start_incremental_export=fake_start_incremental_export,
        )

        self.assertEqual(captured["incremental_index"], 3)
        self.assertEqual(result["mode_selection_reason"], "previous_incremental_had_items")
        self.assertEqual(result["checkpoint_state"]["last_export_item_count"], 5)
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 3)

    def test_incremental_export_clamps_checkpoint_from_to_pitr_earliest(self) -> None:
        ddb_client = FakeIncrementalPitrClient(
            earliest_restorable=datetime(2026, 3, 22, tzinfo=timezone.utc),
            latest_restorable=datetime(2026, 3, 23, tzinfo=timezone.utc),
        )

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-01T00:00:00Z",
                "last_mode": "FULL",
                "incremental_seq": 0,
            },
            ddb_client=ddb_client,
        )

        self.assertEqual(result["status"], "STARTED")
        self.assertEqual(result["mode"], "INCREMENTAL")
        self.assertEqual(result["checkpoint_from"], "2026-03-22T00:00:00Z")
        self.assertEqual(result["checkpoint_to"], "2026-03-23T00:00:00Z")
        self.assertEqual(result["mode_selection_reason"], "initial_incremental_after_full")
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 1)
        self.assertEqual(ddb_client.describe_continuous_backups_calls, ["orders"])
        self.assertEqual(len(ddb_client.export_calls), 1)
        self.assertEqual(
            ddb_client.export_calls[0]["IncrementalExportSpecification"]["ExportFromTime"],
            datetime(2026, 3, 22, tzinfo=timezone.utc),
        )
        self.assertEqual(
            ddb_client.export_calls[0]["IncrementalExportSpecification"]["ExportToTime"],
            datetime(2026, 3, 23, tzinfo=timezone.utc),
        )

    def test_incremental_export_returns_pending_when_pitr_adjusted_window_is_below_15_minutes(self) -> None:
        ddb_client = FakeIncrementalPitrClient(
            earliest_restorable=datetime(2026, 3, 22, 23, 50, tzinfo=timezone.utc),
            latest_restorable=datetime(2026, 3, 23, 0, 0, tzinfo=timezone.utc),
        )

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T23:40:00Z",
                "last_mode": "FULL",
                "incremental_seq": 0,
            },
            ddb_client=ddb_client,
        )

        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["mode"], "INCREMENTAL")
        self.assertEqual(result["source"], "window_outside_pitr")
        self.assertIn("15 minutos", result["message"])
        self.assertEqual(result["checkpoint_from"], "2026-03-22T23:50:00Z")
        self.assertEqual(result["checkpoint_to"], "2026-03-23T00:00:00Z")
        self.assertEqual(result["mode_selection_reason"], "initial_incremental_after_full")
        self.assertEqual(ddb_client.describe_continuous_backups_calls, ["orders"])
        self.assertEqual(ddb_client.export_calls, [])

    def test_reaches_six_incrementals_and_starts_new_full(self) -> None:
        captured: dict[str, Any] = {"full_calls": 0}

        def fake_start_full_export(**kwargs: Any) -> dict:
            captured["full_calls"] += 1
            return {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "FULL",
                "status": "COMPLETED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/full-001",
                "checkpoint_to": "2026-03-23T00:00:00Z",
                "item_count": 42,
            }

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T00:00:00Z",
                "last_mode": "INCREMENTAL",
                "last_export_arn": f"{TABLE_ARN}/export/030",
                "last_export_item_count": 9,
                "incremental_seq": 6,
            },
            ddb_client=FakeDescribeExportClient(),
            start_full_export=fake_start_full_export,
        )

        self.assertEqual(captured["full_calls"], 1)
        self.assertEqual(result["mode"], "FULL")
        self.assertEqual(result["mode_selection_reason"], "incremental_cycle_limit_reached")
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 0)
        self.assertEqual(result["checkpoint_state"]["last_mode"], "FULL")

    def test_reaches_configured_incremental_limit_and_starts_new_full(self) -> None:
        captured: dict[str, Any] = {"full_calls": 0}

        def fake_start_full_export(**kwargs: Any) -> dict:
            captured["full_calls"] += 1
            return {
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "mode": "FULL",
                "status": "COMPLETED",
                "source": "native",
                "export_arn": f"{TABLE_ARN}/export/full-002",
                "checkpoint_to": "2026-03-23T00:00:00Z",
                "item_count": 13,
            }

        result = self.run_process_table(
            previous_state={
                "table_name": "orders",
                "table_arn": TABLE_ARN,
                "last_to": "2026-03-22T00:00:00Z",
                "last_mode": "INCREMENTAL",
                "last_export_arn": f"{TABLE_ARN}/export/040",
                "last_export_item_count": 2,
                "incremental_seq": 3,
            },
            ddb_client=FakeDescribeExportClient(),
            start_full_export=fake_start_full_export,
            config_override={"max_incremental_exports_per_cycle": 3},
        )

        self.assertEqual(captured["full_calls"], 1)
        self.assertEqual(result["mode"], "FULL")
        self.assertEqual(result["mode_selection_reason"], "incremental_cycle_limit_reached")
        self.assertEqual(result["checkpoint_state"]["incremental_seq"], 0)
        self.assertEqual(result["checkpoint_state"]["last_mode"], "FULL")


if __name__ == "__main__":
    unittest.main()
