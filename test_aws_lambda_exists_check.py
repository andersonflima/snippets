import importlib
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


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

    boto3_module.Session = Session
    boto3_session_module.Session = Session
    boto3_module.session = boto3_session_module

    botocore_module.exceptions = botocore_exceptions_module
    botocore_exceptions_module.BotoCoreError = BotoCoreError
    botocore_exceptions_module.ClientError = ClientError

    sys.modules.setdefault("boto3", boto3_module)
    sys.modules.setdefault("boto3.session", boto3_session_module)
    sys.modules.setdefault("botocore", botocore_module)
    sys.modules.setdefault("botocore.exceptions", botocore_exceptions_module)


install_aws_stubs()

checker = importlib.import_module("aws-lambda-exists-check")
ClientError = sys.modules["botocore.exceptions"].ClientError


def client_error(code: str) -> ClientError:
    return ClientError(response={"Error": {"Code": code}}, operation_name="GetFunction")


class FakeLambdaClient:
    def __init__(self, *, behavior: str) -> None:
        self.behavior = behavior

    def get_function(self, **kwargs: Any) -> dict:
        if self.behavior == "exists":
            return {"Configuration": {"FunctionName": kwargs.get("FunctionName")}}
        if self.behavior == "missing":
            raise client_error("ResourceNotFoundException")
        if self.behavior == "denied":
            raise client_error("AccessDeniedException")
        raise AssertionError(f"unknown behavior: {self.behavior}")


class FakeAssumedSession:
    def __init__(self, behavior: str) -> None:
        self.behavior = behavior

    def client(self, service_name: str, region_name: str | None = None) -> Any:
        assert service_name == "lambda"
        return FakeLambdaClient(behavior=self.behavior)


def make_args(**overrides: Any) -> Any:
    defaults = {
        "function_name": "my-fn",
        "region": "us-east-1",
        "assume_role": "OrgReadOnly",
        "role_session_name": "lambda-exists-check",
        "external_id": None,
    }
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


class LoadAccountsTest(unittest.TestCase):
    def test_csv_with_account_id_header_dedupes_and_strips(self) -> None:
        with TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "accounts.csv"
            csv_path.write_text(
                "account_id,name\n111111111111,a\n 222222222222 ,b\n111111111111,dup\n\n",
                encoding="utf-8",
            )
            result = checker.load_accounts(None, None, csv_path)
        accounts = [item["account"] for item in result["accounts"]]
        self.assertEqual(accounts, ["111111111111", "222222222222"])
        self.assertEqual(result["duplicate_count"], 1)

    def test_csv_without_known_header_uses_first_column(self) -> None:
        with TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "accounts.csv"
            csv_path.write_text("333333333333\n444444444444\n", encoding="utf-8")
            result = checker.load_accounts(None, None, csv_path)
        self.assertEqual(
            [item["account"] for item in result["accounts"]],
            ["333333333333", "444444444444"],
        )

    def test_empty_csv_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "accounts.csv"
            csv_path.write_text("account_id\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                checker.load_accounts(None, None, csv_path)


class CheckAccountTest(unittest.TestCase):
    def _run(self, behavior: str) -> dict:
        args = make_args()
        original = checker.assume_role_for_account
        checker.assume_role_for_account = lambda **kwargs: FakeAssumedSession(behavior)
        try:
            _, result = checker.check_account(
                index=0,
                account={"account": "111111111111"},
                args=args,
                source_session=object(),
            )
        finally:
            checker.assume_role_for_account = original
        return result

    def test_existing_lambda_is_ok_and_present(self) -> None:
        result = self._run("exists")
        self.assertTrue(result["ok"])
        self.assertTrue(result["lambda_exists"])
        self.assertIsNone(result["error"])

    def test_missing_lambda_is_ok_but_absent(self) -> None:
        result = self._run("missing")
        self.assertTrue(result["ok"])  # acesso funcionou -> success sheet
        self.assertFalse(result["lambda_exists"])
        self.assertIsNone(result["error"])

    def test_access_denied_is_failure(self) -> None:
        result = self._run("denied")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "AccessDeniedException")


class ReportTest(unittest.TestCase):
    def test_result_confirmation_values(self) -> None:
        present = {"account": "1", "ok": True, "lambda_exists": True, "error": None}
        absent = {"account": "1", "ok": True, "lambda_exists": False, "error": None}
        failed = {"account": "1", "ok": False, "lambda_exists": False, "error": "boom"}
        self.assertEqual(checker.result_confirmation(present), "sim")
        self.assertEqual(checker.result_confirmation(absent), "nao")
        self.assertEqual(checker.result_confirmation(failed), "")

    def test_split_results_separates_ok_from_error(self) -> None:
        results = [
            {"account": "1", "ok": True, "lambda_exists": True, "error": None},
            {"account": "2", "ok": True, "lambda_exists": False, "error": None},
            {"account": "3", "ok": False, "lambda_exists": False, "error": "boom"},
        ]
        success, failed = checker.split_results(results)
        self.assertEqual([item["account"] for item in success], ["1", "2"])
        self.assertEqual([item["account"] for item in failed], ["3"])

    def test_normalize_report_path_forces_xlsx(self) -> None:
        self.assertTrue(checker.normalize_report_path("report").endswith(".xlsx"))
        self.assertEqual(checker.normalize_report_path("r.xlsx"), "r.xlsx")


if __name__ == "__main__":
    unittest.main()
