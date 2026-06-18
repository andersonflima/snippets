#!/usr/bin/env python3
"""Check whether a Lambda function exists across multiple AWS accounts.

Workflow:
1. Read source (bastion) credentials from the environment (AWS_SECRETS or the
   standard AWS_* variables).
2. For each target account (from a CSV with an `account_id` column), assume the
   role passed via --assume-role using the bastion credentials.
3. With the assumed credentials, call lambda:GetFunction in --region to confirm
   whether --function-name exists in that account.
4. Emit an Excel report with two sheets:
   - `success`: accounts accessed successfully (existence determined), with a
     `lambda_exists` column (sim/nao).
   - `failed`: accounts that could not be processed (assume-role denied, API
     errors, etc.).

A Lambda that simply does not exist (ResourceNotFoundException) is NOT an error:
the account is accessed fine, so it lands in `success` with lambda_exists=nao.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import getenv
from pathlib import Path
from typing import List, Optional, Set

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verifica a existência de uma Lambda em múltiplas contas assumindo uma "
            "role comum a partir de credenciais de uma conta bastion."
        )
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--accounts",
        help="Lista de contas separadas por vírgula (ex: 111111111111,222222222222).",
    )
    target.add_argument(
        "--accounts-file",
        type=Path,
        help="Arquivo com um account id por linha.",
    )
    target.add_argument(
        "--accounts-csv",
        type=Path,
        help="Arquivo CSV com a coluna `account_id` (ou `account`, `accountId`, primeira coluna).",
    )
    parser.add_argument(
        "--assume-role",
        required=True,
        help="Nome da role a assumir em cada conta-alvo (ex: OrgReadOnly).",
    )
    parser.add_argument(
        "--function-name",
        required=True,
        help="Nome (ou ARN) da Lambda cuja existência será verificada em cada conta.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Região onde a Lambda é procurada (padrão: us-east-1).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Quantidade máxima de contas processadas em paralelo.",
    )
    parser.add_argument(
        "--report",
        dest="report",
        help=(
            "Arquivo Excel de saída com abas success/failed "
            "(padrão: lambda-exists-report-<timestamp>.xlsx)."
        ),
    )
    parser.add_argument(
        "--external-id",
        help="ExternalId opcional para o AssumeRole.",
    )
    parser.add_argument(
        "--role-session-name",
        default="lambda-exists-check",
        help="SessionName do AssumeRole (padrão: lambda-exists-check).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime também a saída detalhada em JSON.",
    )
    return parser.parse_args()


# --------------------------------------------------------------------------- #
# Account loading                                                             #
# --------------------------------------------------------------------------- #
def _account_record(account_id: str) -> dict:
    return {"account": account_id}


def _normalize_header(header: str) -> str:
    return "".join(char for char in header.strip().lower() if char.isalnum())


def _account_id_headers() -> Set[str]:
    return {"account", "accountid", "accountnumber", "id", "conta", "contaid", "numerodaconta"}


def _load_accounts_from_csv(accounts_csv: Path) -> List[dict]:
    with accounts_csv.open(encoding="utf-8-sig", newline="") as handler:
        rows = list(csv.reader(handler))
    if not rows or not rows[0]:
        return []

    headers = [_normalize_header(cell) for cell in rows[0]]
    if any(header in _account_id_headers() for header in headers):
        account_index = next(
            index for index, header in enumerate(headers) if header in _account_id_headers()
        )
        return [
            _account_record(row[account_index].strip())
            for row in rows[1:]
            if len(row) > account_index and row[account_index].strip()
        ]

    return [
        _account_record(row[0].strip())
        for row in rows
        if row and row[0].strip()
    ]


def _dedupe_accounts(values: List[dict]) -> List[dict]:
    seen: Set[str] = set()
    deduped: List[dict] = []
    for value in values:
        account_id = value["account"]
        if account_id and account_id not in seen:
            seen.add(account_id)
            deduped.append(value)
    return deduped


def _account_load_result(values: List[dict]) -> dict:
    valid_accounts = [value for value in values if value["account"]]
    unique_accounts = _dedupe_accounts(valid_accounts)
    return {
        "accounts": unique_accounts,
        "raw_count": len(values),
        "valid_count": len(valid_accounts),
        "empty_count": len(values) - len(valid_accounts),
        "duplicate_count": len(valid_accounts) - len(unique_accounts),
    }


def load_accounts(
    accounts_csv: Optional[str],
    accounts_file: Optional[Path],
    accounts_csv_file: Optional[Path],
) -> dict:
    if accounts_csv:
        values = [_account_record(acc.strip()) for acc in accounts_csv.split(",")]
    elif accounts_file:
        values = [_account_record(line.strip()) for line in accounts_file.read_text().splitlines()]
    elif accounts_csv_file:
        values = _load_accounts_from_csv(accounts_csv_file)
    else:
        raise ValueError("Informe --accounts, --accounts-file ou --accounts-csv.")

    result = _account_load_result(values)
    if not result["accounts"]:
        raise ValueError("Nenhuma conta válida encontrada.")
    return result


# --------------------------------------------------------------------------- #
# Source (bastion) credentials                                                #
# --------------------------------------------------------------------------- #
def _read_json_path_or_text(raw: str) -> dict:
    raw = raw.strip()
    if not raw:
        return {}

    path = Path(raw)
    if path.exists():
        raw = path.read_text(encoding="utf-8")

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("AWS_SECRETS deve ser um JSON válido de mapa.")
    return data


def _resolve_source_credentials() -> dict:
    aws_secrets = getenv("AWS_SECRETS")
    if aws_secrets:
        secrets = _read_json_path_or_text(aws_secrets)
        return {
            "aws_access_key_id": (
                secrets.get("aws_access_key_id")
                or secrets.get("AccessKeyId")
                or secrets.get("accessKeyId")
                or getenv("AWS_ACCESS_KEY_ID")
            ),
            "aws_secret_access_key": (
                secrets.get("aws_secret_access_key")
                or secrets.get("SecretAccessKey")
                or secrets.get("secretAccessKey")
                or getenv("AWS_SECRET_ACCESS_KEY")
            ),
            "aws_session_token": (
                secrets.get("aws_session_token")
                or secrets.get("SessionToken")
                or secrets.get("sessionToken")
                or getenv("AWS_SESSION_TOKEN")
            ),
            "region": (
                secrets.get("region")
                or secrets.get("aws_region")
                or secrets.get("AWS_REGION")
                or getenv("AWS_REGION")
            ),
        }

    return {
        "aws_access_key_id": getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": getenv("AWS_SESSION_TOKEN"),
        "region": getenv("AWS_REGION") or getenv("AWS_DEFAULT_REGION"),
    }


def build_source_session(region_name: str) -> boto3.Session:
    credentials = _resolve_source_credentials()
    access_key_id = credentials.get("aws_access_key_id")
    secret_access_key = credentials.get("aws_secret_access_key")

    if not access_key_id or not secret_access_key:
        raise ValueError(
            "Credenciais da conta bastion não encontradas. Defina AWS_SECRETS ou "
            "AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY no ambiente."
        )

    return boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=credentials.get("aws_session_token"),
        region_name=credentials.get("region") or region_name,
    )


# --------------------------------------------------------------------------- #
# AWS work per account                                                        #
# --------------------------------------------------------------------------- #
def assume_role_for_account(
    source_session: boto3.Session,
    account_id: str,
    role_name: str,
    role_session_name: str,
    external_id: Optional[str],
    region: str,
) -> boto3.Session:
    sts = source_session.client("sts", region_name=region)
    params = {
        "RoleArn": f"arn:aws:iam::{account_id}:role/{role_name}",
        "RoleSessionName": role_session_name,
    }
    if external_id:
        params["ExternalId"] = external_id

    creds = sts.assume_role(**params)["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


def lambda_function_exists(assumed_session: boto3.Session, function_name: str, region: str) -> bool:
    """Return True if the function exists, False on ResourceNotFoundException.

    Any other client/transport error propagates so the account is reported as
    failed instead of being silently treated as "not found".
    """
    client = assumed_session.client("lambda", region_name=region)
    try:
        client.get_function(FunctionName=function_name)
        return True
    except ClientError as error:
        if _extract_error_code(error) == "ResourceNotFoundException":
            return False
        raise


def _extract_error_code(error: Exception) -> str:
    if not isinstance(error, ClientError):
        return ""
    return (error.response or {}).get("Error", {}).get("Code", "")


def log_step(message: str) -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] {message}", file=sys.stderr, flush=True)


def check_account(
    index: int,
    account: dict,
    args: argparse.Namespace,
    source_session: boto3.Session,
) -> tuple[int, dict]:
    account_id = account["account"]
    result = {
        "account": account_id,
        "function_name": args.function_name,
        "region": args.region,
        "lambda_exists": False,
        "ok": False,
        "error": None,
        "error_code": None,
    }
    try:
        log_step(f"Conta {index + 1}: iniciando verificação {account_id}")
        assumed_session = assume_role_for_account(
            source_session=source_session,
            account_id=account_id,
            role_name=args.assume_role,
            role_session_name=args.role_session_name,
            external_id=args.external_id,
            region=args.region,
        )
        log_step(f"Conta {account_id}: assumeRole concluido")

        exists = lambda_function_exists(assumed_session, args.function_name, args.region)
        result["lambda_exists"] = exists
        result["ok"] = True
        if exists:
            log_step(f"Conta {account_id}: Lambda '{args.function_name}' existe.")
        else:
            log_step(f"Conta {account_id}: Lambda '{args.function_name}' nao existe.")
    except (ClientError, BotoCoreError, ValueError) as error:
        result["error_code"] = _extract_error_code(error) or None
        result["error"] = str(error)
        result["ok"] = False
        log_step(f"Conta {account_id}: erro: {result['error']}")
    return index, result


# --------------------------------------------------------------------------- #
# Reporting                                                                   #
# --------------------------------------------------------------------------- #
def report_headers() -> List[str]:
    return ["account", "lambda_exists", "function_name", "region", "error"]


def result_confirmation(item: dict) -> str:
    if not item["ok"]:
        return ""
    return "sim" if item["lambda_exists"] else "nao"


def result_row(item: dict) -> List[str]:
    return [
        item["account"],
        result_confirmation(item),
        item["function_name"],
        item["region"],
        item["error"] or "",
    ]


def split_results(results: List[dict]) -> tuple[List[dict], List[dict]]:
    success = [item for item in results if item["ok"]]
    failed = [item for item in results if not item["ok"]]
    return success, failed


def write_xlsx_report(report_path: str, success: List[dict], failed: List[dict]) -> None:
    try:
        from openpyxl import Workbook
    except ImportError as error:
        raise ValueError("Para gerar relatório Excel, instale openpyxl.") from error

    workbook = Workbook()
    workbook.remove(workbook.active)

    for sheet_name, rows in (("success", success), ("failed", failed)):
        sheet = workbook.create_sheet(sheet_name)
        sheet.append(report_headers())
        for item in rows:
            sheet.append(result_row(item))

    workbook.save(report_path)


def normalize_report_path(report_path: str) -> str:
    path = Path(report_path)
    if path.suffix.lower() == ".xlsx":
        return str(path)
    return str(path.with_suffix(".xlsx"))


def write_reports(report_path: str, results: List[dict]) -> str:
    report_path = normalize_report_path(report_path)
    success, failed = split_results(results)
    write_xlsx_report(report_path, success, failed)
    log_step(
        f"Relatorio Excel gerado: {report_path} sheets=success,failed "
        f"success={len(success)} failed={len(failed)}"
    )
    return report_path


# --------------------------------------------------------------------------- #
# Orchestration                                                               #
# --------------------------------------------------------------------------- #
def run_check(args: argparse.Namespace) -> int:
    if args.workers <= 0:
        raise ValueError("workers precisa ser maior que 0.")

    account_load = load_accounts(args.accounts, args.accounts_file, args.accounts_csv)
    accounts = account_load["accounts"]
    source_session = build_source_session(args.region)
    workers = min(args.workers, len(accounts))

    log_step(
        "Carga de contas: "
        f"entradas_lidas={account_load['raw_count']} "
        f"validas={account_load['valid_count']} "
        f"vazias_ignoradas={account_load['empty_count']} "
        f"duplicadas_removidas={account_load['duplicate_count']} "
        f"unicas_processadas={len(accounts)}"
    )
    log_step(
        f"Iniciando verificacao de Lambda: total_contas={len(accounts)} workers={workers} "
        f"assume_role={args.assume_role} function={args.function_name} region={args.region}"
    )

    results: list[dict] = [None for _ in accounts]  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                check_account,
                index=index,
                account=account,
                args=args,
                source_session=source_session,
            )
            for index, account in enumerate(accounts)
        ]
        for future in as_completed(futures):
            index, result = future.result()
            results[index] = result

    report_path = args.report or f"lambda-exists-report-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.xlsx"
    write_reports(report_path, results)

    exists = [item["account"] for item in results if item["ok"] and item["lambda_exists"]]
    missing = [item["account"] for item in results if item["ok"] and not item["lambda_exists"]]
    errors = [item["account"] for item in results if not item["ok"]]

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        for item in results:
            if not item["ok"]:
                detail = item["error"] or "erro desconhecido"
                print(f'{item["account"]}: ERRO - {detail}')
            elif item["lambda_exists"]:
                print(f'{item["account"]}: OK - Lambda existe')
            else:
                print(f'{item["account"]}: OK - Lambda nao existe')

    log_step(
        f"Concluido: existem={len(exists)} nao_existem={len(missing)} com_erro={len(errors)}"
    )

    if errors:
        return 2
    return 0


def main() -> int:
    args = parse_args()
    return run_check(args)


if __name__ == "__main__":
    raise SystemExit(main())
