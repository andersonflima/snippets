#!/usr/bin/env python3
"""Check IAM role trust policy across multiple AWS accounts.

Workflow:
1. Assume a role in each target account using source credentials from environment.
2. Fetch trust policy from a target role.
3. Confirm whether the required role appears in the trust policy.
"""

from __future__ import annotations

import csv
import argparse
import json
from datetime import datetime
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Set, Union
import fnmatch

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consulta a trust policy de uma role em múltiplas contas assumindo uma role "
            "de origem comum e validando se uma role esperada existe no trust."
        )
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--accounts",
        help="Lista de contas separadas por vírgula (ex: 111111111111,222222222222)",
    )
    target.add_argument(
        "--accounts-file",
        type=Path,
        help="Arquivo com um account id por linha.",
    )
    target.add_argument(
        "--accounts-csv",
        type=Path,
        help="Arquivo CSV com uma coluna de account id (`account_id`, `accountId`, `account` ou primeira coluna).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Quantidade máxima de contas processadas em paralelo.",
    )
    parser.add_argument(
        "--report-csv",
        help="Arquivo CSV de saída com o resultado por conta (padrão: trust-check-report-<timestamp>.csv).",
    )
    parser.add_argument(
        "--assume-role",
        required=True,
        help="Nome da role a assumir em cada conta-alvo (ex: OrgReadOnly).",
    )
    parser.add_argument(
        "--trust-role",
        required=True,
        help="Nome da role cuja trust policy será conferida.",
    )
    parser.add_argument(
        "--required-role",
        required=True,
        help=(
            "Role esperada no trust. Pode ser ARN completo "
            "('arn:aws:iam::<account_id>:role/<role>') ou apenas o nome da role."
        ),
    )
    parser.add_argument(
        "--external-id",
        help="ExternalId opcional para o AssumeRole.",
    )
    parser.add_argument(
        "--role-session-name",
        default="trust-check",
        help="SessionName do AssumeRole (padrão: trust-check).",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Região para clientes AWS (padrão: us-east-1).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Retorna saída em JSON.",
    )
    return parser.parse_args()


def _load_accounts_from_csv(accounts_csv: Path) -> List[str]:
    with accounts_csv.open(encoding="utf-8-sig", newline="") as handler:
        rows = list(csv.reader(handler))
    if not rows:
        return []

    first_row = rows[0]
    if not first_row:
        return []

    # CSV com cabeçalho: usa a coluna de conta se existir
    headers = [cell.strip().lower().replace("-", "_") for cell in first_row]
    if headers and any(h in {"account_id", "accountid", "account"} for h in headers):
        account_index = next(
            i
            for i, header in enumerate(headers)
            if header in {"account_id", "accountid", "account"}
        )
        values = [row[account_index].strip() for row in rows[1:] if len(row) > account_index]
    else:
        values = [first_row[0].strip()] if first_row and first_row[0] else []
        values.extend(
            row[0].strip()
            for row in rows[1:]
            if row and len(row) > 0 and row[0].strip()
        )

    return values


def load_accounts(
    accounts_csv: Optional[str],
    accounts_file: Optional[Path],
    accounts_csv_file: Optional[Path],
) -> List[str]:
    if accounts_csv:
        values = [acc.strip() for acc in accounts_csv.split(",")]
    elif accounts_file:
        values = [line.strip() for line in accounts_file.read_text().splitlines()]
    elif accounts_csv_file:
        values = _load_accounts_from_csv(accounts_csv_file)
    else:
        raise ValueError("Informe --accounts, --accounts-file ou --accounts-csv.")

    accounts: List[str] = []
    for value in values:
        if value:
            accounts.append(value)
    if not accounts:
        raise ValueError("Nenhuma conta válida encontrada.")
    return accounts


def as_list(value: Union[str, Sequence[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def extract_role_name_from_arn(arn: str) -> Optional[str]:
    if ":role/" not in arn:
        return None
    return arn.split(":role/", 1)[1]


def assume_role_for_account(
    source_session: boto3.Session,
    account_id: str,
    role_name: str,
    role_session_name: str,
    external_id: Optional[str],
    region: str,
) -> boto3.Session:
    sts = source_session.client("sts", region_name=region)
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    params = {
        "RoleArn": role_arn,
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


def is_trust_matching_principal(
    principal: str,
    account_id: str,
    required_role_arn: Optional[str],
    required_role_name: str,
) -> bool:
    if principal == "*":
        return True
    if required_role_arn:
        if principal == required_role_arn:
            return True
        return fnmatch.fnmatch(principal, required_role_arn)

    # fallback by role name when only a name was provided
    principal_name = extract_role_name_from_arn(principal)
    if not principal_name:
        return False
    return principal_name == required_role_name or principal_name.endswith(f"/{required_role_name}")


def has_required_principal(statement: dict, account_id: str, required_role_arn: Optional[str], required_role_name: str) -> bool:
    principal_entry = statement.get("Principal", {})
    if not isinstance(principal_entry, dict):
        return False

    aws_principals = as_list(principal_entry.get("AWS"))
    if not aws_principals:
        return False

    for principal in aws_principals:
        if is_trust_matching_principal(principal, account_id, required_role_arn, required_role_name):
            return True
    return False


def statement_allows_assume_role(statement: dict) -> bool:
    if statement.get("Effect") != "Allow":
        return False

    actions = as_list(statement.get("Action", []))
    if not actions:
        return False

    for action in actions:
        if action == "*" or action == "sts:*" or action == "sts:AssumeRole":
            return True
    return False


def trust_contains_required_role(iam_session: boto3.Session, trust_role: str, account_id: str, required_role_ref: str) -> bool:
    iam = iam_session.client("iam")
    trust = iam.get_role(RoleName=trust_role)["Role"]
    policy = trust["AssumeRolePolicyDocument"]

    statements = policy.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]

    required_role_arn = required_role_ref if required_role_ref.startswith("arn:") else None
    required_role_name = (
        extract_role_name_from_arn(required_role_ref)
        if required_role_arn
        else required_role_ref
    )
    if not required_role_name:
        raise ValueError("required-role inválida. Use ARN completo ou nome de role.")

    for statement in as_list(statements):
        if statement_allows_assume_role(statement):
            if has_required_principal(statement, account_id, required_role_arn, required_role_name):
                return True
    return False


def log_step(message: str) -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] {message}", file=sys.stderr, flush=True)


def check_account(
    index: int,
    account_id: str,
    args: argparse.Namespace,
    source_session: boto3.Session,
) -> tuple[int, dict]:
    result = {"account": account_id, "has_role": False, "ok": False, "error": None}
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

        result["has_role"] = trust_contains_required_role(
            iam_session=assumed_session,
            trust_role=args.trust_role,
            account_id=account_id,
            required_role_ref=args.required_role,
        )
        result["ok"] = result["has_role"]
        log_step(f"Conta {account_id}: trust {'OK' if result['has_role'] else 'FALHOU'}")
    except (ClientError, BotoCoreError, ValueError) as error:
        result["error"] = str(error)
        result["ok"] = False
        log_step(f"Conta {account_id}: erro: {error}")
    return index, result


def run_check(args: argparse.Namespace) -> int:
    accounts = load_accounts(args.accounts, args.accounts_file, args.accounts_csv)
    source_session = boto3.Session(region_name=args.region)

    if args.workers <= 0:
        raise ValueError("workers precisa ser maior que 0.")
    workers = min(args.workers, len(accounts))
    log_step(
        f"Iniciando verificacao de trust: total_contas={len(accounts)} workers={workers} "
        f"assume_role={args.assume_role} trust_role={args.trust_role}"
    )

    results: list[dict] = [None for _ in accounts]  # type: ignore[list-item]
    futures = []
    had_error = False

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for index, account_id in enumerate(accounts):
            future = executor.submit(
                check_account,
                index=index,
                account_id=account_id,
                args=args,
                source_session=source_session,
            )
            futures.append(future)

        for future in as_completed(futures):
            index, result = future.result()
            results[index] = result

    missing: Set[str] = set(
        item["account"] for item in results if isinstance(item, dict) and item["error"] is None and not item["ok"]
    )
    errors = sum(1 for item in results if isinstance(item, dict) and item["error"] is not None)
    if errors > 0:
        had_error = True

    report_path = args.report_csv or f"trust-check-report-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    with open(report_path, "w", encoding="utf-8", newline="") as handler:
        writer = csv.writer(handler)
        writer.writerow(["account", "has_role_in_trust", "error"])
        for item in results:
            if item["error"]:
                confirmation = "erro"
            else:
                confirmation = "sim" if item["has_role"] else "nao"
            writer.writerow([item["account"], confirmation, item["error"] or ""])
    log_step(f"Relatorio gerado: {report_path}")

    if had_error:
        status = (
            f"Concluido com {len(missing)} conta(s) sem role esperada e "
            f"{errors} conta(s) com erro de execucao."
        )
    elif missing:
        status = f"Concluido com {len(missing)} conta(s) sem a role esperada no trust."
    else:
        status = "Concluido com sucesso."
    log_step(status)

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        for item in results:
            status = (
                "OK"
                if item["ok"]
                else "FALHOU"
            )
            detail = "trust contém role"
            if item["error"]:
                detail = item["error"]
            elif not item["has_role"]:
                detail = "trust não contém role"
            print(f'{item["account"]}: {status} - {detail}')

    if had_error:
        return 2
    if missing:
        return 1
    return 0


def main() -> int:
    args = parse_args()
    return run_check(args)


if __name__ == "__main__":
    raise SystemExit(main())
