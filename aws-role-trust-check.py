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
from os import getenv
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


def _account_record(account_id: str, account_name: str = "") -> dict:
    return {"account": account_id, "account_name": account_name}


def _account_name_headers() -> Set[str]:
    return {"account_name", "accountname", "name", "nome", "nome_da_conta", "account_alias", "alias"}


def _load_accounts_from_csv(accounts_csv: Path) -> List[dict]:
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
        name_index = next(
            (i for i, header in enumerate(headers) if header in _account_name_headers()),
            None,
        )
        values = [
            _account_record(
                row[account_index].strip(),
                row[name_index].strip() if name_index is not None and len(row) > name_index else "",
            )
            for row in rows[1:]
            if len(row) > account_index and row[account_index].strip()
        ]
    else:
        values = [_account_record(first_row[0].strip())] if first_row and first_row[0] else []
        values.extend(
            _account_record(row[0].strip())
            for row in rows[1:]
            if row and len(row) > 0 and row[0].strip()
        )

    return values


def _dedupe_accounts(values: List[dict]) -> List[dict]:
    seen: Set[str] = set()
    deduped: List[dict] = []
    for value in values:
        account_id = value["account"]
        if not account_id:
            continue
        if account_id not in seen:
            seen.add(account_id)
            deduped.append(value)
    return deduped


def load_accounts(
    accounts_csv: Optional[str],
    accounts_file: Optional[Path],
    accounts_csv_file: Optional[Path],
) -> List[dict]:
    if accounts_csv:
        values = [_account_record(acc.strip()) for acc in accounts_csv.split(",")]
    elif accounts_file:
        values = [_account_record(line.strip()) for line in accounts_file.read_text().splitlines()]
    elif accounts_csv_file:
        values = _load_accounts_from_csv(accounts_csv_file)
    else:
        raise ValueError("Informe --accounts, --accounts-file ou --accounts-csv.")

    accounts: List[dict] = []
    for value in values:
        if value["account"]:
            accounts.append(value)
    accounts = _dedupe_accounts(accounts)
    if not accounts:
        raise ValueError("Nenhuma conta válida encontrada.")
    return accounts


def fill_missing_account_names(accounts: List[dict], source_session: boto3.Session) -> List[dict]:
    missing_name_accounts = [account for account in accounts if not account["account_name"]]
    if not missing_name_accounts:
        return accounts

    try:
        organizations = source_session.client("organizations", region_name="us-east-1")
    except (ClientError, BotoCoreError) as error:
        log_step(f"Nao foi possivel criar cliente AWS Organizations para obter account_name: {error}")
        return accounts

    names = {}
    for account in missing_name_accounts:
        try:
            names[account["account"]] = organizations.describe_account(AccountId=account["account"])["Account"]["Name"]
        except (ClientError, BotoCoreError) as error:
            log_step(f"Nao foi possivel obter account_name da conta {account['account']}: {error}")

    return [
        _account_record(account["account"], account["account_name"] or names.get(account["account"], ""))
        for account in accounts
    ]


def _read_json_path_or_text(raw: str) -> dict:
    raw = raw.strip()
    if not raw:
        return {}

    # If AWS_SECRETS points to a file path, load from file; else parse as JSON string.
    path = Path(raw)
    if path.exists():
        raw = path.read_text(encoding="utf-8")

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("AWS_SECRETS deve ser um JSON válido de mapa.")
    return data


def _resolve_source_credentials() -> dict:
    # Priority 1: explicit AWS_SECRETS (JSON or file path to JSON).
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
            "region": secrets.get("region") or secrets.get("aws_region") or secrets.get("AWS_REGION") or getenv("AWS_REGION"),
        }

    # Priority 2: environment credentials.
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
            "Credenciais da conta origem não encontradas. Defina AWS_SECRETS ou "
            "AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY no ambiente."
        )

    session_region = credentials.get("region") or region_name
    return boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=credentials.get("aws_session_token"),
        region_name=session_region,
    )


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


def role_name_matches_pattern(principal_name: str, required_role_pattern: str) -> bool:
    return (
        fnmatch.fnmatch(principal_name, required_role_pattern)
        or fnmatch.fnmatch(principal_name, f"*/{required_role_pattern}")
    )


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
        return fnmatch.fnmatch(principal, required_role_arn)

    if fnmatch.fnmatch(principal, f"arn:*:iam::*:role/{required_role_name}"):
        return True
    if fnmatch.fnmatch(principal, f"arn:*:iam::*:role/*/{required_role_name}"):
        return True

    principal_name = extract_role_name_from_arn(principal)
    if not principal_name:
        return False
    return role_name_matches_pattern(principal_name, required_role_name)


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


def extract_aws_principals(statement: dict) -> List[str]:
    principal_entry = statement.get("Principal", {})
    if not isinstance(principal_entry, dict):
        return []

    return [principal for principal in as_list(principal_entry.get("AWS")) if principal]


def dedupe_values(values: List[str]) -> List[str]:
    seen: Set[str] = set()
    deduped: List[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


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


def check_trust_policy(iam_session: boto3.Session, trust_role: str, account_id: str, required_role_ref: str) -> dict:
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

    trust_roles: List[str] = []
    has_role = False
    for statement in as_list(statements):
        if statement_allows_assume_role(statement):
            trust_roles.extend(extract_aws_principals(statement))
            if has_required_principal(statement, account_id, required_role_arn, required_role_name):
                has_role = True
    return {"has_role": has_role, "trust_roles": dedupe_values(trust_roles)}


def _is_access_denied_get_role(error: Exception) -> bool:
    if not isinstance(error, ClientError):
        return False
    error_code = (error.response or {}).get("Error", {}).get("Code", "")
    message = (error.response or {}).get("Error", {}).get("Message", "")
    return error_code in {"AccessDenied", "UnauthorizedOperation"} and "iam:GetRole" in message


def _is_no_such_entity(error: Exception) -> bool:
    if not isinstance(error, ClientError):
        return False
    error_code = (error.response or {}).get("Error", {}).get("Code", "")
    return error_code == "NoSuchEntity"


def _extract_error_code(error: Exception) -> str:
    if not isinstance(error, ClientError):
        return ""
    return (error.response or {}).get("Error", {}).get("Code", "")


def _format_access_error(account_id: str, trust_role: str, error: Exception) -> str:
    if _is_access_denied_get_role(error):
        role_arn = f"arn:aws:iam::{account_id}:role/{trust_role}"
        return (
            f"{error}. Ajuste a policy da role assumida para incluir iam:GetRole em {role_arn} "
            "(ou Resource: * apenas para teste). Exemplo IAM action: \"iam:GetRole\"."
        )
    return str(error)


def log_step(message: str) -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] {message}", file=sys.stderr, flush=True)


def check_account(
    index: int,
    account: dict,
    args: argparse.Namespace,
    source_session: boto3.Session,
) -> tuple[int, dict]:
    account_id = account["account"]
    account_name = account["account_name"]
    result = {
        "account": account_id,
        "account_name": account_name,
        "has_role": False,
        "ok": False,
        "error": None,
        "error_code": None,
        "trust_roles": [],
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

        trust_result = check_trust_policy(
            iam_session=assumed_session,
            trust_role=args.trust_role,
            account_id=account_id,
            required_role_ref=args.required_role,
        )
        result["has_role"] = trust_result["has_role"]
        result["trust_roles"] = trust_result["trust_roles"]
        result["ok"] = result["has_role"]
        if result["has_role"]:
            log_step(f"Conta {account_id}: verificacao deu certo. Trust contem a role requerida.")
        else:
            roles = ";".join(result["trust_roles"]) if result["trust_roles"] else "(nenhuma role AWS no trust)"
            log_step(f"Conta {account_id}: verificacao falhou. Trust nao contem a role requerida. Roles no trust: {roles}")
    except (ClientError, BotoCoreError, ValueError) as error:
        result["error_code"] = _extract_error_code(error) or None
        if _is_no_such_entity(error):
            result["error"] = (
                f"NoSuchEntity: a role alvo '{args.trust_role}' não foi encontrada "
                f"na conta {account_id}."
            )
            result["has_role"] = False
            log_step(
                f"Conta {account_id}: role alvo '{args.trust_role}' nao encontrada "
                "(NoSuchEntity)."
            )
            return index, result

        result["error"] = _format_access_error(account_id, args.trust_role, error)
        result["ok"] = False
        log_step(f"Conta {account_id}: erro: {result['error']}")
    return index, result


def run_check(args: argparse.Namespace) -> int:
    accounts = load_accounts(args.accounts, args.accounts_file, args.accounts_csv)
    source_session = build_source_session(args.region)
    accounts = fill_missing_account_names(accounts, source_session)

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
        for index, account in enumerate(accounts):
            future = executor.submit(
                check_account,
                index=index,
                account=account,
                args=args,
                source_session=source_session,
            )
            futures.append(future)

        for future in as_completed(futures):
            index, result = future.result()
            results[index] = result

    missing: Set[str] = set(
        item["account"]
        for item in results
        if isinstance(item, dict)
        and not item["ok"]
        and item["error_code"] in {None, "NoSuchEntity"}
    )
    errors = sum(
        1
        for item in results
        if isinstance(item, dict)
        and item["error"] is not None
        and item["error_code"] not in {None, "NoSuchEntity"}
    )
    if errors > 0:
        had_error = True

    report_path = args.report_csv or f"trust-check-report-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    with open(report_path, "w", encoding="utf-8", newline="") as handler:
        writer = csv.writer(handler)
        writer.writerow(["account", "account_name", "has_role_in_trust", "roles_in_trust", "error"])
        for item in results:
            if item["error"]:
                confirmation = "erro"
            else:
                confirmation = "sim" if item["has_role"] else "nao"
            writer.writerow([
                item["account"],
                item["account_name"],
                confirmation,
                ";".join(item["trust_roles"]),
                item["error"] or "",
            ])
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
            detail = "verificacao deu certo: trust contém role requerida"
            if item["error"]:
                detail = item["error"]
            elif not item["has_role"]:
                detail = "trust não contém role"
            print(f'{item["account"]}: {status} - {detail}')
        contas_ok = [item["account"] for item in results if isinstance(item, dict) and item["ok"]]
        contas_sem_trust = [
            item["account"]
            for item in results
            if isinstance(item, dict) and not item["ok"] and item["error_code"] in {None, "NoSuchEntity"}
        ]
        contas_erro = [
            item["account"]
            for item in results
            if isinstance(item, dict) and item["error"] is not None and item["error_code"] not in {None, "NoSuchEntity"}
        ]
        print(f"Contas com sucesso: {', '.join(contas_ok) if contas_ok else '(nenhuma)'}")
        print(f"Contas sem trust: {', '.join(contas_sem_trust) if contas_sem_trust else '(nenhuma)'}")
        print(f"Contas com erro: {', '.join(contas_erro) if contas_erro else '(nenhuma)'}")

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
