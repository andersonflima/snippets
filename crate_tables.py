#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
import time
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterator, Sequence


GIB = 1024 ** 3
MIB = 1024 ** 2
KIB = 1024
MAX_DYNAMODB_ITEM_BYTES = 400 * KIB
MAX_SAFE_ITEM_KIB = 350
DEFAULT_ITEM_KIB = 320
DEFAULT_PROGRESS_EVERY_MIB = 512
DEFAULT_WORKERS = 4
DEFAULT_TABLE_WRITE_WORKERS = 4
BATCH_WRITE_LIMIT = 25
PARTITION_KEY_NAME = "pk"
PAYLOAD_ATTRIBUTE_NAME = "payload"
PAYLOAD_FILL_CHARACTER = "x"
AWS_RESOURCE_NOT_FOUND = "ResourceNotFoundException"
AWS_RESOURCE_IN_USE = "ResourceInUseException"
TABLE_STATUS_ACTIVE = "ACTIVE"
PendingWrite = tuple[Future[None], int, int]


def positive_int(value: str) -> int:
    try:
        parsed_value = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"valor inteiro invalido: {value}") from error
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("o valor precisa ser maior que zero")
    return parsed_value


def positive_decimal(value: str) -> Decimal:
    try:
        parsed_value = Decimal(value)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError(f"valor decimal invalido: {value}") from error
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("o valor precisa ser maior que zero")
    return parsed_value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cria tabelas DynamoDB e popula cada uma ate um volume aproximado em GiB."
    )
    parser.add_argument("--tables", required=True, type=positive_int, help="Quantidade de tabelas a criar.")
    parser.add_argument(
        "--gib",
        required=True,
        type=positive_decimal,
        help="Volume aproximado em GiB por tabela.",
    )
    parser.add_argument(
        "--table-prefix",
        default=None,
        help="Prefixo das tabelas. Quando omitido, usa 'tabela'.",
    )
    parser.add_argument(
        "--item-kib",
        default=DEFAULT_ITEM_KIB,
        type=positive_int,
        help="Tamanho alvo de cada item em KiB. Padrao: 320.",
    )
    parser.add_argument(
        "--workers",
        default=DEFAULT_WORKERS,
        type=positive_int,
        help="Quantidade de tabelas processadas em paralelo. Padrao: 4.",
    )
    parser.add_argument(
        "--table-write-workers",
        default=DEFAULT_TABLE_WRITE_WORKERS,
        type=positive_int,
        help="Quantidade de batches escritos em paralelo por tabela. Padrao: 4.",
    )
    parser.add_argument("--region", default=None, help="Regiao AWS para criar as tabelas.")
    parser.add_argument("--profile", default=None, help="AWS profile do boto3.")
    parser.add_argument(
        "--progress-every-mib",
        default=DEFAULT_PROGRESS_EVERY_MIB,
        type=positive_int,
        help="Intervalo de progresso por tabela em MiB. Padrao: 512.",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        default=900,
        type=positive_int,
        help="Tempo maximo para esperar a tabela ficar ACTIVE. Padrao: 900.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o plano sem criar tabelas nem gravar dados.",
    )
    return parser


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.item_kib > MAX_SAFE_ITEM_KIB:
        parser.error(
            f"--item-kib precisa ser menor ou igual a {MAX_SAFE_ITEM_KIB} para manter margem segura abaixo de 400 KiB por item."
        )
    return args


def build_default_prefix() -> str:
    return "tabela"


def build_table_names(prefix: str, count: int) -> list[str]:
    return [f"{prefix}{index}" for index in range(1, count + 1)]


def bytes_from_gib(gib: Decimal) -> int:
    return int((gib * Decimal(GIB)).to_integral_value(rounding=ROUND_HALF_UP))


def bytes_from_kib(kib: int) -> int:
    return kib * KIB


def format_gib(value: int) -> str:
    return f"{value / GIB:.2f} GiB"


def format_mib(value: int) -> str:
    return f"{value / MIB:.1f} MiB"


def build_partition_key(table_name: str, item_index: int) -> str:
    return f"{table_name}#{item_index:08d}"


def estimate_item_size_bytes(item: dict[str, str]) -> int:
    return sum(len(key.encode("utf-8")) + len(value.encode("utf-8")) for key, value in item.items())


def minimum_item_size_bytes(
    table_name: str,
    *,
    partition_key_name: str = PARTITION_KEY_NAME,
    payload_attribute_name: str = PAYLOAD_ATTRIBUTE_NAME,
) -> int:
    return estimate_item_size_bytes(
        {
            partition_key_name: build_partition_key(table_name, 1),
            payload_attribute_name: "",
        }
    )


def calculate_item_count(total_bytes: int, max_item_bytes: int) -> int:
    return max(1, math.ceil(total_bytes / max_item_bytes))


def iter_item_target_sizes(total_bytes: int, max_item_bytes: int) -> Iterator[int]:
    item_count = calculate_item_count(total_bytes, max_item_bytes)
    base_size, remainder = divmod(total_bytes, item_count)
    for index in range(item_count):
        yield base_size + (1 if index < remainder else 0)


def build_item_for_target_size(
    table_name: str,
    item_index: int,
    target_bytes: int,
    *,
    partition_key_name: str = PARTITION_KEY_NAME,
    payload_attribute_name: str = PAYLOAD_ATTRIBUTE_NAME,
) -> dict[str, str]:
    item = {
        partition_key_name: build_partition_key(table_name, item_index),
        payload_attribute_name: "",
    }
    base_size = estimate_item_size_bytes(item)
    if target_bytes < base_size:
        raise ValueError(
            f"nao foi possivel montar um item de {target_bytes} bytes para {table_name}; minimo necessario: {base_size}"
        )
    item[payload_attribute_name] = PAYLOAD_FILL_CHARACTER * (target_bytes - base_size)
    return item


def to_dynamodb_item(item: dict[str, str]) -> dict[str, dict[str, str]]:
    return {key: {"S": value} for key, value in item.items()}


def aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    details = response.get("Error")
    if not isinstance(details, dict):
        return None
    code = details.get("Code")
    return str(code) if code else None


def build_session(profile_name: str | None, region_name: str | None) -> Any:
    try:
        import boto3
    except ModuleNotFoundError as error:
        raise RuntimeError("boto3 nao encontrado. Instale boto3 antes de executar o script.") from error

    session_kwargs: dict[str, str] = {}
    if profile_name:
        session_kwargs["profile_name"] = profile_name
    if region_name:
        session_kwargs["region_name"] = region_name
    return boto3.session.Session(**session_kwargs)


def build_dynamodb_client(
    profile_name: str | None,
    region_name: str | None,
    *,
    max_pool_connections: int,
) -> Any:
    session = build_session(profile_name, region_name)
    from botocore.config import Config

    config = Config(max_pool_connections=max(10, max_pool_connections))
    if region_name:
        return session.client("dynamodb", region_name=region_name, config=config)
    return session.client("dynamodb", config=config)


def create_table(client: Any, table_name: str) -> None:
    try:
        client.create_table(
            TableName=table_name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": PARTITION_KEY_NAME, "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": PARTITION_KEY_NAME, "KeyType": "HASH"},
            ],
        )
    except Exception as error:
        if aws_error_code(error) == AWS_RESOURCE_IN_USE:
            raise RuntimeError(f"a tabela {table_name} ja existe. Use outro --table-prefix.") from error
        raise


def wait_for_table_active(client: Any, table_name: str, timeout_seconds: int) -> None:
    started_at = time.time()
    while time.time() - started_at <= timeout_seconds:
        try:
            response = client.describe_table(TableName=table_name)
        except Exception as error:
            if aws_error_code(error) == AWS_RESOURCE_NOT_FOUND:
                time.sleep(2)
                continue
            raise
        status = response.get("Table", {}).get("TableStatus")
        if status == TABLE_STATUS_ACTIVE:
            return
        time.sleep(5)
    raise TimeoutError(f"a tabela {table_name} nao ficou ACTIVE em ate {timeout_seconds} segundos")


def write_batch(client: Any, table_name: str, items: list[dict[str, str]]) -> None:
    request_items = {
        table_name: [
            {"PutRequest": {"Item": to_dynamodb_item(item)}}
            for item in items
        ]
    }
    attempt = 0
    while True:
        response = client.batch_write_item(RequestItems=request_items)
        unprocessed_items = response.get("UnprocessedItems", {}).get(table_name, [])
        if not unprocessed_items:
            return
        attempt += 1
        time.sleep(min(5.0, 0.25 * (2 ** attempt)))
        request_items = {table_name: unprocessed_items}


def update_progress(
    *,
    table_name: str,
    target_table_bytes: int,
    progress_every_bytes: int,
    next_progress_bytes: int,
    written_bytes: int,
    items_written: int,
) -> int:
    while progress_every_bytes > 0 and written_bytes >= next_progress_bytes:
        print(
            f"[{table_name}] {format_mib(written_bytes)} de {format_gib(target_table_bytes)} gravados, {items_written} itens",
            flush=True,
        )
        next_progress_bytes += progress_every_bytes

    return next_progress_bytes


def iter_write_batches(
    table_name: str,
    target_table_bytes: int,
    max_item_bytes: int,
) -> Iterator[tuple[list[dict[str, str]], int]]:
    batch_items: list[dict[str, str]] = []
    batch_bytes = 0

    for item_index, target_item_bytes in enumerate(
        iter_item_target_sizes(target_table_bytes, max_item_bytes),
        start=1,
    ):
        batch_items.append(build_item_for_target_size(table_name, item_index, target_item_bytes))
        batch_bytes += target_item_bytes

        if len(batch_items) != BATCH_WRITE_LIMIT:
            continue

        yield batch_items, batch_bytes
        batch_items = []
        batch_bytes = 0

    if batch_items:
        yield batch_items, batch_bytes


def settle_pending_writes(
    pending_writes: list[PendingWrite],
    *,
    return_when: Any,
    table_name: str,
    target_table_bytes: int,
    progress_every_bytes: int,
    next_progress_bytes: int,
    written_bytes: int,
    items_written: int,
) -> tuple[list[PendingWrite], int, int, int]:
    if not pending_writes:
        return pending_writes, written_bytes, items_written, next_progress_bytes

    done_futures, _ = wait(
        [future for future, _, _ in pending_writes],
        return_when=return_when,
    )
    done_lookup = set(done_futures)
    remaining_writes: list[PendingWrite] = []

    for future, batch_bytes, batch_items_count in pending_writes:
        if future not in done_lookup:
            remaining_writes.append((future, batch_bytes, batch_items_count))
            continue

        future.result()
        written_bytes += batch_bytes
        items_written += batch_items_count
        next_progress_bytes = update_progress(
            table_name=table_name,
            target_table_bytes=target_table_bytes,
            progress_every_bytes=progress_every_bytes,
            next_progress_bytes=next_progress_bytes,
            written_bytes=written_bytes,
            items_written=items_written,
        )

    return remaining_writes, written_bytes, items_written, next_progress_bytes


def populate_table(
    client: Any,
    table_name: str,
    target_table_bytes: int,
    max_item_bytes: int,
    progress_every_bytes: int,
    table_write_workers: int,
) -> dict[str, int]:
    total_items = calculate_item_count(target_table_bytes, max_item_bytes)
    next_progress_bytes = progress_every_bytes if progress_every_bytes > 0 else sys.maxsize
    written_bytes = 0
    items_written = 0
    max_pending_writes = max(1, table_write_workers * 2)
    pending_writes: list[PendingWrite] = []

    with ThreadPoolExecutor(max_workers=table_write_workers) as executor:
        try:
            for batch_items, batch_bytes in iter_write_batches(table_name, target_table_bytes, max_item_bytes):
                pending_writes.append(
                    (
                        executor.submit(write_batch, client, table_name, batch_items),
                        batch_bytes,
                        len(batch_items),
                    )
                )

                if len(pending_writes) < max_pending_writes:
                    continue

                pending_writes, written_bytes, items_written, next_progress_bytes = settle_pending_writes(
                    pending_writes,
                    return_when=FIRST_COMPLETED,
                    table_name=table_name,
                    target_table_bytes=target_table_bytes,
                    progress_every_bytes=progress_every_bytes,
                    next_progress_bytes=next_progress_bytes,
                    written_bytes=written_bytes,
                    items_written=items_written,
                )

            _, written_bytes, items_written, next_progress_bytes = settle_pending_writes(
                pending_writes,
                return_when=ALL_COMPLETED,
                table_name=table_name,
                target_table_bytes=target_table_bytes,
                progress_every_bytes=progress_every_bytes,
                next_progress_bytes=next_progress_bytes,
                written_bytes=written_bytes,
                items_written=items_written,
            )
        except Exception:
            for future, _, _ in pending_writes:
                future.cancel()
            raise

    print(
        f"[{table_name}] concluida: {items_written} itens, {format_gib(written_bytes)} gravados",
        flush=True,
    )

    return {
        "bytes_written": written_bytes,
        "items_written": items_written,
        "planned_items": total_items,
    }


def create_and_populate_table(
    table_name: str,
    *,
    profile_name: str | None,
    region_name: str | None,
    target_table_bytes: int,
    max_item_bytes: int,
    progress_every_bytes: int,
    table_write_workers: int,
    wait_timeout_seconds: int,
) -> dict[str, int | str]:
    client = build_dynamodb_client(
        profile_name,
        region_name,
        max_pool_connections=table_write_workers * 2,
    )
    print(f"[{table_name}] criando tabela", flush=True)
    create_table(client, table_name)
    wait_for_table_active(client, table_name, wait_timeout_seconds)
    print(
        f"[{table_name}] tabela ACTIVE, iniciando carga de {format_gib(target_table_bytes)}",
        flush=True,
    )
    stats = populate_table(
        client=client,
        table_name=table_name,
        target_table_bytes=target_table_bytes,
        max_item_bytes=max_item_bytes,
        progress_every_bytes=progress_every_bytes,
        table_write_workers=table_write_workers,
    )
    return {"table_name": table_name, **stats}


def print_plan(
    *,
    table_prefix: str,
    tables: int,
    gib_per_table: Decimal,
    target_table_bytes: int,
    max_item_bytes: int,
    workers: int,
    table_write_workers: int,
    region_name: str | None,
    dry_run: bool,
) -> None:
    estimated_items = calculate_item_count(target_table_bytes, max_item_bytes)
    total_bytes = target_table_bytes * tables
    print("Plano de execucao", flush=True)
    print(f"- prefixo: {table_prefix}", flush=True)
    print(f"- tabelas: {tables}", flush=True)
    print(f"- volume por tabela: {gib_per_table} GiB ({target_table_bytes} bytes)", flush=True)
    print(f"- volume total planejado: {format_gib(total_bytes)}", flush=True)
    print(f"- tamanho alvo do item: {format_mib(max_item_bytes)}", flush=True)
    print(f"- itens estimados por tabela: {estimated_items}", flush=True)
    print(f"- workers: {workers}", flush=True)
    print(f"- writers por tabela: {table_write_workers}", flush=True)
    print(f"- regiao: {region_name or 'padrao do boto3'}", flush=True)
    print(f"- dry-run: {'sim' if dry_run else 'nao'}", flush=True)


def print_summary(
    successes: list[dict[str, int | str]],
    failures: list[tuple[str, Exception]],
) -> None:
    if successes:
        total_tables = len(successes)
        total_bytes = sum(int(result["bytes_written"]) for result in successes)
        total_items = sum(int(result["items_written"]) for result in successes)
        print("Resumo", flush=True)
        print(f"- tabelas concluidas: {total_tables}", flush=True)
        print(f"- itens gravados: {total_items}", flush=True)
        print(f"- volume gravado: {format_gib(total_bytes)}", flush=True)
    if failures:
        print("Falhas", flush=True)
        for table_name, error in failures:
            print(f"- {table_name}: {error}", flush=True)


def run(args: argparse.Namespace) -> int:
    table_prefix = args.table_prefix or build_default_prefix()
    table_names = build_table_names(table_prefix, args.tables)
    target_table_bytes = bytes_from_gib(args.gib)
    max_item_bytes = bytes_from_kib(args.item_kib)
    progress_every_bytes = args.progress_every_mib * MIB

    minimum_size = minimum_item_size_bytes(table_names[0])
    if target_table_bytes < minimum_size:
        raise ValueError(
            f"--gib muito baixo para gerar ao menos um item. Minimo necessario para este prefixo: {minimum_size} bytes"
        )

    print_plan(
        table_prefix=table_prefix,
        tables=args.tables,
        gib_per_table=args.gib,
        target_table_bytes=target_table_bytes,
        max_item_bytes=max_item_bytes,
        workers=min(args.workers, args.tables),
        table_write_workers=args.table_write_workers,
        region_name=args.region,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        for table_name in table_names:
            print(f"- {table_name}", flush=True)
        return 0

    successes: list[dict[str, int | str]] = []
    failures: list[tuple[str, Exception]] = []
    max_workers = min(args.workers, args.tables)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(
                create_and_populate_table,
                table_name,
                profile_name=args.profile,
                region_name=args.region,
                target_table_bytes=target_table_bytes,
                max_item_bytes=max_item_bytes,
                progress_every_bytes=progress_every_bytes,
                table_write_workers=args.table_write_workers,
                wait_timeout_seconds=args.wait_timeout_seconds,
            ): table_name
            for table_name in table_names
        }

        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                result = future.result()
                successes.append(result)
            except Exception as error:
                failures.append((table_name, error))
                print(f"[{table_name}] falhou: {error}", file=sys.stderr, flush=True)

    print_summary(successes, failures)
    return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = parse_args(resolved_argv)
        return run(args)
    except Exception as error:
        print(f"Erro: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
