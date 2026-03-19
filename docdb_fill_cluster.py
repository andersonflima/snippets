from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import AutoReconnect, BulkWriteError, DuplicateKeyError
from pymongo.write_concern import WriteConcern

from benchmark.seed_docdb import (
    GIB,
    MIB,
    batched,
    build_chunk_pool,
    build_document,
    build_payload,
    split_counts,
)


@dataclass(frozen=True)
class ClusterConnectionInfo:
    target_name: str
    endpoint: str
    port: int
    username: str
    database: str
    tls_ca_file: str | None
    uri: str


@dataclass(frozen=True)
class SeedRuntime:
    parallelism: int
    doc_size_bytes: int
    batch_size: int
    pool_chunks: int
    pool_chunk_size_bytes: int
    round_bytes: int
    size_metric: str
    stats_poll_seconds: float
    collection_prefix: str


class ClusterSeedError(RuntimeError):
    pass


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Popula um DocumentDB/MongoDB até atingir o tamanho informado em GiB."
    )
    parser.add_argument("--uri", required=True, help="URI direta do MongoDB/DocumentDB.")
    parser.add_argument("--gib", required=True, type=float, help="Tamanho alvo do banco em GiB.")
    parser.add_argument("--target-name", help="Nome lógico do target. Default: direct-target.")
    parser.add_argument(
        "--database",
        help="Database alvo. Se omitido, usa o database da URI, env ou `benchmark`.",
    )
    parser.add_argument("--tls-ca-file", help="Arquivo CA para conexão TLS com DocumentDB.")
    parser.add_argument(
        "--size-metric",
        choices=("dataSize", "storageSize"),
        default="dataSize",
        help="Métrica de `dbStats` usada para decidir quando parar.",
    )
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--doc-size-mib", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--pool-chunks", type=int, default=48)
    parser.add_argument("--pool-chunk-size-mib", type=int, default=1)
    parser.add_argument(
        "--round-mib",
        type=int,
        default=512,
        help="Quantidade máxima de payload planejado por rodada antes de reavaliar `dbStats`.",
    )
    parser.add_argument(
        "--stats-poll-seconds",
        type=float,
        default=1.0,
        help="Tempo de espera antes de reler `dbStats` após cada rodada.",
    )
    parser.add_argument(
        "--collection-prefix",
        help="Prefixo das collections criadas neste run. Se omitido, gera um valor com timestamp.",
    )
    parser.add_argument("--report-file", help="Arquivo JSON opcional com o relatório final.")
    return parser.parse_args(argv)


def normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def first_env_value(names: Sequence[str]) -> str | None:
    for name in names:
        candidate = normalize_optional_text(os.getenv(name))
        if candidate is not None:
            return candidate
    return None


def require_positive(value: int | float, label: str) -> int | float:
    if value <= 0:
        raise ClusterSeedError(f"{label} precisa ser maior que zero.")
    return value


def sanitize_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    compacted = re.sub(r"-{2,}", "-", normalized)
    return compacted.strip("-") or "documentdb"


def resolve_uri_tail(uri: str) -> str:
    _, separator, tail = uri.partition("://")
    return tail if separator else uri


def extract_uri_authority(uri: str) -> str:
    return resolve_uri_tail(uri).partition("/")[0]


def extract_uri_username(uri: str) -> str | None:
    authority = extract_uri_authority(uri)
    user_info, separator, _ = authority.rpartition("@")
    if not separator:
        return None
    username, _, _ = user_info.partition(":")
    return normalize_optional_text(username)


def extract_uri_primary_host(uri: str) -> tuple[str, int]:
    authority = extract_uri_authority(uri)
    host_list = authority.rpartition("@")[2] or authority
    primary_host = host_list.split(",", 1)[0]
    host, separator, raw_port = primary_host.rpartition(":")
    if not separator or not raw_port.isdigit():
        return primary_host, 27017
    return host or primary_host, int(raw_port)


def extract_uri_database(uri: str) -> str | None:
    path = resolve_uri_tail(uri).partition("/")[2]
    if not path:
        return None
    database = path.split("?", 1)[0].strip("/")
    return normalize_optional_text(database)


def resolve_database(explicit_database: str | None, uri: str) -> str:
    return (
        normalize_optional_text(explicit_database)
        or extract_uri_database(uri)
        or first_env_value(("DOCDB_FILL_CLUSTER_DEFAULT_DATABASE", "DOCDB_SNAPSHOT_DEFAULT_DATABASE"))
        or "benchmark"
    )


def resolve_tls_ca_file(explicit_tls_ca_file: str | None) -> str | None:
    return normalize_optional_text(explicit_tls_ca_file) or first_env_value(
        ("DOCDB_FILL_CLUSTER_TLS_CA_FILE", "DOCDB_SNAPSHOT_TLS_CA_FILE")
    )


def build_default_collection_prefix(target_name: str, started_at: datetime) -> str:
    return f"fill_{sanitize_name(target_name)}_{started_at.strftime('%Y%m%dT%H%M%SZ').lower()}"


def resolve_connection_from_args(args: argparse.Namespace) -> ClusterConnectionInfo:
    uri = normalize_optional_text(args.uri)
    if uri is None:
        raise ClusterSeedError("`--uri` é obrigatório.")
    endpoint, port = extract_uri_primary_host(uri)
    return ClusterConnectionInfo(
        target_name=normalize_optional_text(args.target_name) or "direct-target",
        endpoint=endpoint,
        port=port,
        username=extract_uri_username(uri) or "direct-user",
        database=resolve_database(args.database, uri),
        tls_ca_file=resolve_tls_ca_file(args.tls_ca_file),
        uri=uri,
    )


def build_mongo_client(uri: str, tls_ca_file: str | None, app_name: str) -> MongoClient[Any]:
    client_options: dict[str, Any] = {
        "appname": app_name,
        "retryWrites": False,
        "connectTimeoutMS": 10_000,
        "serverSelectionTimeoutMS": 10_000,
        "socketTimeoutMS": 300_000,
    }
    if tls_ca_file:
        client_options["tls"] = True
        client_options["tlsCAFile"] = tls_ca_file
    return MongoClient(uri, **client_options)


def build_collection(
    uri: str,
    tls_ca_file: str | None,
    database_name: str,
    collection_name: str,
) -> tuple[MongoClient[Any], Collection[Any]]:
    client = build_mongo_client(uri, tls_ca_file, app_name="docdb-fill-cluster")
    collection = client.get_database(database_name).get_collection(
        collection_name,
        write_concern=WriteConcern(w=1, j=False),
    )
    return client, collection


def read_database_stats_from_connection(
    uri: str,
    tls_ca_file: str | None,
    database_name: str,
) -> dict[str, Any]:
    client = build_mongo_client(uri, tls_ca_file, app_name="docdb-fill-cluster-stats")
    try:
        database = client.get_database(database_name)
        stats = database.command("dbStats")
        collection_names = database.list_collection_names()
        return {
            "collections": len(collection_names),
            "dataSize": int(stats.get("dataSize", 0)),
            "storageSize": int(stats.get("storageSize", 0)),
            "objects": int(stats.get("objects", 0)),
            "avgObjSize": float(stats.get("avgObjSize", 0.0) or 0.0),
        }
    finally:
        client.close()


def metric_bytes(stats: dict[str, Any], size_metric: str) -> int:
    return int(stats.get(size_metric, 0))


def build_documents_from_offset(
    collection_name: str,
    worker_index: int,
    start_sequence: int,
    document_count: int,
    pool: Sequence[bytes],
    doc_size_bytes: int,
):
    chunk_size_bytes = len(pool[0])
    chunk_count_per_doc = doc_size_bytes // chunk_size_bytes
    if chunk_count_per_doc <= 0:
        raise ClusterSeedError("doc_size_mib precisa ser maior ou igual ao tamanho do chunk.")
    for sequence_offset in range(document_count):
        sequence = start_sequence + sequence_offset
        payload = build_payload(pool, chunk_count_per_doc, worker_index, sequence)
        yield build_document(collection_name, worker_index, sequence, payload)


def insert_document(collection: Collection[Any], document: dict[str, Any], attempts: int = 4) -> int:
    for attempt in range(1, attempts + 1):
        try:
            collection.insert_one(document, bypass_document_validation=True)
            return 1
        except DuplicateKeyError:
            return 1
        except AutoReconnect:
            if attempt == attempts:
                raise
            time.sleep(attempt)
    return 0


def insert_batch(collection: Collection[Any], batch: Sequence[dict[str, Any]], attempts: int = 3) -> int:
    for attempt in range(1, attempts + 1):
        try:
            collection.insert_many(
                list(batch),
                ordered=False,
                bypass_document_validation=True,
            )
            return len(batch)
        except BulkWriteError as exc:
            duplicated_only = all(
                item.get("code") == 11000 for item in exc.details.get("writeErrors", [])
            )
            if duplicated_only:
                return len(batch)
            raise
        except AutoReconnect:
            if attempt == attempts:
                break
            time.sleep(attempt)
    return sum(insert_document(collection, document) for document in batch)


def seed_worker(
    connection: ClusterConnectionInfo,
    collection_name: str,
    worker_index: int,
    start_sequence: int,
    document_count: int,
    runtime: SeedRuntime,
) -> dict[str, Any]:
    client, collection = build_collection(
        connection.uri,
        connection.tls_ca_file,
        connection.database,
        collection_name,
    )
    try:
        pool = build_chunk_pool(
            worker_index,
            runtime.pool_chunks,
            runtime.pool_chunk_size_bytes,
        )
        started_at = time.perf_counter()
        inserted_count = 0
        inserted_bytes = 0
        for batch in batched(
            build_documents_from_offset(
                collection_name=collection_name,
                worker_index=worker_index,
                start_sequence=start_sequence,
                document_count=document_count,
                pool=pool,
                doc_size_bytes=runtime.doc_size_bytes,
            ),
            runtime.batch_size,
        ):
            inserted_documents = insert_batch(collection, batch)
            inserted_count += inserted_documents
            inserted_bytes += sum(int(item["size_bytes"]) for item in batch[:inserted_documents])
        finished_at = time.perf_counter()
        return {
            "collection": collection_name,
            "worker_index": worker_index,
            "start_sequence": start_sequence,
            "documents": inserted_count,
            "payload_bytes": inserted_bytes,
            "elapsed_seconds": round(finished_at - started_at, 3),
        }
    finally:
        client.close()


def planned_round_bytes(remaining_bytes: int, runtime: SeedRuntime) -> int:
    return max(runtime.doc_size_bytes, min(remaining_bytes, runtime.round_bytes))


def build_runtime(
    args: argparse.Namespace,
    started_at: datetime,
    target_name: str,
) -> SeedRuntime:
    parallelism = int(require_positive(args.parallelism, "parallelism"))
    doc_size_mib = int(require_positive(args.doc_size_mib, "doc-size-mib"))
    batch_size = int(require_positive(args.batch_size, "batch-size"))
    pool_chunks = int(require_positive(args.pool_chunks, "pool-chunks"))
    pool_chunk_size_mib = int(require_positive(args.pool_chunk_size_mib, "pool-chunk-size-mib"))
    round_mib = int(require_positive(args.round_mib, "round-mib"))
    stats_poll_seconds = float(require_positive(args.stats_poll_seconds, "stats-poll-seconds"))
    collection_prefix = args.collection_prefix or build_default_collection_prefix(
        target_name,
        started_at,
    )
    return SeedRuntime(
        parallelism=parallelism,
        doc_size_bytes=doc_size_mib * MIB,
        batch_size=batch_size,
        pool_chunks=pool_chunks,
        pool_chunk_size_bytes=pool_chunk_size_mib * MIB,
        round_bytes=round_mib * MIB,
        size_metric=args.size_metric,
        stats_poll_seconds=stats_poll_seconds,
        collection_prefix=collection_prefix,
    )


def worker_collection_name(collection_prefix: str, worker_index: int) -> str:
    return f"{collection_prefix}_{worker_index:02d}"


def build_round_plan(remaining_bytes: int, runtime: SeedRuntime) -> tuple[int, tuple[int, ...]]:
    round_payload_bytes = planned_round_bytes(remaining_bytes, runtime)
    total_documents = max(1, math.ceil(round_payload_bytes / runtime.doc_size_bytes))
    return round_payload_bytes, split_counts(total_documents, runtime.parallelism)


def run_seed_round(
    connection: ClusterConnectionInfo,
    runtime: SeedRuntime,
    next_sequences: Sequence[int],
    distribution: Sequence[int],
) -> tuple[dict[str, Any], ...]:
    with ThreadPoolExecutor(
        max_workers=runtime.parallelism,
        thread_name_prefix="docdb-fill",
    ) as executor:
        futures = [
            executor.submit(
                seed_worker,
                connection=connection,
                collection_name=worker_collection_name(runtime.collection_prefix, worker_index),
                worker_index=worker_index,
                start_sequence=next_sequences[worker_index],
                document_count=document_count,
                runtime=runtime,
            )
            for worker_index, document_count in enumerate(distribution)
            if document_count > 0
        ]
        return tuple(future.result() for future in futures)


def next_sequence_offsets(
    current_offsets: Sequence[int],
    worker_reports: Sequence[dict[str, Any]],
) -> tuple[int, ...]:
    updates = {int(report["worker_index"]): int(report["documents"]) for report in worker_reports}
    return tuple(current_offsets[index] + updates.get(index, 0) for index in range(len(current_offsets)))


def print_round_progress(
    round_number: int,
    size_metric: str,
    before_bytes: int,
    after_bytes: int,
    target_bytes: int,
    planned_payload_bytes: int,
) -> None:
    print(
        json.dumps(
            {
                "event": "round",
                "round": round_number,
                "metric": size_metric,
                "before_bytes": before_bytes,
                "after_bytes": after_bytes,
                "target_bytes": target_bytes,
                "planned_payload_bytes": planned_payload_bytes,
                "before_gib": round(before_bytes / GIB, 3),
                "after_gib": round(after_bytes / GIB, 3),
                "target_gib": round(target_bytes / GIB, 3),
            },
            ensure_ascii=False,
        )
    )


def build_report(
    connection: ClusterConnectionInfo,
    runtime: SeedRuntime,
    target_gib: float,
    target_bytes: int,
    start_stats: dict[str, Any],
    final_stats: dict[str, Any],
    round_reports: Sequence[dict[str, Any]],
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    return {
        "target_name": connection.target_name,
        "endpoint": connection.endpoint,
        "port": connection.port,
        "database": connection.database,
        "username": connection.username,
        "collection_prefix": runtime.collection_prefix,
        "size_metric": runtime.size_metric,
        "target_gib": target_gib,
        "target_bytes": target_bytes,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "start_stats": start_stats,
        "final_stats": final_stats,
        "start_metric_bytes": metric_bytes(start_stats, runtime.size_metric),
        "final_metric_bytes": metric_bytes(final_stats, runtime.size_metric),
        "documents_inserted": sum(
            int(worker_report["documents"])
            for round_report in round_reports
            for worker_report in round_report["worker_reports"]
        ),
        "payload_bytes_inserted": sum(
            int(worker_report["payload_bytes"])
            for round_report in round_reports
            for worker_report in round_report["worker_reports"]
        ),
        "rounds": list(round_reports),
    }


def seed_database_to_target(
    connection: ClusterConnectionInfo,
    runtime: SeedRuntime,
    target_gib: float,
    report_file: str | None = None,
) -> dict[str, Any]:
    target_bytes = math.ceil(require_positive(target_gib, "gib") * GIB)
    started_at = datetime.now(timezone.utc)
    start_stats = read_database_stats_from_connection(
        connection.uri,
        connection.tls_ca_file,
        connection.database,
    )
    current_metric_bytes = metric_bytes(start_stats, runtime.size_metric)
    next_sequences = tuple(0 for _ in range(runtime.parallelism))
    round_number = 0
    round_reports: list[dict[str, Any]] = []

    while current_metric_bytes < target_bytes:
        round_number += 1
        remaining_bytes = target_bytes - current_metric_bytes
        round_payload_bytes, distribution = build_round_plan(remaining_bytes, runtime)
        worker_reports = run_seed_round(connection, runtime, next_sequences, distribution)
        next_sequences = next_sequence_offsets(next_sequences, worker_reports)
        time.sleep(runtime.stats_poll_seconds)
        current_stats = read_database_stats_from_connection(
            connection.uri,
            connection.tls_ca_file,
            connection.database,
        )
        updated_metric_bytes = metric_bytes(current_stats, runtime.size_metric)
        round_report = {
            "round": round_number,
            "remaining_bytes_before": remaining_bytes,
            "planned_payload_bytes": round_payload_bytes,
            "distribution": list(distribution),
            "metric_bytes_before": current_metric_bytes,
            "metric_bytes_after": updated_metric_bytes,
            "worker_reports": list(worker_reports),
            "db_stats": current_stats,
        }
        round_reports.append(round_report)
        print_round_progress(
            round_number=round_number,
            size_metric=runtime.size_metric,
            before_bytes=current_metric_bytes,
            after_bytes=updated_metric_bytes,
            target_bytes=target_bytes,
            planned_payload_bytes=round_payload_bytes,
        )
        current_metric_bytes = updated_metric_bytes

    finished_at = datetime.now(timezone.utc)
    final_stats = round_reports[-1]["db_stats"] if round_reports else start_stats
    report = build_report(
        connection=connection,
        runtime=runtime,
        target_gib=target_gib,
        target_bytes=target_bytes,
        start_stats=start_stats,
        final_stats=final_stats,
        round_reports=round_reports,
        started_at=started_at,
        finished_at=finished_at,
    )
    if report_file:
        report_path = Path(report_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        started_at = datetime.now(timezone.utc)
        connection = resolve_connection_from_args(args)
        runtime = build_runtime(args, started_at, connection.target_name)
        print(
            json.dumps(
                {
                    "event": "connection",
                    "target_name": connection.target_name,
                    "endpoint": connection.endpoint,
                    "port": connection.port,
                    "database": connection.database,
                    "size_metric": runtime.size_metric,
                    "target_gib": args.gib,
                    "collection_prefix": runtime.collection_prefix,
                },
                ensure_ascii=False,
            )
        )
        report = seed_database_to_target(
            connection=connection,
            runtime=runtime,
            target_gib=args.gib,
            report_file=args.report_file,
        )
        print(
            json.dumps(
                {
                    "event": "completed",
                    "target_name": report["target_name"],
                    "database": report["database"],
                    "size_metric": report["size_metric"],
                    "final_metric_bytes": report["final_metric_bytes"],
                    "final_metric_gib": round(report["final_metric_bytes"] / GIB, 3),
                    "target_gib": report["target_gib"],
                    "documents_inserted": report["documents_inserted"],
                    "payload_bytes_inserted": report["payload_bytes_inserted"],
                    "collection_prefix": report["collection_prefix"],
                },
                ensure_ascii=False,
            )
        )
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except ClusterSeedError as exc:
        print(json.dumps({"event": "error", "message": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
