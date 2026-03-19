from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import re
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Iterator, Sequence, TypeVar

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import AutoReconnect, BulkWriteError, DuplicateKeyError
from pymongo.write_concern import WriteConcern

GIB = 1024 ** 3
MIB = 1024 ** 2
T = TypeVar("T")
_PAYLOAD_VARIANT_CACHE: dict[tuple[int, int, int, int, int], tuple[bytes, ...]] = {}
_PAYLOAD_VARIANT_CACHE_LOCK = Lock()


@dataclass(frozen=True)
class ClusterConnectionInfo:
    target_name: str
    endpoint: str
    port: int
    username: str
    database: str
    tls_ca_file: str | None
    tls_allow_invalid_hostnames: bool
    tls_allow_invalid_certificates: bool
    uri: str


@dataclass(frozen=True)
class SeedRuntime:
    parallelism: int
    insert_threads_per_worker: int
    payload_variants_per_worker: int
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


def split_counts(total: int, parts: int) -> tuple[int, ...]:
    if parts <= 0:
        raise ClusterSeedError("parts precisa ser maior que zero.")
    if total <= 0:
        return tuple(0 for _ in range(parts))
    base, remainder = divmod(total, parts)
    return tuple(base + (1 if index < remainder else 0) for index in range(parts))


def batched(values: Iterable[T], batch_size: int) -> Iterator[tuple[T, ...]]:
    if batch_size <= 0:
        raise ClusterSeedError("batch_size precisa ser maior que zero.")
    iterator = iter(values)
    while True:
        batch = tuple(itertools.islice(iterator, batch_size))
        if not batch:
            return
        yield batch


def _build_chunk(seed: str, size_bytes: int) -> bytes:
    if size_bytes <= 0:
        raise ClusterSeedError("size_bytes precisa ser maior que zero.")
    seed_digest = hashlib.sha256(seed.encode("utf-8")).digest()
    repeated = (seed_digest * ((size_bytes // len(seed_digest)) + 1))[:size_bytes]
    return repeated


def build_chunk_pool(worker_index: int, pool_chunks: int, chunk_size_bytes: int) -> tuple[bytes, ...]:
    if pool_chunks <= 0:
        raise ClusterSeedError("pool_chunks precisa ser maior que zero.")
    return tuple(
        _build_chunk(
            f"wk:{worker_index}:chunk:{chunk_index}",
            chunk_size_bytes,
        )
        for chunk_index in range(pool_chunks)
    )


def build_payload(
    pool: Sequence[bytes],
    chunk_count_per_doc: int,
    worker_index: int,
    sequence: int,
) -> bytes:
    if not pool:
        raise ClusterSeedError("pool não pode estar vazio.")
    if chunk_count_per_doc <= 0:
        raise ClusterSeedError("chunk_count_per_doc precisa ser maior que zero.")
    pool_size = len(pool)
    first_index = ((worker_index + 1) * 131 + sequence * 17) % pool_size
    ordered_chunks = (
        pool[(first_index + offset) % pool_size]
        for offset in range(chunk_count_per_doc)
    )
    return b"".join(ordered_chunks)


def build_document(
    collection_name: str,
    worker_index: int,
    sequence: int,
    payload: bytes,
) -> dict[str, Any]:
    payload_size = len(payload)
    return {
        "_id": f"{collection_name}:{worker_index}:{sequence}",
        "collection": collection_name,
        "worker": worker_index,
        "sequence": sequence,
        "size_bytes": payload_size,
        "payload": payload,
    }


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
        "--tls-allow-invalid-hostnames",
        action="store_true",
        help="Define tlsAllowInvalidHostnames=true no MongoClient (útil em túnel SSH).",
    )
    parser.add_argument(
        "--tls-allow-invalid-certificates",
        action="store_true",
        help="Define tlsAllowInvalidCertificates=true no MongoClient (apenas troubleshooting).",
    )
    parser.add_argument(
        "--size-metric",
        choices=("dataSize", "storageSize"),
        default="dataSize",
        help="Métrica de `dbStats` usada para decidir quando parar.",
    )
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument(
        "--insert-threads-per-worker",
        type=int,
        default=8,
        help="Threads de inserção por worker/collection (usa insert_many em paralelo).",
    )
    parser.add_argument(
        "--payload-variants-per-worker",
        type=int,
        default=4,
        help="Quantidade de payloads pré-montados por worker para reduzir custo de CPU.",
    )
    parser.add_argument("--doc-size-mib", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--pool-chunks", type=int, default=16)
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


def parse_bool_text(value: str | None, default: bool = False) -> bool:
    normalized = normalize_optional_text(value)
    if normalized is None:
        return default
    lowered = normalized.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise ClusterSeedError(f"Valor booleano inválido: {value!r}")


def error_details_enabled() -> bool:
    raw_value = normalize_optional_text(os.getenv("DOCDB_FILL_CLUSTER_DEBUG_ERRORS"))
    if raw_value is None:
        return False
    lowered = raw_value.lower()
    return lowered in {"1", "true", "yes", "y", "on"}


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
    tls_ca_file = normalize_optional_text(explicit_tls_ca_file) or first_env_value(
        ("DOCDB_FILL_CLUSTER_TLS_CA_FILE", "DOCDB_SNAPSHOT_TLS_CA_FILE")
    )
    if tls_ca_file is None:
        return None
    resolved_path = str(Path(tls_ca_file).expanduser().resolve())
    if not Path(resolved_path).is_file():
        raise ClusterSeedError(f"Arquivo de certificado TLS não encontrado: {resolved_path}")
    return resolved_path


def resolve_tls_allow_invalid_hostnames(args: argparse.Namespace) -> bool:
    if bool(args.tls_allow_invalid_hostnames):
        return True
    return parse_bool_text(
        first_env_value(
            (
                "DOCDB_FILL_CLUSTER_TLS_ALLOW_INVALID_HOSTNAMES",
                "DOCDB_SNAPSHOT_TLS_ALLOW_INVALID_HOSTNAMES",
            )
        ),
        False,
    )


def resolve_tls_allow_invalid_certificates(args: argparse.Namespace) -> bool:
    if bool(args.tls_allow_invalid_certificates):
        return True
    return parse_bool_text(
        first_env_value(
            (
                "DOCDB_FILL_CLUSTER_TLS_ALLOW_INVALID_CERTIFICATES",
                "DOCDB_SNAPSHOT_TLS_ALLOW_INVALID_CERTIFICATES",
            )
        ),
        False,
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
        tls_allow_invalid_hostnames=resolve_tls_allow_invalid_hostnames(args),
        tls_allow_invalid_certificates=resolve_tls_allow_invalid_certificates(args),
        uri=uri,
    )


def build_mongo_client(
    uri: str,
    tls_ca_file: str | None,
    tls_allow_invalid_hostnames: bool,
    tls_allow_invalid_certificates: bool,
    app_name: str,
) -> MongoClient[Any]:
    client_options: dict[str, Any] = {
        "appname": app_name,
        "retryWrites": False,
        "connectTimeoutMS": 10_000,
        "serverSelectionTimeoutMS": 10_000,
        "socketTimeoutMS": 300_000,
    }
    if tls_ca_file or tls_allow_invalid_hostnames or tls_allow_invalid_certificates:
        client_options["tls"] = True
    if tls_ca_file:
        client_options["tlsCAFile"] = tls_ca_file
    if tls_allow_invalid_hostnames:
        client_options["tlsAllowInvalidHostnames"] = True
    if tls_allow_invalid_certificates:
        client_options["tlsAllowInvalidCertificates"] = True
    return MongoClient(uri, **client_options)


def resolve_database_case_from_cluster(connection: ClusterConnectionInfo) -> str:
    client = build_mongo_client(
        connection.uri,
        connection.tls_ca_file,
        connection.tls_allow_invalid_hostnames,
        connection.tls_allow_invalid_certificates,
        app_name="docdb-fill-cluster-dbcase",
    )
    try:
        database_names = client.list_database_names()
    except Exception:
        return connection.database
    finally:
        client.close()

    requested_database = connection.database
    requested_database_lower = requested_database.lower()
    for existing_database in database_names:
        if str(existing_database).lower() == requested_database_lower:
            return str(existing_database)
    return requested_database


def build_collection(
    uri: str,
    tls_ca_file: str | None,
    tls_allow_invalid_hostnames: bool,
    tls_allow_invalid_certificates: bool,
    database_name: str,
    collection_name: str,
) -> tuple[MongoClient[Any], Collection[Any]]:
    client = build_mongo_client(
        uri,
        tls_ca_file,
        tls_allow_invalid_hostnames,
        tls_allow_invalid_certificates,
        app_name="docdb-fill-cluster",
    )
    collection = client.get_database(database_name).get_collection(
        collection_name,
        write_concern=WriteConcern(w=1, j=False),
    )
    return client, collection


def read_database_stats_from_connection(
    uri: str,
    tls_ca_file: str | None,
    tls_allow_invalid_hostnames: bool,
    tls_allow_invalid_certificates: bool,
    database_name: str,
) -> dict[str, Any]:
    client = build_mongo_client(
        uri,
        tls_ca_file,
        tls_allow_invalid_hostnames,
        tls_allow_invalid_certificates,
        app_name="docdb-fill-cluster-stats",
    )
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


def chunk_count_per_document(doc_size_bytes: int, chunk_size_bytes: int) -> int:
    if doc_size_bytes < chunk_size_bytes:
        raise ClusterSeedError("doc_size_mib precisa ser maior ou igual ao tamanho do chunk.")
    chunk_count, remainder = divmod(doc_size_bytes, chunk_size_bytes)
    if remainder != 0:
        raise ClusterSeedError("doc_size_mib precisa ser múltiplo de pool-chunk-size-mib.")
    if chunk_count <= 0:
        raise ClusterSeedError("chunk_count_per_doc precisa ser maior que zero.")
    return chunk_count


def build_payload_variants(
    pool: Sequence[bytes],
    worker_index: int,
    chunk_count_per_doc: int,
    payload_variants_per_worker: int,
) -> tuple[bytes, ...]:
    if payload_variants_per_worker <= 0:
        raise ClusterSeedError("payload-variants-per-worker precisa ser maior que zero.")
    return tuple(
        build_payload(pool, chunk_count_per_doc, worker_index, sequence)
        for sequence in range(payload_variants_per_worker)
    )


def resolve_payload_variants(worker_index: int, runtime: SeedRuntime) -> tuple[bytes, ...]:
    cache_key = (
        worker_index,
        runtime.pool_chunks,
        runtime.pool_chunk_size_bytes,
        runtime.doc_size_bytes,
        runtime.payload_variants_per_worker,
    )
    with _PAYLOAD_VARIANT_CACHE_LOCK:
        cached_payloads = _PAYLOAD_VARIANT_CACHE.get(cache_key)
    if cached_payloads is not None:
        return cached_payloads

    pool = build_chunk_pool(
        worker_index,
        runtime.pool_chunks,
        runtime.pool_chunk_size_bytes,
    )
    chunk_count = chunk_count_per_document(
        runtime.doc_size_bytes,
        runtime.pool_chunk_size_bytes,
    )
    payload_variants = build_payload_variants(
        pool=pool,
        worker_index=worker_index,
        chunk_count_per_doc=chunk_count,
        payload_variants_per_worker=runtime.payload_variants_per_worker,
    )

    with _PAYLOAD_VARIANT_CACHE_LOCK:
        existing_payloads = _PAYLOAD_VARIANT_CACHE.get(cache_key)
        if existing_payloads is not None:
            return existing_payloads
        _PAYLOAD_VARIANT_CACHE[cache_key] = payload_variants
    return payload_variants


def build_documents_from_offset(
    collection_name: str,
    worker_index: int,
    start_sequence: int,
    document_count: int,
    payload_variants: Sequence[bytes],
):
    if not payload_variants:
        raise ClusterSeedError("payload_variants não pode estar vazio.")
    payload_variants_count = len(payload_variants)
    for sequence_offset in range(document_count):
        sequence = start_sequence + sequence_offset
        payload = payload_variants[sequence % payload_variants_count]
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


def summarize_bulk_write_error(exc: BulkWriteError) -> str:
    details = exc.details if isinstance(exc.details, dict) else {}
    write_errors = details.get("writeErrors", [])
    if isinstance(write_errors, list) and write_errors:
        error_codes = sorted(
            {
                str(item.get("code", "unknown"))
                for item in write_errors
                if isinstance(item, dict)
            }
        )
        if "13297" in error_codes:
            return (
                "Falha no insert_many (code=13297 DatabaseDifferCase). "
                "Use o database com o mesmo case já existente no cluster."
            )
        return (
            "Falha no insert_many "
            f"(write_errors={len(write_errors)}, codes={','.join(error_codes) or 'unknown'})."
        )

    write_concern_errors = details.get("writeConcernErrors", [])
    if isinstance(write_concern_errors, list) and write_concern_errors:
        wc_codes = sorted(
            {
                str(item.get("code", "unknown"))
                for item in write_concern_errors
                if isinstance(item, dict)
            }
        )
        return (
            "Falha de writeConcern "
            f"(errors={len(write_concern_errors)}, codes={','.join(wc_codes) or 'unknown'})."
        )

    return "Falha no insert_many sem detalhes de erro disponíveis."


def insert_batch_with_single_fallback(collection: Collection[Any], batch: Sequence[dict[str, Any]]) -> int:
    inserted_documents = 0
    for document in batch:
        inserted_documents += insert_document(collection, document)
    return inserted_documents


def insert_batch(collection: Collection[Any], batch: Sequence[dict[str, Any]], attempts: int = 3) -> int:
    last_insert_many_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            collection.insert_many(
                list(batch),
                ordered=False,
                bypass_document_validation=True,
            )
            return len(batch)
        except BulkWriteError as exc:
            write_errors = exc.details.get("writeErrors", [])
            duplicated_only = bool(write_errors) and all(
                isinstance(item, dict) and item.get("code") == 11000
                for item in write_errors
            )
            if duplicated_only:
                return len(batch)
            last_insert_many_error = exc
            break
        except AutoReconnect:
            last_insert_many_error = AutoReconnect("AutoReconnect while calling insert_many.")
            if attempt == attempts:
                break
            time.sleep(attempt)
        except Exception as exc:
            last_insert_many_error = exc
            break

    try:
        return insert_batch_with_single_fallback(collection, batch)
    except Exception as exc:
        insert_many_summary = (
            summarize_bulk_write_error(last_insert_many_error)
            if isinstance(last_insert_many_error, BulkWriteError)
            else last_insert_many_error.__class__.__name__
            if last_insert_many_error is not None
            else "unknown"
        )
        raise ClusterSeedError(
            "Falha de inserção no batch "
            f"(insert_many={insert_many_summary}, insert_one={exc.__class__.__name__})."
        ) from None


def _batch_insert_result(collection: Collection[Any], batch: Sequence[dict[str, Any]]) -> tuple[int, int]:
    try:
        inserted_documents = insert_batch(collection, batch)
        inserted_payload_bytes = sum(int(item["size_bytes"]) for item in batch[:inserted_documents])
        return inserted_documents, inserted_payload_bytes
    except ClusterSeedError:
        raise
    except Exception as exc:
        raise ClusterSeedError(
            f"Falha de inserção no batch ({exc.__class__.__name__})."
        ) from None


def _accumulate_insert_results(insert_futures: Iterable[Any]) -> tuple[int, int]:
    inserted_documents = 0
    inserted_payload_bytes = 0
    for future in insert_futures:
        batch_documents, batch_payload_bytes = future.result()
        inserted_documents += batch_documents
        inserted_payload_bytes += batch_payload_bytes
    return inserted_documents, inserted_payload_bytes


def _insert_batches_with_threads(
    collection: Collection[Any],
    batch_iterable: Iterable[tuple[dict[str, Any], ...]],
    *,
    insert_threads_per_worker: int,
) -> tuple[int, int]:
    if insert_threads_per_worker <= 1:
        inserted_documents = 0
        inserted_payload_bytes = 0
        for batch in batch_iterable:
            batch_documents, batch_payload_bytes = _batch_insert_result(collection, batch)
            inserted_documents += batch_documents
            inserted_payload_bytes += batch_payload_bytes
        return inserted_documents, inserted_payload_bytes

    in_flight_limit = max(1, insert_threads_per_worker * 2)
    with ThreadPoolExecutor(
        max_workers=insert_threads_per_worker,
        thread_name_prefix="docdb-insert",
    ) as executor:
        in_flight: set[Any] = set()
        inserted_documents = 0
        inserted_payload_bytes = 0

        for batch in batch_iterable:
            in_flight.add(executor.submit(_batch_insert_result, collection, batch))
            if len(in_flight) >= in_flight_limit:
                done, pending = wait(in_flight, return_when=FIRST_COMPLETED)
                done_documents, done_payload_bytes = _accumulate_insert_results(done)
                inserted_documents += done_documents
                inserted_payload_bytes += done_payload_bytes
                in_flight = set(pending)

        if in_flight:
            done, _ = wait(in_flight)
            done_documents, done_payload_bytes = _accumulate_insert_results(done)
            inserted_documents += done_documents
            inserted_payload_bytes += done_payload_bytes

    return inserted_documents, inserted_payload_bytes


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
        connection.tls_allow_invalid_hostnames,
        connection.tls_allow_invalid_certificates,
        connection.database,
        collection_name,
    )
    try:
        payload_variants = resolve_payload_variants(worker_index, runtime)
        started_at = time.perf_counter()
        batch_iterable = batched(
            build_documents_from_offset(
                collection_name=collection_name,
                worker_index=worker_index,
                start_sequence=start_sequence,
                document_count=document_count,
                payload_variants=payload_variants,
            ),
            runtime.batch_size,
        )
        inserted_count, inserted_bytes = _insert_batches_with_threads(
            collection,
            batch_iterable,
            insert_threads_per_worker=runtime.insert_threads_per_worker,
        )
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
    insert_threads_per_worker = int(
        require_positive(args.insert_threads_per_worker, "insert-threads-per-worker")
    )
    payload_variants_per_worker = int(
        require_positive(args.payload_variants_per_worker, "payload-variants-per-worker")
    )
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
        insert_threads_per_worker=insert_threads_per_worker,
        payload_variants_per_worker=payload_variants_per_worker,
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


def format_gib(value_bytes: int) -> str:
    return f"{value_bytes / GIB:.3f} GiB"


def format_mib(value_bytes: int) -> str:
    return f"{value_bytes / MIB:.1f} MiB"


def format_elapsed_seconds(seconds: float) -> str:
    safe_seconds = max(0, int(round(seconds)))
    minutes, remaining_seconds = divmod(safe_seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours:02d}:{remaining_minutes:02d}:{remaining_seconds:02d}"


def estimate_eta_seconds(
    remaining_bytes: int,
    inserted_payload_total_bytes: int,
    total_elapsed_seconds: float,
) -> float | None:
    if remaining_bytes <= 0:
        return 0.0
    if inserted_payload_total_bytes <= 0 or total_elapsed_seconds <= 0:
        return None
    bytes_per_second = inserted_payload_total_bytes / total_elapsed_seconds
    if bytes_per_second <= 0:
        return None
    return remaining_bytes / bytes_per_second


def format_worker_progress(worker_reports: Sequence[dict[str, Any]]) -> str:
    if not worker_reports:
        return "-"
    ordered_reports = sorted(
        worker_reports,
        key=lambda report: int(report.get("worker_index", 0)),
    )
    return " | ".join(
        (
            f"w{int(report['worker_index']):02d} "
            f"docs={int(report['documents'])} "
            f"bytes={format_mib(int(report['payload_bytes']))} "
            f"rate={((int(report['payload_bytes']) / MIB) / max(float(report['elapsed_seconds']), 0.001)):.1f} MiB/s"
        )
        for report in ordered_reports
    )


def growth_status(before_bytes: int, after_bytes: int) -> str:
    return "growing" if after_bytes > before_bytes else "no-growth"


def print_round_progress(
    round_number: int,
    size_metric: str,
    before_bytes: int,
    after_bytes: int,
    target_bytes: int,
    planned_payload_bytes: int,
    observed_after_bytes: int,
    inserted_payload_bytes: int,
    inserted_documents: int,
    inserted_payload_total_bytes: int,
    inserted_documents_total: int,
    metric_source: str,
    round_elapsed_seconds: float,
    total_elapsed_seconds: float,
    worker_reports: Sequence[dict[str, Any]],
) -> None:
    progress_percent = (after_bytes / target_bytes * 100.0) if target_bytes > 0 else 0.0
    remaining_bytes = max(target_bytes - after_bytes, 0)
    database_growth_bytes = max(0, after_bytes - before_bytes)
    print(
        (
            f"[round {round_number:03d}] "
            f"{progress_percent:5.1f}% {size_metric}={format_gib(after_bytes)}/{format_gib(target_bytes)} "
            f"db_growth={format_gib(database_growth_bytes)} "
            f"payload_round={format_gib(inserted_payload_bytes)} "
            f"payload_total={format_gib(inserted_payload_total_bytes)} "
            f"remaining={format_gib(remaining_bytes)} "
            f"status={growth_status(before_bytes, after_bytes)}"
        ),
        flush=True,
    )


def summarize_worker_reports(worker_reports: Sequence[dict[str, Any]]) -> tuple[int, int]:
    inserted_documents = sum(int(report.get("documents", 0)) for report in worker_reports)
    inserted_payload_bytes = sum(int(report.get("payload_bytes", 0)) for report in worker_reports)
    return inserted_documents, inserted_payload_bytes


def resolve_effective_after_bytes(
    before_bytes: int,
    observed_after_bytes: int,
    inserted_payload_bytes: int,
) -> tuple[int, str]:
    estimated_after_bytes = before_bytes + max(0, inserted_payload_bytes)
    if observed_after_bytes >= estimated_after_bytes:
        return observed_after_bytes, "observed_dbstats"
    return estimated_after_bytes, "estimated_from_inserted_payload"


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
    loop_started_at = time.perf_counter()
    start_stats = read_database_stats_from_connection(
        connection.uri,
        connection.tls_ca_file,
        connection.tls_allow_invalid_hostnames,
        connection.tls_allow_invalid_certificates,
        connection.database,
    )
    current_metric_bytes = metric_bytes(start_stats, runtime.size_metric)
    next_sequences = tuple(0 for _ in range(runtime.parallelism))
    round_number = 0
    round_reports: list[dict[str, Any]] = []
    inserted_payload_total_bytes = 0
    inserted_documents_total = 0

    while current_metric_bytes < target_bytes:
        round_started_at = time.perf_counter()
        round_number += 1
        remaining_bytes = target_bytes - current_metric_bytes
        round_payload_bytes, distribution = build_round_plan(remaining_bytes, runtime)
        worker_reports = run_seed_round(connection, runtime, next_sequences, distribution)
        next_sequences = next_sequence_offsets(next_sequences, worker_reports)
        inserted_documents, inserted_payload_bytes = summarize_worker_reports(worker_reports)
        inserted_payload_total_bytes += inserted_payload_bytes
        inserted_documents_total += inserted_documents
        time.sleep(runtime.stats_poll_seconds)
        current_stats = read_database_stats_from_connection(
            connection.uri,
            connection.tls_ca_file,
            connection.tls_allow_invalid_hostnames,
            connection.tls_allow_invalid_certificates,
            connection.database,
        )
        observed_metric_bytes = metric_bytes(current_stats, runtime.size_metric)
        updated_metric_bytes, metric_source = resolve_effective_after_bytes(
            current_metric_bytes,
            observed_metric_bytes,
            inserted_payload_bytes,
        )
        round_elapsed_seconds = max(0.001, time.perf_counter() - round_started_at)
        round_report = {
            "round": round_number,
            "remaining_bytes_before": remaining_bytes,
            "planned_payload_bytes": round_payload_bytes,
            "distribution": list(distribution),
            "metric_bytes_before": current_metric_bytes,
            "metric_bytes_after": updated_metric_bytes,
            "metric_bytes_observed_after": observed_metric_bytes,
            "inserted_payload_bytes": inserted_payload_bytes,
            "inserted_documents": inserted_documents,
            "inserted_payload_total_bytes": inserted_payload_total_bytes,
            "inserted_documents_total": inserted_documents_total,
            "metric_source": metric_source,
            "round_elapsed_seconds": round_elapsed_seconds,
            "worker_reports": list(worker_reports),
            "db_stats": current_stats,
        }
        round_reports.append(round_report)
        total_elapsed_seconds = max(0.001, time.perf_counter() - loop_started_at)
        print_round_progress(
            round_number=round_number,
            size_metric=runtime.size_metric,
            before_bytes=current_metric_bytes,
            after_bytes=updated_metric_bytes,
            target_bytes=target_bytes,
            planned_payload_bytes=round_payload_bytes,
            observed_after_bytes=observed_metric_bytes,
            inserted_payload_bytes=inserted_payload_bytes,
            inserted_documents=inserted_documents,
            inserted_payload_total_bytes=inserted_payload_total_bytes,
            inserted_documents_total=inserted_documents_total,
            metric_source=metric_source,
            round_elapsed_seconds=round_elapsed_seconds,
            total_elapsed_seconds=total_elapsed_seconds,
            worker_reports=worker_reports,
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
        resolved_database = resolve_database_case_from_cluster(connection)
        if resolved_database != connection.database:
            connection = replace(connection, database=resolved_database)
        runtime = build_runtime(args, started_at, connection.target_name)
        print(
            (
                f"[connection] target={connection.target_name} "
                f"endpoint={connection.endpoint}:{connection.port} "
                f"database={connection.database} "
                f"metric={runtime.size_metric} "
                f"target={args.gib:.3f} GiB"
            ),
            flush=True,
        )
        print(
            (
                f"[runtime] workers={runtime.parallelism} "
                f"insert_threads_per_worker={runtime.insert_threads_per_worker} "
                f"payload_variants_per_worker={runtime.payload_variants_per_worker} "
                f"batch_size={runtime.batch_size} "
                f"doc_size={runtime.doc_size_bytes / MIB:.1f} MiB "
                f"round_limit={runtime.round_bytes / MIB:.1f} MiB "
                f"stats_poll={runtime.stats_poll_seconds:.1f}s "
                f"collection_prefix={runtime.collection_prefix}"
            ),
            flush=True,
        )
        report = seed_database_to_target(
            connection=connection,
            runtime=runtime,
            target_gib=args.gib,
            report_file=args.report_file,
        )
        duration_seconds = max(float(report["duration_seconds"]), 0.001)
        average_rate_mib_per_second = (int(report["payload_bytes_inserted"]) / MIB) / duration_seconds
        print(
            (
                f"[completed] target={report['target_name']} "
                f"database={report['database']} "
                f"metric={report['size_metric']} "
                f"final={format_gib(int(report['final_metric_bytes']))} "
                f"target={float(report['target_gib']):.3f} GiB "
                f"inserted={format_gib(int(report['payload_bytes_inserted']))} "
                f"docs={int(report['documents_inserted'])} "
                f"duration={float(report['duration_seconds']):.1f}s "
                f"avg_rate={average_rate_mib_per_second:.1f} MiB/s"
            ),
            flush=True,
        )
        if args.report_file:
            print(f"[report] saved={args.report_file}", flush=True)
        return 0
    except ClusterSeedError as exc:
        if error_details_enabled():
            print(f"[error] {str(exc)}", flush=True)
        else:
            print(
                "[error] falha durante a carga. Defina DOCDB_FILL_CLUSTER_DEBUG_ERRORS=1 para detalhes.",
                flush=True,
            )
        return 1
    except Exception as exc:
        if error_details_enabled():
            compact_message = " ".join(str(exc).split())[:320]
            print(
                f"[error] falha inesperada ({exc.__class__.__name__}): {compact_message}",
                flush=True,
            )
        else:
            print(
                "[error] falha inesperada durante a carga. Defina DOCDB_FILL_CLUSTER_DEBUG_ERRORS=1 para detalhes.",
                flush=True,
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
