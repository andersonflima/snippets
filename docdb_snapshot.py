import argparse
import contextlib
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, BinaryIO, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


LOGGER = logging.getLogger("docdb_snapshot")
MB = 1024 * 1024
MIN_MULTIPART_CHUNK_BYTES = 5 * MB
MAX_MULTIPART_PARTS = 10_000
S3_BUCKET_ARN_PATTERN = re.compile(r"^arn:[^:]+:s3:::(?P<bucket>[^/]+)$")


@dataclass(frozen=True)
class RuntimeSettings:
    upload_workers: int
    multipart_chunk_bytes: int
    queue_size: int
    compressor: str
    compressor_threads: int
    compression_level: int


@dataclass(frozen=True)
class S3Settings:
    bucket: str
    prefix: str
    region: Optional[str]
    endpoint_url: Optional[str]
    force_path_style: bool
    bucket_owner: Optional[str]
    connect_timeout_seconds: int
    read_timeout_seconds: int
    storage_class: Optional[str]
    server_side_encryption: Optional[str]
    kms_key_id: Optional[str]
    tags: Optional[str]


@dataclass(frozen=True)
class Target:
    name: str
    uri: str
    tls_ca_file: Optional[str]
    database: Optional[str]
    collection: Optional[str]
    num_parallel_collections: int
    extra_args: Tuple[str, ...]


@dataclass(frozen=True)
class AppConfig:
    s3: S3Settings
    runtime: RuntimeSettings
    target: Target


@dataclass(frozen=True)
class MultipartUploadPlan:
    bucket: str
    key: str
    upload_id: str
    expected_bucket_owner: Optional[str] = None


@dataclass(frozen=True)
class UploadedPart:
    part_number: int
    etag: str
    size_bytes: int


@dataclass(frozen=True)
class UploadResult:
    bucket: str
    key: str
    size_bytes: int
    part_count: int


@dataclass(frozen=True)
class SnapshotResult:
    target_name: str
    bucket: str
    key: str
    size_bytes: int
    started_at: datetime
    finished_at: datetime


class SnapshotError(RuntimeError):
    pass


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Faz snapshot streaming de um DocumentDB/MongoDB direto para o S3."
    )
    parser.add_argument("--uri", required=True, help="URI direta do MongoDB/DocumentDB.")
    parser.add_argument("--bucket", required=True, help="Bucket S3 de destino.")
    parser.add_argument("--target-name", help="Nome lógico do target. Default: direct-target.")
    parser.add_argument("--tls-ca-file", help="Bundle CA para TLS no mongodump.")
    parser.add_argument("--database", help="Database alvo.")
    parser.add_argument("--collection", help="Collection alvo.")
    parser.add_argument(
        "--num-parallel-collections",
        type=int,
        help="Valor de `--numParallelCollections` no mongodump.",
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Argumento extra repassado para o mongodump. Pode ser repetido.",
    )
    parser.add_argument("--s3-prefix", help="Prefixo S3.")
    parser.add_argument("--s3-region", help="Região do S3.")
    parser.add_argument("--s3-endpoint-url", help="Endpoint S3 opcional.")
    parser.add_argument(
        "--s3-force-path-style",
        action="store_true",
        help="Força path-style no cliente S3.",
    )
    parser.add_argument("--s3-bucket-owner", help="ExpectedBucketOwner opcional.")
    parser.add_argument(
        "--s3-connect-timeout-seconds",
        type=int,
        help="Timeout de conexão do cliente S3.",
    )
    parser.add_argument(
        "--s3-read-timeout-seconds",
        type=int,
        help="Timeout de leitura do cliente S3.",
    )
    parser.add_argument("--s3-storage-class", help="StorageClass opcional.")
    parser.add_argument(
        "--s3-server-side-encryption",
        help="ServerSideEncryption opcional.",
    )
    parser.add_argument("--s3-kms-key-id", help="SSEKMSKeyId opcional.")
    parser.add_argument("--s3-tags", help="Tags opcionais em formato querystring.")
    parser.add_argument("--upload-workers", type=int, help="Workers paralelos do multipart upload.")
    parser.add_argument("--multipart-chunk-mb", type=int, help="Tamanho de cada parte multipart em MB.")
    parser.add_argument("--queue-size", type=int, help="Quantidade máxima de partes pendentes em memória.")
    parser.add_argument(
        "--compressor",
        choices=("auto", "pigz", "gzip"),
        help="Compressor a usar no pipeline.",
    )
    parser.add_argument("--compressor-threads", type=int, help="Threads do compressor.")
    parser.add_argument("--compression-level", type=int, help="Nível de compressão entre 1 e 9.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida parâmetros e dependências sem executar o snapshot.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Nível de log.",
    )
    return parser.parse_args(argv)


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def first_env_value(names: Sequence[str]) -> Optional[str]:
    for name in names:
        candidate = normalize_optional_text(os.getenv(name))
        if candidate is not None:
            return candidate
    return None


def parse_optional_positive_int(value: Optional[str], label: str) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise SnapshotError(f"{label} precisa ser um inteiro positivo.") from exc
    if parsed <= 0:
        raise SnapshotError(f"{label} precisa ser um inteiro positivo.")
    return parsed


def parse_bool_text(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise SnapshotError(f"Valor booleano inválido: {value}")


def resolve_bucket_name(value: Any) -> str:
    normalized = normalize_optional_text(value)
    if normalized is None:
        raise SnapshotError("`--bucket` é obrigatório.")
    arn_match = S3_BUCKET_ARN_PATTERN.fullmatch(normalized)
    if arn_match:
        return arn_match.group("bucket")
    if normalized.startswith("arn:"):
        raise SnapshotError("Bucket ARN inválido. Use `arn:aws:s3:::nome-do-bucket`.")
    return normalized


def parse_extra_args(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        normalized = value.strip()
        return tuple(shlex.split(normalized)) if normalized else ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if normalize_optional_text(item) is not None)
    raise SnapshotError("`extra_args` precisa ser string, lista ou tupla.")


def sanitize_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    compacted = re.sub(r"-{2,}", "-", normalized)
    return compacted.strip("-") or "documentdb"


def build_s3_key(prefix: str, target_name: str, started_at: datetime) -> str:
    safe_target_name = sanitize_name(target_name)
    base_prefix = "/".join(
        part
        for part in (prefix.strip("/"), safe_target_name, started_at.strftime("%Y/%m/%d"))
        if part
    )
    filename = f"{safe_target_name}-{started_at.strftime('%Y%m%dT%H%M%SZ')}.archive.gz"
    return "/".join(part for part in (base_prefix, filename) if part)


def default_runtime_settings(cpu_count: Optional[int] = None) -> RuntimeSettings:
    available_cpus = max(1, cpu_count or os.cpu_count() or 1)
    compressor_threads = 1 if available_cpus <= 2 else min(3, available_cpus - 1)
    return RuntimeSettings(
        upload_workers=2,
        multipart_chunk_bytes=32 * MB,
        queue_size=2,
        compressor="auto",
        compressor_threads=compressor_threads,
        compression_level=1,
    )


def merge_runtime_settings(runtime_payload: Dict[str, Any]) -> RuntimeSettings:
    defaults = default_runtime_settings()
    multipart_chunk_mb = int(
        runtime_payload.get("multipart_chunk_mb", defaults.multipart_chunk_bytes // MB)
    )
    multipart_chunk_bytes = multipart_chunk_mb * MB
    if multipart_chunk_bytes < MIN_MULTIPART_CHUNK_BYTES:
        raise SnapshotError("multipart_chunk_mb precisa ser no mínimo 5 MB.")
    return RuntimeSettings(
        upload_workers=max(1, int(runtime_payload.get("upload_workers", defaults.upload_workers))),
        multipart_chunk_bytes=multipart_chunk_bytes,
        queue_size=max(1, int(runtime_payload.get("queue_size", defaults.queue_size))),
        compressor=str(runtime_payload.get("compressor", defaults.compressor)).lower(),
        compressor_threads=max(
            1,
            int(runtime_payload.get("compressor_threads", defaults.compressor_threads)),
        ),
        compression_level=min(
            9,
            max(1, int(runtime_payload.get("compression_level", defaults.compression_level))),
        ),
    )


def build_runtime_payload_from_env() -> Dict[str, Any]:
    env_mapping = (
        ("DOCDB_SNAPSHOT_UPLOAD_WORKERS", "upload_workers"),
        ("DOCDB_SNAPSHOT_MULTIPART_CHUNK_MB", "multipart_chunk_mb"),
        ("DOCDB_SNAPSHOT_QUEUE_SIZE", "queue_size"),
        ("DOCDB_SNAPSHOT_COMPRESSOR", "compressor"),
        ("DOCDB_SNAPSHOT_COMPRESSOR_THREADS", "compressor_threads"),
        ("DOCDB_SNAPSHOT_COMPRESSION_LEVEL", "compression_level"),
    )
    payload: Dict[str, Any] = {}
    for env_name, payload_key in env_mapping:
        env_value = first_env_value((env_name,))
        if env_value is not None:
            payload[payload_key] = env_value
    return payload


def load_default_target_settings_from_env() -> Dict[str, Any]:
    return {
        "tls_ca_file": first_env_value(("DOCDB_SNAPSHOT_TLS_CA_FILE",)),
        "database": first_env_value(("DOCDB_SNAPSHOT_DEFAULT_DATABASE",)),
        "collection": first_env_value(("DOCDB_SNAPSHOT_DEFAULT_COLLECTION",)),
        "num_parallel_collections": parse_optional_positive_int(
            first_env_value(("DOCDB_SNAPSHOT_NUM_PARALLEL_COLLECTIONS",)),
            "DOCDB_SNAPSHOT_NUM_PARALLEL_COLLECTIONS",
        )
        or 1,
        "extra_args": parse_extra_args(first_env_value(("DOCDB_SNAPSHOT_MONGODUMP_EXTRA_ARGS",))),
    }


def resolve_timeout_value(
    direct_value: Optional[int],
    env_names: Sequence[str],
    label: str,
    default: int,
) -> int:
    if direct_value is not None:
        if direct_value <= 0:
            raise SnapshotError(f"{label} precisa ser um inteiro positivo.")
        return direct_value
    return parse_optional_positive_int(first_env_value(env_names), label) or default


def resolve_region_from_env() -> Optional[str]:
    return first_env_value(("DOCDB_SNAPSHOT_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"))


def build_runtime_settings_from_args(args: argparse.Namespace) -> RuntimeSettings:
    runtime_payload = build_runtime_payload_from_env()
    direct_payload = {
        "upload_workers": args.upload_workers,
        "multipart_chunk_mb": args.multipart_chunk_mb,
        "queue_size": args.queue_size,
        "compressor": args.compressor,
        "compressor_threads": args.compressor_threads,
        "compression_level": args.compression_level,
    }
    runtime_payload.update(
        {
            key: value
            for key, value in direct_payload.items()
            if value is not None
        }
    )
    return merge_runtime_settings(runtime_payload)


def build_target_from_args(args: argparse.Namespace) -> Target:
    defaults = load_default_target_settings_from_env()
    uri = normalize_optional_text(args.uri)
    if uri is None:
        raise SnapshotError("`--uri` é obrigatório.")
    return Target(
        name=normalize_optional_text(args.target_name) or "direct-target",
        uri=uri,
        tls_ca_file=normalize_optional_text(args.tls_ca_file) or defaults["tls_ca_file"],
        database=normalize_optional_text(args.database) or defaults["database"],
        collection=normalize_optional_text(args.collection) or defaults["collection"],
        num_parallel_collections=max(
            1,
            int(args.num_parallel_collections or defaults["num_parallel_collections"]),
        ),
        extra_args=tuple(args.extra_arg) or tuple(defaults["extra_args"]),
    )


def load_s3_settings_from_args(args: argparse.Namespace) -> S3Settings:
    return S3Settings(
        bucket=resolve_bucket_name(args.bucket),
        prefix=normalize_optional_text(args.s3_prefix)
        or first_env_value(("DOCDB_SNAPSHOT_S3_PREFIX",))
        or "documentdb-snapshots",
        region=normalize_optional_text(args.s3_region) or resolve_region_from_env(),
        endpoint_url=normalize_optional_text(args.s3_endpoint_url)
        or first_env_value(("DOCDB_SNAPSHOT_S3_ENDPOINT_URL",)),
        force_path_style=bool(args.s3_force_path_style)
        or parse_bool_text(
            first_env_value(("DOCDB_SNAPSHOT_S3_FORCE_PATH_STYLE",)),
            default=False,
        ),
        bucket_owner=normalize_optional_text(args.s3_bucket_owner)
        or first_env_value(("DOCDB_SNAPSHOT_BUCKET_OWNER",)),
        connect_timeout_seconds=resolve_timeout_value(
            args.s3_connect_timeout_seconds,
            ("DOCDB_SNAPSHOT_S3_CONNECT_TIMEOUT_SECONDS",),
            "DOCDB_SNAPSHOT_S3_CONNECT_TIMEOUT_SECONDS",
            10,
        ),
        read_timeout_seconds=resolve_timeout_value(
            args.s3_read_timeout_seconds,
            ("DOCDB_SNAPSHOT_S3_READ_TIMEOUT_SECONDS",),
            "DOCDB_SNAPSHOT_S3_READ_TIMEOUT_SECONDS",
            60,
        ),
        storage_class=normalize_optional_text(args.s3_storage_class)
        or first_env_value(("DOCDB_SNAPSHOT_S3_STORAGE_CLASS",)),
        server_side_encryption=normalize_optional_text(args.s3_server_side_encryption)
        or first_env_value(("DOCDB_SNAPSHOT_S3_SERVER_SIDE_ENCRYPTION",)),
        kms_key_id=normalize_optional_text(args.s3_kms_key_id)
        or first_env_value(("DOCDB_SNAPSHOT_S3_KMS_KEY_ID",)),
        tags=normalize_optional_text(args.s3_tags) or first_env_value(("DOCDB_SNAPSHOT_S3_TAGS",)),
    )


def load_app_config_from_args(args: argparse.Namespace) -> AppConfig:
    return AppConfig(
        s3=load_s3_settings_from_args(args),
        runtime=build_runtime_settings_from_args(args),
        target=build_target_from_args(args),
    )


def build_mongodump_command(target: Target) -> Tuple[str, ...]:
    command = [
        "mongodump",
        "--uri",
        target.uri,
        "--archive",
        "--numParallelCollections",
        str(target.num_parallel_collections),
    ]
    if target.tls_ca_file:
        command.extend(["--ssl", "--sslCAFile", target.tls_ca_file])
    if target.database:
        command.extend(["--db", target.database])
    if target.collection:
        command.extend(["--collection", target.collection])
    command.extend(target.extra_args)
    return tuple(command)


def select_compressor(runtime: RuntimeSettings) -> Tuple[Tuple[str, ...], str]:
    use_pigz = runtime.compressor in {"auto", "pigz"} and shutil.which("pigz")
    if use_pigz:
        return (
            (
                "pigz",
                f"-{runtime.compression_level}",
                "-c",
                "-p",
                str(runtime.compressor_threads),
            ),
            "pigz",
        )
    if runtime.compressor in {"auto", "gzip"} and shutil.which("gzip"):
        return (("gzip", f"-{runtime.compression_level}", "-c"), "gzip")
    raise SnapshotError("Nem `pigz` nem `gzip` estão disponíveis no host.")


def require_binary(binary_name: str) -> None:
    if not shutil.which(binary_name):
        raise SnapshotError(f"Dependência ausente no host: {binary_name}")


def build_create_multipart_args(
    s3_settings: S3Settings,
    target: Target,
    key: str,
    started_at: datetime,
) -> Dict[str, Any]:
    metadata = {
        "target-name": sanitize_name(target.name),
        "started-at": started_at.isoformat(),
    }
    payload: Dict[str, Any] = {
        "Bucket": s3_settings.bucket,
        "Key": key,
        "ContentType": "application/gzip",
        "Metadata": metadata,
    }
    if s3_settings.storage_class:
        payload["StorageClass"] = s3_settings.storage_class
    if s3_settings.server_side_encryption:
        payload["ServerSideEncryption"] = s3_settings.server_side_encryption
    if s3_settings.kms_key_id:
        payload["SSEKMSKeyId"] = s3_settings.kms_key_id
    if s3_settings.tags:
        payload["Tagging"] = s3_settings.tags
    if s3_settings.bucket_owner:
        payload["ExpectedBucketOwner"] = s3_settings.bucket_owner
    return payload


def create_s3_client(s3_settings: S3Settings, max_pool_connections: int) -> Any:
    try:
        import boto3
        from botocore.config import Config
    except ModuleNotFoundError as exc:
        raise SnapshotError("boto3 e botocore precisam estar instalados para enviar para o S3.") from exc

    client_config = Config(
        retries={"max_attempts": 10, "mode": "adaptive"},
        connect_timeout=s3_settings.connect_timeout_seconds,
        read_timeout=s3_settings.read_timeout_seconds,
        max_pool_connections=max_pool_connections,
        s3={"addressing_style": "path" if s3_settings.force_path_style else "auto"},
    )
    return boto3.client(
        "s3",
        region_name=s3_settings.region,
        endpoint_url=s3_settings.endpoint_url,
        config=client_config,
    )


def read_pipe_text(pipe: Optional[BinaryIO], bucket: List[str]) -> None:
    if pipe is None:
        return
    content = pipe.read()
    if isinstance(content, bytes):
        bucket.append(content.decode("utf-8", errors="replace"))
        return
    bucket.append(content)


def spawn_stderr_collector(pipe: Optional[BinaryIO]) -> Tuple[Optional[threading.Thread], List[str]]:
    buffer: List[str] = []
    if pipe is None:
        return None, buffer
    thread = threading.Thread(target=read_pipe_text, args=(pipe, buffer), daemon=True)
    thread.start()
    return thread, buffer


def iter_stream_chunks(stream: BinaryIO, chunk_size: int, read_size: int = MB) -> Iterator[bytes]:
    buffer = bytearray()
    for piece in iter(lambda: stream.read(read_size), b""):
        if not piece:
            break
        buffer.extend(piece)
        while len(buffer) >= chunk_size:
            yield bytes(buffer[:chunk_size])
            del buffer[:chunk_size]
    if buffer:
        yield bytes(buffer)


def upload_part(
    s3_client: Any,
    upload_plan: MultipartUploadPlan,
    part_number: int,
    chunk: bytes,
) -> UploadedPart:
    response = s3_client.upload_part(
        Bucket=upload_plan.bucket,
        Key=upload_plan.key,
        UploadId=upload_plan.upload_id,
        PartNumber=part_number,
        Body=chunk,
        **(
            {"ExpectedBucketOwner": upload_plan.expected_bucket_owner}
            if upload_plan.expected_bucket_owner
            else {}
        ),
    )
    return UploadedPart(
        part_number=part_number,
        etag=response["ETag"],
        size_bytes=len(chunk),
    )


def upload_stream_parts(
    s3_client: Any,
    upload_plan: MultipartUploadPlan,
    stream: BinaryIO,
    runtime: RuntimeSettings,
) -> Tuple[UploadedPart, ...]:
    backlog_limit = runtime.upload_workers + runtime.queue_size
    part_futures: deque = deque()
    uploaded_parts: List[UploadedPart] = []
    with ThreadPoolExecutor(
        max_workers=runtime.upload_workers,
        thread_name_prefix="s3-part-upload",
    ) as executor:
        for part_number, chunk in enumerate(
            iter_stream_chunks(stream, runtime.multipart_chunk_bytes),
            start=1,
        ):
            if part_number > MAX_MULTIPART_PARTS:
                raise SnapshotError(
                    "O snapshot excedeu o limite de 10.000 partes do S3. "
                    "Aumente `multipart_chunk_mb`."
                )
            part_futures.append(
                executor.submit(upload_part, s3_client, upload_plan, part_number, chunk)
            )
            if len(part_futures) >= backlog_limit:
                uploaded_parts.append(part_futures.popleft().result())
        while part_futures:
            uploaded_parts.append(part_futures.popleft().result())
    return tuple(uploaded_parts)


def create_multipart_upload(
    s3_client: Any,
    s3_settings: S3Settings,
    target: Target,
    key: str,
    started_at: datetime,
) -> MultipartUploadPlan:
    response = s3_client.create_multipart_upload(
        **build_create_multipart_args(s3_settings, target, key, started_at)
    )
    return MultipartUploadPlan(
        bucket=s3_settings.bucket,
        key=key,
        upload_id=response["UploadId"],
        expected_bucket_owner=s3_settings.bucket_owner,
    )


def complete_multipart_upload(
    s3_client: Any,
    upload_plan: MultipartUploadPlan,
    uploaded_parts: Iterable[UploadedPart],
) -> UploadResult:
    ordered_parts = sorted(uploaded_parts, key=lambda item: item.part_number)
    if not ordered_parts:
        raise SnapshotError("Nenhum dado foi gerado pelo pipeline de backup.")
    s3_client.complete_multipart_upload(
        Bucket=upload_plan.bucket,
        Key=upload_plan.key,
        UploadId=upload_plan.upload_id,
        MultipartUpload={
            "Parts": [
                {"PartNumber": part.part_number, "ETag": part.etag}
                for part in ordered_parts
            ]
        },
        **(
            {"ExpectedBucketOwner": upload_plan.expected_bucket_owner}
            if upload_plan.expected_bucket_owner
            else {}
        ),
    )
    return UploadResult(
        bucket=upload_plan.bucket,
        key=upload_plan.key,
        size_bytes=sum(part.size_bytes for part in ordered_parts),
        part_count=len(ordered_parts),
    )


def abort_multipart_upload(s3_client: Any, upload_plan: MultipartUploadPlan) -> None:
    s3_client.abort_multipart_upload(
        Bucket=upload_plan.bucket,
        Key=upload_plan.key,
        UploadId=upload_plan.upload_id,
        **(
            {"ExpectedBucketOwner": upload_plan.expected_bucket_owner}
            if upload_plan.expected_bucket_owner
            else {}
        ),
    )


def redact_uri(uri: str) -> str:
    return re.sub(r"//([^:@/]+):([^@/]+)@", r"//\1:***@", uri)


def describe_plan(app_config: AppConfig, started_at: datetime) -> Dict[str, Any]:
    command = build_mongodump_command(app_config.target)
    compressor_command, compressor_name = select_compressor(app_config.runtime)
    key = build_s3_key(app_config.s3.prefix, app_config.target.name, started_at)
    return {
        "target": app_config.target.name,
        "bucket": app_config.s3.bucket,
        "key": key,
        "mongodump_command": [
            redact_uri(item) if index == 2 else item
            for index, item in enumerate(command)
        ],
        "compressor": compressor_name,
        "compressor_command": list(compressor_command),
        "multipart_chunk_mb": app_config.runtime.multipart_chunk_bytes // MB,
        "upload_workers": app_config.runtime.upload_workers,
        "queue_size": app_config.runtime.queue_size,
    }


def terminate_process(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_process(
    process: subprocess.Popen,
    stderr_thread: Optional[threading.Thread],
    stderr_buffer: List[str],
    label: str,
) -> None:
    return_code = process.wait()
    if stderr_thread:
        stderr_thread.join(timeout=5)
    if return_code != 0:
        stderr_text = "".join(stderr_buffer).strip()
        lowered_stderr = stderr_text.lower()
        if "atlasproxy" in lowered_stderr and "atlasversion" in lowered_stderr:
            raise SnapshotError(
                "Falha de compatibilidade do mongodump com DocumentDB: "
                "erro atlasProxy/atlasVersion. "
                "Use MongoDB Database Tools versão 100.6.1 (recomendação AWS para DocumentDB)."
            )
        raise SnapshotError(
            f"{label} falhou com código {return_code}. stderr: {stderr_text or 'sem detalhes'}"
        )


def backup_target(app_config: AppConfig) -> SnapshotResult:
    started_at = datetime.now(timezone.utc)
    key = build_s3_key(app_config.s3.prefix, app_config.target.name, started_at)
    mongodump_command = build_mongodump_command(app_config.target)
    compressor_command, compressor_name = select_compressor(app_config.runtime)
    max_pool_connections = max(8, app_config.runtime.upload_workers + app_config.runtime.queue_size + 2)
    s3_client = create_s3_client(app_config.s3, max_pool_connections=max_pool_connections)
    LOGGER.info(
        "Iniciando target=%s compressor=%s chunk_mb=%s upload_workers=%s key=s3://%s/%s",
        app_config.target.name,
        compressor_name,
        app_config.runtime.multipart_chunk_bytes // MB,
        app_config.runtime.upload_workers,
        app_config.s3.bucket,
        key,
    )

    mongodump_process = subprocess.Popen(
        mongodump_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    compressor_process = subprocess.Popen(
        compressor_command,
        stdin=mongodump_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if mongodump_process.stdout is not None:
        mongodump_process.stdout.close()

    mongodump_stderr_thread, mongodump_stderr_buffer = spawn_stderr_collector(
        mongodump_process.stderr
    )
    compressor_stderr_thread, compressor_stderr_buffer = spawn_stderr_collector(
        compressor_process.stderr
    )

    upload_plan = create_multipart_upload(
        s3_client=s3_client,
        s3_settings=app_config.s3,
        target=app_config.target,
        key=key,
        started_at=started_at,
    )
    try:
        if compressor_process.stdout is None:
            raise SnapshotError("O compressor não expôs stdout para streaming.")
        uploaded_parts = upload_stream_parts(
            s3_client=s3_client,
            upload_plan=upload_plan,
            stream=compressor_process.stdout,
            runtime=app_config.runtime,
        )
        compressor_process.stdout.close()
        wait_process(
            mongodump_process,
            mongodump_stderr_thread,
            mongodump_stderr_buffer,
            "mongodump",
        )
        wait_process(
            compressor_process,
            compressor_stderr_thread,
            compressor_stderr_buffer,
            compressor_name,
        )
        upload_result = complete_multipart_upload(s3_client, upload_plan, uploaded_parts)
    except Exception:
        terminate_process(compressor_process)
        terminate_process(mongodump_process)
        with contextlib.suppress(Exception):
            abort_multipart_upload(s3_client, upload_plan)
        raise

    finished_at = datetime.now(timezone.utc)
    LOGGER.info(
        "Target concluído=%s size_mb=%.2f parts=%s duration_seconds=%.2f",
        app_config.target.name,
        upload_result.size_bytes / MB,
        upload_result.part_count,
        (finished_at - started_at).total_seconds(),
    )
    return SnapshotResult(
        target_name=app_config.target.name,
        bucket=upload_result.bucket,
        key=upload_result.key,
        size_bytes=upload_result.size_bytes,
        started_at=started_at,
        finished_at=finished_at,
    )


def format_result_line(result: SnapshotResult) -> str:
    duration_seconds = (result.finished_at - result.started_at).total_seconds()
    return (
        f"target={result.target_name} "
        f"s3://{result.bucket}/{result.key} "
        f"size_mb={result.size_bytes / MB:.2f} "
        f"duration_seconds={duration_seconds:.2f}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    configure_logging(args.log_level)
    try:
        app_config = load_app_config_from_args(args)
        require_binary("mongodump")
        plan = describe_plan(app_config, datetime.now(timezone.utc))
        if args.dry_run:
            LOGGER.info("Plano payload=%s", json.dumps(plan, ensure_ascii=False))
            return 0
        result = backup_target(app_config)
    except SnapshotError as exc:
        LOGGER.error("%s", exc)
        return 1
    except subprocess.SubprocessError as exc:
        LOGGER.error("Falha ao executar subprocesso: %s", exc)
        return 1

    LOGGER.info("Resumo %s", format_result_line(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
