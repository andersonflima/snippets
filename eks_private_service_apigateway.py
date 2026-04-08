#!/usr/bin/env python3
"""Provisiona NLB, VPC Link e API Gateway REST.

Instrucoes de uso
=================

Pre-requisitos para AWS real:
- python3 com boto3 instalado
- boto3 autenticado na conta correta
- para o modo direto de NLB: subnets do NLB e targets, quando o NLB ainda nao existir
- para o modo legado de EKS: aws CLI e kubectl instalados e com acesso ao cluster

Comando minimo no modo direto:

    python3 eks_private_service_apigateway.py \
      --region us-east-1 \
      --nlb-name meu-nlb \
      --nlb-subnet-id subnet-aaa \
      --nlb-subnet-id subnet-bbb \
      --target-group-name meu-nlb-tg \
      --target-id i-0123456789abcdef0

Comando minimo no modo legado de EKS:

    python3 eks_private_service_apigateway.py \
      --region us-east-1 \
      --cluster-name meu-cluster-eks \
      --service-name my-api

Defaults importantes:
- namespace: default
- service-port: 80
- target-port: 3000
- selector padrao: app=<service-name>
- nlb-scheme: internal
- stage-name: prod

Exemplo com parametros mais comuns:

    python3 eks_private_service_apigateway.py \
      --region us-east-1 \
      --cluster-name meu-cluster-eks \
      --service-name my-api \
      --namespace backend \
      --target-port 8080 \
      --selector app.kubernetes.io/name=my-api \
      --selector app.kubernetes.io/component=api

Quando usar alguns parametros:
- --nlb-arn: para integrar com um NLB existente de forma direta
- --nlb-name: para reutilizar um NLB pelo nome ou criar se nao existir
- --nlb-subnet-id: subnets usadas para criar o NLB quando ele ainda nao existe
- --target-group-name: nome do target group a reutilizar ou criar
- --target-id: targets a registrar no target group quando ele ainda nao existir ou estiver incompleto
- --namespace: quando o Service nao fica em default
- --target-port: quando a aplicacao escuta em outra porta
- --selector chave=valor: quando o label selector nao eh app=<service-name>
- --service-port: quando voce quer expor outra porta no Service
- --nlb-scheme: use internet-facing se o NLB precisar ser publico
- --api-name: para customizar o nome da REST API
- --vpc-link-name: para customizar o nome do VPC Link
- --stage-name: para publicar em outro stage
- --skip-kubeconfig-update: use so quando o kubeconfig ja estiver pronto
- --dry-run: mostra manifesto e configuracao derivada sem aplicar mudancas

Pre-check recomendado no modo direto:

    aws sts get-caller-identity
    aws elbv2 describe-load-balancers --names meu-nlb

Pre-check recomendado no modo legado de EKS:

    aws sts get-caller-identity
    aws eks describe-cluster --region us-east-1 --name meu-cluster-eks
    kubectl config current-context
    kubectl get pods -n backend --show-labels

Permissoes AWS esperadas:
- eks:DescribeCluster
- elasticloadbalancing:DescribeLoadBalancers
- apigateway:GET
- apigateway:POST
- apigateway:PUT

Observacao:
- para AWS real, nao passe --aws-endpoint-url
- o modo direto nao usa kubectl
- o modo legado de EKS continua disponivel quando voce nao informa NLB direto
- no modo legado, o script normaliza automaticamente exec apiVersion legado no kubeconfig
  (`v1alpha1` ou typo `v1aplha1`) para `v1beta1` antes de usar o kubectl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Sequence
from urllib.parse import urlparse

LOGGER = logging.getLogger("eks_private_service_apigateway")
DIRECT_NLB_MODE = "nlb-direct"
EKS_SERVICE_MODE = "eks-service"
DEFAULT_STAGE_NAME = "prod"
DEFAULT_NAMESPACE = "default"
DEFAULT_SERVICE_PORT = 80
DEFAULT_TARGET_PORT = 3000
DEFAULT_TIMEOUT_SECONDS = 900
DEFAULT_POLL_INTERVAL_SECONDS = 10
LEGACY_EXEC_API_VERSION_TYPO = "client.authentication.k8s.io/v1aplha1"
LEGACY_EXEC_API_VERSION = "client.authentication.k8s.io/v1alpha1"
SUPPORTED_EXEC_API_VERSION = "client.authentication.k8s.io/v1beta1"
STATIC_TOKEN_REFRESH_SKEW_SECONDS = 60


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Garante NLB, VPC Link e API Gateway REST. "
            "No modo direto usa apenas boto3; no modo legado ainda pode usar EKS + kubectl."
        )
    )
    parser.add_argument(
        "--region",
        default=first_non_empty_text(
            os.getenv("AWS_REGION"),
            os.getenv("AWS_DEFAULT_REGION"),
            "us-east-1",
        ),
        help="Região AWS. Default: AWS_REGION, AWS_DEFAULT_REGION ou us-east-1.",
    )
    parser.add_argument("--cluster-name", help="Nome do cluster EKS no modo legado.")
    parser.add_argument("--service-name", help="Nome do Service Kubernetes no modo legado.")
    parser.add_argument("--nlb-arn", help="ARN do NLB existente a integrar.")
    parser.add_argument(
        "--nlb-name",
        help="Nome do NLB existente ou a criar no modo direto.",
    )
    parser.add_argument(
        "--nlb-subnet-id",
        action="append",
        default=[],
        help="Subnet do NLB no modo direto. Pode ser repetido.",
    )
    parser.add_argument(
        "--target-group-arn",
        help="ARN do target group existente no modo direto.",
    )
    parser.add_argument(
        "--target-group-name",
        help="Nome do target group existente ou a criar no modo direto.",
    )
    parser.add_argument(
        "--target-id",
        action="append",
        default=[],
        help="Target do target group no modo direto. Pode ser repetido.",
    )
    parser.add_argument(
        "--target-type",
        choices=("instance", "ip"),
        default="instance",
        help="Tipo de target do target group no modo direto. Default: instance.",
    )
    parser.add_argument(
        "--listener-protocol",
        choices=("TCP", "TLS"),
        default="TCP",
        help="Protocolo do listener do NLB no modo direto. Default: TCP.",
    )
    parser.add_argument(
        "--aws-endpoint-url",
        default=first_non_empty_text(os.getenv("AWS_ENDPOINT_URL")),
        help="Endpoint AWS customizado, como LocalStack. Default: AWS_ENDPOINT_URL.",
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help=f"Namespace Kubernetes. Default: {DEFAULT_NAMESPACE}.",
    )
    parser.add_argument(
        "--service-port",
        type=positive_int,
        default=DEFAULT_SERVICE_PORT,
        help=f"Porta exposta no Service. Default: {DEFAULT_SERVICE_PORT}.",
    )
    parser.add_argument(
        "--target-port",
        type=positive_int,
        default=DEFAULT_TARGET_PORT,
        help=f"Target port do backend. Default: {DEFAULT_TARGET_PORT}.",
    )
    parser.add_argument(
        "--selector",
        action="append",
        default=[],
        help=(
            "Selector do Service no formato chave=valor. "
            "Pode ser repetido. Default: app=<service-name>."
        ),
    )
    parser.add_argument(
        "--annotation",
        action="append",
        default=[],
        help=(
            "Annotation extra do Service no formato chave=valor. "
            "Pode ser repetido."
        ),
    )
    parser.add_argument(
        "--nlb-scheme",
        choices=("internal", "internet-facing"),
        default="internal",
        help="Scheme do NLB. Default: internal.",
    )
    parser.add_argument(
        "--vpc-link-name",
        help="Nome do VPC Link. Default: <cluster-name>-vpc-link.",
    )
    parser.add_argument(
        "--api-name",
        help="Nome da REST API. Default: <cluster-name>-api.",
    )
    parser.add_argument(
        "--stage-name",
        default=DEFAULT_STAGE_NAME,
        help=f"Stage da API. Default: {DEFAULT_STAGE_NAME}.",
    )
    parser.add_argument(
        "--api-endpoint-type",
        choices=("REGIONAL", "EDGE", "PRIVATE"),
        default="REGIONAL",
        help="Tipo de endpoint da REST API. Default: REGIONAL.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=positive_int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Timeout total por etapa de espera. Default: {DEFAULT_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=positive_int,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help=(
            "Intervalo entre polls de espera. "
            f"Default: {DEFAULT_POLL_INTERVAL_SECONDS}."
        ),
    )
    parser.add_argument(
        "--skip-kubeconfig-update",
        action="store_true",
        help="Não executa aws eks update-kubeconfig.",
    )
    parser.add_argument(
        "--skip-cluster-check",
        action="store_true",
        help="Não valida o status do cluster via EKS antes de seguir.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Nível de log.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o manifesto e os nomes derivados sem aplicar mudanças.",
    )
    return parser.parse_args(argv)


def first_non_empty_text(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        normalized = value.strip()
        if normalized:
            return normalized
    return None


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("O valor precisa ser um inteiro positivo.")
    return parsed


def parse_key_value_items(items: Sequence[str], label: str) -> Dict[str, str]:
    pairs = [parse_key_value_item(item, label) for item in items]
    return {key: value for key, value in pairs}


def parse_key_value_item(item: str, label: str) -> tuple[str, str]:
    if "=" not in item:
        raise ValueError(f"{label} inválido: {item}. Use o formato chave=valor.")
    key, value = item.split("=", 1)
    normalized_key = key.strip()
    normalized_value = value.strip()
    if not normalized_key or not normalized_value:
        raise ValueError(f"{label} inválido: {item}. Use o formato chave=valor.")
    return normalized_key, normalized_value


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def direct_nlb_mode_requested(args: argparse.Namespace) -> bool:
    return any(
        [
            bool(args.nlb_arn),
            bool(args.nlb_name),
            bool(args.nlb_subnet_id),
            bool(args.target_group_arn),
            bool(args.target_group_name),
            bool(args.target_id),
        ]
    )


def derive_resource_base_name(args: argparse.Namespace) -> str:
    return (
        first_non_empty_text(
            args.nlb_name,
            args.cluster_name,
            args.service_name,
            "managed-nlb",
        )
        or "managed-nlb"
    )


def build_config_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    mode = DIRECT_NLB_MODE if direct_nlb_mode_requested(args) else EKS_SERVICE_MODE
    base_name = derive_resource_base_name(args)

    if mode == DIRECT_NLB_MODE and not first_non_empty_text(args.nlb_arn, args.nlb_name):
        raise ValueError(
            "No modo direto informe --nlb-arn ou --nlb-name."
        )
    if mode == EKS_SERVICE_MODE and not args.cluster_name:
        raise ValueError(
            "Informe --cluster-name no modo legado de EKS."
        )
    if mode == EKS_SERVICE_MODE and not args.service_name:
        raise ValueError(
            "Informe --service-name no modo legado de EKS."
        )

    selector = parse_key_value_items(args.selector, "selector") or {
        "app": args.service_name or base_name,
    }
    annotations = {
        "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
        "service.beta.kubernetes.io/aws-load-balancer-scheme": args.nlb_scheme,
    }
    annotations.update(parse_key_value_items(args.annotation, "annotation"))
    return {
        "mode": mode,
        "region": args.region,
        "cluster_name": args.cluster_name,
        "service_name": args.service_name or base_name,
        "aws_endpoint_url": args.aws_endpoint_url,
        "namespace": args.namespace,
        "service_port": args.service_port,
        "target_port": args.target_port,
        "selector": selector,
        "annotations": annotations,
        "nlb_arn": first_non_empty_text(args.nlb_arn),
        "nlb_name": first_non_empty_text(args.nlb_name),
        "nlb_subnet_ids": list(args.nlb_subnet_id),
        "target_group_arn": first_non_empty_text(args.target_group_arn),
        "target_group_name": (
            first_non_empty_text(args.target_group_name, f"{base_name}-tg") or f"{base_name}-tg"
        ),
        "target_ids": list(args.target_id),
        "target_type": args.target_type,
        "listener_protocol": args.listener_protocol,
        "vpc_link_name": args.vpc_link_name or f"{base_name}-vpc-link",
        "api_name": args.api_name or f"{base_name}-api",
        "stage_name": args.stage_name,
        "api_endpoint_type": args.api_endpoint_type,
        "timeout_seconds": args.timeout_seconds,
        "poll_interval_seconds": args.poll_interval_seconds,
        "skip_kubeconfig_update": args.skip_kubeconfig_update,
        "skip_cluster_check": args.skip_cluster_check,
        "dry_run": args.dry_run,
        "_kubectl_auth_state": None,
    }


def namespace_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    return build_config_from_args(args)


def ensure_commands_exist(commands: Iterable[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(f"Dependências ausentes no PATH: {joined}.")


def build_clients(region: str, endpoint_url: Optional[str]) -> Dict[str, Any]:
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "boto3 não está instalado no ambiente atual. Instale a dependência para executar o script."
        ) from exc

    return {
        "eks": boto3.client("eks", region_name=region, endpoint_url=endpoint_url),
        "apigateway": boto3.client("apigateway", region_name=region, endpoint_url=endpoint_url),
        "elbv2": boto3.client("elbv2", region_name=region, endpoint_url=endpoint_url),
    }


def run_command(
    command: Sequence[str],
    *,
    input_text: Optional[str] = None,
    check: bool = True,
) -> str:
    LOGGER.debug("Executando comando: %s", " ".join(command))
    result = subprocess.run(
        command,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        details = stderr or stdout or "sem saída adicional"
        raise RuntimeError(
            f"Falha ao executar {' '.join(command)}: {details}"
    )
    return result.stdout


def build_kubectl_auth_state() -> Dict[str, Any]:
    return {
        "mode": "default",
        "warning_logged": False,
        "cluster": None,
        "token": None,
        "token_expiration": None,
        "kubeconfig_path": None,
    }


def is_legacy_exec_credential_error(message: str) -> bool:
    normalized_message = message.lower()
    return (
        "execcredential" in normalized_message
        and (
            LEGACY_EXEC_API_VERSION in normalized_message
            or LEGACY_EXEC_API_VERSION_TYPO in normalized_message
        )
    )


def assert_cluster_available(eks_client: Any, cluster_name: str) -> Dict[str, Any]:
    cluster = eks_client.describe_cluster(name=cluster_name)["cluster"]
    status = cluster.get("status")
    if status != "ACTIVE":
        raise RuntimeError(
            f"Cluster {cluster_name} não está ACTIVE. Status atual: {status}."
        )
    LOGGER.info("Cluster EKS validado: %s", cluster_name)
    return cluster


def update_kubeconfig(config: Dict[str, Any]) -> None:
    if config["skip_kubeconfig_update"]:
        LOGGER.info("Pulando update-kubeconfig por configuração explícita.")
        return
    LOGGER.info("Atualizando kubeconfig do cluster %s", config["cluster_name"])
    command = [
        "aws",
        "eks",
        "update-kubeconfig",
        "--region",
        config["region"],
        "--name",
        config["cluster_name"],
    ]
    if config["aws_endpoint_url"]:
        command.extend(["--endpoint-url", config["aws_endpoint_url"]])
    run_command(command)


def resolve_kubeconfig_paths() -> list[str]:
    configured_paths = first_non_empty_text(os.getenv("KUBECONFIG"))
    if configured_paths:
        return [
            path
            for path in configured_paths.split(os.pathsep)
            if path.strip()
        ]
    return [os.path.expanduser("~/.kube/config")]


def normalize_kubeconfig_exec_api_version_file(kubeconfig_path: str) -> bool:
    if not kubeconfig_path or not os.path.exists(kubeconfig_path):
        return False

    with open(kubeconfig_path, "r", encoding="utf-8") as kubeconfig_file:
        original_content = kubeconfig_file.read()

    normalized_content = (
        original_content
        .replace(LEGACY_EXEC_API_VERSION_TYPO, SUPPORTED_EXEC_API_VERSION)
        .replace(LEGACY_EXEC_API_VERSION, SUPPORTED_EXEC_API_VERSION)
    )
    if normalized_content == original_content:
        return False

    backup_path = f"{kubeconfig_path}.bak"
    shutil.copyfile(kubeconfig_path, backup_path)
    with open(kubeconfig_path, "w", encoding="utf-8") as kubeconfig_file:
        kubeconfig_file.write(normalized_content)
    LOGGER.info(
        "Kubeconfig normalizado: %s (backup em %s)",
        kubeconfig_path,
        backup_path,
    )
    return True


def normalize_kubeconfig_exec_api_versions() -> list[str]:
    changed_paths = [
        kubeconfig_path
        for kubeconfig_path in resolve_kubeconfig_paths()
        if normalize_kubeconfig_exec_api_version_file(kubeconfig_path)
    ]
    if changed_paths:
        LOGGER.info(
            "Exec apiVersion legado corrigido em: %s",
            ", ".join(changed_paths),
        )
    return changed_paths


def load_cluster_details(eks_client: Any, cluster_name: str) -> Dict[str, Any]:
    return eks_client.describe_cluster(name=cluster_name)["cluster"]


def ensure_kubectl_auth_state(config: Dict[str, Any]) -> Dict[str, Any]:
    existing_state = config.get("_kubectl_auth_state")
    if existing_state is not None:
        return existing_state

    auth_state = build_kubectl_auth_state()
    config["_kubectl_auth_state"] = auth_state
    return auth_state


def build_eks_get_token_command(config: Dict[str, Any]) -> list[str]:
    command = [
        "aws",
        "eks",
        "get-token",
        "--region",
        config["region"],
        "--cluster-name",
        config["cluster_name"],
    ]
    if config["aws_endpoint_url"]:
        command.extend(["--endpoint-url", config["aws_endpoint_url"]])
    return command


def parse_eks_token_expiration(expiration_timestamp: str) -> datetime:
    normalized_timestamp = expiration_timestamp.strip()
    if normalized_timestamp.endswith("Z"):
        normalized_timestamp = normalized_timestamp[:-1] + "+00:00"
    return datetime.fromisoformat(normalized_timestamp).astimezone(timezone.utc)


def extract_eks_token(config: Dict[str, Any]) -> tuple[str, datetime]:
    output = run_command(build_eks_get_token_command(config))
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Falha ao decodificar a saída do aws eks get-token."
        ) from exc

    token = payload.get("status", {}).get("token")
    if not token:
        raise RuntimeError(
            "aws eks get-token não retornou status.token."
        )
    expiration_timestamp = payload.get("status", {}).get("expirationTimestamp")
    if not expiration_timestamp:
        raise RuntimeError(
            "aws eks get-token não retornou status.expirationTimestamp."
        )
    return token, parse_eks_token_expiration(expiration_timestamp)


def build_ephemeral_kubeconfig(cluster: Dict[str, Any], token: str) -> Dict[str, Any]:
    cluster_name = cluster.get("name") or "cluster"
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster_name,
                "cluster": {
                    "server": cluster["endpoint"],
                    "certificate-authority-data": cluster["certificateAuthority"]["data"],
                },
            }
        ],
        "users": [
            {
                "name": f"{cluster_name}-token",
                "user": {"token": token},
            }
        ],
        "contexts": [
            {
                "name": f"{cluster_name}-context",
                "context": {
                    "cluster": cluster_name,
                    "user": f"{cluster_name}-token",
                },
            }
        ],
        "current-context": f"{cluster_name}-context",
    }


def ensure_static_token_kubeconfig(
    config: Dict[str, Any],
    eks_client: Any,
    auth_state: Dict[str, Any],
) -> str:
    now = datetime.now(timezone.utc)
    token_expiration = auth_state.get("token_expiration")
    kubeconfig_path = auth_state.get("kubeconfig_path")
    refresh_deadline = now + timedelta(seconds=STATIC_TOKEN_REFRESH_SKEW_SECONDS)
    token_is_still_valid = (
        isinstance(token_expiration, datetime)
        and token_expiration > refresh_deadline
        and isinstance(kubeconfig_path, str)
        and bool(kubeconfig_path)
        and os.path.exists(kubeconfig_path)
    )
    if token_is_still_valid:
        return kubeconfig_path

    cluster = auth_state.get("cluster")
    if cluster is None:
        cluster = load_cluster_details(eks_client, config["cluster_name"])
        auth_state["cluster"] = cluster

    cluster_name = cluster.get("name", config["cluster_name"])
    token, token_expiration = extract_eks_token(config)
    kubeconfig_payload = build_ephemeral_kubeconfig(cluster, token)
    kubeconfig_path = auth_state.get("kubeconfig_path")

    if not isinstance(kubeconfig_path, str) or not kubeconfig_path:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f"{cluster_name}-kubectl-",
            suffix=".json",
            delete=False,
        ) as kubeconfig_file:
            kubeconfig_path = kubeconfig_file.name
    with open(kubeconfig_path, "w", encoding="utf-8") as kubeconfig_file:
        json.dump(kubeconfig_payload, kubeconfig_file)

    auth_state["token"] = token
    auth_state["token_expiration"] = token_expiration
    auth_state["kubeconfig_path"] = kubeconfig_path
    auth_state["mode"] = "static-token"
    return kubeconfig_path


def run_kubectl_with_static_eks_token(
    config: Dict[str, Any],
    eks_client: Any,
    kubectl_args: Sequence[str],
    *,
    input_text: Optional[str] = None,
) -> str:
    auth_state = ensure_kubectl_auth_state(config)
    kubeconfig_path = ensure_static_token_kubeconfig(config, eks_client, auth_state)

    if not auth_state.get("warning_logged"):
        LOGGER.warning(
            "ExecCredential legado detectado no kubeconfig. "
            "Reexecutando kubectl com token estático do aws eks get-token "
            "e reutilizando esse modo durante o polling."
        )
        auth_state["warning_logged"] = True
    return run_command(
        ["kubectl", "--kubeconfig", kubeconfig_path, *kubectl_args],
        input_text=input_text,
    )


def cleanup_kubectl_auth_state(config: Dict[str, Any]) -> None:
    auth_state = config.get("_kubectl_auth_state")
    if not isinstance(auth_state, dict):
        return

    kubeconfig_path = auth_state.get("kubeconfig_path")
    if isinstance(kubeconfig_path, str) and kubeconfig_path:
        try:
            os.remove(kubeconfig_path)
        except OSError:
            LOGGER.debug("Não foi possível remover kubeconfig temporário: %s", kubeconfig_path)


def run_kubectl_command(
    config: Dict[str, Any],
    eks_client: Any,
    kubectl_args: Sequence[str],
    *,
    input_text: Optional[str] = None,
) -> str:
    auth_state = ensure_kubectl_auth_state(config)
    if auth_state.get("mode") == "static-token":
        return run_kubectl_with_static_eks_token(
            config,
            eks_client,
            kubectl_args,
            input_text=input_text,
        )

    try:
        return run_command(["kubectl", *kubectl_args], input_text=input_text)
    except RuntimeError as exc:
        if not is_legacy_exec_credential_error(str(exc)):
            raise
        return run_kubectl_with_static_eks_token(
            config,
            eks_client,
            kubectl_args,
            input_text=input_text,
        )


def build_service_manifest(config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": config["service_name"],
            "namespace": config["namespace"],
            "annotations": config["annotations"],
        },
        "spec": {
            "type": "LoadBalancer",
            "selector": config["selector"],
            "ports": [
                {
                    "name": "http",
                    "port": config["service_port"],
                    "targetPort": config["target_port"],
                    "protocol": "TCP",
                }
            ],
        },
    }


def apply_service_manifest(config: Dict[str, Any], eks_client: Any) -> None:
    manifest = build_service_manifest(config)
    manifest_text = json.dumps(manifest, indent=2)
    LOGGER.info(
        "Aplicando Service %s/%s",
        config["namespace"],
        config["service_name"],
    )
    run_kubectl_command(
        config,
        eks_client,
        ["apply", "-f", "-"],
        input_text=manifest_text,
    )


def load_service(config: Dict[str, Any], eks_client: Any) -> Dict[str, Any]:
    output = run_kubectl_command(
        config,
        eks_client,
        [
            "get",
            "svc",
            config["service_name"],
            "-n",
            config["namespace"],
            "-o",
            "json",
        ],
    )
    return json.loads(output)


def load_service_events(config: Dict[str, Any], eks_client: Any) -> list[Dict[str, Any]]:
    output = run_kubectl_command(
        config,
        eks_client,
        [
            "get",
            "events",
            "-n",
            config["namespace"],
            "--field-selector",
            (
                "involvedObject.kind=Service,"
                f"involvedObject.name={config['service_name']}"
            ),
            "-o",
            "json",
        ],
    )
    return json.loads(output).get("items", [])


def wait_for(
    description: str,
    supplier: Callable[[], Optional[Any]],
    *,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> Any:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = supplier()
        if result is not None:
            return result
        LOGGER.info("Aguardando %s", description)
        time.sleep(poll_interval_seconds)
    raise TimeoutError(
        f"Timeout aguardando {description} após {timeout_seconds} segundos."
    )


def extract_service_hostname(service: Dict[str, Any]) -> Optional[str]:
    ingress = (
        service.get("status", {})
        .get("loadBalancer", {})
        .get("ingress", [])
    )
    endpoints = [
        item.get("hostname") or item.get("ip")
        for item in ingress
        if item.get("hostname") or item.get("ip")
    ]
    return endpoints[0] if endpoints else None


def service_event_timestamp(event: Dict[str, Any]) -> str:
    series = event.get("series", {})
    return first_non_empty_text(
        event.get("eventTime"),
        series.get("lastObservedTime"),
        event.get("lastTimestamp"),
        event.get("firstTimestamp"),
        event.get("metadata", {}).get("creationTimestamp"),
        "",
    ) or ""


def summarize_service_events(events: Sequence[Dict[str, Any]], *, limit: int = 3) -> list[str]:
    sorted_events = sorted(
        events,
        key=service_event_timestamp,
    )
    selected_events = sorted_events[-limit:]
    summaries: list[str] = []
    for event in selected_events:
        event_type = first_non_empty_text(event.get("type"), "Normal")
        reason = first_non_empty_text(event.get("reason"), "SemReason")
        message = first_non_empty_text(event.get("message"), "sem mensagem")
        timestamp = service_event_timestamp(event)
        if timestamp:
            summaries.append(f"{timestamp} {event_type}/{reason}: {message}")
        else:
            summaries.append(f"{event_type}/{reason}: {message}")
    return summaries


def summarize_pending_service_nlb(
    service: Dict[str, Any],
    events: Sequence[Dict[str, Any]],
) -> str:
    service_type = first_non_empty_text(service.get("spec", {}).get("type"), "desconhecido")
    cluster_ip = first_non_empty_text(service.get("spec", {}).get("clusterIP"), "pendente")
    event_summaries = summarize_service_events(events)
    if event_summaries:
        return (
            f"type={service_type}, clusterIP={cluster_ip}, ingress=pendente. "
            f"Últimos eventos: {' | '.join(event_summaries)}"
        )
    return f"type={service_type}, clusterIP={cluster_ip}, ingress=pendente, sem eventos úteis."


def wait_for_nlb_hostname(config: Dict[str, Any], eks_client: Any) -> str:
    deadline = time.time() + config["timeout_seconds"]
    description = f"NLB do Service {config['namespace']}/{config['service_name']}"
    last_diagnostic = ""

    while time.time() < deadline:
        service = load_service(config, eks_client)
        hostname = extract_service_hostname(service)
        if hostname is not None:
            return hostname

        events = load_service_events(config, eks_client)
        diagnostic = summarize_pending_service_nlb(service, events)
        if diagnostic != last_diagnostic:
            LOGGER.warning(
                "Service %s/%s ainda sem NLB. %s",
                config["namespace"],
                config["service_name"],
                diagnostic,
            )
            last_diagnostic = diagnostic

        LOGGER.info("Aguardando %s", description)
        time.sleep(config["poll_interval_seconds"])

    if last_diagnostic:
        raise TimeoutError(
            f"Timeout aguardando {description} após {config['timeout_seconds']} segundos. "
            f"Último diagnóstico: {last_diagnostic}"
        )
    raise TimeoutError(
        f"Timeout aguardando {description} após {config['timeout_seconds']} segundos."
    )


def paginate(fetch_page: Callable[[Optional[str]], Dict[str, Any]], key: str) -> list[Dict[str, Any]]:
    items: list[Dict[str, Any]] = []
    position: Optional[str] = None
    while True:
        response = fetch_page(position)
        items.extend(response.get(key, []))
        position = response.get("position")
        if not position:
            return items


def normalize_dns_name(dns_name: str) -> str:
    return dns_name.rstrip(".")


def find_load_balancer_by_name(elbv2_client: Any, load_balancer_name: str) -> Optional[Dict[str, Any]]:
    load_balancers = paginate(
        lambda marker: elbv2_client.describe_load_balancers(Marker=marker)
        if marker
        else elbv2_client.describe_load_balancers(),
        "LoadBalancers",
    )
    for load_balancer in load_balancers:
        if load_balancer.get("LoadBalancerName") == load_balancer_name:
            return load_balancer
    return None


def load_load_balancer_by_arn(elbv2_client: Any, load_balancer_arn: str) -> Dict[str, Any]:
    response = elbv2_client.describe_load_balancers(LoadBalancerArns=[load_balancer_arn])
    load_balancers = response.get("LoadBalancers", [])
    if not load_balancers:
        raise RuntimeError(f"NLB não encontrado para o ARN {load_balancer_arn}.")
    return load_balancers[0]


def assert_network_load_balancer(load_balancer: Dict[str, Any]) -> Dict[str, Any]:
    if load_balancer.get("Type") != "network":
        raise RuntimeError(
            f"O load balancer {load_balancer.get('LoadBalancerName') or load_balancer.get('LoadBalancerArn')} não é do tipo network."
        )
    return load_balancer


def load_load_balancer_state(load_balancer: Dict[str, Any]) -> str:
    return first_non_empty_text(load_balancer.get("State", {}).get("Code"), "unknown") or "unknown"


def wait_for_nlb_active(elbv2_client: Any, load_balancer_arn: str, config: Dict[str, Any]) -> Dict[str, Any]:
    def load_when_ready() -> Optional[Dict[str, Any]]:
        load_balancer = assert_network_load_balancer(
            load_load_balancer_by_arn(elbv2_client, load_balancer_arn)
        )
        state = load_load_balancer_state(load_balancer)
        if state == "active":
            return load_balancer
        if state in {"failed"}:
            raise RuntimeError(
                f"NLB {load_balancer_arn} terminou em estado inválido: {state}."
            )
        return None

    return wait_for(
        f"NLB {load_balancer_arn}",
        load_when_ready,
        timeout_seconds=config["timeout_seconds"],
        poll_interval_seconds=config["poll_interval_seconds"],
    )


def create_network_load_balancer(elbv2_client: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    if not config["nlb_name"]:
        raise RuntimeError("Para criar NLB informe --nlb-name.")
    if len(config["nlb_subnet_ids"]) < 2:
        raise RuntimeError(
            "Para criar NLB informe ao menos duas --nlb-subnet-id."
        )
    response = elbv2_client.create_load_balancer(
        Name=config["nlb_name"],
        Subnets=config["nlb_subnet_ids"],
        Scheme=config["nlb_scheme"],
        Type="network",
        IpAddressType="ipv4",
    )
    return response["LoadBalancers"][0]


def ensure_network_load_balancer(elbv2_client: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    if config["nlb_arn"]:
        LOGGER.info("Reutilizando NLB informado por ARN: %s", config["nlb_arn"])
        return wait_for_nlb_active(elbv2_client, config["nlb_arn"], config)

    existing_load_balancer = find_load_balancer_by_name(elbv2_client, config["nlb_name"])
    if existing_load_balancer is not None:
        LOGGER.info("Reutilizando NLB existente: %s", existing_load_balancer["LoadBalancerArn"])
        return wait_for_nlb_active(
            elbv2_client,
            existing_load_balancer["LoadBalancerArn"],
            config,
        )

    LOGGER.info("Criando NLB %s", config["nlb_name"])
    created_load_balancer = create_network_load_balancer(elbv2_client, config)
    return wait_for_nlb_active(elbv2_client, created_load_balancer["LoadBalancerArn"], config)


def build_api_url(api_id: str, config: Dict[str, Any]) -> str:
    endpoint_url = config["aws_endpoint_url"]
    if endpoint_url:
        parsed = urlparse(endpoint_url)
        hostname = parsed.hostname or ""
        port = parsed.port
        scheme = parsed.scheme or "http"
        is_localstack = (
            "localstack" in hostname
            or hostname in {"localhost", "127.0.0.1", "::1", "host.docker.internal"}
        )
        if is_localstack:
            port_suffix = f":{port}" if port else ""
            return (
                f"{scheme}://{api_id}.execute-api.localhost.localstack.cloud"
                f"{port_suffix}/{config['stage_name']}"
            )

    return f"https://{api_id}.execute-api.{config['region']}.amazonaws.com/{config['stage_name']}"


def find_nlb_arn(elbv2_client: Any, dns_name: str) -> str:
    normalized_dns = normalize_dns_name(dns_name)
    load_balancers = paginate(
        lambda marker: elbv2_client.describe_load_balancers(Marker=marker)
        if marker
        else elbv2_client.describe_load_balancers(),
        "LoadBalancers",
    )
    for load_balancer in load_balancers:
        candidate = normalize_dns_name(load_balancer["DNSName"])
        if candidate == normalized_dns:
            if load_balancer.get("Type") != "network":
                raise RuntimeError(
                    f"Load balancer encontrado para {dns_name}, mas não é NLB."
                )
            return load_balancer["LoadBalancerArn"]
    raise RuntimeError(f"NLB não encontrado para o DNS {dns_name}.")


def load_listener_target_group_arn(listener: Dict[str, Any]) -> Optional[str]:
    for action in listener.get("DefaultActions", []):
        if action.get("Type") != "forward":
            continue
        direct_target_group = action.get("TargetGroupArn")
        if direct_target_group:
            return direct_target_group
        forward_config = action.get("ForwardConfig", {})
        target_groups = forward_config.get("TargetGroups", [])
        if target_groups:
            return target_groups[0].get("TargetGroupArn")
    return None


def load_nlb_listeners(elbv2_client: Any, load_balancer_arn: str) -> list[Dict[str, Any]]:
    response = elbv2_client.describe_listeners(LoadBalancerArn=load_balancer_arn)
    return response.get("Listeners", [])


def find_listener_by_port(
    listeners: Sequence[Dict[str, Any]],
    *,
    listener_port: int,
) -> Optional[Dict[str, Any]]:
    for listener in listeners:
        if listener.get("Port") == listener_port:
            return listener
    return None


def load_target_group_by_arn(elbv2_client: Any, target_group_arn: str) -> Dict[str, Any]:
    response = elbv2_client.describe_target_groups(TargetGroupArns=[target_group_arn])
    target_groups = response.get("TargetGroups", [])
    if not target_groups:
        raise RuntimeError(f"Target group não encontrado para o ARN {target_group_arn}.")
    return target_groups[0]


def find_target_group_by_name(elbv2_client: Any, target_group_name: str) -> Optional[Dict[str, Any]]:
    try:
        response = elbv2_client.describe_target_groups(Names=[target_group_name])
    except Exception:
        return None
    target_groups = response.get("TargetGroups", [])
    return target_groups[0] if target_groups else None


def ensure_target_group(elbv2_client: Any, load_balancer: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if config["target_group_arn"]:
        return load_target_group_by_arn(elbv2_client, config["target_group_arn"])

    existing_target_group = find_target_group_by_name(elbv2_client, config["target_group_name"])
    if existing_target_group is not None:
        return existing_target_group

    if not config["target_ids"]:
        return None

    LOGGER.info("Criando target group %s", config["target_group_name"])
    response = elbv2_client.create_target_group(
        Name=config["target_group_name"],
        Protocol=config["listener_protocol"],
        Port=config["target_port"],
        VpcId=load_balancer["VpcId"],
        TargetType=config["target_type"],
        HealthCheckProtocol=config["listener_protocol"],
        HealthCheckPort=str(config["target_port"]),
    )
    return response["TargetGroups"][0]


def ensure_target_group_targets(elbv2_client: Any, target_group_arn: str, config: Dict[str, Any]) -> None:
    if not config["target_ids"]:
        return
    targets = [{"Id": target_id, "Port": config["target_port"]} for target_id in config["target_ids"]]
    LOGGER.info("Registrando %s target(s) no target group %s", len(targets), target_group_arn)
    elbv2_client.register_targets(
        TargetGroupArn=target_group_arn,
        Targets=targets,
    )


def ensure_nlb_listener(
    elbv2_client: Any,
    load_balancer_arn: str,
    target_group_arn: Optional[str],
    config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    listeners = load_nlb_listeners(elbv2_client, load_balancer_arn)
    existing_listener = find_listener_by_port(
        listeners,
        listener_port=config["service_port"],
    )

    if existing_listener is not None:
        current_target_group_arn = load_listener_target_group_arn(existing_listener)
        if target_group_arn and current_target_group_arn != target_group_arn:
            LOGGER.info(
                "Atualizando listener %s para o target group %s",
                existing_listener["ListenerArn"],
                target_group_arn,
            )
            elbv2_client.modify_listener(
                ListenerArn=existing_listener["ListenerArn"],
                DefaultActions=[{"Type": "forward", "TargetGroupArn": target_group_arn}],
            )
            updated_listeners = load_nlb_listeners(elbv2_client, load_balancer_arn)
            return find_listener_by_port(updated_listeners, listener_port=config["service_port"])
        return existing_listener

    if not target_group_arn:
        return None

    LOGGER.info("Criando listener %s/%s", config["listener_protocol"], config["service_port"])
    response = elbv2_client.create_listener(
        LoadBalancerArn=load_balancer_arn,
        Protocol=config["listener_protocol"],
        Port=config["service_port"],
        DefaultActions=[{"Type": "forward", "TargetGroupArn": target_group_arn}],
    )
    return response["Listeners"][0]


def ensure_direct_nlb_backend(elbv2_client: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    load_balancer = ensure_network_load_balancer(elbv2_client, config)
    target_group = ensure_target_group(elbv2_client, load_balancer, config)

    if target_group is not None:
        ensure_target_group_targets(elbv2_client, target_group["TargetGroupArn"], config)

    listener = ensure_nlb_listener(
        elbv2_client,
        load_balancer["LoadBalancerArn"],
        target_group["TargetGroupArn"] if target_group is not None else None,
        config,
    )
    if listener is None:
        raise RuntimeError(
            "NLB sem listener utilizável na porta configurada. "
            "Informe --target-group-arn/--target-group-name/--target-id para o modo direto."
        )

    return {
        "load_balancer": load_balancer,
        "target_group": target_group,
        "listener": listener,
    }


def list_vpc_links(apigateway_client: Any) -> list[Dict[str, Any]]:
    return paginate(
        lambda position: apigateway_client.get_vpc_links(position=position)
        if position
        else apigateway_client.get_vpc_links(),
        "items",
    )


def get_named_vpc_links(apigateway_client: Any, name: str) -> list[Dict[str, Any]]:
    return [item for item in list_vpc_links(apigateway_client) if item["name"] == name]


def wait_for_vpc_link(apigateway_client: Any, vpc_link_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    def load_vpc_link() -> Optional[Dict[str, Any]]:
        response = apigateway_client.get_vpc_link(vpcLinkId=vpc_link_id)
        status = response.get("status")
        if status == "AVAILABLE":
            return response
        if status in {"FAILED", "DELETING"}:
            raise RuntimeError(
                f"VPC Link {vpc_link_id} terminou em status inválido: {status}."
            )
        return None

    return wait_for(
        f"VPC Link {vpc_link_id}",
        load_vpc_link,
        timeout_seconds=config["timeout_seconds"],
        poll_interval_seconds=config["poll_interval_seconds"],
    )


def wait_for_vpc_link_deletion(apigateway_client: Any, vpc_link_id: str, config: Dict[str, Any]) -> None:
    def load_until_deleted() -> Optional[bool]:
        try:
            response = apigateway_client.get_vpc_link(vpcLinkId=vpc_link_id)
        except Exception:
            return True
        status = response.get("status")
        if status == "DELETING":
            return None
        return None

    wait_for(
        f"remoção do VPC Link {vpc_link_id}",
        load_until_deleted,
        timeout_seconds=config["timeout_seconds"],
        poll_interval_seconds=config["poll_interval_seconds"],
    )


def ensure_vpc_link(apigateway_client: Any, nlb_arn: str, config: Dict[str, Any]) -> Dict[str, Any]:
    named_vpc_links = get_named_vpc_links(apigateway_client, config["vpc_link_name"])
    if len(named_vpc_links) > 1:
        ids = ", ".join(item["id"] for item in named_vpc_links)
        raise RuntimeError(
            f"Existe mais de um VPC Link com nome {config['vpc_link_name']}: {ids}."
        )

    if named_vpc_links:
        existing = named_vpc_links[0]
        targets = existing.get("targetArns", [])
        if targets == [nlb_arn]:
            LOGGER.info("Reutilizando VPC Link existente: %s", existing["id"])
            return wait_for_vpc_link(apigateway_client, existing["id"], config)

        LOGGER.info(
            "VPC Link %s aponta para outro targetArn. Recriando para %s.",
            existing["id"],
            nlb_arn,
        )
        apigateway_client.delete_vpc_link(vpcLinkId=existing["id"])
        wait_for_vpc_link_deletion(apigateway_client, existing["id"], config)

    LOGGER.info("Criando VPC Link %s", config["vpc_link_name"])
    response = apigateway_client.create_vpc_link(
        name=config["vpc_link_name"],
        targetArns=[nlb_arn],
    )
    return wait_for_vpc_link(apigateway_client, response["id"], config)


def list_rest_apis(apigateway_client: Any) -> list[Dict[str, Any]]:
    return paginate(
        lambda position: apigateway_client.get_rest_apis(position=position)
        if position
        else apigateway_client.get_rest_apis(),
        "items",
    )


def ensure_rest_api(apigateway_client: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    matching_apis = [
        item for item in list_rest_apis(apigateway_client) if item["name"] == config["api_name"]
    ]
    if len(matching_apis) > 1:
        ids = ", ".join(item["id"] for item in matching_apis)
        raise RuntimeError(
            f"Existe mais de uma REST API com nome {config['api_name']}: {ids}."
        )

    if matching_apis:
        LOGGER.info("Reutilizando REST API existente: %s", matching_apis[0]["id"])
        return matching_apis[0]

    LOGGER.info("Criando REST API %s", config["api_name"])
    return apigateway_client.create_rest_api(
        name=config["api_name"],
        endpointConfiguration={"types": [config["api_endpoint_type"]]},
    )


def list_api_resources(apigateway_client: Any, api_id: str) -> list[Dict[str, Any]]:
    return paginate(
        lambda position: apigateway_client.get_resources(restApiId=api_id, position=position)
        if position
        else apigateway_client.get_resources(restApiId=api_id),
        "items",
    )


def find_root_resource(resources: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    for resource in resources:
        if resource.get("path") == "/":
            return resource
    raise RuntimeError("Recurso raiz da API não encontrado.")


def ensure_proxy_resource(apigateway_client: Any, api_id: str, root_id: str) -> Dict[str, Any]:
    resources = list_api_resources(apigateway_client, api_id)
    for resource in resources:
        if resource.get("path") == "/{proxy+}":
            return resource
    LOGGER.info("Criando recurso /{proxy+} na API %s", api_id)
    return apigateway_client.create_resource(
        restApiId=api_id,
        parentId=root_id,
        pathPart="{proxy+}",
    )


def ensure_method(
    apigateway_client: Any,
    *,
    api_id: str,
    resource_id: str,
    request_parameters: Optional[Dict[str, bool]] = None,
) -> None:
    apigateway_client.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="ANY",
        authorizationType="NONE",
        requestParameters=request_parameters or {},
    )


def ensure_root_integration(
    apigateway_client: Any,
    *,
    api_id: str,
    resource_id: str,
    nlb_dns_name: str,
    vpc_link_id: str,
) -> None:
    apigateway_client.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="ANY",
        type="HTTP_PROXY",
        integrationHttpMethod="ANY",
        uri=f"http://{nlb_dns_name}",
        connectionType="VPC_LINK",
        connectionId=vpc_link_id,
        passthroughBehavior="WHEN_NO_MATCH",
    )


def ensure_proxy_integration(
    apigateway_client: Any,
    *,
    api_id: str,
    resource_id: str,
    nlb_dns_name: str,
    vpc_link_id: str,
) -> None:
    apigateway_client.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="ANY",
        type="HTTP_PROXY",
        integrationHttpMethod="ANY",
        uri=f"http://{nlb_dns_name}/{{proxy}}",
        connectionType="VPC_LINK",
        connectionId=vpc_link_id,
        requestParameters={
            "integration.request.path.proxy": "method.request.path.proxy",
        },
        passthroughBehavior="WHEN_NO_MATCH",
    )


def deploy_api(apigateway_client: Any, api_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    LOGGER.info("Publicando deployment no stage %s", config["stage_name"])
    return apigateway_client.create_deployment(
        restApiId=api_id,
        stageName=config["stage_name"],
        description=(
            f"Deployment gerado por script em "
            f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
        ),
    )


def ensure_api_gateway(
    apigateway_client: Any,
    *,
    nlb_dns_name: str,
    vpc_link_id: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    api = ensure_rest_api(apigateway_client, config)
    api_id = api["id"]
    resources = list_api_resources(apigateway_client, api_id)
    root_resource = find_root_resource(resources)
    proxy_resource = ensure_proxy_resource(apigateway_client, api_id, root_resource["id"])

    ensure_method(
        apigateway_client,
        api_id=api_id,
        resource_id=root_resource["id"],
    )
    ensure_root_integration(
        apigateway_client,
        api_id=api_id,
        resource_id=root_resource["id"],
        nlb_dns_name=nlb_dns_name,
        vpc_link_id=vpc_link_id,
    )

    ensure_method(
        apigateway_client,
        api_id=api_id,
        resource_id=proxy_resource["id"],
        request_parameters={"method.request.path.proxy": True},
    )
    ensure_proxy_integration(
        apigateway_client,
        api_id=api_id,
        resource_id=proxy_resource["id"],
        nlb_dns_name=nlb_dns_name,
        vpc_link_id=vpc_link_id,
    )

    deployment = deploy_api(apigateway_client, api_id, config)
    return {
        "api_id": api_id,
        "stage_name": config["stage_name"],
        "deployment_id": deployment["id"],
        "api_url": build_api_url(api_id, config),
    }


def build_dry_run_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "config": {
            "mode": config["mode"],
            "region": config["region"],
            "cluster_name": config["cluster_name"],
            "service_name": config["service_name"],
            "aws_endpoint_url": config["aws_endpoint_url"],
            "namespace": config["namespace"],
            "service_port": config["service_port"],
            "target_port": config["target_port"],
            "selector": config["selector"],
            "annotations": config["annotations"],
            "nlb_arn": config["nlb_arn"],
            "nlb_name": config["nlb_name"],
            "nlb_subnet_ids": config["nlb_subnet_ids"],
            "target_group_arn": config["target_group_arn"],
            "target_group_name": config["target_group_name"],
            "target_ids": config["target_ids"],
            "target_type": config["target_type"],
            "listener_protocol": config["listener_protocol"],
            "vpc_link_name": config["vpc_link_name"],
            "api_name": config["api_name"],
            "stage_name": config["stage_name"],
            "api_endpoint_type": config["api_endpoint_type"],
            "skip_cluster_check": config["skip_cluster_check"],
        }
    }
    if config["mode"] == EKS_SERVICE_MODE:
        payload["service_manifest"] = build_service_manifest(config)
    return payload


def execute(config: Dict[str, Any]) -> Dict[str, Any]:
    if config["mode"] == DIRECT_NLB_MODE:
        required_commands: tuple[str, ...] = ()
    elif config["skip_kubeconfig_update"]:
        required_commands = ("kubectl",)
    else:
        required_commands = ("aws", "kubectl")
    ensure_commands_exist(required_commands)
    clients = build_clients(config["region"], config["aws_endpoint_url"])

    try:
        if config["mode"] == DIRECT_NLB_MODE:
            direct_backend = ensure_direct_nlb_backend(clients["elbv2"], config)
            load_balancer = direct_backend["load_balancer"]
            nlb_dns_name = normalize_dns_name(load_balancer["DNSName"])
            nlb_arn = load_balancer["LoadBalancerArn"]
            LOGGER.info("NLB direto garantido: %s", nlb_arn)
        else:
            if config["skip_cluster_check"]:
                LOGGER.info("Pulando validação de status do cluster por configuração explícita.")
            else:
                assert_cluster_available(clients["eks"], config["cluster_name"])
            update_kubeconfig(config)
            normalize_kubeconfig_exec_api_versions()

            apply_service_manifest(config, clients["eks"])
            nlb_dns_name = wait_for_nlb_hostname(config, clients["eks"])
            LOGGER.info("NLB resolvido: %s", nlb_dns_name)

            nlb_arn = find_nlb_arn(clients["elbv2"], nlb_dns_name)
            LOGGER.info("ARN do NLB: %s", nlb_arn)

        vpc_link = ensure_vpc_link(clients["apigateway"], nlb_arn, config)
        api = ensure_api_gateway(
            clients["apigateway"],
            nlb_dns_name=nlb_dns_name,
            vpc_link_id=vpc_link["id"],
            config=config,
        )

        return {
            "mode": config["mode"],
            "cluster_name": config["cluster_name"],
            "service_name": config["service_name"],
            "namespace": config["namespace"],
            "nlb_dns_name": nlb_dns_name,
            "nlb_arn": nlb_arn,
            "vpc_link_id": vpc_link["id"],
            "rest_api_id": api["api_id"],
            "deployment_id": api["deployment_id"],
            "api_url": api["api_url"],
        }
    finally:
        cleanup_kubectl_auth_state(config)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = namespace_from_args(args)
        if config["dry_run"]:
            print(json.dumps(build_dry_run_payload(config), indent=2))
            return 0

        result = execute(config)
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        LOGGER.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
