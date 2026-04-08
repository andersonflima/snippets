#!/usr/bin/env python3
"""Provisiona um Service interno no EKS, VPC Link e API Gateway REST.

Instrucoes de uso
=================

Pre-requisitos para AWS real:
- python3 com boto3 instalado
- aws CLI autenticado na conta correta
- kubectl instalado e com acesso ao cluster EKS
- workload ja rodando no cluster com labels que batam no selector do Service
- suporte no cluster para provisionar Service do tipo LoadBalancer

Comando minimo:

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

Pre-check recomendado:

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
- se o aws CLI nao estiver instalado, use --skip-kubeconfig-update apenas quando
  o kubectl ja estiver apontando para o cluster correto
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from typing import Any, Callable, Dict, Iterable, Optional, Sequence
from urllib.parse import urlparse

LOGGER = logging.getLogger("eks_private_service_apigateway")
DEFAULT_STAGE_NAME = "prod"
DEFAULT_NAMESPACE = "default"
DEFAULT_SERVICE_PORT = 80
DEFAULT_TARGET_PORT = 3000
DEFAULT_TIMEOUT_SECONDS = 900
DEFAULT_POLL_INTERVAL_SECONDS = 10


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cria/atualiza um Service LoadBalancer interno no EKS e publica o "
            "backend via API Gateway REST com VPC Link."
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
    parser.add_argument("--cluster-name", required=True, help="Nome do cluster EKS.")
    parser.add_argument("--service-name", required=True, help="Nome do Service Kubernetes.")
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


def namespace_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    selector = parse_key_value_items(args.selector, "selector") or {
        "app": args.service_name,
    }
    annotations = {
        "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
        "service.beta.kubernetes.io/aws-load-balancer-scheme": args.nlb_scheme,
    }
    annotations.update(parse_key_value_items(args.annotation, "annotation"))
    return {
        "region": args.region,
        "cluster_name": args.cluster_name,
        "service_name": args.service_name,
        "aws_endpoint_url": args.aws_endpoint_url,
        "namespace": args.namespace,
        "service_port": args.service_port,
        "target_port": args.target_port,
        "selector": selector,
        "annotations": annotations,
        "vpc_link_name": args.vpc_link_name or f"{args.cluster_name}-vpc-link",
        "api_name": args.api_name or f"{args.cluster_name}-api",
        "stage_name": args.stage_name,
        "api_endpoint_type": args.api_endpoint_type,
        "timeout_seconds": args.timeout_seconds,
        "poll_interval_seconds": args.poll_interval_seconds,
        "skip_kubeconfig_update": args.skip_kubeconfig_update,
        "skip_cluster_check": args.skip_cluster_check,
        "dry_run": args.dry_run,
    }


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


def apply_service_manifest(config: Dict[str, Any]) -> None:
    manifest = build_service_manifest(config)
    manifest_text = json.dumps(manifest, indent=2)
    LOGGER.info(
        "Aplicando Service %s/%s",
        config["namespace"],
        config["service_name"],
    )
    run_command(["kubectl", "apply", "-f", "-"], input_text=manifest_text)


def load_service(config: Dict[str, Any]) -> Dict[str, Any]:
    output = run_command(
        [
            "kubectl",
            "get",
            "svc",
            config["service_name"],
            "-n",
            config["namespace"],
            "-o",
            "json",
        ]
    )
    return json.loads(output)


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
    hostnames = [item.get("hostname") for item in ingress if item.get("hostname")]
    return hostnames[0] if hostnames else None


def wait_for_nlb_hostname(config: Dict[str, Any]) -> str:
    return wait_for(
        f"NLB do Service {config['namespace']}/{config['service_name']}",
        lambda: extract_service_hostname(load_service(config)),
        timeout_seconds=config["timeout_seconds"],
        poll_interval_seconds=config["poll_interval_seconds"],
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
        if targets != [nlb_arn]:
            raise RuntimeError(
                "Já existe VPC Link com o mesmo nome apontando para outro targetArn."
            )
        LOGGER.info("Reutilizando VPC Link existente: %s", existing["id"])
        return wait_for_vpc_link(apigateway_client, existing["id"], config)

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
    return {
        "config": {
            "region": config["region"],
            "cluster_name": config["cluster_name"],
            "service_name": config["service_name"],
            "aws_endpoint_url": config["aws_endpoint_url"],
            "namespace": config["namespace"],
            "service_port": config["service_port"],
            "target_port": config["target_port"],
            "selector": config["selector"],
            "annotations": config["annotations"],
            "vpc_link_name": config["vpc_link_name"],
            "api_name": config["api_name"],
            "stage_name": config["stage_name"],
            "api_endpoint_type": config["api_endpoint_type"],
            "skip_cluster_check": config["skip_cluster_check"],
        },
        "service_manifest": build_service_manifest(config),
    }


def execute(config: Dict[str, Any]) -> Dict[str, Any]:
    required_commands = ("kubectl",) if config["skip_kubeconfig_update"] else ("aws", "kubectl")
    ensure_commands_exist(required_commands)
    clients = build_clients(config["region"], config["aws_endpoint_url"])

    if config["skip_cluster_check"]:
        LOGGER.info("Pulando validação de status do cluster por configuração explícita.")
    else:
        assert_cluster_available(clients["eks"], config["cluster_name"])
    update_kubeconfig(config)

    apply_service_manifest(config)
    nlb_dns_name = wait_for_nlb_hostname(config)
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
