"""Catálogo e constantes por produto AWS (namespaces, métricas, preços, templates).

Fonte única de verdade compartilhada pelos caminhos mock e AWS real. Manter aqui
os mapeamentos declarativos (o que descrever, quais métricas, dimensão CloudWatch,
grupo de log, preço aproximado) evita espalhar strings mágicas pelo handler.
"""
from __future__ import annotations

# Produtos suportados (exclui 'all', que é expandido em runtime).
PRODUCTS: tuple[str, ...] = (
    "rds", "ec2", "ebs", "elb", "eip", "snapshot", "kms", "vpc-endpoint",
)

ENVS: tuple[str, ...] = ("prod", "staging", "homol", "dev")

# Produtos com metadados "deep" via banco (psycopg + pg_catalog).
DB_PRODUCTS: frozenset[str] = frozenset({"rds"})

# Dimensão CloudWatch usada para localizar as métricas de cada produto.
CW_DIMENSION: dict[str, str] = {
    "rds": "DBInstanceIdentifier",
    "ec2": "InstanceId",
    "ebs": "VolumeId",
    "elb": "LoadBalancer",
}

# Catálogo de métricas por produto: nome CloudWatch, unidade e estatística default.
# Ordem relevante: a primeira costuma ser a métrica "principal" (ex.: CPU).
METRICS: dict[str, list[dict]] = {
    "rds": [
        {"metric": "CPUUtilization", "unit": "Percent", "namespace": "AWS/RDS", "stat": "Average"},
        {"metric": "FreeableMemory", "unit": "Bytes", "namespace": "AWS/RDS", "stat": "Average"},
        {"metric": "DatabaseConnections", "unit": "Count", "namespace": "AWS/RDS", "stat": "Average"},
        {"metric": "ReadIOPS", "unit": "Count/Second", "namespace": "AWS/RDS", "stat": "Average"},
        {"metric": "WriteIOPS", "unit": "Count/Second", "namespace": "AWS/RDS", "stat": "Average"},
        {"metric": "FreeStorageSpace", "unit": "Bytes", "namespace": "AWS/RDS", "stat": "Average"},
    ],
    "ec2": [
        {"metric": "CPUUtilization", "unit": "Percent", "namespace": "AWS/EC2", "stat": "Average"},
        {"metric": "NetworkIn", "unit": "Bytes", "namespace": "AWS/EC2", "stat": "Average"},
        {"metric": "NetworkOut", "unit": "Bytes", "namespace": "AWS/EC2", "stat": "Average"},
    ],
    "ebs": [
        {"metric": "VolumeReadOps", "unit": "Count", "namespace": "AWS/EBS", "stat": "Sum"},
        {"metric": "VolumeWriteOps", "unit": "Count", "namespace": "AWS/EBS", "stat": "Sum"},
        {"metric": "BurstBalance", "unit": "Percent", "namespace": "AWS/EBS", "stat": "Average"},
    ],
    "elb": [
        {"metric": "RequestCount", "unit": "Count", "namespace": "AWS/ApplicationELB", "stat": "Sum"},
        {"metric": "TargetResponseTime", "unit": "Seconds", "namespace": "AWS/ApplicationELB", "stat": "Average"},
        {"metric": "HTTPCode_Target_5XX_Count", "unit": "Count", "namespace": "AWS/ApplicationELB", "stat": "Sum"},
    ],
}
# Produtos sem métricas CloudWatch úteis retornam série vazia (eip/snapshot/kms/vpc-endpoint).

# Aliases amigáveis (params.metric) -> nome CloudWatch canônico.
METRIC_ALIASES: dict[str, str] = {
    "cpu": "CPUUtilization",
    "memory": "FreeableMemory",
    "freeablememory": "FreeableMemory",
    "connections": "DatabaseConnections",
    "iops": "ReadIOPS",
    "storageused": "FreeStorageSpace",
    "latency": "TargetResponseTime",
}

# Template de grupo de log (CloudWatch Logs) por produto para o caminho real.
LOG_GROUP_TEMPLATE: dict[str, str] = {
    "rds": "/aws/rds/instance/{resourceId}/postgresql",
    "ec2": "/var/log/{resourceId}",
    "elb": "/aws/elb/{resourceId}",
}

# Preço mensal aproximado (USD, sa-east-1) por classe/tipo — usado no finops.
PRICE_RDS: dict[str, float] = {
    "db.t3.medium": 100.0, "db.t3.large": 200.0, "db.m5.large": 260.0,
    "db.m5.xlarge": 520.0, "db.r5.large": 330.0, "db.r5.xlarge": 660.0,
    "db.r6g.large": 300.0, "db.r6g.xlarge": 600.0,
}
PRICE_EC2: dict[str, float] = {
    "t3.medium": 36.0, "t3.large": 72.0, "m5.large": 100.0, "m5.xlarge": 200.0,
    "c5.large": 88.0, "c5.xlarge": 176.0, "r5.large": 132.0,
}
PRICE_EBS_GB_MONTH: dict[str, float] = {"gp2": 0.19, "gp3": 0.152, "io1": 0.238, "io2": 0.238}
PRICE_SNAPSHOT_GB_MONTH: float = 0.055
PRICE_EIP_MONTH: float = 3.65
PRICE_ELB_MONTH: float = 22.0
PRICE_KMS_MONTH: float = 1.0
PRICE_VPC_ENDPOINT_MONTH: float = 7.5

# Downgrade sugerido para recursos superdimensionados (finops rightsizing).
DOWNGRADE: dict[str, str] = {
    "db.r6g.xlarge": "db.r6g.large", "db.m5.xlarge": "db.m5.large",
    "db.r5.xlarge": "db.r5.large", "db.m5.large": "db.t3.large",
    "m5.xlarge": "m5.large", "c5.xlarge": "c5.large", "m5.large": "t3.large",
}


def resolve_products(product: str) -> list[str]:
    """Expande 'all' na lista completa; caso contrário retorna o produto único."""
    return list(PRODUCTS) if product == "all" else [product]
