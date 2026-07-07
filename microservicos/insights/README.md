# insights — microserviço action-driven (analytics multi-produto AWS)

Fornece analytics ricos (recursos, métricas, logs, metadados profundos e FinOps)
para todos os produtos AWS mapeados, num único endpoint. Autocontido (sem packages
compartilhadas). Roda em EKS atrás do NLB interno; o acesso externo é pelo API
Gateway (Cognito JWT) -> VPC Link -> NLB -> este pod.

## Modo de dados — `INSIGHTS_MODE`

Env `INSIGHTS_MODE` controla a origem dos dados (default `mock`):

- `mock` — dados sintéticos determinísticos (semeados por hash), sem AWS/credenciais.
  Ideal para o frontend renderizar dashboards previsíveis localmente.
- `aws` — caminho real: `STS:AssumeRole` na conta-alvo e coleta via
  `describe*` + CloudWatch (`get_metric_data`/`get_metric_statistics`) +
  CloudWatch Logs (`filter_log_events`) + Secrets Manager + `psycopg` no
  `pg_catalog` (para metadados profundos de banco).

Ambos os caminhos produzem exatamente o mesmo shape de `detail` — trocar de mock
para real é só ligar a env, sem mudança no frontend.

## API

`POST /insights/execute` — executa a ação. Health: `GET /healthz`, `GET /readyz`. Porta `8080`.

Erros seguem o envelope `ErrorResponse` (`{code, message, requestId}`).

### Envelope da requisição

```json
{
  "account": "123456789012",
  "roleArn": "arn:aws:iam::123456789012:role/insights-reader",
  "region": "sa-east-1",
  "environment": "dev",
  "requestId": "opcional",
  "dryRun": false,
  "params": { "action": "resources", "product": "all" }
}
```

`region` (default `sa-east-1`) e `environment` (default `dev`) são opcionais para
permitir chamadas mínimas do frontend; `account` e `roleArn` seguem padrão e são
fornecidos pelas Settings do frontend.

### `params` (discriminado por `action`)

| campo        | tipo    | uso |
|--------------|---------|-----|
| `action`     | enum    | `resources` \| `metrics` \| `logs` \| `metadata` \| `finops` |
| `product`    | enum    | `rds` \| `ec2` \| `ebs` \| `elb` \| `eip` \| `snapshot` \| `kms` \| `vpc-endpoint` \| `all` (default) |
| `resourceId` | string? | recurso alvo (metrics/logs/metadata) |
| `filters`    | dict?   | `{search?, status?, env?, type?, tag?}` (resources); `{search?}` (logs); `{secretId?}` (metadata db) |
| `metric`     | string? | `cpu\|memory\|connections\|iops\|storageUsed\|latency\|freeableMemory` |
| `lookback`   | int?    | minutos (metrics/logs) ou dias (tendências finops) |
| `level`      | string? | logs: `error\|warn\|info` |
| `limit`      | int?    | limite de itens |

### Resposta (`ActionResult`)

```json
{ "operationId": "uuid", "status": "ok", "product": "all", "action": "resources", "detail": { } }
```

## Ações

- **resources** — lista recursos do(s) produto(s). Item:
  `{id, name, product, type, env, region, status, size, createdAt, tags, monthlyCost, utilizationPct}`.
  Suporta filtros. Real: `describe_db_instances` / `describe_instances` /
  `describe_volumes` / `describe_load_balancers` / `describe_addresses` /
  `describe_snapshots` / `list_keys` / `describe_vpc_endpoints`.
- **metrics** — série temporal por `resourceId`:
  `{resourceId, series:[{metric, unit, points:[{t, value}], stats:{avg,max,min,p95}}]}`.
  Métricas por produto (rds: CPU/FreeableMemory/Connections/Read+WriteIOPS/FreeStorageSpace;
  ec2: CPU/NetworkIn/Out; ebs: VolumeRead+WriteOps/BurstBalance). Real: `cloudwatch.get_metric_data`.
- **logs** — `{resourceId, entries:[{ts, level, message, source}], total}`, filtra por
  `level` e `filters.search`. Real: `logs.filter_log_events` (grupo derivado do produto/recurso).
- **metadata** — profundo para RDS/DB: engine, storage, conexões, tabelas (com
  particionamento, índices, índices não usados), slow queries, bloat e recomendações.
  Não-DB: `{config, tags, related, recommendations}`. Real DB: Secrets Manager +
  `psycopg` consultando `pg_catalog`/`pg_stat_user_indexes`/`pg_stat_statements`/`pg_partitioned_table`.
- **finops** — `{summary, utilization[], savingsByType[], savingsTrend[] (12 meses)}` para
  os gráficos de FinOps (idle/oversized/rightsizing). Real: `describe*` + CloudWatch.

## Local

```bash
pip install -r requirements.txt
INSIGHTS_MODE=mock uvicorn app.main:app --reload --port 8080

curl -s localhost:8080/insights/execute -H 'content-type: application/json' -d '{
  "account":"123456789012",
  "roleArn":"arn:aws:iam::123456789012:role/insights-reader",
  "params":{"action":"metadata","product":"rds","resourceId":"prod-orders-db"}
}'
```

## Container

```bash
docker build -t insights .
docker run -p 8080:8080 -e INSIGHTS_MODE=mock insights
```
