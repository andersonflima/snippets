# dbca — microserviço de analytics de metadados de banco

Conecta em **qualquer recurso de banco em qualquer conta** e roda **queries de
metadados** para extrair analytics — **sem o usuário saber como**. As queries são
**configuradas por admin** e o nome de cada uma vira um **botão-ação** no frontend.
O usuário informa apenas **conta + recurso + ambiente**; o serviço descobre o
resto (tipo de recurso, VPC, credencial). Read-only.

`POST /dbca/execute`, `GET /dbca/queries` (+ `/healthz`, `/readyz`). Exposto via
API Gateway → VPC Link → NLB → EKS; roda na conta-alvo por `STS:AssumeRole`.

## Como funciona

1. **Query** (`params.queryId`) resolvida no catálogo admin.
2. **AssumeRole** na conta (roleArn derivada da conta se não informada).
3. **Auto-descoberta** (`describe`): classifica o recurso e lê VPC/endpoint:
   - Cluster **Aurora** (engine `aurora-postgresql`/`aurora-mysql`) — via `rds:DescribeDBClusters`; VPC via subnet group.
   - Tabela **DynamoDB** — via `dynamodb:DescribeTable`.
4. **Execução** conforme o engine:
   - Aurora → **RDS Data API** (`ExecuteStatement`) com o SQL da query. **Sem rota de VPC** (é chamada de API). Credencial resolvida pela tag `dbca:secretArn` no cluster, com fallback no `secretMap` admin.
   - DynamoDB → metadados via API (`op` da query).
5. **Resultado** normalizado em `{columns, rows, rowCount}` para o frontend exibir.

`dryRun: true` descobre o recurso e resolve a query sem executar.

## Envelope

```jsonc
{
  "account": "111111111111",
  "resource": "meu-cluster-aurora",     // ou tabela DynamoDB (nome/id/ARN)
  "environment": "prod",
  "params": { "queryId": "table-sizes" }
  // roleArn e region são opcionais (derivados / default)
}
```

## Queries (admin, `queries.example.json` → S3)

Catálogo embutido como default; sobreposto por um JSON externo em
`QUERIES_BUCKET/QUERIES_KEY` (deep-merge, sem redeploy). Cada query:

```jsonc
{
  "id": "table-sizes",
  "label": "Tamanho das tabelas",     // vira o botão
  "description": "...",
  "category": "Storage",
  "engines": {
    "aurora-postgresql": { "sql": "SELECT ... (read-only)" },
    "aurora-mysql":      { "sql": "SELECT ..." },
    "dynamodb":          { "op": "table-metadata" }
  }
}
```

- SQL é **100% read-only** — o serviço rejeita escrita/DDL (`sql_forbidden`).
- `secretMap`: recurso → secretArn (fallback quando falta a tag no cluster).
- Ops DynamoDB atuais: `overview`, `table-metadata`, `capacity`.

## Escopo v1

Aurora (Data API) + DynamoDB. RDS clássico (não-Aurora) é follow-up (precisa de
rota de rede até o endpoint privado). O dispatch por engine já está pronto para
estender.

## Env

`TARGET_ROLE_NAME` (default `microservicos-dbca-target`), `DEFAULT_TARGET_REGION`
(default `sa-east-1`), `QUERIES_BUCKET`/`QUERIES_KEY`/`QUERIES_CACHE_TTL`.
