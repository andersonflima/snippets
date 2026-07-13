# dynamodb — microserviço action-driven

Executa **qualquer operação do DynamoDB** (control plane + data plane) numa única
ação, com **todas as regras de negócio externalizadas** num JSON — segregadas por
ambiente (`dev`/`homol`/`staging`/`prod`) e com **exceções** por conta + recurso.

Exposto via API Gateway → VPC Link → NLB interno → EKS. A ação roda na conta-alvo
por `STS:AssumeRole`. `POST /dynamodb/execute` (+ `/healthz`, `/readyz`).

## Envelope

```jsonc
{
  "account": "111111111111",              // conta AWS alvo (12 dígitos)
  "resource": "prod-orders",              // tabela/backup/ARN, ou "*" (ListTables etc.)
  "roleArn": "arn:aws:iam::111111111111:role/plataforma-dynamodb",
  "region": "sa-east-1",
  "environment": "prod",                  // dev | homol | staging | prod
  "changeNumber": "CHG0012345",           // GMUD — exigida p/ mutações de estrutura em prod
  "dryRun": false,
  "params": {
    "operation": "PutItem",               // ver catálogo abaixo
    "args": {                             // kwargs boto3 (formato low-level DynamoDB)
      "TableName": "prod-orders",
      "Item": { "pk": { "S": "o#1" }, "total": { "N": "42" } }
    }
  }
}
```

`params.args` são passados **verbatim** para o client boto3 `dynamodb` — ou seja,
todo o poder da API (expressões, transações, PartiQL, GSIs, streams, PITR, backup,
export/import...) fica disponível sem o serviço precisar conhecer cada campo.

## Catálogo de operações (50)

Agrupadas por **categoria** (as regras liberam/bloqueiam por operação **ou** por
categoria):

- **read** — GetItem, Query, Scan, BatchGetItem, TransactGetItems, DescribeTable,
  ListTables, DescribeTimeToLive, DescribeContinuousBackups, ListTagsOfResource,
  DescribeBackup, ListBackups, DescribeLimits, DescribeEndpoints, DescribeExport,
  ListExports, DescribeImport, ListImports, DescribeGlobalTable(+Settings),
  ListGlobalTables, DescribeContributorInsights, DescribeKinesisStreamingDestination.
- **write** — PutItem, UpdateItem, DeleteItem, BatchWriteItem, TransactWriteItems,
  ExecuteStatement, BatchExecuteStatement, ExecuteTransaction (PartiQL = escrita).
- **ddl** — CreateTable, DeleteTable, UpdateTable, UpdateTimeToLive,
  CreateGlobalTable, UpdateGlobalTable(+Settings).
- **config** — UpdateContinuousBackups (PITR), TagResource, UntagResource,
  UpdateContributorInsights, Enable/DisableKinesisStreamingDestination.
- **backup** — CreateBackup, DeleteBackup, RestoreTableFromBackup,
  RestoreTableToPointInTime, ExportTableToPointInTime, ImportTable.

## Regras externas (`../rules/dynamodb.json`)

Fora da imagem, atualizáveis sem redeploy (S3 ou DynamoDB — ver `../rules/README.md`).
Tudo é **opt-in**: chave ausente / lista vazia = sem restrição.

Por ambiente (`environments.<env>`):

| Chave | Efeito |
|-------|--------|
| `allowedOperations` / `deniedOperations` | allow/deny por operação |
| `allowedCategories` / `deniedCategories` | allow/deny por categoria |
| `allowedBillingModes` | modos permitidos em Create/UpdateTable |
| `maxReadCapacityUnits` / `maxWriteCapacityUnits` | teto de capacidade (tabela + GSIs) |
| `maxGlobalSecondaryIndexes` | teto de GSIs no CreateTable |
| `tableNamePrefixes` | naming: tabelas criadas devem ter um dos prefixos |
| `requireEncryption` | CreateTable exige `SSESpecification.Enabled=true` |
| `requireDeletionProtection` | exige `DeletionProtectionEnabled=true` (e não deixa desabilitar) |
| `requirePITR` | não deixa desabilitar PITR (UpdateContinuousBackups) |
| `requireTags` | CreateTable exige as tags listadas |
| `requireGmudForMutations` | GMUD para toda mutação |
| `gmudForCategories` | GMUD só para as categorias listadas (ex.: `["ddl","config","backup"]`) — tem precedência |

Globais: `allowedRegions`.

### Exceções

Liberam uma ação **bloqueada** para um alvo específico. Casam por **conta +
ambiente** e, se informado, **recurso** (nome/ARN da tabela). Uma exceção casada
autoriza a operação (ignora o allow/deny do ambiente) **e dispensa a GMUD** — ela
própria é a autorização explícita. Expiram por `expiresAt` (opcional).

```jsonc
{
  "id": "EXC-2026-014",
  "account": "111111111111",
  "environment": "prod",
  "resource": "prod-orders",        // opcional; ausente = qualquer recurso na conta/env
  "allowOperations": ["DeleteTable", "UpdateTable"],
  "allowCategories": [],            // ou libere uma categoria inteira; "*" = tudo
  "skipModeling": false,           // true também pula naming/hardening
  "reason": "CHG0012345 — reestruturação aprovada",
  "expiresAt": "2026-12-31T23:59:59Z"
}
```

## dryRun

`dryRun: true` valida contra as regras e **resolve** a operação (categoria, método,
se exige GMUD, qual exceção casou), sem nenhuma chamada AWS.

## Erros

Envelope `{code, message, requestId}`: `validation_error` (operação inexistente /
payload), `rule_violation` (regra de ambiente/modelagem), `gmud_required`,
`assume_role_denied`, `not_found`, `conflict`, `upstream_error`, `internal_error`.
