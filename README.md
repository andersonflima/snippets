# DynamoDB Snapshot Lambda

Repositório configurado apenas para deploy em AWS Lambda.

- Handler: `dynamodb_snapshot_lambda.lambda_handler`
- Implementação completa da Lambda: `dynamodb_snapshot_lambda.py`

## Requisitos

- Python `3.10+`
- `boto3`
- tabelas DynamoDB existentes
- bucket S3 para snapshots
- tabela DynamoDB dedicada para checkpoint

## Variáveis de ambiente

### Obrigatórias

| Variável | Descrição |
| --- | --- |
| `SNAPSHOT_BUCKET` | Bucket S3 onde os exports serão gravados quando `snapshot_bucket` não vier no evento |
| `TARGET_TABLE_ARNS` ou `TARGET_TABLES` | Lista CSV inline com ARNs ou nomes de tabelas alvo |
| `CHECKPOINT_DYNAMODB_TABLE_ARN` | ARN da tabela DynamoDB usada para persistir o checkpoint por tabela |

Observações:

- As variáveis de ambiente têm precedência sobre o payload da Lambda.
- Em vez de `TARGET_TABLE_ARNS`/`TARGET_TABLES`, você também pode fornecer os alvos via `targets` no evento ou `TARGETS_CSV`, desde que as envs equivalentes não estejam definidas.

### Operacionais

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `S3_PREFIX` | `dynamodb-snapshots` | Prefixo base dos exports e do lookup legado de baseline |
| `IGNORE_TARGETS` ou `IGNORE_TABLES` | vazio | Lista CSV inline com ARNs/nomes para ignorar |
| `DRY_RUN` | `false` | Faz preflight e gera plano sem exportar |
| `WAIT_FOR_COMPLETION` | `false` | Aguarda `DescribeExport` até concluir |
| `MAX_WORKERS` | `4` | Paralelismo por tabela |
| `S3_BUCKET_OWNER` | vazio | Account ID AWS de 12 dígitos do bucket de export. Necessário quando o bucket S3 é cross-account |
| `TARGETS_CSV` | vazio | Fonte CSV de alvos: inline, caminho empacotado com a função ou `s3://` |
| `IGNORE_CSV` | vazio | Fonte CSV de ignorados: inline, caminho empacotado com a função ou `s3://` |
| `PERMISSION_PRECHECK` | `true` | Valida resolução de contexto/sessão por tabela antes da execução |
| `INCREMENTAL_EXPORT_VIEW_TYPE` | `NEW_IMAGE` | Define o tipo de imagem no export incremental (`NEW_IMAGE` ou `NEW_AND_OLD_IMAGES`) |
| `LOG_LEVEL` | `INFO` | Nível de log |
| `OUTPUT_CLOUDWATCH_ENABLED` | `false` | Quando `true`, publica o payload final da execução nos logs estruturados da própria Lambda |
| `OUTPUT_DYNAMODB_ENABLED` | `false` | Quando `true`, persiste o payload final da execução em uma tabela DynamoDB padronizada |
| `OUTPUT_DYNAMODB_TABLE` | vazio | Nome da tabela DynamoDB de destino usada pelo output padronizado. Obrigatória quando `OUTPUT_DYNAMODB_ENABLED=true` |
| `OUTPUT_DYNAMODB_REGION` | região atual da execução | Região AWS da tabela usada para persistir o output padronizado |

Observações:

- Quando `S3_BUCKET_OWNER` estiver configurado, a Lambda envia esse valor como `S3BucketOwner` para `ExportTableToPointInTime`, permitindo export para bucket cross-account.
- O bucket efetivo do export é derivado por tabela: a Lambda concatena `-<table_region>` ao final de `SNAPSHOT_BUCKET`. Exemplo: `dander` + `sa-east-1` => `dander-sa-east-1`.
- `bucket_owner` segue a mesma regra: primeiro `S3_BUCKET_OWNER` no ambiente, depois o payload da Lambda.
- `s3_prefix`, `checkpoint_dynamodb_table_arn`, `output_cloudwatch_enabled`, `output_dynamodb_enabled`, `output_dynamodb_table` e `output_dynamodb_region` também podem ser enviados no payload da Lambda, mas só são usados quando a env correspondente não existir.
- Para persistir os dados da execução em DynamoDB, configure no mínimo `OUTPUT_DYNAMODB_ENABLED=true` e `OUTPUT_DYNAMODB_TABLE=<nome-da-tabela>`.
- A Lambda salva o estado do checkpoint por tabela em DynamoDB, usando chave composta com `PK=TableName` e `SK=RecordType`. O valor de `TableName` é o `TableArn` para evitar colisão entre tabelas homônimas de contas/regiões diferentes, e `TargetTableName` mantém o nome legível da tabela. Se a tabela não existir, a Lambda cria automaticamente com `BillingMode=PAY_PER_REQUEST`.
- A Lambda também registra `TableCreatedAt` no checkpoint para detectar recriação de tabela; se o timestamp atual divergir do checkpoint, o estado antigo é invalidado e o bootstrap `FULL_EXPORT` é reexecutado.
- A escolha entre `FULL_EXPORT` e `INCREMENTAL_EXPORT` agora é automática por tabela. Não é mais necessário enviar `mode` no evento nem configurar `SNAPSHOT_MODE`.
- O `ClientToken` do export é gerado com um salt único por execução da Lambda para evitar deduplicação idempotente indesejada entre execuções diferentes no mesmo dia.

### Fallback por `Scan`

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `SCAN_FALLBACK_ENABLED` | `true` | Ativa fallback quando incremental nativo não for suportado |
| `SCAN_UPDATED_ATTR` | `_updated_at` | Campo usado como watermark |
| `SCAN_UPDATED_ATTR_TYPE` | `string` | `string` ou `number` |
| `SCAN_PARTITION_SIZE` | `10000` | Quantidade de itens por partição gravada em S3 |
| `SCAN_COMPRESS` | `true` | Gera partições `.jsonl.gz` |

### AssumeRole

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `ASSUME_ROLE` | vazio | ARN fixo ou template com `{account_id}` |
| `ASSUME_ROLE_EXTERNAL_ID` | vazio | External ID opcional |
| `ASSUME_ROLE_SESSION_NAME` | `dynamodb-snapshot-<run_id>` | Nome da sessão STS |
| `ASSUME_ROLE_DURATION_SECONDS` | `3600` | Entre `900` e `43200` |

## Evento de exemplo

```json
{
  "targets": [
    "arn:aws:dynamodb:us-east-1:111111111111:table/orders",
    "arn:aws:dynamodb:us-east-1:111111111111:table/payments"
  ],
  "snapshot_bucket": "meu-bucket-snapshots",
  "bucket_owner": "222222222222",
  "s3_prefix": "dynamodb-snapshots",
  "checkpoint_dynamodb_table_arn": "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints",
  "ignore": [
    "orders_tmp"
  ],
  "max_workers": 4,
  "wait_for_completion": false,
  "dry_run": false,
  "scan_fallback_enabled": true,
  "scan_updated_attr": "_updated_at",
  "scan_updated_attr_type": "string",
  "scan_partition_size": 10000,
  "scan_compress": true,
  "permission_precheck": true,
  "output_cloudwatch_enabled": true,
  "output_dynamodb_enabled": true,
  "output_dynamodb_table": "snapshot-output",
  "output_dynamodb_region": "sa-east-1",
  "assume_role": "arn:aws:iam::{account_id}:role/central-snapshot-access",
  "assume_role_external_id": "external-id-opcional"
}
```

Observações:

- Com `assume_role` templado por `{account_id}`, agrupe os targets por conta em execuções separadas.
- Informe `S3_BUCKET_OWNER` no ambiente ou `bucket_owner` no payload quando o bucket de export pertencer a outra conta AWS. Se ambos existirem, a env tem precedência.

## Fluxo operacional

### Export nativo

Antes de chamar `ExportTableToPointInTime`, a Lambda:

1. resolve a sessão AWS efetiva da tabela;
2. valida se a sessão pertence à conta dona da tabela quando o target vem por ARN;
3. consulta `DescribeContinuousBackups`;
4. se `PointInTimeRecoveryStatus` estiver `DISABLED`, executa `UpdateContinuousBackups`;
5. aguarda até o PITR ficar `ENABLED`;
6. dispara o export `FULL_EXPORT` ou `INCREMENTAL_EXPORT`.

Destino S3 por tabela:

- `SNAPSHOT_BUCKET=<bucket-base>`
- bucket efetivo = `<bucket-base>-<região-da-tabela>`
- exemplo: tabela em `arn:aws:dynamodb:sa-east-1:...` com `SNAPSHOT_BUCKET=dander` exporta para `dander-sa-east-1`

### Incremental

Sem receber `mode`, a Lambda decide automaticamente assim:

1. reconcilia exports pendentes via `DescribeExport`;
2. se ainda houver export pendente, não dispara novo export para a tabela;
3. se não existir checkpoint válido com `last_to`, executa `FULL`;
4. após existir um `FULL`, passa a executar incrementais automaticamente;
5. a contagem incremental vai até `6`; ao atingir esse limite, a próxima execução volta para `FULL` e zera a contagem;
6. quando a contagem incremental já saiu de `0`, a Lambda valida o `ItemCount` do export incremental anterior;
7. se o export anterior teve `ItemCount > 0`, a contagem avança para o próximo incremental;
8. se o export anterior não exportou itens, a contagem não avança e o próximo export reutiliza o mesmo índice incremental.

Regras da janela incremental nativa:

- a Lambda só dispara export nativo quando a janela entre `checkpoint_from` e `run_time` tiver pelo menos `15 minutos`
- quando o atraso acumulado ultrapassa `24 horas`, a Lambda corta a janela no limite de `24 horas` e avança em fatias por execução até alcançar o horário atual
- antes de iniciar `INCREMENTAL_EXPORT`, a Lambda ajusta `checkpoint_from/checkpoint_to` para a janela PITR retornada por `DescribeContinuousBackups` (`EarliestRestorableDateTime` e `LatestRestorableDateTime`)
- se a janela ajustada ao PITR ficar inválida ou menor que `15 minutos`, a Lambda retorna `PENDING` para a tabela e não chama `ExportTableToPointInTime`
Se o incremental nativo não puder ser usado e `SCAN_FALLBACK_ENABLED=true`, a Lambda pode cair no fallback por `Scan`.

Regras de progressão `INCR` -> `INCR2` -> `INCR3`:

- após um `FULL`, o primeiro incremental sempre usa `INCR`
- a Lambda só avança para o próximo índice (`INCR2`, `INCR3`, ...) quando o incremental anterior estiver `COMPLETED` e com `ItemCount > 0`
- se o incremental anterior tiver `ItemCount = 0`, o índice é reutilizado na execução seguinte
- se o incremental anterior ainda estiver `STARTED/IN_PROGRESS/PENDING`, a Lambda não inicia um novo export para a tabela (retorna `PENDING`)
- se o `ItemCount` ainda não estiver disponível (`null`/ausente), a Lambda mantém o mesmo índice até conseguir confirmar o resultado anterior

## Layout no S3

- `DDB/YYYYMMDD/<account_id>/<table_name>/FULL/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR2/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR3/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR4/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR5/run_id=YYYYMMDDThhmmssZ`
- `DDB/YYYYMMDD/<account_id>/<table_name>/INCR6/run_id=YYYYMMDDThhmmssZ`

Observação:

- O sufixo `run_id=...` garante prefixo único por execução, inclusive quando há múltiplos incrementais no mesmo dia.
- Quando o incremental anterior termina sem itens exportados, a Lambda reutiliza o mesmo índice incremental na próxima execução em vez de avançar para o próximo sufixo.

No fallback por `Scan`, além das partições `.jsonl` ou `.jsonl.gz`, a Lambda grava `manifest.json` com `files`, `total_items`, `total_parts`, `from` e `to`.

## Checkpoint em DynamoDB

O checkpoint é persistido em uma tabela DynamoDB dedicada, informada em `CHECKPOINT_DYNAMODB_TABLE_ARN`.

Regras:

- a tabela usa chave composta com `PK=TableName` e `SK=RecordType`
- se a tabela não existir, a Lambda cria automaticamente com `BillingMode=PAY_PER_REQUEST`
- se a tabela já existir, a Lambda valida o schema antes de usar
- existe um item `CURRENT` por tabela target com o estado atual do checkpoint
- existem itens históricos com `SK=SNAPSHOT#<observed_at>#<event_id>` para manter o tracking das versões anteriores
- a tabela DynamoDB é resolvida pelo ARN informado
- o item `CURRENT` não usa `Payload` serializado; os campos do estado atual são gravados como atributos nativos da tabela
- a gravação do item `CURRENT` usa merge otimista por `Revision`, reaplicando apenas os eventos locais ainda não materializados para evitar perda de estado quando múltiplas Lambdas salvam a mesma tabela

## Retorno do handler

Campos comuns de retorno do `lambda_handler`:

- `ok`
- `status`
- `snapshot_bucket`
- `run_id`
- `mode`
- `dry_run`
- `updated_checkpoint`
- `checkpoint_error`
- `checkpoint_error_detail`
- `checkpoint_user_message`
- `checkpoint_resolution`
- `results`

Cada item em `results` pode conter:

- `snapshot_bucket`
- `table_name`
- `table_arn`
- `mode`
- `status`
- `source`
- `s3_prefix`
- `export_arn`
- `export_job_id`
- `started_at`
- `checkpoint_from`
- `checkpoint_to`
- `checkpoint_source`
- `full_run_id`
- `full_export_s3_prefix`
- `pending_exports`
- `checkpoint_state`
- `assume_role`
- `table_account_id`
- `table_region`
- `files_written`
- `items_written`
- `manifest`
- `pages_scanned`
- `error`
- `error_detail`
- `error_type`
- `error_category`
- `error_code`
- `user_message`
- `resolution`
- `retryable`
- `http_status`
- `request_id`

Observação:

- `message` pode aparecer em respostas específicas, como execuções sem tabelas selecionadas ou resultados `SKIPPED`, mas não faz parte do payload comum de todas as execuções.

## Saída opcional em DynamoDB

Quando `OUTPUT_DYNAMODB_ENABLED=true`, a Lambda grava o output final do handler em uma tabela DynamoDB padronizada.

Configuração mínima via variáveis de ambiente:

```env
OUTPUT_DYNAMODB_ENABLED=true
OUTPUT_DYNAMODB_TABLE=snapshot-output
OUTPUT_DYNAMODB_REGION=sa-east-1
```

Se `OUTPUT_DYNAMODB_ENABLED=true` e `OUTPUT_DYNAMODB_TABLE` não estiver definida, a Lambda falha na montagem da configuração com erro de validação.

Se a tabela ainda não existir, a Lambda tenta criá-la automaticamente com `BillingMode=PAY_PER_REQUEST` e chaves:

- partition key `Export ARN` do tipo `String`

Se a tabela já existir, o schema esperado continua sendo:

- partition key `Export ARN` do tipo `String`

Itens gravados por execução:

- apenas exports com `export_arn` disponível são persistidos
- cada item representa uma linha do `Export to S3` no console do DynamoDB

Campos relevantes:

- `Export ARN`
- `Table name`
- `Destination S3 Bucket`
- `Status`
- `Export job start time (utc-03:00)`
- `Export Type`

Mapeamento aplicado no item do DynamoDB:

- `Destination S3 Bucket` salva a URL completa do prefix do export no S3, por exemplo `s3://meu-bucket/DDB/20260309/111111111111/orders/INCR/run_id=20260309T153000Z`
- `Status` é salvo em formato de leitura do console, por exemplo `In progress` e `Completed`
- `Export Type` é salvo como `Full export` ou `Incremental export`
- `Export job start time (utc-03:00)` é convertido para `UTC-03:00` em ISO-8601, por exemplo `2026-03-08T21:00:00-03:00`

## Exemplo de retorno

```json
{
  "ok": true,
  "status": "ok",
  "snapshot_bucket": "meu-bucket-snapshots",
  "run_id": "20260309T153000Z",
  "mode": "automatic",
  "dry_run": false,
  "checkpoint_error": null,
  "updated_checkpoint": "arn:aws:dynamodb:sa-east-1:111111111111:table/snapshot-checkpoints",
  "results": [
    {
      "snapshot_bucket": "meu-bucket-snapshots-us-east-1",
      "table_name": "orders",
      "table_arn": "arn:aws:dynamodb:us-east-1:111111111111:table/orders",
      "mode": "INCREMENTAL",
      "status": "STARTED",
      "source": "native",
      "export_job_id": "016...",
      "s3_prefix": "DDB/20260309/111111111111/orders/INCR/run_id=20260309T153000Z",
      "checkpoint_from": "2026-03-08T00:00:00Z",
      "checkpoint_source": "checkpoint",
      "checkpoint_to": "2026-03-09T15:30:00Z",
      "assume_role": "arn:aws:iam::111111111111:role/central-snapshot-access",
      "table_account_id": "111111111111",
      "table_region": "us-east-1"
    }
  ]
}
```

## Erros comuns

- Sem targets: ocorre quando nenhum alvo é informado.
- Bucket ausente: ocorre quando nem `snapshot_bucket` no evento nem `SNAPSHOT_BUCKET` no ambiente foram definidos.
- Conta AWS incorreta para export: ocorre quando a sessão atual não pertence à conta dona da tabela.
- Timeout aguardando export: ocorre quando `WAIT_FOR_COMPLETION=true` e o export ultrapassa o limite interno de espera.
- PITR não pode ser validado ou habilitado: ocorre quando faltam permissões ou a tabela não chega a `ENABLED` no tempo esperado.
- Permissão insuficiente em S3 ou DynamoDB: ocorre quando a identidade AWS não possui as ações necessárias.

## Observabilidade

Os logs são estruturados em JSON. Eventos relevantes:

- `handler.*`
- `output.cloudwatch*`
- `output.cloudwatch.table`
- `output.dynamodb.write.*`
- `snapshot.run.*`
- `snapshot.permissions.*`
- `checkpoint.save.*`
- `export.full.*`
- `export.incremental.*`
- `export.wait.*`
- `fallback.scan.*`
- `fallback.partition.*`
- `fallback.manifest.*`
- `aws.assume_role.*`

Quando `OUTPUT_CLOUDWATCH_ENABLED=true`, o payload final da execução também é emitido como evento estruturado no fluxo de logs da própria Lambda.

Para consultas em CloudWatch, prefira os campos `table_name`, `table_status`, `export_job_id` e `checkpoint_to` do evento `output.cloudwatch.table`. O `export_arn` continua existindo apenas no estado interno de checkpoint e nos logs técnicos usados para reconciliação.
