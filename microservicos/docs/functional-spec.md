# Especificação Funcional — Plataforma Action-Driven de Automação de Infraestrutura AWS

> Documento de referência para validar a implementação atual contra o
> comportamento esperado. Onde houver diferença entre o **esperado** (alvo
> arquitetural) e o **atual** (o que está implementado neste repositório), o
> texto sinaliza explicitamente. A Seção 7 consolida a matriz de aderência.

---

## 1. Visão geral da arquitetura

Plataforma corporativa **action-driven**: cada **ação** de infraestrutura é um
microserviço independente, capaz de operar sobre **qualquer recurso AWS
compatível** com aquela ação (não há serviço "por banco" — há serviço "por
ação"). Usada principalmente em um **pipeline de mascaramento de dados** que cria
uma cópia produtiva, troca credenciais, expõe o banco de forma privada para o
time de mascaramento, gera snapshot mascarado com chave KMS própria, promove para
Homologação e limpa os recursos temporários.

**Modelo de serviço (refatoração atual):** as ações de mutação/leitura sobre
recursos foram consolidadas em **8 serviços genéricos particionados por verbo**
(`create`, `modify`, `destroy`, `start-stop`, `restore`, `replicate`,
`describe`, `data`). Cada um é um **dispatcher genérico** que cobre **100% das
APIs boto3** dos clients **RDS/Aurora, ElastiCache e DynamoDB** para aquele
verbo: recebe `params = { operation, args }`, valida `operation` (no formato
`"<client>:<Op>"`) contra um **catálogo gerado** (`catalog.json`), aplica as
**regras externas** (S3/DynamoDB), assume a role na conta-alvo e chama
`getattr(client, method)(**args)`. O padrão espelha o antigo serviço `dynamodb`.
Ao lado deles ficam os **serviços especiais** com lógica própria (`kms`,
`db-password`, `vpc-link`, `servicenow`, `rds-data`, `finops`, `insights`,
`dbca`). Os serviços dedicados `dynamodb` e `storage` foram **dissolvidos** (ver
§2.1).

**Princípios:**

- **Sem orquestrador central** (sem Step Functions). A sequência é dirigida por
  **eventos** e pelas **ações** disparadas externamente (ServiceNow/GMUD).
- **Início via ServiceNow → API Gateway** (governança por GMUD/change).
- **Backend em Amazon EKS**, um Deployment por microserviço, stateless.
- **Multi-account / multi-região**: a plataforma roda numa conta dedicada
  (DataDevOps) e atua nas contas-alvo via **STS AssumeRole** (identidade dos
  pods por **IRSA**).
- **Operações assíncronas**: a chamada inicial é aceita (202) e o resultado é
  acompanhado por um **Status Service** alimentado por eventos.
- **Orientada a eventos**: EventBridge + SQS conectam produtores e consumidores;
  estado das operações em DynamoDB.

**Estado atual (resumo):** os microserviços existem como apps **FastAPI**
autocontidos com handlers **boto3** reais e contratos **OpenAPI** (integração
API Gateway → VPC Link → NLB interno → EKS). Os **8 dispatchers por verbo**
somam **296 operações** cobrindo integralmente RDS/Aurora, ElastiCache e
DynamoDB; os **serviços especiais** (`kms`, `db-password`, `vpc-link`,
`servicenow`, `rds-data`, `finops`, `insights`, `dbca`) mantêm handler próprio.
A camada **assíncrona/eventos**
(EventBridge, SQS, DynamoDB de status, Execution API, Status Service) e as
integrações **ServiceNow**, **observabilidade** (X-Ray/Prometheus/Grafana) e
**GitOps/CI-CD** estão **especificadas mas ainda não implementadas** — hoje a
resposta é síncrona com `202` retornado direto pelo handler.

### 1.1 Topologia (alvo)

```
ServiceNow (GMUD)
  └─ HTTPS → API Gateway (REST, edge)
       └─ Execution API (valida, persiste intenção, enfileira)
            ├─ EventBridge (bus de domínio)   ─┐
            └─ SQS (fila por ação + DLQ)        │ async
                 └─ EKS: <ação>-worker (pod)  ──┘
                      ├─ IRSA → STS AssumeRole → conta-alvo (microservicos-actions-role)
                      ├─ executa via boto3 (RDS/Aurora/EC2/KMS/Secrets)
                      ├─ publica eventos (started/succeeded/failed) → EventBridge
                      └─ atualiza estado → DynamoDB (Status Store)
  Status Service (lê DynamoDB) → ServiceNow consulta progresso/conclusão
Observabilidade: CloudWatch Logs/Métricas, X-Ray (tracing), Prometheus + Grafana
Rede cross-account: PrivateLink + AWS RAM (compartilhamento de sub-redes/recursos)
```

---

## 2. Especificação funcional dos microserviços

### 2.0 Baseline comum (vale para todos)

Os itens abaixo são idênticos entre os serviços; cada serviço descreve apenas
seus **deltas**.

- **Forma do serviço:** container único, FastAPI; rotas `POST /<ação>/execute`,
  `GET /healthz`, `GET /readyz`. Stateless.
- **Entrada (envelope):**
  `account` (12 dígitos), `resource` (nome/ARN), `roleArn` (role assumível na
  conta-alvo), `region`, **`environment`** (`dev|homol|staging|prod`,
  **obrigatório**), `changeNumber` (GMUD, obrigatório p/ produção),
  `requestId` (chave de idempotência, opcional), `dryRun` (bool), `params`
  (objeto específico da ação). **Nos 8 dispatchers por verbo**, `params` tem a
  forma canônica `{ operation, args }`, onde `operation` é `"<client>:<Op>"`
  (ex.: `"elasticache:CreateCacheCluster"`) e `args` é repassado cru para o
  método boto3 correspondente. Os serviços especiais mantêm `params` próprio.
- **Gate de GMUD:** quando `environment == prod`, o `changeNumber` (código da
  GMUD) é **obrigatório** (faltando → `400 validation_error`); o serviço chama o
  microserviço **`servicenow`** (`operation=validate`) e só prossegue se a change
  estiver em **Implement** e **dentro da janela agendada** — caso contrário
  `403 gmud_required`. A change é **sempre criada no ServiceNow** (nunca pela
  plataforma); aqui ela é apenas **buscada, validada e acompanhada**. Ambientes
  não-produtivos não passam pelo gate. (`gmud.py` por serviço; o `servicenow` não
  se auto-gateia.)
- **Saída:** `202 ActionAccepted { operationId, status, resource, account,
  detail }`. Erros: `400` validação, `403` assume-role/permissão, `404` não
  encontrado, `409` conflito/estado inválido, `502` upstream.
- **STS AssumeRole:** o pod (IRSA na conta da plataforma) assume a
  `microservicos-actions-role` na conta-alvo (`assumed_session(account, roleArn,
  region)`), e só então chama a AWS. **Esperado:** cache de credenciais por
  (conta,região) e renovação antes do TTL. **Atual:** assume a cada request.
- **Tratamento de erros:** exceção `ActionError(code, message, http)` +
  mapeamento `ClientError` → HTTP (AccessDenied→403, *NotFound*→404, demais→409,
  transporte→502). `dryRun` curto-circuita antes de qualquer mutação.
- **Idempotência (esperado):** `requestId` registrado no Status Store (DynamoDB)
  com *conditional put*; repetição retorna o mesmo `operationId`. **Atual:** sem
  store — depende da idempotência natural da API AWS (ex.: criar recurso já
  existente → `409`).
- **Eventos (esperado):** publica `action.requested|started|succeeded|failed`
  no EventBridge; consumo da fila SQS da ação; DLQ para falhas. **Atual:** não
  publica/consome (execução in-process).
- **Observabilidade (esperado):** logs estruturados JSON → CloudWatch; métricas
  (latência, sucesso/erro, fila) → Prometheus/Grafana; tracing → X-Ray
  (propaga `requestId`/trace-id). **Atual:** logs default do uvicorn; sem
  métricas/tracing.
- **Retry/Timeout (esperado):** `botocore` com `retries=adaptive`, timeouts de
  connect/read; reprocessamento via SQS (visibility timeout + DLQ);
  back-off exponencial; *poison-pill* para DLQ. **Atual:** defaults boto3;
  timeout do API Gateway (29s) na borda.
- **Escalabilidade:** HPA por CPU/【fila SQS】via KEDA; Karpenter para nós;
  serviços stateless → escala horizontal linear.
- **IAM (na conta-alvo):** usa a `microservicos-actions-role` (trust na role de
  plataforma). A policy `iam/permissions-policy.json` foi estendida com listas
  explícitas de ações **rds/elasticache/dynamodb** (incluindo o **PartiQL do
  DynamoDB** — `dynamodb:PartiQLSelect/Insert/Update/Delete` — para o data-plane
  do serviço `data`), além de `ec2/kms/secretsmanager/rds-data/cloudwatch/
  elasticloadbalancing/iam:PassRole`. O pod tem uma role IRSA com permissão de
  `sts:AssumeRole` nessa role.
- **Contrato de operação (dispatchers):** a `operation` (`"<client>:<Op>"`) é
  resolvida contra o **catálogo gerado** (`catalog.json`); operação fora do
  catálogo do serviço → `400 validation_error`. Em seguida os **args** são
  validados contra o *input shape* real da operação via **botocore**
  (`ParamValidator`, offline, sem rede/credenciais) — required faltando, tipo
  errado ou campo desconhecido → `400 validation_error`; isso vale inclusive no
  `dryRun`. Só então as **regras externas** (§2.3) decidem allow/deny e se exige
  GMUD; por fim `getattr(client, method)(**args)`.

---

### 2.1 Serviços genéricos por verbo (dispatchers)

As ações de mutação/leitura sobre recursos deixaram de ser handlers *bespoke*
por operação e passaram a **8 serviços genéricos particionados por verbo**. Cada
serviço é um **dispatcher** que cobre **100% das APIs boto3** dos clients
**RDS/Aurora**, **ElastiCache** e **DynamoDB** para aquele verbo — **296
operações** no total.

**Contrato único (`params = { operation, args }`):**

- `operation`: string `"<client>:<Op>"` (ex.: `"elasticache:CreateCacheCluster"`,
  `"rds:ModifyDBInstance"`, `"dynamodb:UpdateTable"`).
- `args`: dicionário repassado **cru** ao método boto3 (`getattr(client,
  method)(**args)`), com o nome de método derivado do catálogo (ex.:
  `CreateCacheCluster` → `create_cache_cluster`).

**Fluxo interno (idêntico nos 8):** `resolve(operation)` no catálogo → se não
existe no serviço → `400`; carrega **regras externas** e `evaluate(...)`
(allow/deny + região + categorias + GMUD) → se `dryRun`, devolve o **plano**
(`operation`, `client`, `method`, `category`, `mutating`, `resourceType`,
`gmudRequired`, `exceptionApplied`, `args`) sem mutar; senão, quando a regra
exige, `ensure_change_authorized(...)` (gate de GMUD) → `assumed_session` na
conta-alvo → `client = session.client(op.client)` →
`getattr(client, op.method)(**args)` → `detail.result` (limpo de
`ResponseMetadata`; valores `Decimal`/`bytes` serializados). O padrão espelha o
antigo serviço `dynamodb`.

| Serviço | Verbo / categoria | Ops | GMUD | Observações |
|---------|-------------------|:---:|:----:|-------------|
| `create` | provisionar (provision) | 37 | sim | Cria instâncias, clusters, cache, tabelas, subnet/parameter groups, backups. |
| `modify` | configurar (config) | 68 | sim | Modificações + tags/atributos; **absorveu o antigo `storage`** (AllocatedStorage/StorageType/Iops/Throughput). |
| `destroy` | remover (delete) | 42 | sim | Deletes destrutivos (instância/cluster/snapshot/cache/tabela/grupos). |
| `start-stop` | power | 20 | sim | Start/Stop/Reboot/Failover de instâncias, clusters e cache. |
| `restore` | backup | 20 | sim | Snapshots e restores (RDS/Aurora/cache/DynamoDB PITR e from-backup). |
| `replicate` | replicate | 5 | sim | Read replicas, cross-region/global e share de snapshot. |
| `describe` | ler (read) | 91 | **não** | **Read-only** — não passa pelo gate de GMUD; `Describe*`/`List*`. |
| `data` | dados (data) | 13 | conforme regra | **Data-plane do DynamoDB** (`GetItem`/`PutItem`/`Query`/`Scan`/`BatchGetItem`/`PartiQL*`, etc.). |

Clients cobertos: `rds`, `elasticache`, `dynamodb`. Categorias do catálogo:
`provision | config | delete | power | backup | replicate | read | data`.

**Serviços dissolvidos:** o serviço dedicado `dynamodb` deixou de existir — seu
**control-plane** foi distribuído entre `create`/`modify`/`destroy`/`restore`/
`describe` e seu **data-plane** virou o novo serviço `data`; o serviço `storage`
foi **absorvido** por `modify`.

### 2.2 Catálogo de operações e geração

O comportamento dos dispatchers é **gerado a partir do botocore**, não escrito à
mão:

- **`gen_catalog.py`** — introspecta o botocore e produz **`catalog.json`**: para
  cada operação, `key` (`"<client>:<Op>"`), `name`, `method` (snake_case),
  `client`, `service` (verbo), `category`, `mutating`, `resourceArg` e
  `resourceType`.
- **`gen_action_services.py`** — gera os **8 serviços por verbo** (FastAPI +
  `operations.py`/`policy.py`/`rules.py`/`handler.py`) a partir do catálogo,
  cada um autocontido.
- **`gen_gateway.py`** — gera o **contrato consolidado** do gateway
  (`api-gateway/openapi.yaml`) **mesclando os contratos de todos os serviços**.

(`gen_services.py` segue gerando/scaffoldando os serviços especiais.)

### 2.3 Regras de negócio externas (schema por serviço)

Cada serviço lê suas regras de um backend externo (**S3 ou DynamoDB**, ver §3 e
`architecture.md`), em `rules/<serviço>.json`, atualizáveis sem redeploy. O
schema é **opt-in**: **a ausência de uma chave = sem restrição** naquele eixo.

- **`allowedRegions`**: lista de regiões permitidas (fora dela → negado).
- **`environments.<dev|homol|staging|prod>`**, cada um com:
  - `allowedOperations` / `deniedOperations` — allow/deny por nome de operação.
  - `allowedCategories` / `deniedCategories` — allow/deny por **categoria**
    (`provision | config | delete | power | backup | replicate | read | data`).
  - `allowedResourceTypes` / `deniedResourceTypes` — allow/deny por tipo de
    recurso (ex.: `cache-cluster`, `table`).
  - `requireGmudForMutations` (bool) — exige GMUD para operações mutantes.
  - `gmudForCategories` — lista de categorias que exigem GMUD.
- **`exceptions[]`**: liberações pontuais com `account`, `environment`,
  `resource`, `allowOperations`, `allowCategories`, `reason` e `expiresAt`
  (exceção expirada não vale).

A decisão (`evaluate`) devolve resolução de `resource`/`resourceType`, se **exige
GMUD** e qual `exceptionId` foi aplicada. O gate de GMUD (§2.0) só é acionado
quando a regra o exige (`describe`, read-only, nunca exige).

---

### 2.4 `db-password`

- **Objetivo:** trocar a senha de um usuário **dentro do banco**, conectando nele.
- **Responsabilidades:** descobrir endpoint/engine, obter credencial admin e a
  nova senha do Secrets Manager, executar `ALTER ROLE/USER`.
- **Recursos AWS suportados:** RDS/Aurora PostgreSQL e MySQL/MariaDB.
- **Operações:** troca de senha (in-database), via `psycopg`/`pymysql`.
- **Fluxo interno:** assume-role → `DescribeDBInstances` (endpoint, engine,
  `MasterUserSecret`) → `GetSecretValue` (admin + nova senha) → conecta TLS →
  `ALTER ROLE "<u>" WITH PASSWORD` (PG) / `ALTER USER ... IDENTIFIED BY` (MySQL).
- **Entradas:** `dbIdentifier`, `username`, `newPasswordSecretArn`, `engine`.
  **Saídas:** `detail.rotated=true`.
- **Validações:** instância existe; possui `MasterUserSecret`; engine suportada;
  `newPasswordSecretArn` válido. **Nunca** recebe senha em plaintext.
- **Erros (delta):** sem `MasterUserSecret` → `409`; falha de conexão de rede →
  `502`. Requer rota de rede (mesma VPC/PrivateLink) ao banco.
- **Idempotência:** naturalmente idempotente (setar a mesma senha é seguro).
- **IAM:** `rds:DescribeDBInstances`, `secretsmanager:GetSecretValue`,
  `kms:Decrypt` (segredo cifrado).
- **Limitações:** depende de conectividade L4 ao banco; admin via
  `MasterUserSecret` gerenciado (sem isso, exige credencial admin configurada).
- **Casos de uso:** trocar senha do usuário na cópia e no banco de HOMOL.
- **Evoluções:** rotação via Secrets Manager nativo, suporte a IAM auth do RDS,
  pool de conexões, suporte a SQL Server/Oracle.

### 2.5 `kms`

- **Objetivo:** criar Custom KMS Key e vinculá-la a um recurso, substituindo a
  default/herdada (re-encriptação).
- **Recursos:** KMS key/alias; RDS DB snapshot (re-encripta via cópia).
- **Operações:** `CreateKey`, `CreateAlias`, `PutKeyPolicy`; para snapshot,
  `CopyDBSnapshot` com `KmsKeyId`.
- **Fluxo interno:** assume-role → cria key + alias → política opcional → se alvo
  é `db-snapshot`, copia o snapshot sob a nova key (`<id>-cmk`).
- **Entradas:** `keyAlias`, `description`, `targetResourceType`,
  `targetResourceId`, `replaceInherited`, `keyPolicyJson`.
  **Saídas:** `detail.keyId`, `alias`, `reEncryptedSnapshot`.
- **Validações:** `targetResourceType ∈ {db-instance, db-snapshot}`; alias único.
- **Idempotência (delta):** `CreateKey` **não** é idempotente — re-execução cria
  nova key. Mitigar com `requestId`/alias determinístico + checagem de alias.
- **IAM:** `kms:CreateKey/CreateAlias/PutKeyPolicy/DescribeKey`,
  `CreateGrant`, cripto (`Encrypt/Decrypt/ReEncrypt*/GenerateDataKey*`),
  `rds:CopyDBSnapshot`.
- **Limitações:** `db-instance` não re-encripta in-place (KMS só via novo
  snapshot/restore); risco de keys órfãs se falhar após `CreateKey`.
- **Casos de uso:** chave própria para o snapshot mascarado em PRD e em HOMOL.
- **Evoluções:** rotação automática, key policies por template, multi-Region keys,
  *idempotency by alias*, limpeza de keys órfãs.

### 2.6 `privatelink` (atual: `vpc-link`)

- **Objetivo:** dar acesso privado da conta consumidora (time) ao banco via
  PrivateLink/VPC Endpoint Service.
- **Recursos:** EC2 VPC Endpoint Service / permissões de endpoint.
- **Operações:** `ModifyVpcEndpointServicePermissions` (autoriza principal do
  consumidor); evolução: `CreateVpcEndpointServiceConfiguration`.
- **Fluxo interno:** assume-role → adiciona o principal
  `arn:aws:iam::<consumerAccount>:root` ao endpoint service informado.
- **Entradas:** `dbIdentifier`, `consumerAccount`, `allowedPrincipals`,
  `endpointServiceId`, `ports`. **Saídas:** `detail.grantedPrincipals`.
- **Validações:** `endpointServiceId` obrigatório para autorizar.
- **Idempotência:** add-principal é idempotente.
- **IAM:** `ec2:ModifyVpcEndpointServicePermissions`,
  `CreateVpcEndpointServiceConfiguration`, `Describe*`.
- **Limitações:** assume que o endpoint service (com NLB) já existe; não cria o
  NLB; integra com **AWS RAM** quando há compartilhamento de sub-redes.
- **Casos de uso:** liberar o time de mascaramento a conectar no DB cópia.
- **Evoluções:** criar o endpoint service do zero (NLB + targets), auto-aceitar
  conexões, integração RAM, limpeza no fim do fluxo.

### 2.7 `servicenow` (GMUD)

- **Objetivo:** integrar com o ServiceNow para **acompanhamento de GMUD** e
  **autorização de execução produtiva** — é a autoridade que os demais serviços
  consultam.
- **Responsabilidades:** validar se uma change autoriza a execução; registrar
  progresso (work notes) no change; consultar o estado do change.
- **Recursos/Sistemas:** ServiceNow Change Management (Table API
  `change_request`).
- **Operações:** `validate` (libera se estado ∈ Implement **e** dentro da janela
  `start_date`/`end_date`), `register` (adiciona work note), `status` (estado do
  change).
- **Fluxo interno:** monta cliente HTTP (instância + auth via env/Secrets) →
  `GET /api/now/table/change_request?number=<n>` → avalia estado/janela; em
  `register`, `PATCH` do change.
- **Entradas:** envelope + `params.operation`, `action`, `changeNumber`,
  `operationId`, `workNote`, `state`. **Saídas:** `detail.allowed`/`state`/
  `withinWindow`/`registered`.
- **Validações:** `operation ∈ {validate, register, status}`; `changeNumber`
  obrigatório em `validate`/`status`.
- **Erros:** credencial inválida → `403`; change inexistente → `404`; falha de
  transporte → `502`.
- **Idempotência:** `validate`/`status` read-only; `register` aditivo.
- **Eventos/Obs/Retry:** baseline. **Não usa STS/IAM AWS** (chama o ServiceNow);
  precisa apenas ler as credenciais do ServiceNow (env/Secrets Manager).
- **Config (env):** `SERVICENOW_INSTANCE_URL`, `SERVICENOW_USER`/`PASSWORD` ou
  `SERVICENOW_TOKEN`, `SERVICENOW_ALLOWED_STATES` (default `-1,implement`),
  `SERVICENOW_CHANGE_TABLE` (default `change_request`).
- **Limitações:** mapeamento de estados/janela assume o modelo padrão do
  ServiceNow Change; campos/estados configuráveis por env.
- **Casos de uso:** liberar `destroy`/`modify`/etc. em produção apenas com GMUD;
  registrar a execução no change; fechar a mudança.
- **Evoluções:** anexar resultado/evidência ao change, **webhook inbound** de
  aprovação do ServiceNow, cache de validação, suporte a CAB/aprovação
  multinível. (A change continua sendo **criada no ServiceNow** — fora do escopo
  da plataforma.)

### 2.8 `rds-data`

- **Objetivo:** wrapper **seguro** do **RDS Data API** — uma camada extra que
  **avalia o SQL** contra regras de negócio antes de executar.
- **Responsabilidades:** carregar as regras (JSON em **S3**), avaliar o SQL,
  bloquear o que viola a política e, se permitido, executar via Data API.
- **Recursos AWS suportados:** Aurora (clusters com **Data API** habilitado);
  segredo de credenciais no Secrets Manager; regras em **S3**.
- **Operações:** `ExecuteStatement` (e parâmetros nomeados).
- **Fluxo interno:** carrega regras de `s3://RULES_BUCKET/RULES_KEY` (cache TTL,
  **identidade da plataforma/IRSA**) → `evaluate_sql` (sqlparse) → se negado
  `403 sql_forbidden`; senão assume-role na conta-alvo → `rds-data`
  `execute_statement`.
- **Entradas:** `params.sql`, `secretArn`, `resourceArn` (default `resource`),
  `database`, `schema`, `parameters` (name→value), `includeResultMetadata`,
  `rulesBucket`/`rulesKey` (override). **Saídas:** `detail.records`/
  `numberOfRecordsUpdated`/`columnMetadata` + `allowed`/`reason`.
- **Regras (JSON em S3):** `default` (allow|deny), `maxStatements`,
  `allowedStatements`/`deniedStatements` (por tipo), `deniedKeywords`,
  `denyPatterns` (regex), `requireWhereOnWrite`, `tables.allow|deny`,
  `environments.<env>` (override por ambiente). Exemplo em
  `rds-data/rules.example.json`.
- **Validações:** `sql` e `secretArn` obrigatórios; SQL avaliado contra as
  regras (tipo de statement, keywords/regex, WHERE em escrita, nº de statements,
  allow/deny de tabelas — best-effort).
- **Erros:** SQL bloqueado → `403 sql_forbidden`; bucket não configurado/regras
  inválidas → `400`; regras não encontradas → `404`.
- **Idempotência:** depende do SQL (responsabilidade do chamador); `dryRun` só
  avalia (não executa).
- **STS/IAM:** Data API na conta-alvo (`rds-data:*` + `secretsmanager`); leitura
  das regras com a **IRSA da plataforma** (`s3:GetObject` no bucket de regras).
- **Gate de GMUD:** sim (produção exige change), como os demais.
- **Limitações:** extração de tabelas é best-effort (regex); não substitui
  privilégios do banco — é uma camada **adicional**. Exige Aurora com Data API.
- **Casos de uso:** permitir queries operacionais controladas em produção sem dar
  acesso direto ao banco; bloquear DDL/DML perigosos por política central.
- **Evoluções:** parser AST completo p/ tabelas/colunas, limites de linhas,
  mascaramento de resultado, allowlist por usuário/role, auditoria do SQL
  avaliado, transações (Begin/Commit/Rollback).

### 2.9 `finops` · `insights` · `dbca` (analytics read-only)

Serviços especiais **read-only** que complementam os dispatchers, sem mutar
recursos e sem gate de GMUD:

- **`finops`** — varredura de **desperdício e recomendações de economia**
  (RDS/EC2/EBS/EIP/ELB/snapshots). Externaliza thresholds de ociosidade, tabela
  de preços (sa-east-1) e idade de snapshot órfão via regras (`rules/finops.json`).
- **`insights`** — **analytics multi-produto AWS**: recursos, métricas, logs,
  metadados profundos e FinOps agregados.
- **`dbca`** — **analytics de metadados de banco**: conecta em qualquer recurso
  de banco em qualquer conta e roda queries de metadados/catálogo.

---

## 3. Componentes compartilhados da plataforma

| Componente | Responsabilidade |
|------------|------------------|
| **ServiceNow (GMUD)** | Origem da execução; cria a mudança (change/GMUD), aplica aprovação/governança e chama a plataforma via API Gateway; consulta status para fechar a mudança. |
| **API Gateway** | Front-door REST (edge), 1 path por ação; autenticação/authorizer; request validation; throttling/usage plans; integração privada via VPC Link → NLB interno. |
| **Execution API** | Recebe a intenção da ação, valida o envelope, registra a operação (DynamoDB) e **enfileira** o trabalho (SQS/EventBridge). Devolve `operationId` (202). Desacopla borda de execução. |
| **Status Service** | Expõe o estado/histórico de uma operação (lendo o Status Store em DynamoDB) para o ServiceNow/observadores acompanharem progresso e desfecho. |
| **EventBridge** | Bus de eventos de domínio (`action.requested/started/succeeded/failed`); roteamento por regra para filas/consumidores; base do desacoplamento orientado a eventos. |
| **Amazon SQS** | Fila por ação (buffer, *back-pressure*, retry com visibility timeout, **DLQ** para *poison pills*); permite escala dos workers por profundidade de fila (KEDA). |
| **Amazon EKS** | Runtime dos microserviços (Deployments stateless por ação), HPA/Karpenter, Ingress interno atrás do NLB; isolamento por namespace; *rolling updates*. |
| **IRSA** | Identidade do pod (IAM Roles for Service Accounts) sem credenciais estáticas; a role IRSA tem permissão de `sts:AssumeRole` na role da conta-alvo. |
| **STS AssumeRole** | Ponte cross-account: o pod assume a `microservicos-actions-role` na conta-alvo e executa as ações com credenciais temporárias e escopadas. |
| **Secrets Manager** | Guarda credenciais (master secret do RDS, nova senha do usuário); o `db-password` lê via `GetSecretValue`; senhas nunca trafegam em plaintext. |
| **KMS** | Chaves de criptografia (custom keys do pipeline, cifragem de segredos e snapshots); o serviço `kms` cria/gerencia; demais serviços usam grants. |
| **CloudWatch** | Logs estruturados e métricas básicas; alarmes; base de auditoria operacional; *log groups* por serviço. |
| **X-Ray** | Tracing distribuído ponta-a-ponta (borda → fila → worker → AWS API), correlacionado por `requestId`/trace-id. |
| **Prometheus** | Coleta de métricas dos pods/serviços (latência, taxa de erro, profundidade de fila, saturação) via scrape. |
| **Grafana** | Dashboards e alertas sobre as métricas do Prometheus/CloudWatch; visão operacional e SLO. |
| **DynamoDB** | **Status Store**: estado/idempotência das operações (chave `requestId`/`operationId`), histórico e *conditional writes*; baixa latência e escala. |
| **CI/CD** | Build/test/scan das imagens, push para ECR e promoção entre ambientes; versiona contratos e manifests. |
| **GitOps** | Fonte da verdade declarativa (Argo CD/Flux) dos manifests do EKS; *drift detection* e *rollback* por git. |
| **PrivateLink** | Acesso privado entre VPCs/contas ao banco (sem exposição pública); base do `privatelink`. |
| **AWS RAM** | Compartilhamento de recursos entre contas (sub-redes, Transit Gateway, etc.) para viabilizar a rede cross-account do PrivateLink e do pipeline. |

---

## 4. Fluxo completo de execução do pipeline

1. **GMUD no ServiceNow** aprova a mudança e chama o **API Gateway** (ação +
   envelope: conta, recurso, role, região, params).
2. **API Gateway** autentica/valida e encaminha à **Execution API**.
3. **Execution API** registra a operação no **DynamoDB** (idempotência por
   `requestId`), publica `action.requested` no **EventBridge** e/ou enfileira na
   **SQS** da ação; responde `202 { operationId }`.
4. O **worker** da ação (pod no **EKS**) consome a mensagem, publica
   `action.started`, assume a role via **IRSA → STS** na conta-alvo e executa
   via boto3.
5. Ao concluir, publica `action.succeeded|failed`, grava o resultado no
   **DynamoDB** e (em falha) envia para a **DLQ**.
6. O **Status Service** reflete o progresso; o **ServiceNow** consulta e fecha a
   GMUD.

**Sequência de negócio (mascaramento), encadeada por GMUDs/eventos:**
`restore` (cópia em PRD) → `db-password` → `privatelink` → (time mascara) →
`restore`/create-snapshot → `kms` → avaliação → `replicate` (PRD→HOMOL) →
`restore` (em HOMOL) → `db-password` → notificação → `destroy`/`start-stop`/
`modify` (cleanup em PRD).

---

## 5. Requisitos não funcionais

- **Segurança:** zero credencial estática (IRSA + STS); least-privilege por
  ação; segredos no Secrets Manager (KMS); criptografia em trânsito/repouso;
  trust com `ExternalId` opcional; isolamento por conta/região; LGPD para dados
  do pipeline; *audit trail* (CloudTrail + DynamoDB de operações).
- **Disponibilidade:** EKS multi-AZ; filas absorvem picos/indisponibilidade;
  *retries*+DLQ; serviços stateless; *rolling updates* sem downtime.
- **Escalabilidade:** escala horizontal por ação (HPA/KEDA por profundidade de
  fila); Karpenter; multi-região; particionamento por conta.
- **Auditoria:** toda operação rastreável por `operationId` (quem/o quê/quando/
  resultado), correlacionada à GMUD; CloudTrail nas contas-alvo.
- **Observabilidade:** logs estruturados, métricas (RED/USE), tracing X-Ray,
  dashboards/alertas (Grafana), SLOs por ação.

---

## 6. Recomendações arquiteturais

**Pontos fortes:** desacoplamento por ação; reuso (action-driven sobre qualquer
recurso); cross-account seguro (IRSA+STS); início governado (GMUD); base
event-driven que tolera falhas e picos.

**Riscos / lacunas (vs. atual):**

- **Assincronismo ainda não real:** hoje a execução é in-process síncrona (202
  "otimista"). Sem SQS/worker, uma operação longa estoura o timeout (29s) do API
  Gateway. → **Implementar Execution API + SQS + worker**.
- **Sem idempotência durável:** sem DynamoDB Status Store, reprocessamento pode
  duplicar efeitos (ex.: `kms:CreateKey`, `create`). → **Store + conditional
  writes + chave determinística**.
- **`kms`/`create` não idempotentes** e com risco de recursos órfãos em falha
  parcial. → **idempotency-by-alias/identifier + compensação/cleanup**.
- **Observabilidade ausente:** sem métricas/tracing → cegueira operacional. →
  **OpenTelemetry → X-Ray + Prometheus/Grafana**.
- **`args` repassado cru ao boto3:** os dispatchers validam a `operation` contra
  o catálogo, mas `args` vai direto ao método (poder com risco). → **validação
  de schema de `args` por operação** (o botocore já dá o shape; gerar validação a
  partir do catálogo).
- **Cobertura ampla = superfície ampla:** com 296 operações expostas, o controle
  fino recai sobre as **regras externas** (allow/deny por categoria/op/tipo +
  GMUD). → **manter as regras versionadas e auditadas** e defaults restritivos
  por ambiente (deny-by-default em prod).
- **Cross-account de 2 lados (replicate):** o *copy/re-encrypt* no destino não é
  orquestrado. → **fluxo de 2 etapas (origem+destino) coordenado por eventos**.
- **Segurança:** `Resource:"*"` na policy (necessário p/ `kms:CreateKey`) →
  **escopar por tag/ARN + condição de região**; trust com `ExternalId`
  obrigatório em produção.

**Gargalos:** timeout do API Gateway para operações longas; assume-role a cada
request (latência) → cache; *throttling* das APIs RDS/EC2 em larga escala →
back-off+fila.

**Operação em larga escala:** particionar filas por conta/região; *rate limit*
por conta-alvo; *circuit breaker* por serviço/conta; *bulk* por tag; catálogo de
ações (descoberta) e versionamento de contrato.

---

## 7. Matriz de aderência

Legenda: **Impl.** = implementado · **Parc.** = parcial · **N/Impl.** = não
implementado. Base: o que existe neste repositório (handlers FastAPI + contratos
OpenAPI + role IAM + arquitetura/diagrama) vs. a especificação-alvo acima.

| Componente | Funcionalidade esperada | Impl. | Parc. | N/Impl. | Observações |
|------------|-------------------------|:----:|:----:|:------:|-------------|
| create (dispatcher) | 37 ops · cobertura boto3 total (RDS/ElastiCache/DynamoDB) | ✅ | | | Provisiona via `operation`/`args`; governado por regras externas + GMUD. |
| modify (dispatcher) | 68 ops · cobertura boto3 total | ✅ | | | Absorveu o antigo `storage`; `args` cru validado por catálogo + regras. |
| destroy (dispatcher) | 42 ops · cobertura boto3 total | ✅ | | | Destrutivo; gate de GMUD conforme regra; `dryRun` disponível. |
| start-stop (dispatcher) | 20 ops · cobertura boto3 total | ✅ | | | Start/Stop/Reboot/Failover de RDS/Aurora/cache. |
| restore (dispatcher) | 20 ops · cobertura boto3 total | ✅ | | | Snapshots/restores RDS/Aurora/cache + PITR DynamoDB. |
| replicate (dispatcher) | 5 ops · cobertura boto3 total | ✅ | | | Read replicas, cross-region/global, share de snapshot. |
| describe (dispatcher) | 91 ops · cobertura boto3 total | ✅ | | | **Read-only**, sem gate de GMUD. |
| data (dispatcher) | 13 ops · data-plane DynamoDB | ✅ | | | GetItem/PutItem/Query/Scan/Batch/**PartiQL**. |
| db-password | troca de senha in-database (PG/MySQL) | ✅ | | | Serviço especial; exige rota de rede ao banco e `MasterUserSecret`. |
| kms | custom key + re-encrypt snapshot | ✅ | | | Serviço especial; `db-instance` não re-encripta in-place; `CreateKey` não idempotente. |
| privatelink | acesso privado ao banco | | ⚠️ | | Serviço especial; só autoriza principal em endpoint service existente; não cria NLB/serviço. |
| rds-data | wrapper seguro do RDS Data API + regras (S3) | ✅ | | | Serviço especial; avaliador (sqlparse) testado; extração de tabelas best-effort; requer Aurora Data API. |
| finops / insights / dbca | analytics read-only (FinOps/recursos/metadados) | ✅ | | | Serviços especiais read-only; sem mutação e sem GMUD. |
| ~~dynamodb~~ / ~~storage~~ | serviços dedicados | | | — | **Dissolvidos**: dynamodb → create/modify/destroy/restore/describe + `data`; storage → `modify`. |
| Regras externas (schema) | allow/deny por op/categoria/tipo + GMUD + exceptions | ✅ | | | `rules/<svc>.json` (S3/DynamoDB); enforcement opt-in (ausência de chave = sem restrição). |
| Catálogo + geração | `gen_catalog`/`gen_action_services`/`gen_gateway` | ✅ | | | `catalog.json` (296 ops) gera os 8 serviços e o OpenAPI consolidado. |
| Envelope/Contrato | account+resource+role+region+**environment**+params `{operation,args}` | ✅ | | | Contratos OpenAPI validados; `environment` obrigatório; gateway consolidado por `gen_gateway`. |
| servicenow (microserviço) | validate/register/status de GMUD | | ⚠️ | | Implementado (ServiceNow Table API); integração real depende de credenciais/instância; não cria change automaticamente. |
| Gate de GMUD (prod) | bloquear execução produtiva sem change aprovada | ✅ | | | Em `prod`, `changeNumber` obrigatório (400); `gmud.py` chama `servicenow validate` (Implement + janela). Change sempre criada no ServiceNow. |
| dryRun | execução de teste sem efeito | ✅ | | | Presente em todos os handlers. |
| STS AssumeRole | execução cross-account | ✅ | | | Implementado; sem cache de credenciais. |
| IRSA | identidade do pod sem credencial estática | | | ❌ | Previsto na arquitetura; não há manifests/deploy. |
| IAM role alvo | role + listas explícitas rds/elasticache/dynamodb (trust+perms) | ✅ | | | `microservicos/iam/`; inclui **PartiQL** do DynamoDB p/ data-plane; `Resource:"*"`. |
| API Gateway | REST + VPC Link + validação | | ⚠️ | | Contrato OpenAPI pronto; **não deployado**; auth a definir (AD). |
| ServiceNow (gatilho/governança) | início via change/GMUD | | ⚠️ | | Gate de produção implementado (prod exige Implement+janela); **disparo a partir do ServiceNow** ainda não. |
| Execution API | aceitar, persistir intenção, enfileirar | | | ❌ | Não existe; execução é in-process. |
| Status Service | consulta de estado/histórico | | | ❌ | Não existe. |
| EventBridge | eventos de domínio | | | ❌ | Não publica/consome. |
| Amazon SQS | fila por ação + DLQ | | | ❌ | Não há fila; sem assíncrono real. |
| DynamoDB (Status Store) | estado + idempotência | | | ❌ | Sem store; idempotência só natural da API. |
| Operação assíncrona | 202 + processamento em background | | ⚠️ | | Retorna 202, mas executa síncrono (risco de timeout). |
| Amazon EKS | runtime dos serviços | | ⚠️ | | Dockerfiles prontos; sem manifests/cluster. |
| CloudWatch (logs) | logs estruturados | | ⚠️ | | Logs default do uvicorn; não estruturados/centralizados. |
| X-Ray (tracing) | tracing distribuído | | | ❌ | Não instrumentado. |
| Prometheus | métricas | | | ❌ | Sem `/metrics`. |
| Grafana | dashboards/alertas | | | ❌ | Não configurado. |
| Retry/Timeout | back-off, DLQ, timeouts | | ⚠️ | | Defaults boto3; sem DLQ/política explícita. |
| Idempotência durável | requestId + conditional write | | ⚠️ | | Campo `requestId` existe; não é persistido/honrado. |
| Secrets Manager | credenciais via secret | ✅ | | | Usado pelo `db-password`. |
| KMS (cripto) | chaves e grants | ✅ | | | Usado pelo `kms` e snapshots. |
| PrivateLink | rede privada cross-account | | ⚠️ | | Ação autoriza principal; topologia de rede fora do escopo do código. |
| AWS RAM | compartilhamento cross-account | | | ❌ | Mencionado na arquitetura; não automatizado. |
| CI/CD | build/scan/push/promover | | | ❌ | Não há pipeline no repo. |
| GitOps | deploy declarativo (Argo/Flux) | | | ❌ | Sem manifests/repo de GitOps. |
| Multi-região | execução em N regiões | | ⚠️ | | Envelope tem `region`; sem roteamento/particionamento por região. |
| Frontend/console | disparo e visualização das ações | | ⚠️ | | Console Angular existe (gera forms do OpenAPI); não é o ServiceNow. |

### Resumo da aderência

- **Núcleo de execução das ações:** **implementado** — 8 dispatchers genéricos
  por verbo com **cobertura boto3 total** de RDS/Aurora, ElastiCache e DynamoDB
  (296 ops), governados por **regras externas** (allow/deny + categorias + GMUD +
  exceptions) e gerados a partir do catálogo (`gen_catalog`/`gen_action_services`/
  `gen_gateway`); + serviços especiais (`kms`, `db-password`, `vpc-link`,
  `servicenow`, `rds-data`, `finops`, `insights`, `dbca`) + contratos + dryRun +
  STS + IAM + Secrets/KMS.
- **Camada assíncrona/event-driven (Execution API, SQS, EventBridge, DynamoDB,
  Status Service), governança ServiceNow, observabilidade (X-Ray/Prometheus/
  Grafana), IRSA/EKS deploy, CI-CD/GitOps, RAM:** **não implementada** (definida
  na arquitetura).
- **API Gateway, EKS, operação assíncrona, idempotência durável, PrivateLink,
  multi-região, console:** **parciais**.

> Conclusão: a plataforma tem o **plano de execução por ação** sólido e seguro,
> mas para atender ao comportamento esperado de uma plataforma corporativa
> assíncrona/event-driven faltam, em ordem de prioridade: **(1)** Execution API +
> SQS + worker (assíncrono real e idempotência via DynamoDB); **(2)**
> observabilidade (OTel→X-Ray/Prometheus/Grafana); **(3)** deploy EKS/IRSA +
> GitOps/CI-CD; **(4)** integração ServiceNow e orquestração cross-account de 2
> lados (share+copy re-encriptado do `replicate`, `privatelink`).
