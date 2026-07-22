# Pipeline de Mascaramento de Dados — Cópia PRD → HOMOL

Arquitetura **action-driven**: mapeamos as **ações** que cada recurso AWS aceita
(não um microserviço por recurso). Cada serviço age sobre qualquer recurso
compatível, respeitando o contrato/parâmetros de cada um, e atua na conta-alvo
via `STS:AssumeRole`.

Imagem renderizada: [`architecture-4k.png`](./architecture-4k.png) (3840×2160).
Fontes: [`architecture.svg`](./architecture.svg) (gerada por
[`gen_architecture.py`](./gen_architecture.py)) e [`architecture.mmd`](./architecture.mmd)
(Mermaid, padrão do repo).

## Princípios

- **Sem packages compartilhadas.** Cada microserviço é autocontido: seu próprio
  `requirements.txt` e seu próprio `Dockerfile`.
- **Entrada padrão de toda ação:** conta AWS + nome do recurso + role para
  `assume-role`. (Espelha o padrão já usado em `automation/aws/` deste repo.)
- **Action-driven, particionado por verbo:** os serviços de ação são **8
  dispatchers genéricos** por verbo (`create`, `modify`, `destroy`,
  `start-stop`, `restore`, `replicate`, `describe`, `data`), cada um cobrindo
  **100% das APIs boto3** de RDS/Aurora, ElastiCache e DynamoDB para aquele
  verbo. Ao lado ficam os **serviços especiais** com lógica própria
  (`db-password`, `kms`, `vpc-link`, `servicenow`, `rds-data`, `finops`,
  `insights`, `dbca`).

## Microserviços (ações)

### Dispatchers genéricos por verbo (296 operações no total)

Contrato único: `params = { operation, args }`, onde `operation` é
`"<client>:<Op>"` (ex.: `"elasticache:CreateCacheCluster"`). O serviço valida a
`operation` contra o **catálogo gerado** (`catalog.json`), aplica as **regras
externas** (allow/deny + GMUD), assume a role na conta-alvo e chama
`getattr(client, method)(**args)`.

| Serviço | Verbo / categoria | Ops | Observações |
|---------|-------------------|:---:|-------------|
| `create` | provisionar | 37 | instâncias, clusters, cache, tabelas, groups, backups |
| `modify` | configurar | 68 | modificações + tags/atributos; **absorveu o antigo `storage`** |
| `destroy` | remover | 42 | deletes destrutivos (gate de GMUD conforme regra) |
| `start-stop` | power | 20 | start/stop/reboot/failover |
| `restore` | backup | 20 | snapshots e restores (RDS/Aurora/cache/DynamoDB PITR) |
| `replicate` | replicate | 5 | read replicas, cross-region/global, share de snapshot |
| `describe` | ler | 91 | **read-only**, sem gate de GMUD |
| `data` | data-plane | 13 | DynamoDB: GetItem/PutItem/Query/Scan/PartiQL |

### Serviços especiais (lógica própria)

| Serviço        | Ação |
|----------------|------|
| `db-password`  | conecta no banco e troca a senha do usuário informado |
| `kms`          | cria Custom KMS Key e vincula/re-encripta, substituindo a default/herdada |
| `vpc-link`     | cria acesso privado (PrivateLink/peering) da conta do time ao banco |
| `servicenow`   | integra com o ServiceNow para GMUD e autorização de execução produtiva |
| `rds-data`     | wrapper seguro do RDS Data API — avalia o SQL contra regras antes de executar |
| `finops`       | varredura **read-only** de desperdício e recomendações de economia (RDS/EC2/EBS/EIP/ELB/snapshots) |
| `insights`     | analytics multi-produto AWS (recursos, métricas, logs, metadados, FinOps) |
| `dbca`         | analytics de metadados de banco (queries de catálogo em qualquer conta) |

> **Serviços dissolvidos:** o serviço dedicado `dynamodb` foi distribuído entre
> os dispatchers (`create`/`modify`/`destroy`/`restore`/`describe`) + o novo
> `data`; o serviço `storage` foi absorvido por `modify`. Ações como "instance
> class", "engine version" e "aumento de storage" são apenas `operation`s do
> dispatcher `modify`.

### Catálogo e geração

- `gen_catalog.py` — introspecta o botocore → `catalog.json` (op → client,
  método, categoria, `mutating`, `resourceType`).
- `gen_action_services.py` — gera os **8 serviços por verbo** a partir do catálogo.
- `gen_gateway.py` — gera o `api-gateway/openapi.yaml` **consolidado** mesclando
  os contratos de todos os serviços.

## Regras de negócio externalizadas

Todo serviço carrega suas **regras de negócio** de um backend externo — **S3 ou
DynamoDB**, escolhido por `RULES_BACKEND` (obrigatório, sem default) —, atualizáveis
**sem redeploy**. A leitura usa a identidade da plataforma (IRSA), com cache TTL
(`RULES_CACHE_TTL`, default 60s) e **fallback resiliente**: se a regra não existir
ou o backend falhar, os defaults embutidos do serviço continuam valendo. O provedor
(`app/rules.py`) é **duplicado por serviço** (sem package compartilhada), como o
resto do scaffold. Env por backend:

- **s3**: `RULES_BUCKET` (+ `RULES_KEY_PREFIX`, default `rules`) → chave `<prefix>/<serviço>.json`
- **dynamodb**: `RULES_TABLE` (+ `RULES_PK`=`service`, `RULES_ATTR`=`rules`)

Exemplos de schema por serviço em [`../rules/`](../rules/). O `finops`, por exemplo,
externaliza thresholds de ociosidade, a tabela de preços (sa-east-1) e a idade de
snapshot considerada órfã.

## Fluxo (ordem de execução)

**Fase 1 — PRD: provisionar cópia**
1. `restore` — restaura snapshot do DB real → **DB Cópia** (PRD).
2. `db-password` — troca a senha do usuário no DB Cópia.
3. `vpc-link` — cria acesso privado para a conta do time.
4. notifica o time: "pode conectar" (endpoint + credenciais).

**Fase 2 — Time: mascarar dados**
5. time conecta e **mascara** os dados produtivos (ferramenta contratada).
6. `restore` (create-snapshot) — gera **Snapshot Mascarado** do DB Cópia.
7. `kms` — cria **Custom KMS Key** e re-encripta o snapshot (substitui a default).
8. time avalia: mascaramento OK? → dispara a promoção.

**Fase 3 — Promover**
9. `replicate` — leva **Snapshot Mascarado + Custom KMS Key** de PRD → HOMOL
   (share cross-account + copy re-encriptado sob a KMS de HOMOL).

**Fase 4 — HOMOL: entregar**
10. `restore` — restaura o snapshot em **DB HOMOL**.
11. `db-password` — troca a senha do usuário no DB HOMOL.
12. notifica os devs: "banco restaurado em HOMOL".

**Fase 5 — Cleanup PRD**
13. `destroy` (+ `start-stop`/`modify`) — apaga os recursos temporários em PRD
    (DB Cópia, snapshot, grants/keys temporárias, vpc-link).

## Orquestração

**Sem orquestrador central (sem Step Functions).** O fluxo é **dirigido pelas
ações do cliente** num **frontend web**: cada passo do pipeline é disparado por
uma ação do cliente, que chama o endpoint da ação correspondente no API Gateway.
Cada serviço executa a ação na conta-alvo via `STS:AssumeRole`.

## Topologia de rede (EKS + API Gateway)

Os microserviços rodam num cluster **EKS** na conta da plataforma (DataDevOps),
atrás de um **NLB interno** (internal-only). Como o NLB não é exposto, o acesso
externo (ação do cliente no frontend) entra por um **API Gateway**:

```
Cliente (time) ─► Frontend (web) ─► API Gateway (REST, edge, Cognito JWT)
                                          │
                                          ▼ VPC Link (privado)
                                    NLB interno (internal-only)
                                          ▼
                                    EKS ─► Service ─► pod (microserviço da ação)
                                          │
                                          └─► STS:AssumeRole ─► Conta PRD / Time / HOMOL
```

- **API Gateway**: tipo **REST** (VPC Link integra com NLB), auth **Cognito
  (JWT authorizer)** — o frontend envia o token do User Pool no header
  `Authorization`. `providerARNs` usa `${COGNITO_USER_POOL_ARN}`, substituído no
  deploy. Um path por ação (`/restore`, `/db-password`, …).
- **VPC Link** e **DNS do NLB** vêm de **stage variables** (`vpcLinkId`,
  `nlbDns`) — o mesmo contrato serve qualquer ambiente.
- Senhas nunca trafegam em plaintext: `db-password` recebe o **ARN de um segredo**
  (Secrets Manager), não a senha.

## Contratos do API Gateway

Gerados por [`../api-gateway/gen_contracts.py`](../api-gateway/gen_contracts.py)
(OpenAPI 3.0, validados):

- [`../api-gateway/openapi.yaml`](../api-gateway/openapi.yaml) — contrato
  **consolidado** do gateway (todas as ações num REST API).
- `../<serviço>/contract/openapi.yaml` — contrato **por microserviço**
  (autocontido), com a integração `x-amazon-apigateway-integration` do tipo
  `http_proxy` + `connectionType: VPC_LINK` apontando para o NLB interno.

Envelope comum de toda requisição: `account` (12 dígitos) + `resource`
(nome/ARN) + `roleArn` (assume-role) + `region`, mais um objeto `params`
específico da ação.

## Estrutura de pastas (proposta)

```
microservicos/
  docs/                   # esta documentação + diagramas
  catalog.json            # catálogo de operações (op -> client/método/categoria)
  gen_catalog.py          # introspecta botocore -> catalog.json
  gen_action_services.py  # gera os 8 dispatchers por verbo a partir do catálogo
  gen_gateway.py          # gera o api-gateway/openapi.yaml consolidado
  gen_services.py         # scaffold dos serviços especiais (FastAPI)
  api-gateway/
    openapi.yaml          # contrato consolidado do API Gateway (REST)
    gen_contracts.py      # gerador dos contratos OpenAPI
  rules/                  # regras externalizadas por serviço (<serviço>.json)
  <serviço>/              # dispatchers: create, modify, destroy, start-stop,
    Dockerfile            #   restore, replicate, describe, data. especiais:
    requirements.txt      #   db-password, kms, vpc-link, servicenow, rds-data,
    .dockerignore         #   finops, insights, dbca
    .dockerignore
    README.md
    contract/openapi.yaml # contrato do path no API Gateway
    infra/k8s/            # manifestos de deploy (namespace + Deployment + Service)
    app/
      main.py             # FastAPI: POST /<serviço>/execute + /healthz /readyz
      aws.py              # STS:AssumeRole -> boto3.Session
      rules.py            # provedor de regras externalizadas (S3/DynamoDB)
      models.py           # envelope + params (pydantic, espelha o contrato)
      handler.py          # execute(): a ação via boto3
```

Cada serviço é **autocontido** (sem packages compartilhadas): `aws.py`, `rules.py`
e modelos são duplicados por design. O scaffold é regenerável por
[`../gen_services.py`](../gen_services.py). Os manifestos em `infra/k8s/` espelham
o padrão do `pdi-portal`: a **imagem** é construída/publicada (ECR) por uma esteira
e o **deploy** no EKS é feito por outra (GitOps), que atualiza a tag da imagem.
