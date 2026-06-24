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
- **Action-driven:** o serviço é a ação (`restore`, `modify`, `create`,
  `destroy`, `replicate`, `start/stop`, `storage`, `db-password`, `kms`,
  `vpc-link`), não o recurso.

## Microserviços (ações)

| Serviço        | Ação |
|----------------|------|
| `restore`      | restaura snapshot → instância; também cria snapshot (create-snapshot) |
| `db-password`  | conecta no banco e troca a senha do usuário informado |
| `kms`          | cria Custom KMS Key e vincula/re-encripta, substituindo a default/herdada |
| `replicate`    | copia qualquer recurso cross-account, ou recria em outra region na mesma conta |
| `vpc-link`     | cria acesso privado (PrivateLink/peering) da conta do time ao banco |
| `modify`       | `modify` genérico em qualquer recurso que aceite a ação — inclui **instance class** (`--db-instance-class`) e **engine version** (`--engine-version`) |
| `create`       | provisiona recursos |
| `destroy`      | remove recursos (cleanup) |
| `start`/`stop` | liga/desliga recursos com power |
| `storage`      | altera storage — **tipo** (gp3/io1/…) e **aumento de tamanho** |

> As ações "alterar instance class", "engine version", "alterar tipo de storage"
> e "aumentar storage" **não viram microserviços novos**: as duas primeiras já são
> `modify` e as duas últimas já são `storage`.

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
13. `destroy` (+ `stop`/`storage`) — apaga os recursos temporários em PRD
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
  gen_services.py         # scaffold dos serviços (FastAPI)
  api-gateway/
    openapi.yaml          # contrato consolidado do API Gateway (REST)
    gen_contracts.py      # gerador dos contratos OpenAPI
  <serviço>/              # restore, db-password, kms, replicate, vpc-link,
    Dockerfile            #   modify, create, destroy, start-stop, storage
    requirements.txt
    .dockerignore
    README.md
    contract/openapi.yaml # contrato do path no API Gateway
    app/
      main.py             # FastAPI: POST /<serviço>/execute + /healthz /readyz
      aws.py              # STS:AssumeRole -> boto3.Session
      models.py           # envelope + params (pydantic, espelha o contrato)
      handler.py          # execute(): a ação via boto3
```

Cada serviço é **autocontido** (sem packages compartilhadas): `aws.py`/modelos
são duplicados por design. O scaffold é regenerável por
[`../gen_services.py`](../gen_services.py).
