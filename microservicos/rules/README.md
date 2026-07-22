# Regras de negócio externalizadas

Regras que cada microserviço deve respeitar, mantidas **fora da imagem** e
atualizáveis **sem redeploy**. Cada serviço lê as suas via `app/rules.py`, com
cache TTL e fallback para os defaults embutidos quando a regra não existir.

Os `.json` deste diretório são o **material de origem** (versionado) das regras.
A esteira publica cada arquivo no backend escolhido:

## Backend (obrigatório — `RULES_BACKEND`, sem default)

- **s3**: sobe `<serviço>.json` para `s3://$RULES_BUCKET/$RULES_KEY_PREFIX/<serviço>.json`
  (prefixo default `rules`). Ex.:

  ```bash
  aws s3 cp finops.json s3://$RULES_BUCKET/rules/finops.json --region sa-east-1
  ```

- **dynamodb**: grava um item por serviço na tabela `$RULES_TABLE`
  (PK `$RULES_PK`=`service`, atributo `$RULES_ATTR`=`rules` com o JSON). Ex.:

  ```bash
  aws dynamodb put-item --table-name "$RULES_TABLE" --region sa-east-1 \
    --item "{\"service\":{\"S\":\"finops\"},\"rules\":{\"S\":$(jq -Rs . < finops.json)}}"
  ```

## Convenção

- Um arquivo por serviço: `<serviço>.json` (nome = nome do microserviço).
- O conteúdo **sobrepõe** (deep-merge) os defaults embutidos do serviço; envie
  apenas o que quiser sobrescrever, ou o schema completo.
- Alterações valem no próximo ciclo de cache (`RULES_CACHE_TTL`, default 60s),
  sem redeploy.

## Enforcement (serviços de escrita)

Os serviços de escrita validam a requisição contra as regras **antes de qualquer
chamada AWS**. O modelo é **opt-in e permissivo por omissão**: uma chave ausente
(ou lista vazia) **não restringe nada** — o comportamento atual é preservado até
você publicar uma regra. Violação retorna `rule_violation` (HTTP 403).

Chaves genéricas (todos os serviços de escrita), via `enforce_common`:

- `allowedRegions`: lista de regiões permitidas (ex.: `["sa-east-1"]`).
- `allowedEnvironments` / `deniedEnvironments`: allow/deny de ambiente.

### Serviços de ação genéricos — política por ambiente + exceções

Os 8 microserviços de ação — `create`, `modify`, `destroy`, `start-stop`,
`restore`, `replicate`, `describe` e `data` — usam o **mesmo schema genérico**
(o mesmo do `dynamodb`), avaliado por `app/policy.py` na ordem: região →
exceção → allow/deny de operação/categoria/tipo de recurso por ambiente → GMUD.
Cada serviço opera sobre uma **única categoria**:

| Serviço      | Categoria (`op.category`) |
|--------------|---------------------------|
| `create`     | `provision` |
| `modify`     | `config` |
| `destroy`    | `delete` |
| `start-stop` | `power` |
| `restore`    | `backup` |
| `replicate`  | `replicate` |
| `describe`   | `read` |
| `data`       | `data` |

Schema efetivo:

- `allowedRegions`: `[str]` — allowlist global de regiões (lista vazia/ausente = qualquer).
- `environments.<dev|homol|staging|prod>`:
  - `allowedOperations` / `deniedOperations`: `[str]` — o `policy.py` casa por
    **nome curto** (`op.name`, ex.: `"DeleteDBCluster"` — vale para qualquer client)
    **ou** pela **chave** `"<client>:<Op>"` (ex.: `"rds:AddTagsToResource"` —
    mira só aquele client). Use a chave quando o mesmo nome existir em mais de um
    client (ex.: `AddTagsToResource` em `rds` e `elasticache`) e você quiser
    distinguir; use o nome curto para valer em todos.
  - `allowedCategories` / `deniedCategories`: `[str]` — a categoria da operação
    (`provision|config|delete|power|backup|replicate|read|data`).
  - `allowedResourceTypes` / `deniedResourceTypes`: `[str]` — ex.: `db-instance`,
    `db-cluster`, `table`, `cache-cluster`, `global-cluster`.
  - `requireGmudForMutations`: `bool` — quando `true`, operações mutáveis exigem
    GMUD. **Default (chave ausente): apenas `prod`.**
  - `gmudForCategories`: `[str]` (opcional) — se presente, sobrepõe
    `requireGmudForMutations` e exige GMUD só para as categorias listadas.
- `exceptions[]`: liberam ações **bloqueadas** casando `account` + `environment`
  (`null` = qualquer) (+ `resource` opcional). Campos: `allowOperations`,
  `allowCategories` (`"*"` casa tudo), `reason`, `expiresAt` (`YYYY-MM-DD`). Uma
  exceção casada autoriza a operação e **dispensa a GMUD**.

Semântica opt-in: `allowlist` vazia/ausente = **sem restrição**; `deny` só
bloqueia o que estiver listado. `describe`/`data` são baixo risco — os exemplos
trazem só `allowedRegions` + `environments` vazio.

> **Legado:** as chaves planas por serviço (`allowedInstanceClasses`,
> `allowedEngines`, `allowedResourceTypes` no topo, `requireFinalSnapshot`,
> `allowedInstanceClassesByEnv`, `maxBackupRetentionDays`, etc.) do modelo
> anterior **não são mais lidas** por estes serviços e foram substituídas pelo
> schema genérico acima. Os arquivos `create/modify/destroy/start-stop/restore/
> replicate.json` foram reescritos; `describe.json` e `data.json` são novos.

Demais serviços mantêm suas chaves próprias:

| Serviço       | Chaves de regra |
|---------------|-----------------|
| `storage`     | `allowedResourceTypes`, `allowedStorageTypes`, `maxAllocatedStorage`, `maxIops` |
| `vpc-link`    | `allowedConsumerAccounts` |
| `kms`         | `allowedTargetResourceTypes` |
| `db-password` | `allowedEngines`, `deniedUsernames` |
| `dynamodb`    | schema genérico por ambiente (`environments.<env>`) + `exceptions` — ver `dynamodb.json` |

> `rds-data` mantém suas próprias regras de SQL (loader S3 existente). `servicenow`
> (gate de GMUD) e `finops` (read-only) não fazem enforcement de escrita.

## Exemplos

- [`finops.json`](./finops.json) — thresholds de ociosidade, tabela de preços
  (sa-east-1) e mapa de downgrade de instância usados pela varredura de desperdício.
- `create.json`, `modify.json`, `destroy.json`, `start-stop.json`, `restore.json`,
  `replicate.json`, `describe.json`, `data.json` — **exemplos do schema genérico**
  (região `sa-east-1`; `dev` permissivo; `homol`/`staging` sem GMUD mas com alguns
  `deniedOperations` para as operações mais destrutivas; `prod` com
  `requireGmudForMutations: true` + deny de exemplo). `destroy.json` inclui um
  exemplo de `exceptions[]`. Listas vazias/ausentes = sem restrição.
- `storage.json`, `vpc-link.json`, `kms.json`, `db-password.json`, `dynamodb.json`
  — schemas dos demais serviços prontos para revisão e publicação.
