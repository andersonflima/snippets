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

Chaves por serviço:

| Serviço       | Chaves de regra |
|---------------|-----------------|
| `modify`      | `allowedResourceTypes`, `allowedInstanceClasses`, `allowedInstanceClassesByEnv`, `allowedEngineVersions`, `maxBackupRetentionDays` |
| `create`      | `allowedResourceTypes`, `allowedInstanceClasses`, `allowedEngines` |
| `destroy`     | `allowedResourceTypes`, `deniedResourceTypes`, `requireFinalSnapshot` |
| `storage`     | `allowedResourceTypes`, `allowedStorageTypes`, `maxAllocatedStorage`, `maxIops` |
| `start-stop`  | `allowedOperations`, `allowedResourceTypes` |
| `restore`     | `allowedOperations`, `allowedInstanceClasses` |
| `replicate`   | `allowedResourceTypes`, `allowedDestinationAccounts`, `allowedDestinationRegions` |
| `vpc-link`    | `allowedConsumerAccounts` |
| `kms`         | `allowedTargetResourceTypes` |
| `db-password` | `allowedEngines`, `deniedUsernames` |

> `rds-data` mantém suas próprias regras de SQL (loader S3 existente). `servicenow`
> (gate de GMUD) e `finops` (read-only) não fazem enforcement de escrita.

## Exemplos

- [`finops.json`](./finops.json) — thresholds de ociosidade, tabela de preços
  (sa-east-1) e mapa de downgrade de instância usados pela varredura de desperdício.
- `modify.json`, `create.json`, `destroy.json`, `storage.json`, `start-stop.json`,
  `restore.json`, `replicate.json`, `vpc-link.json`, `kms.json`, `db-password.json`
  — **schemas de exemplo** (região `sa-east-1`, classes/engines comuns, caps por
  ambiente) prontos para revisão e publicação. Listas vazias = sem restrição.
