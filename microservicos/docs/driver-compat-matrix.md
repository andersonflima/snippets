# Matriz de compatibilidade driver ↔ engine (upgrade de recurso)

Resumo legível de `driver-compat-matrix.json`. Objetivo: dado um **upgrade/migração de recurso** (banco RDS/Aurora ou cache ElastiCache), decidir se os **drivers de conexão** de Java/Go/Python/Node.js continuam compatíveis — e qual **versão mínima** é necessária.

Gerado em 2026-07-03. Fontes verificadas na web (release notes, docs AWS/vendor, PyPI/npm/Maven/pkg.go.dev). Ver `driver-compat-matrix.json` para o detalhe por linha de versão.

## Legenda de veredito

| Veredito | Significado |
|---|---|
| `safe` | Upgrade **não** exige troca de driver; clientes atuais continuam conectando. |
| `needs_bump` | Exige **versão mínima** de driver (ou config extra); drivers antigos quebram. |
| `breaks` | Migração **source-incompatível**: exige mudança de código, não só bump. |

## A regra que resume tudo

> Na quase totalidade dos casos o **wire protocol não quebra** em upgrade de major. O que quebra driver é **AUTENTICAÇÃO** e **EOL de SDK** — não o protocolo.

- **PostgreSQL** → `md5 → scram-sha-256` (default desde PG14; **latente** no RDS até resetar a senha do role).
- **MySQL/Aurora** → `mysql_native_password → caching_sha2_password` (default desde MySQL 8.0.4 / Aurora v3). **MariaDB não adota** → upgrade MariaDB é `safe`.
- **Oracle** → praticamente sem quebra por major (19c↔21c `safe`); o fator é runtime (JDK/Node/Python/Go) e Instant Client (thick).
- **Redis → Valkey** e **Redis 6.2 → 7.x** → **wire-compatíveis**: o cliente **não precisa mudar para conectar**. Driver novo só p/ features RESP3 ou cluster-aware.
- **DynamoDB** → sem versão de servidor; compat = SDK ainda suportado (**AWS SDK v1 já EOL em 2025** em Java/Go/JS → migrar = `breaks`).
- **DocumentDB** → risco é driver Mongo novo **subir o server mínimo** acima da API emulada (3.6/4.0).

---

## PostgreSQL (RDS + Aurora) — majors 11–17

Breaker real: **md5 → scram-sha-256**. Qualquer driver com SCRAM sobrevive. Para `psycopg2` o que importa é **libpq ≥ 10**, não a versão do pacote.

| Linguagem | Driver | Versão mínima p/ scram | Atual recomendado |
|---|---|---|---|
| Java | pgjdbc | **42.2.1+** (artefatos Java 6/7 NÃO fazem SCRAM) | 42.7.x |
| Go | pgx / lib/pq | pgx v4+ / lib/pq ≥1.1 | pgx v5 |
| Python | psycopg2 / psycopg3 / asyncpg | psycopg2 2.8+ **com libpq≥10** | psycopg (v3) 3.3.x |
| Node | pg / postgres.js | **pg 7.9.0+** (pg 6.x e <7.9 quebram) | pg 8.x |

**Cenários:** 12→16 `needs_bump` · 13→16 `needs_bump` · **15→17 `safe`** (ambos já usam scram) · md5→scram `needs_bump`.

## MySQL-family (RDS MySQL/MariaDB + Aurora MySQL)

Breaker real: **caching_sha2_password** (MySQL 8.0.4+ / Aurora v3). Sem TLS precisa RSA. **Não misture** MySQL Connector/J com servidor MariaDB e vice-versa.

| Linguagem | Driver | Mínimo p/ MySQL 8 | Observação |
|---|---|---|---|
| Java | Connector/J / MariaDB Connector/J | **CJ 8.0.x+** ou **MariaDB CJ 3.x** | MariaDB CJ **2.x NÃO conecta** em MySQL 8 |
| Go | go-sql-driver/mysql | **v1.4.0+** (preferir ≥1.7) | ≤1.3 quebra |
| Python | PyMySQL / mysql-connector / mysqlclient | PyMySQL ≥0.9.0 (+`cryptography`) | mysqlclient depende da client lib linkada |
| Node | mysql2 / mysql | **mysql2 3.x** | `mysqljs/mysql` (2.x) não suporta caching_sha2 |

**Cenários:** MySQL 5.7→8.0 `needs_bump` · Aurora v2→v3 `needs_bump` · **MariaDB 10.6→11.4 `safe`**.

## Oracle (RDS 19c/21c)

Sem quebra por major. Fator = runtime + Instant Client.

| Linguagem | Driver | Modo | Precisa Instant Client? |
|---|---|---|---|
| Java | ojdbc8 / ojdbc11 / ojdbc17 | thin JDBC | não |
| Go | godror / **go-ora** (alt.) | thick / **thin** | godror **sim**; go-ora **não** |
| Python | python-oracledb (ex cx_Oracle) | thin (default) | não |
| Node | node-oracledb 6.x+ | thin (desde 6.0) | não (5.x e anteriores sim) |

**Cenário:** 19c→21c `safe`.

## Redis + Valkey (ElastiCache) — o de maior valor

**Redis→Valkey e Redis 6.2→7.x conectam sem trocar cliente.** Só precisa driver novo para **RESP3** (client-side caching, tipos nativos).

| Linguagem | RESP2 (só conectar) | Primeiro com RESP3 | Cliente Valkey-nativo |
|---|---|---|---|
| Java | Jedis 3/4, Lettuce 5 | **Jedis 5.0** / **Lettuce 6.0** | valkey-glide-java 2.x |
| Go | go-redis v8 | **go-redis v9** (pinar ≥**v9.20.1**) / rueidis | valkey-go v1 |
| Python | redis-py 3/4 | **redis-py 5.0** (default no 8.x) | valkey-py 6.x / valkey-glide |
| Node | ioredis 4/5, node-redis v3/v4 | **node-redis v5** (ioredis é **RESP2-only** p/ sempre) | iovalkey / @valkey/valkey-glide |

**Cenários:** Redis 6.2→7.1 `safe` · **Redis 7.1→Valkey 8.0 `safe`** (zero mudança de código; caveats são só config: TLS/AUTH/RBAC/IAM e cluster-aware).

> Gotchas: **ioredis nunca terá RESP3** (usar node-redis se precisar); **go-redis v9 < v9.20.1** pode dropar pub/sub silenciosamente; Jedis 4 e node-redis v4 **não** têm RESP3 first-class (mito comum).

## DynamoDB (compat = lifecycle de SDK)

Sem versão de servidor. **AWS SDK v1 chegou ao EOL em 2025** — migrar para v2/v3 é reescrita (`breaks`).

| Linguagem | Atual | Legado (EOL 2025) |
|---|---|---|
| Java | SDK v2 (`software.amazon.awssdk`) | v1 (`com.amazonaws`) — EOL 2025-12-31 |
| Go | aws-sdk-go-v2 | v1 — EOL 2025-07-31 |
| Python | boto3 (linha única, sempre atual) | — |
| Node | `@aws-sdk/client-dynamodb` v3 (Node 20+) | `aws-sdk` v2 — EOL 2025-09-08 |

## DocumentDB (compat = driver Mongo vs API emulada 3.6/4.0/5.0)

Risco corre para **trás**: subir a API do cluster é `safe`; adotar **driver novo** pode exigir subir o cluster antes.

| Cenário | Veredito |
|---|---|
| DocumentDB 4.0 → 5.0 | `safe` (qualquer driver que rodava em 4.0 roda em 5.0) |
| Driver Mongo novo contra DocDB **3.6** | `breaks` (Java >5.1, PyMongo >4.10, Go sem suporte a 3.6) |
| Java ≥5.5 / PyMongo ≥4.14 contra DocDB **4.0** | `needs_bump` (dropam server 4.0 → subir cluster p/ 5.0 antes) |

Sempre: `retryWrites=false` + TLS com `global-bundle.pem`.
