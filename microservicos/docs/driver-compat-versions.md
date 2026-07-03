# Driver compat — profundidade de versão (últimas ~20 por driver)

Resumo de `driver-compat-versions.json`: as **~20 últimas versões** de cada driver (Java/Go/Python/Node), com data de release real (Maven/PyPI/npm/GitHub/goproxy) e compatibilidade agrupada por `compat_group`. Serve para validar **quem está preso num driver antigo**: mostra a partir de qual versão o driver passa a funcionar num upgrade de recurso.

Gerado em 2026-07-03. Cobertura: **45 drivers**, **855 linhas de versão**. Detalhe completo no JSON.

## Como ler o corte

O que importa num driver antigo é o **ponto de virada**: a versão a partir da qual o driver deixa de quebrar no upgrade. Abaixo, por engine, o corte por linguagem.

## PostgreSQL — corte = suporte a SCRAM-SHA-256 (`md5 → scram`, default PG14)

| Driver | Quebra abaixo de | Observação |
|---|---|---|
| pgjdbc | **42.2.1** (build Java 8/JDBC 4.2) | artefatos Java 6/7 nunca fazem SCRAM |
| pgx | v4+ (todas as 20 listadas são v5, ok) | — |
| lib/pq | **v1.1.0** (SCRAM 2019); todas as 20 listadas ≥v1.7.1 ok | maintenance mode |
| psycopg2 | não é a versão — é **libpq ≥ 10** | wheels `-binary` 2.8+ embutem libpq 10+ |
| psycopg (v3) | qualquer 3.x | SCRAM nativo |
| asyncpg | qualquer (SCRAM desde 0.11) | sem channel binding |
| pg (node) | **7.9.0** | pg 6.x / <7.9 não autenticam em scram |
| postgres.js | qualquer 3.x | SASL/SCRAM nativo |

## MySQL/Aurora — corte = `caching_sha2_password` (default MySQL 8.0.4+/Aurora v3)

| Driver | Quebra abaixo de | Observação |
|---|---|---|
| MySQL Connector/J | **8.0.9** (evitar 5.1.x) | 9.x dropou MySQL 5.7 |
| MariaDB Connector/J | **3.x** (2.x não faz caching_sha2) | driver MariaDB-first |
| go-sql-driver/mysql | **v1.5.0** (≤v1.4.1 quebra) | pure-Go |
| PyMySQL | **0.9.0** (+`cryptography` sem TLS) | pre-0.9 quebra |
| mysqlclient | depende da **client lib C linkada** (libmysqlclient 8.0+) | não é a versão do pacote |
| mysql-connector-python | 8.0.x+ | 2.1/2.2 quebra |
| mysql2 (node) | qualquer 3.x | recomendado |
| mysql (mysqljs) | **nunca** faz caching_sha2 → migrar p/ mysql2 | unmaintained desde 2020 |

> **MariaDB server** não usa caching_sha2 → nenhum corte; upgrade MariaDB é `safe` em qualquer driver da família.

## Oracle — sem corte por major (19c↔21c `safe`); corte é operacional

| Driver | Ponto de atenção |
|---|---|
| ojdbc8/11/17 | todas conectam em 19c/21c; escolha por JDK (8 / 11 / 17) |
| python-oracledb | qualquer versão (thin, sem Instant Client). cx_Oracle era thick-only |
| node-oracledb | **6.0** introduziu thin (sem Instant Client); 5.x e abaixo = thick, exige Instant Client |
| godror (go) | sempre thick → **exige Oracle Instant Client** em runtime |
| go-ora (go) | thin puro-Go, sem Instant Client (alternativa ao godror) |

## Redis + Valkey — driver antigo **ainda conecta**; corte só para RESP3

Todas as ~20 versões listadas conectam em Redis 7.x **e Valkey 8.x** (wire-compat). O corte é só se você quer **RESP3**:

| Driver | RESP3 a partir de | Nota |
|---|---|---|
| Jedis | **5.0** (opt-in) | 3.x/4.x = RESP2 só |
| Lettuce | **6.0** | 5.x = RESP2 só |
| go-redis | **v9** — mas pinar **≥ v9.20.1** | <v9.20.1 dropa pub/sub RESP3 silenciosamente |
| rueidis / valkey-go | sempre RESP3 | não afetados pelo bug do go-redis |
| redis-py | **5.0** (opt-in); default no 8.x | — |
| node-redis | **v5** | v4 = RESP3 experimental/incompleto |
| ioredis | **nunca** (RESP2-only pra sempre) | usar node-redis se precisar RESP3 |
| valkey-glide / valkey-py / iovalkey | clients Valkey-nativos | glide = RESP3; iovalkey = RESP2 |

## DynamoDB — corte = lifecycle de SDK (não há versão de servidor)

Todas as linhas listadas são as **atuais** (boto3 1.43.x, JS v3, Go v2, Java v2). As antigas — **AWS SDK v1 (Java/Go) e aws-sdk v2 (JS)** — já estão **EOL (2025)**; migrar é `breaks` (reescrita, não bump).

## DocumentDB — corte = server mínimo do driver Mongo vs API emulada

| Driver | DocDB 3.6 | DocDB 4.0 | DocDB 5.0 |
|---|---|---|---|
| pymongo | até **4.10** | até **4.13** | qualquer 4.x |
| mongodb-driver-sync (java) | até **5.1.x** | até **5.4** | qualquer 5.x |
| mongo-go-driver | v1.x legado | v1.x legado | **v2.x** (floor server 4.2) |
| mongodb (node) | não-oficial | não-oficial | **6.x/7.x** (floor 4.2) |

> Contraintuitivo: para DocDB **antigo** (3.6/4.0) você precisa de driver Mongo **mais antigo**; subir o driver pode exigir subir o cluster antes. Sempre `retryWrites=false` + TLS (`global-bundle.pem`).
