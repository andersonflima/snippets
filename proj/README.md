# AWS Resource Control Platform (Monorepo)

Monorepo com:

- `apps/frontend`: Angular standalone para operacao da plataforma.
- `apps/backend`: API Fastify em TypeScript funcional.
- `packages/shared`: tipos compartilhados frontend/backend.

## Objetivo

Plataforma para operar recursos AWS com:

- autenticacao JWT,
- selecao dinamica de conta e regiao,
- `assumeRole` por conta escolhida,
- check-up automatico ao trocar contexto,
- descoberta automatica multi-regiao por tipo de recurso selecionado,
- CRUD generico de recursos AWS via Cloud Control API,
- ACL granular por conta + categoria + tipo de recurso + acao,
- delete com segunda confirmacao obrigatoria.

## Arquitetura

### Backend (funcional)

Fluxo principal:

1. Login gera JWT.
2. Usuario escolhe `accountId + region + category`.
3. Backend valida ACL e escopo de conta/regiao.
4. Backend executa `AssumeRole` na conta selecionada.
5. Operacoes CRUD usam Cloud Control API com credenciais temporarias.
6. Na inicializacao, o backend cria schema/tabelas e aplica seed base de usuarios/contas.

Camadas:

- `domain`: regras de ACL, categorias, erros de dominio.
- `application`: casos de uso (`auth`, `context`, `resources`).
- `infra`: repositorios PostgreSQL, bootstrap de schema/seed, seguranca, integracao AWS.
- `http`: rotas e validacao de payload.

### ACL

Perfis:

- `admin`: acesso total.
- `operator`: CRUD em compute/storage/database/network e acesso sem delete em security/management.
- `viewer`: somente leitura.

Modelo granular:

- Permissao avaliada por `user_id + account_id + category + resource_type + action`.
- Wildcards com `*` em `account_id`, `category` e `resource_type`.
- `admin` mantem bypass total para operacao de recursos.

### Delete com segunda confirmacao

1. Cliente chama `POST /api/resources/delete-intent`.
2. Backend gera `intentId` com TTL curto.
3. Cliente chama `DELETE /api/resources` com `intentId`.
4. Backend valida correspondencia estrita de usuario, conta, regiao, categoria e recurso.
5. Para delete de usuario, cliente chama `POST /api/admin/users/:userId/delete-intent`.
6. Depois chama `DELETE /api/admin/users/:userId` com `intentId`.
7. Backend valida correspondencia estrita entre ator e usuario alvo.

## Frontend

Tela unica com:

- login,
- sidebar por categoria,
- seletores de conta/regiao/tipo,
- painel de check-up por tipo,
- tabela de recursos,
- formularios de create/update,
- fluxo de delete com dupla confirmacao.

## Catalogo MVP (20 recursos)

- `compute`: `AWS::EC2::Instance`, `AWS::Lambda::Function`, `AWS::ECS::Cluster`, `AWS::ECS::Service`
- `storage`: `AWS::S3::Bucket`, `AWS::EFS::FileSystem`, `AWS::FSx::FileSystem`
- `database`: `AWS::RDS::DBInstance`, `AWS::RDS::DBCluster`, `AWS::DynamoDB::Table`
- `network`: `AWS::EC2::VPC`, `AWS::EC2::Subnet`, `AWS::EC2::SecurityGroup`, `AWS::ElasticLoadBalancingV2::LoadBalancer`
- `security`: `AWS::IAM::Role`, `AWS::KMS::Key`, `AWS::SecretsManager::Secret`
- `management`: `AWS::CloudFormation::Stack`, `AWS::CloudWatch::Alarm`, `AWS::Events::Rule`

## Credenciais seed

Senha para todos os usuarios: `change-me-please`

- `admin@platform.local`
- `operator@platform.local`
- `viewer@platform.local`

## Configuracao

1. Copie variaveis de ambiente:

```bash
cp apps/backend/.env.example apps/backend/.env
```

2. Defina no minimo:

- `JWT_SECRET`
- `DATABASE_URL`
- `AWS_ASSUME_ROLE_ARN_TEMPLATE` (opcional, ex.: `arn:aws:iam::{account_id}:role/PlatformAssumeRole`)
- `AWS_ENDPOINT_URL` quando apontar para LocalStack (ex.: `http://localhost:4566`)
- `AWS_USE_ACCOUNT_ID_FOR_LOCALSTACK=true` para emular multi-account via `AssumeRole` no LocalStack
- credenciais AWS (opcional, variaveis padrao AWS; se nao informar, usa cadeia padrao do SDK)
- `AWS_TLS_INSECURE=true` apenas para troubleshooting local de certificado TLS
- opcional `AWS_EXTERNAL_ID`

3. Suba a stack completa com Docker Compose:

```bash
docker compose up --build -d
```

4. Configure as roles alvo nas contas AWS para permitir `AssumeRole`.

### LocalStack para AssumeRole

Para usar o `localstack-main` Pro ja rodando na sua maquina com `AssumeRole` e `Organizations`:

1. Provisione a organization, OUs, contas membro e a role `PlatformAssumeRole`:

```bash
bash scripts/localstack/provision-pro-organization.sh
```

2. Configure `apps/backend/.env` com:

```bash
AWS_ASSUME_ROLE_ARN_TEMPLATE=arn:aws:iam::{account_id}:role/PlatformAssumeRole
AWS_ENDPOINT_URL=http://localhost:4566
AWS_USE_ACCOUNT_ID_FOR_LOCALSTACK=true
```

3. Rode o backend/frontend normalmente com `npm run dev`.

O script gera `apps/backend/.localstack-organization-accounts.json`. Quando esse arquivo existe, o backend passa a seedar as contas da aplicacao com os IDs reais criados na organization do LocalStack Pro.

Se voce quiser subir um LocalStack isolado pelo compose do projeto, o servico `localstack` continua disponivel no profile `embedded-localstack`:

```bash
docker compose --profile embedded-localstack up -d localstack
```

No compose do backend, use `AWS_ENDPOINT_URL=http://host.docker.internal:4566` para falar com o `localstack-main` externo.

O provisionamento do `localstack-main` cria 5 contas membro na organization, distribuidas nas OUs `Workloads`, `Data` e `Platform`, e configura `PlatformAssumeRole` em cada uma delas.

Limite conhecido: a documentacao oficial do LocalStack informa que setups multi-account podem nao funcionar bem em cenarios de acesso cross-account e cross-service. Por isso o backend passa a usar o `accountId` selecionado como namespace de credenciais de origem quando `AWS_USE_ACCOUNT_ID_FOR_LOCALSTACK=true`, mantendo o fluxo de `AssumeRole` funcional no ambiente local.

Observacao de data: a documentacao do LocalStack indica que, a partir de 23 de marco de 2026, a imagem unificada passa a exigir autenticacao. O `docker-compose.yml` foi fixado em `localstack/localstack:4.12.0` para manter o setup local previsivel sem depender de token. Se voce quiser usar uma imagem mais nova, ajuste `LOCALSTACK_IMAGE` e informe `LOCALSTACK_AUTH_TOKEN`.

## Rodando local

```bash
npm install
npm run dev
```

Backend: `http://localhost:3000`  
Frontend: `http://localhost:4200`

## Rodando com Docker Compose

```bash
docker compose up --build -d
```

Backend: `http://localhost:3000`  
Frontend: `http://localhost:4200`

## Scripts

```bash
npm run typecheck
npm run build
npm run dev
```

## Endpoints principais

- `POST /api/auth/login`
- `POST /api/auth/register`
- `GET /api/auth/me`
- `GET /api/context/current`
- `POST /api/context/switch`
- `GET /api/resources/types`
- `GET /api/resources`
- `GET /api/resources/discovery`
- `GET /api/resources/details`
- `POST /api/resources`
- `PUT /api/resources`
- `POST /api/resources/delete-intent`
- `DELETE /api/resources`

## Endpoints de administracao (ACL + usuarios)

- `GET /api/admin/users`
- `POST /api/admin/users`
- `PATCH /api/admin/users/:userId`
- `PUT /api/admin/users/:userId/accounts`
- `GET /api/admin/users/:userId/permissions`
- `PUT /api/admin/users/:userId/permissions`
- `POST /api/admin/users/:userId/permissions/reset`
- `POST /api/admin/users/:userId/delete-intent`
- `DELETE /api/admin/users/:userId`
