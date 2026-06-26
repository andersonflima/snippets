# BFF — Backend for Frontend (console de ações)

Única fronteira entre o frontend Angular e os microserviços. O frontend fala
**somente** com o BFF; o BFF cuida de **login/sessão** e repassa as ações aos
microserviços via **API Gateway**.

## Arquitetura

```
Angular ──(cookie httpOnly)──► BFF (FastAPI) ──(token M2M Cognito)──► API Gateway ──► microserviços
```

- **Login/JWT próprio:** o BFF valida credenciais (bcrypt), emite um JWT HS256 e
  o guarda num **cookie httpOnly + Secure + SameSite** (`bff_session`). O browser
  nunca enxerga o token (sem Bearer no SPA, sem token em localStorage).
- **BFF → microserviços:** credencial de serviço **M2M** (OAuth2
  `client_credentials`) contra o IdP do gateway (Cognito). A identidade do usuário
  viaja como contexto nos headers `X-Actor` / `X-Actor-Roles`.
- **Envelope de erro** idêntico ao dos microserviços: `{ code, message, requestId }`.

## Endpoints

| Método | Rota                    | Descrição                                         |
| ------ | ----------------------- | ------------------------------------------------- |
| POST   | `/auth/login`           | `{username,password}` → seta cookie de sessão     |
| POST   | `/auth/logout`          | limpa o cookie (204)                              |
| GET    | `/auth/me`              | usuário da sessão (401 se não autenticado)        |
| POST   | `/api/{service}/execute`| proxy autenticado da ação para o microserviço     |
| GET    | `/healthz` `/readyz`    | health/readiness                                  |

Swagger UI em `/docs`, ReDoc em `/redoc`, contrato em `/openapi.json`.

## Configuração (env)

| Variável                | Obrigatória | Descrição                                              |
| ----------------------- | ----------- | ------------------------------------------------------ |
| `BFF_JWT_SECRET`        | sim         | segredo HS256 do JWT de sessão                         |
| `BFF_JWT_TTL_SECONDS`   | não (1800)  | validade da sessão                                     |
| `BFF_USERS`             | não (`[]`)  | JSON: `[{"username","passwordHash","roles":[...]}]`    |
| `BFF_COOKIE_SECURE`     | não (true)  | `false` apenas em dev http                             |
| `BFF_COOKIE_SAMESITE`   | não (strict)| `strict` \| `lax` \| `none`                            |
| `BFF_CORS_ORIGINS`      | não         | origens do frontend (CSV), p/ cookie cross-site        |
| `API_GATEWAY_BASE_URL`  | sim (proxy) | base do API Gateway dos microserviços                  |
| `BFF_M2M_TOKEN_URL`     | sim (proxy) | token endpoint do Cognito (`client_credentials`)       |
| `BFF_M2M_CLIENT_ID`     | sim (proxy) | app-client M2M                                         |
| `BFF_M2M_CLIENT_SECRET` | sim (proxy) | secret do app-client M2M                               |
| `BFF_M2M_SCOPE`         | não         | scope do token M2M                                     |

### Gerar um hash de senha para `BFF_USERS`

```bash
python -c "from app.security.passwords import hash_password; print(hash_password('minha-senha'))"
```

## Local

```bash
pip install -r requirements.txt
export BFF_JWT_SECRET=dev-secret BFF_COOKIE_SECURE=false
export BFF_USERS='[{"username":"alice","passwordHash":"<bcrypt>","roles":["operator"]}]'
uvicorn app.main:app --reload --port 8081
```

## Testes

```bash
pip install -r requirements.txt pytest
pytest -q
```

## Container

```bash
docker build -t actions-bff .
docker run -p 8081:8081 --env-file .env actions-bff
```

## Premissas desta versão

- **Store de usuários** via `BFF_USERS` (secret/env) com senhas bcrypt; trocar por
  LDAP/AD ou banco é implementar `UserRepository` (`app/auth/users.py`).
- **Contexto do usuário** repassado por header (`X-Actor`); os microserviços não
  foram alterados para receber o ator no corpo.
