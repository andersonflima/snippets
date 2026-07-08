# servicenow — microserviço action-driven

Integração com ServiceNow para GMUD: valida/registra/consulta a change.

Autocontido (sem packages compartilhadas). Roda em EKS atrás do NLB interno;
o acesso externo é pelo API Gateway (Cognito JWT) -> VPC Link -> NLB -> este pod.

## API

`POST /servicenow/execute` — executa a ação. Health: `GET /healthz`, `GET /readyz`.

Corpo (envelope + params), conforme `contract/openapi.yaml`:

```json
{
  "account": "123456789012",
  "resource": "<nome-ou-arn-do-recurso>",
  "roleArn": "arn:aws:iam::123456789012:role/<role-assumivel>",
  "region": "us-east-1",
  "dryRun": false,
  "params": { }
}
```

A ação roda na conta-alvo via `STS:AssumeRole` no `roleArn`.

## Operações (`params.operation`)

- `validate` — decide se a GMUD libera execução; retorna `detail.allowed`.
- `status` — retrato da GMUD para monitoramento (mesmas checagens, sem gatear).
- `register` — anexa uma work note à change (`params.workNote`).

## Validação da GMUD

Uma change só libera execução produtiva quando **todas** as checagens exigidas
passam. Cada uma é configurável por ambiente:

| Checagem | Regra | Env |
|---|---|---|
| Status de implementação | `state ∈` estados liberados | `SERVICENOW_ALLOWED_STATES` (`-1,implement`) |
| Janela planejada | `start_date ≤ agora ≤ end_date` | — (sempre exigida) |
| Aprovações | `approval ∈` valores aprovados | `SERVICENOW_REQUIRE_APPROVAL` (`true`), `SERVICENOW_APPROVED_STATES` (`approved`) |
| Conflitos | `conflict_status ∈` valores ok | `SERVICENOW_REQUIRE_NO_CONFLICT` (`true`), `SERVICENOW_CONFLICT_OK` (`no conflict`) |
| Tarefas registradas | `change_task ≥` mínimo | `SERVICENOW_REQUIRE_TASKS` (`true`), `SERVICENOW_MIN_TASKS` (`1`) |

Conexão com o ServiceNow: `SERVICENOW_INSTANCE_URL` + `SERVICENOW_TOKEN` (Bearer)
ou `SERVICENOW_USER`/`SERVICENOW_PASSWORD` (Basic). Tabelas ajustáveis por
`SERVICENOW_CHANGE_TABLE` (`change_request`) e `SERVICENOW_TASK_TABLE` (`change_task`).

O `detail` traz o resultado por checagem (`checks`), os motivos de bloqueio
(`reasons`) e o booleano `allowed` consumido pelo gate `ensure_change_authorized`.

## Local

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

## Container

```bash
docker build -t servicenow .
docker run -p 8080:8080 servicenow
```
