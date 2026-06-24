# db-password — microserviço action-driven

Conecta no banco e troca a senha do usuário informado.

Autocontido (sem packages compartilhadas). Roda em EKS atrás do NLB interno;
o acesso externo é pelo API Gateway (Cognito JWT) -> VPC Link -> NLB -> este pod.

## API

`POST /db-password/execute` — executa a ação. Health: `GET /healthz`, `GET /readyz`.

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

## Local

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

## Container

```bash
docker build -t db-password .
docker run -p 8080:8080 db-password
```
