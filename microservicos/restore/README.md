# restore — microserviço action-driven

Snapshot/restore de **instância RDS** e de **cluster Aurora**.

Autocontido (sem packages compartilhadas). Roda em EKS atrás do NLB interno;
o acesso externo é pelo API Gateway (Cognito JWT) -> VPC Link -> NLB -> este pod.

## Operações (`params.operation`)

| operação | recurso | efeito |
| --- | --- | --- |
| `create-snapshot` | RDS instance | snapshot da instância `resource` |
| `restore-snapshot` | RDS instance | cria instância nova (`targetInstanceIdentifier`) a partir do snapshot |
| `create-cluster-snapshot` | Aurora cluster | snapshot do cluster `resource` |
| `restore-cluster-snapshot` | Aurora cluster | cria cluster novo (`targetClusterIdentifier`) a partir do snapshot e seus membros de compute |

O restore restaura os dados no estado do snapshot (cópia point-in-time), não é
réplica contínua. Aurora nasce sem instâncias de compute: o
`restore-cluster-snapshot` cria `clusterInstanceCount` membros (default 1) da
classe `dbInstanceClass`.

## API

`POST /restore/execute` — executa a ação. Health: `GET /healthz`, `GET /readyz`.

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
docker build -t restore .
docker run -p 8080:8080 restore
```
