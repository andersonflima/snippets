# replicate

Replicação/migração e share de snapshots cross-account.

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /replicate/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 5 operações.
