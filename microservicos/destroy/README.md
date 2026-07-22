# destroy

Remove recursos (delete_/deregister/cancel).

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /destroy/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 42 operações.
