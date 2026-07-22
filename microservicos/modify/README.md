# modify

Altera recursos (modify_/update_/tag/attach/enable/scaling/resource-policy).

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /modify/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 68 operações.
