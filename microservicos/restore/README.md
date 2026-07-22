# restore

Restore/copy/backtrack/export/import de recursos.

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /restore/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 20 operações.
