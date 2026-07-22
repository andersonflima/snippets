# create

Provisiona recursos (create_*, purchase, register).

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /create/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 37 operações.
