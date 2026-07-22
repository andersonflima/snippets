# describe

Leitura read-only (describe_/list_/get_). Sem GMUD.

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /describe/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 91 operações.
