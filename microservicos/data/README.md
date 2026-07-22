# data

Data-plane DynamoDB (item/query/scan/batch/transact/PartiQL).

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /data/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 13 operações.
