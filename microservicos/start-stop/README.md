# start-stop

Liga/desliga e failover (start_/stop_/reboot_/failover_).

Dispatcher genérico governado por regra externa (S3/DynamoDB). Contrato: `POST /start-stop/execute` com `params.operation` (`<client>:<Op>`) + `params.args` (kwargs boto3). Catálogo gerado de `catalog.json`. 20 operações.
