"""Executor DynamoDB: extrai metadados via API (DescribeTable) — não é SQL.

Cada `op` da query vira uma tabela {columns, rows} de metadados, para o frontend
exibir do mesmo jeito que os resultados de SQL.
"""
from __future__ import annotations

from ..aws import ActionError


def run(session, discovery, query: dict, params: dict | None) -> dict:
    impl = (query.get("engines") or {}).get("dynamodb")
    if not impl or "op" not in impl:
        raise ActionError("validation_error", f"query '{query.get('id')}' não suporta DynamoDB", 400)
    op = impl["op"]

    ddb = session.client("dynamodb")
    table = ddb.describe_table(TableName=discovery.identifier)["Table"]

    if op == "overview":
        billing = (table.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED")
        rows = [
            ["Status", table.get("TableStatus")],
            ["Itens (aprox.)", table.get("ItemCount")],
            ["Tamanho (bytes, aprox.)", table.get("TableSizeBytes")],
            ["Cobrança", billing],
            ["Criada em", str(table.get("CreationDateTime"))],
            ["GSIs", len(table.get("GlobalSecondaryIndexes") or [])],
            ["LSIs", len(table.get("LocalSecondaryIndexes") or [])],
        ]
        return {"columns": ["Métrica", "Valor"], "rows": rows, "rowCount": len(rows)}

    if op == "table-metadata":
        key_roles = {k["AttributeName"]: k["KeyType"] for k in table.get("KeySchema", [])}
        rows = []
        for attr in table.get("AttributeDefinitions", []):
            name = attr["AttributeName"]
            rows.append([name, attr["AttributeType"], key_roles.get(name, "-")])
        return {"columns": ["Atributo", "Tipo", "Chave"], "rows": rows, "rowCount": len(rows)}

    if op == "capacity":
        billing = (table.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED")
        pt = table.get("ProvisionedThroughput") or {}
        rows = [["Tabela", billing, pt.get("ReadCapacityUnits", "-"), pt.get("WriteCapacityUnits", "-")]]
        for gsi in table.get("GlobalSecondaryIndexes") or []:
            gpt = gsi.get("ProvisionedThroughput") or {}
            rows.append(
                [f"GSI {gsi.get('IndexName')}", billing, gpt.get("ReadCapacityUnits", "-"), gpt.get("WriteCapacityUnits", "-")]
            )
        return {"columns": ["Alvo", "Cobrança", "RCU", "WCU"], "rows": rows, "rowCount": len(rows)}

    raise ActionError("validation_error", f"operação DynamoDB desconhecida: {op}", 400)
