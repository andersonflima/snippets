"""Catálogo de operações do serviço `data` (gerado de catalog.json).

NÃO editar à mão: rode `python gen_catalog.py && python gen_action_services.py`.
Cada operação mapeia name -> (client boto3, método, categoria, mutating,
resourceArg, resourceType). O handler despacha genericamente via getattr.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    key: str            # "<client>:<Name>" — chave global única
    name: str
    method: str
    client: str
    category: str
    mutating: bool
    resource_arg: str | None
    resource_type: str | None


_OPS: tuple[Operation, ...] = (
    Operation("dynamodb:BatchExecuteStatement", "BatchExecuteStatement", "batch_execute_statement", "dynamodb", "data", True, "Statements", None),
    Operation("dynamodb:BatchGetItem", "BatchGetItem", "batch_get_item", "dynamodb", "data", True, "RequestItems", None),
    Operation("dynamodb:BatchWriteItem", "BatchWriteItem", "batch_write_item", "dynamodb", "data", True, "RequestItems", None),
    Operation("dynamodb:DeleteItem", "DeleteItem", "delete_item", "dynamodb", "data", True, "TableName", "table"),
    Operation("dynamodb:ExecuteStatement", "ExecuteStatement", "execute_statement", "dynamodb", "data", True, "Statement", None),
    Operation("dynamodb:ExecuteTransaction", "ExecuteTransaction", "execute_transaction", "dynamodb", "data", True, "TransactStatements", None),
    Operation("dynamodb:GetItem", "GetItem", "get_item", "dynamodb", "data", True, "TableName", "table"),
    Operation("dynamodb:PutItem", "PutItem", "put_item", "dynamodb", "data", True, "TableName", "table"),
    Operation("dynamodb:Query", "Query", "query", "dynamodb", "data", True, "TableName", "table"),
    Operation("dynamodb:Scan", "Scan", "scan", "dynamodb", "data", True, "TableName", "table"),
    Operation("dynamodb:TransactGetItems", "TransactGetItems", "transact_get_items", "dynamodb", "data", True, "TransactItems", None),
    Operation("dynamodb:TransactWriteItems", "TransactWriteItems", "transact_write_items", "dynamodb", "data", True, "TransactItems", None),
    Operation("dynamodb:UpdateItem", "UpdateItem", "update_item", "dynamodb", "data", True, "TableName", "table"),
)

CATALOG: dict[str, Operation] = {op.key: op for op in _OPS}
CLIENTS: tuple[str, ...] = tuple(sorted({op.client for op in _OPS}))


def resolve(key: str) -> Operation | None:
    """Resolve por chave '<client>:<Name>'. Aceita também o nome cru quando não
    houver colisão entre clients (conveniência)."""
    op = CATALOG.get(key)
    if op is not None:
        return op
    matches = [o for o in _OPS if o.name == key]
    return matches[0] if len(matches) == 1 else None


def resource_of(op: Operation, args: dict) -> str | None:
    if op.resource_arg and isinstance(args.get(op.resource_arg), str):
        return args[op.resource_arg]
    return None
