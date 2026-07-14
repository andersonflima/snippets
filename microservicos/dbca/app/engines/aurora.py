"""Executor Aurora: roda a query de metadados via RDS Data API (sem rota de VPC).

Resolve o secret do banco pela tag `dbca:secretArn` no cluster (descoberta) ou,
como fallback, pelo mapa admin (secretMap no catálogo). Normaliza o resultado do
Data API em {columns, rows} para o frontend exibir direto.
"""
from __future__ import annotations

from ..aws import ActionError
from ..guard import ensure_read_only


def _field_value(field: dict):
    if field.get("isNull"):
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    if "blobValue" in field:
        return "<blob>"
    if "arrayValue" in field:
        return field["arrayValue"]
    return None


def _to_parameters(params: dict) -> list:
    out = []
    for name, value in params.items():
        if value is None:
            field = {"isNull": True}
        elif isinstance(value, bool):
            field = {"booleanValue": value}
        elif isinstance(value, int):
            field = {"longValue": value}
        elif isinstance(value, float):
            field = {"doubleValue": value}
        else:
            field = {"stringValue": str(value)}
        out.append({"name": name, "value": field})
    return out


def run(session, discovery, query: dict, params: dict | None, database: str | None, secret_map_arn: str | None) -> dict:
    impl = (query.get("engines") or {}).get(discovery.engine)
    if not impl or "sql" not in impl:
        raise ActionError(
            "validation_error", f"query '{query.get('id')}' não tem SQL para engine {discovery.engine}", 400
        )
    sql = impl["sql"]
    ensure_read_only(sql)

    secret_arn = discovery.secret_arn or secret_map_arn
    if not secret_arn:
        raise ActionError(
            "validation_error",
            "credencial do banco não resolvida: adicione a tag 'dbca:secretArn' no cluster ou o mapa admin (secretMap)",
            400,
        )

    kwargs = {
        "resourceArn": discovery.arn,
        "secretArn": secret_arn,
        "sql": sql,
        "includeResultMetadata": True,
    }
    db = database or impl.get("database")
    if db:
        kwargs["database"] = db
    if params:
        kwargs["parameters"] = _to_parameters(params)

    client = session.client("rds-data")
    result = client.execute_statement(**kwargs)

    columns = [c.get("label") or c.get("name") for c in result.get("columnMetadata", [])]
    rows = [[_field_value(f) for f in record] for record in result.get("records", [])]
    return {"columns": columns, "rows": rows, "rowCount": len(rows)}
