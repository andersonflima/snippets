"""Guarda de leitura: garante que o SQL configurado é somente-leitura.

Defense-in-depth: mesmo as queries sendo definidas por admin, o dbca só executa
SELECT/WITH/SHOW/EXPLAIN, uma instrução, sem verbos de escrita/DDL.
"""
from __future__ import annotations

import re

from .aws import ActionError

_WRITE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE|"
    r"REPLACE|CALL|DO|COPY|VACUUM|REINDEX|LOCK|SET|RESET)\b",
    re.IGNORECASE,
)
_READ_START = re.compile(r"^\s*(SELECT|WITH|SHOW|EXPLAIN)\b", re.IGNORECASE)


def ensure_read_only(sql: str) -> None:
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise ActionError("sql_forbidden", "sql vazio", 403)
    if ";" in stripped:
        raise ActionError("sql_forbidden", "múltiplas instruções não permitidas", 403)
    if not _READ_START.match(stripped):
        raise ActionError("sql_forbidden", "apenas leitura: query deve começar com SELECT/WITH/SHOW/EXPLAIN", 403)
    if _WRITE.search(stripped):
        raise ActionError("sql_forbidden", "apenas queries de leitura são permitidas (verbo de escrita/DDL detectado)", 403)
