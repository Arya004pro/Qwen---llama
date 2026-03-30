"""Pre-execution SQL validator for safety and reliability."""

from __future__ import annotations

import re
from typing import Iterable, Any

from db.duckdb_connection import get_read_connection, pg_to_duck

_SQL_START_RE = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|CALL|DO|ATTACH|DETACH)\b",
    re.IGNORECASE,
)
_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\")(?:\s+(?:AS\s+)?[A-Za-z_][A-Za-z0-9_]*)?",
    re.IGNORECASE,
)
_CTE_NAME_RE = re.compile(
    r"(?:\bWITH\b|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(",
    re.IGNORECASE,
)


def _extract_tables(sql: str) -> set[str]:
    tables: set[str] = set()
    for m in _TABLE_REF_RE.finditer(sql):
        raw = m.group(1).strip()
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1]
        tables.add(raw.lower())
    return tables


def _extract_cte_names(sql: str) -> set[str]:
    names: set[str] = set()
    for m in _CTE_NAME_RE.finditer(sql or ""):
        names.add(m.group(1).lower())
    return names


def validate_query(sql: str, params: Iterable[Any] | None = None) -> tuple[bool, list[str], list[str]]:
    """
    Validate SQL before execution.

    Returns: (is_valid, errors, warnings)
    """
    errors: list[str] = []
    warnings: list[str] = []
    raw = (sql or "").strip()

    if not raw:
        return False, ["Empty SQL query."], warnings
    if not _SQL_START_RE.search(raw):
        errors.append("Only SELECT/WITH queries are allowed.")
    if _FORBIDDEN_RE.search(raw):
        errors.append("Query contains forbidden keyword (potentially dangerous statement).")
    if ";" in raw:
        errors.append("Semicolons are not allowed (single statement only).")

    conn = get_read_connection()
    try:
        existing = {
            r[0].lower()
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }

        referenced = _extract_tables(raw)
        cte_names = _extract_cte_names(raw)
        missing = sorted(t for t in referenced if t not in existing and t not in cte_names)
        if missing:
            errors.append(f"Referenced table(s) not found: {', '.join(missing)}")

        # Validate syntax + table/column references with DuckDB parser/binder.
        try:
            q = pg_to_duck(raw)
            p = list(params or [])
            conn.execute(f"EXPLAIN {q}", p).fetchall()
        except Exception as exc:
            # Keep only first line for cleaner API errors.
            first_line = str(exc).split("\n")[0].strip()
            errors.append(f"Schema validation failed: {first_line}")
    finally:
        conn.close()

    return len(errors) == 0, errors, warnings

