"""db/sql_builder.py

Fully dynamic SQL builder.  Reads live schema from DuckDB and constructs
parameterised SQL for any dataset — no hardcoded table/column names.

Public API
----------
  build_sql(parsed_intent) -> str | None
      Returns a parameterised SQL string or None when the schema does not
      contain the requested entity/metric/date columns.
"""

from __future__ import annotations

from db.duckdb_connection import get_read_connection


# ── Schema introspection helpers ──────────────────────────────────────────────

_TEXT_TYPES  = ("VARCHAR", "CHAR", "TEXT", "STRING")
_NUM_TYPES   = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
_DATE_HINTS  = ("date", "time", "created", "updated", "timestamp", "at")


def _is_text(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in _TEXT_TYPES)


def _is_numeric(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in _NUM_TYPES)


def _is_date_type(col_name: str, dtype: str) -> bool:
    d = dtype.upper()
    return ("DATE" in d or "TIMESTAMP" in d
            or any(k in col_name.lower() for k in _DATE_HINTS))


def _load_schema() -> dict[str, list[tuple[str, str]]]:
    """Return {table: [(col, dtype), ...]} for all user tables."""
    conn = get_read_connection()
    try:
        tables = [
            r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_name NOT LIKE '_raw_%' "
                "ORDER BY table_name"
            ).fetchall()
        ]
        schema: dict[str, list[tuple[str, str]]] = {}
        for t in tables:
            cols = [(c[0], c[1]) for c in conn.execute(f'DESCRIBE "{t}"').fetchall()]
            schema[t] = cols
        return schema
    finally:
        conn.close()


def _find_date_col(cols: list[tuple[str, str]]) -> str | None:
    """Pick the best date/time column from a column list."""
    candidates = [c for c, d in cols if _is_date_type(c, d)]
    if not candidates:
        return None
    # Prefer columns with 'date' in the name, then 'created', then first
    for pref in ("date", "created", "time"):
        for c in candidates:
            if pref in c.lower():
                return c
    return candidates[0]


def _find_metric_col(cols: list[tuple[str, str]], metric: str) -> str | None:
    """Find the best numeric column matching the requested metric."""
    col_names = {c.lower() for c, _ in cols}

    # Exact match
    if metric.lower() in col_names:
        return metric.lower()

    # Keyword-based search for revenue/sales/earnings → monetary columns
    _MONETARY_KEYWORDS = ("final", "total", "amount", "revenue", "sales",
                          "earning", "fare", "price", "cost", "payment", "profit")
    _QUANTITY_KEYWORDS = ("quantity", "qty", "units", "volume", "count")
    
    numeric_cols = [c for c, d in cols if _is_numeric(d) and not c.lower().endswith("_id")]

    if metric.lower() in ("revenue", "sales", "earnings", "income", "money", "amount"):
        for pref in _MONETARY_KEYWORDS:
            for c in numeric_cols:
                if pref in c.lower():
                    return c
    elif metric.lower() in ("quantity", "units", "items", "pieces", "volume"):
        for kw in _QUANTITY_KEYWORDS:
            for c in numeric_cols:
                if kw in c.lower():
                    return c

    # Fuzzy: metric keyword as substring in any numeric column
    for c in numeric_cols:
        if metric.lower().replace("_", "") in c.lower().replace("_", ""):
            return c

    # Default: first monetary-like numeric column, else first numeric
    for c in numeric_cols:
        if any(k in c.lower() for k in _MONETARY_KEYWORDS):
            return c
    return numeric_cols[0] if numeric_cols else None


def _find_entity_col(cols: list[tuple[str, str]], entity: str) -> str | None:
    """Find the best text column matching the requested entity."""
    col_names = {c.lower() for c, _ in cols}

    # Exact match
    if entity.lower() in col_names:
        return entity.lower()

    # Try entity_name pattern
    name_col = f"{entity.lower()}_name"
    if name_col in col_names:
        return name_col

    text_cols = [c for c, d in cols if _is_text(d)
                 and not c.lower().endswith("_id")
                 and not _is_date_type(c, d)]

    # Fuzzy substring match
    for c in text_cols:
        if entity.lower().replace("_", "") in c.lower().replace("_", ""):
            return c

    return None


def _find_best_table(
    schema: dict[str, list[tuple[str, str]]],
    entity_col: str | None,
    metric_col: str | None,
    need_date: bool = True,
) -> tuple[str, str | None, str | None, str | None] | None:
    """
    Find the single table (or closest match) that contains the needed columns.
    Returns (table, entity_col_resolved, metric_col_resolved, date_col) or None.
    """
    best = None
    best_score = -1

    for table, cols in schema.items():
        col_names = {c.lower() for c, _ in cols}
        score = 0

        # Entity presence
        e_found = None
        if entity_col:
            if entity_col.lower() in col_names:
                e_found = entity_col.lower()
                score += 5
            else:
                # Try to find it in this table
                e_found = _find_entity_col(cols, entity_col)
                if e_found:
                    score += 3

        # Metric presence
        m_found = None
        if metric_col:
            if metric_col.lower() in col_names:
                m_found = metric_col.lower()
                score += 5
            else:
                m_found = _find_metric_col(cols, metric_col)
                if m_found:
                    score += 3

        # Date presence
        d_found = _find_date_col(cols)
        if d_found:
            score += 2

        if need_date and not d_found:
            continue  # Skip tables without date columns

        if score > best_score:
            best_score = score
            best = (table, e_found, m_found, d_found)

    return best


def _sql_literal(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    sval = str(value).replace("'", "''")
    return f"'{sval}'"


def _build_filter_clause(filters: dict, table_cols: set[str], alias: str | None = None) -> str:
    if not filters:
        return ""
    parts: list[str] = []
    prefix = f'{alias}.' if alias else ""
    for key, val in filters.items():
        k = str(key).lower()
        if k not in table_cols:
            continue
        parts.append(f'AND {prefix}"{k}" = {_sql_literal(val)}')
    return "\n" + "\n".join(parts) if parts else ""


# ── Public builder ────────────────────────────────────────────────────────────

def build_sql(parsed: dict) -> str | None:
    """
    Build parameterised SQL from parsed intent using live schema introspection.

    Returns a SQL string with ? placeholders, or None if the schema
    cannot satisfy the request (caller should fall through to LLM).
    """
    entity_raw = parsed.get("entity")
    metric_raw = parsed.get("metric", "revenue")
    qt         = parsed.get("query_type", "top_n")
    filters    = parsed.get("filters", {})

    # Complex query types are better handled by LLM
    if qt in ("comparison", "growth_ranking", "intersection",
              "threshold", "time_series"):
        return None

    try:
        schema = _load_schema()
    except Exception:
        return None

    if not schema:
        return None

    # Handle count metric
    is_count = metric_raw and metric_raw.lower() == "count"
    is_avg   = metric_raw and metric_raw.lower().startswith("avg_")

    # Find columns across all tables
    metric_col = None
    if not is_count:
        actual_metric = metric_raw[4:] if is_avg else metric_raw
        for table, cols in schema.items():
            metric_col = _find_metric_col(cols, actual_metric)
            if metric_col:
                break

    entity_col = None
    if entity_raw and qt not in ("aggregate",):
        for table, cols in schema.items():
            entity_col = _find_entity_col(cols, entity_raw)
            if entity_col:
                break

    # If we need entity but can't find it, bail to LLM
    if entity_raw and qt in ("top_n", "bottom_n", "zero_filter") and not entity_col:
        return None

    # If we need metric but can't find it (and it's not count), bail
    if not is_count and not metric_col:
        return None

    # Find the best table containing these columns
    result = _find_best_table(schema, entity_col, metric_col, need_date=True)
    if not result:
        return None

    table, e_col, m_col, d_col = result

    if not d_col:
        return None

    table_cols = {c.lower() for c, _ in schema.get(table, [])}
    filter_clause = _build_filter_clause(filters, table_cols)

    # Build aggregation expression
    if is_count:
        # Find best ID column for COUNT(DISTINCT ...)
        table_cols = schema.get(table, [])
        id_col = None
        for c, d in table_cols:
            if c.lower().endswith("_id") and _is_numeric(d):
                id_col = c
                break
        if not id_col:
            for c, d in table_cols:
                if c.lower().endswith("_id"):
                    id_col = c
                    break
        agg_expr = f'COUNT(DISTINCT "{id_col}")' if id_col else "COUNT(*)"
    elif is_avg:
        agg_expr = f'AVG("{m_col}")'
    else:
        agg_expr = f'SUM("{m_col}")'

    # Build date filter
    date_filter = f'CAST("{d_col}" AS DATE) >= ? AND CAST("{d_col}" AS DATE) < ?{filter_clause}'

    ascending = qt == "bottom_n"
    direction = "ASC" if ascending else "DESC"

    if qt == "aggregate":
        return (
            f'SELECT {agg_expr} AS value\n'
            f'FROM "{table}"\n'
            f'WHERE {date_filter}'
        )

    if qt == "zero_filter":
        sub_filter_clause = _build_filter_clause(filters, table_cols, alias="t2")
        sub_date_filter = (
            f't2."{d_col}" >= ? AND t2."{d_col}" < ?{sub_filter_clause}'
        )
        return (
            f'SELECT "{e_col}" AS name, 0 AS value\n'
            f'FROM "{table}"\n'
            f'WHERE NOT EXISTS (\n'
            f'  SELECT 1 FROM "{table}" t2\n'
            f'  WHERE t2."{e_col}" = "{table}"."{e_col}"\n'
            f'    AND {sub_date_filter}\n'
            f')\n'
            f'ORDER BY name'
        )

    if qt in ("top_n", "bottom_n"):
        if not e_col:
            return None
        return (
            f'SELECT "{e_col}" AS name,\n'
            f'       {agg_expr} AS value\n'
            f'FROM "{table}"\n'
            f'WHERE {date_filter}\n'
            f'GROUP BY "{e_col}"\n'
            f'ORDER BY value {direction}\n'
            f'LIMIT ?'
        )

    return None