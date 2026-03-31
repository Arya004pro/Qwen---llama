"""db/sql_builder.py

Fully dynamic SQL builder.  Reads live schema from DuckDB and constructs
parameterised SQL for any dataset — no hardcoded table/column names.

Key fix: the builder now detects BIGINT epoch datetime columns and generates
the correct epoch_ms()/to_timestamp() cast instead of CAST(col AS DATE),
which would fail with "Unimplemented type for cast (BIGINT -> DATE)".

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
_BIGINT_TYPES = ("BIGINT", "INT8", "LONG", "HUGEINT", "INT64")


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


def _is_bigint_type(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in _BIGINT_TYPES)


def _detect_epoch_scale(conn, table: str, col: str) -> str:
    """
    Detect if a BIGINT column is millisecond or second epoch and return
    the appropriate DuckDB conversion expression.

    epoch_ms()       → milliseconds (e.g. 1_700_000_000_000 = Nov 2023)
    to_timestamp()   → seconds      (e.g. 1_700_000_000    = Nov 2023)
    """
    try:
        r = conn.execute(
            f'SELECT MAX("{col}") FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 1'
        ).fetchone()
        if r and r[0] is not None:
            mv = int(r[0])
            if mv > 1_500_000_000_000:
                return f'epoch_ms("{col}")'
            elif mv > 1_000_000_000:
                return f'to_timestamp("{col}")'
    except Exception:
        pass
    return f'epoch_ms("{col}")'  # safe default


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
    for pref in ("date", "created", "time"):
        for c in candidates:
            if pref in c.lower():
                return c
    return candidates[0]


def _get_date_cast_expr(table: str, col: str, cols: list[tuple[str, str]]) -> str:
    """
    Return the correct SQL expression to cast a date column for comparison.

    For native DATE/TIMESTAMP columns: CAST("col" AS DATE)
    For BIGINT epoch columns:          CAST(epoch_ms("col") AS DATE)
                                    or CAST(to_timestamp("col") AS DATE)

    This is the key fix for "Unimplemented type for cast (BIGINT -> DATE)".
    """
    # Find the dtype for this column
    col_dtype = next(
        (d.upper() for c, d in cols if c.lower() == col.lower()),
        ""
    )

    if any(t in col_dtype for t in _BIGINT_TYPES):
        # BIGINT epoch — must use epoch conversion
        conn = get_read_connection()
        try:
            epoch_expr = _detect_epoch_scale(conn, table, col)
        finally:
            conn.close()
        return f'CAST({epoch_expr} AS DATE)'

    # Native date/timestamp
    return f'CAST("{col}" AS DATE)'


def _find_metric_col(cols: list[tuple[str, str]], metric: str) -> str | None:
    """Find the best numeric column matching the requested metric."""
    col_names = {c.lower() for c, _ in cols}

    if metric.lower() in col_names:
        return metric.lower()

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

    for c in numeric_cols:
        if metric.lower().replace("_", "") in c.lower().replace("_", ""):
            return c

    for c in numeric_cols:
        if any(k in c.lower() for k in _MONETARY_KEYWORDS):
            return c
    return numeric_cols[0] if numeric_cols else None


def _score_revenue_col(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0
    if any(k in c for k in ("revenue", "sales", "amount", "earning", "total", "final", "net", "paid")):
        score += 10
    if "final" in c or "net" in c or "paid" in c:
        score += 8
    if "total" in c:
        score += 6
    if "price" in c or "fare" in c:
        score += 3
    if any(k in c for k in ("unit", "base", "list", "mrp", "msrp", "catalog", "original",
                             "cost", "tax", "discount", "coupon", "shipping", "commission",
                             "refund", "refunded", "before_")):
        score -= 7
    return score


def _pick_best_revenue_col(cols: list[tuple[str, str]]) -> str | None:
    numeric_cols = [c for c, d in cols if _is_numeric(d) and not c.lower().endswith("_id")]
    if not numeric_cols:
        return None
    ranked = sorted(
        numeric_cols,
        key=lambda c: (_score_revenue_col(c), 1 if "final" in c.lower() else 0,
                       1 if "total" in c.lower() else 0, -len(c)),
        reverse=True,
    )
    return ranked[0]


def _pick_best_count_key(cols: list[tuple[str, str]]) -> str | None:
    id_cols = [c for c, _ in cols if c.lower().endswith("_id") or c.lower() == "id"]
    if not id_cols:
        return None

    def _score(c: str) -> tuple[int, int, int, int]:
        n = c.lower()
        s = 0
        if any(k in n for k in ("order", "transaction", "invoice", "booking", "trip", "ride",
                                "ticket", "request", "visit", "session", "sale", "payment")):
            s += 10
        if any(k in n for k in ("row", "line", "item", "detail", "record", "event", "log")):
            s -= 10
        if n == "id":
            s -= 2
        return (s, 1 if n.endswith("_id") else 0, 1 if "order" in n else 0, -len(n))

    ranked = sorted(id_cols, key=_score, reverse=True)
    return ranked[0]


def _find_entity_col(cols: list[tuple[str, str]], entity: str) -> str | None:
    """Find the best text column matching the requested entity."""
    col_names = {c.lower() for c, _ in cols}

    if entity.lower() in col_names:
        return entity.lower()

    name_col = f"{entity.lower()}_name"
    if name_col in col_names:
        return name_col

    text_cols = [c for c, d in cols if _is_text(d)
                 and not c.lower().endswith("_id")
                 and not _is_date_type(c, d)]

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
    Find the single table that contains the needed columns.
    Returns (table, entity_col_resolved, metric_col_resolved, date_col) or None.
    """
    best = None
    best_score = -1

    for table, cols in schema.items():
        col_names = {c.lower() for c, _ in cols}
        score = 0

        e_found = None
        if entity_col:
            if entity_col.lower() in col_names:
                e_found = entity_col.lower()
                score += 5
            else:
                e_found = _find_entity_col(cols, entity_col)
                if e_found:
                    score += 3

        m_found = None
        if metric_col:
            if metric_col.lower() in col_names:
                m_found = metric_col.lower()
                score += 5
            else:
                m_found = _find_metric_col(cols, metric_col)
                if m_found:
                    score += 3

        d_found = _find_date_col(cols)
        if d_found:
            score += 2

        if need_date and not d_found:
            continue

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

    Key change: uses _get_date_cast_expr() which handles BIGINT epoch
    columns with the correct epoch_ms()/to_timestamp() conversion.
    """
    entity_raw = parsed.get("entity")
    metric_raw = parsed.get("metric", "revenue")
    qt         = parsed.get("query_type", "top_n")
    disable_limit = bool(parsed.get("_disable_limit"))
    filters    = parsed.get("filters", {})
    aov_revenue_raw = parsed.get("_aov_revenue_col")
    aov_count_key_raw = parsed.get("_count_distinct_key")

    if qt in ("comparison", "growth_ranking", "intersection",
              "threshold", "time_series"):
        return None

    try:
        schema = _load_schema()
    except Exception:
        return None

    if not schema:
        return None

    is_count = metric_raw and metric_raw.lower() == "count"
    is_avg   = metric_raw and metric_raw.lower().startswith("avg_")
    is_aov   = metric_raw and metric_raw.lower() == "aov"

    metric_col = None
    if is_aov:
        if isinstance(aov_revenue_raw, str) and aov_revenue_raw.strip():
            metric_col = aov_revenue_raw.strip().lower()
        else:
            for table, cols in schema.items():
                metric_col = _pick_best_revenue_col(cols)
                if metric_col:
                    break
    elif not is_count:
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

    if entity_raw and qt in ("top_n", "bottom_n", "zero_filter") and not entity_col:
        return None

    if not is_count and not metric_col:
        return None

    result = _find_best_table(schema, entity_col, metric_col, need_date=True)
    if not result:
        return None

    table, e_col, m_col, d_col = result

    if not d_col:
        return None

    # ── Epoch-aware date cast (THE KEY FIX) ──────────────────────────────────
    # Determines the right SQL expression based on actual column type.
    # For BIGINT epoch: CAST(epoch_ms("col") AS DATE)
    # For native DATE:  CAST("col" AS DATE)
    table_cols_list = schema.get(table, [])
    date_cast = _get_date_cast_expr(table, d_col, table_cols_list)

    table_cols = {c.lower() for c, _ in table_cols_list}
    filter_clause = _build_filter_clause(filters, table_cols)

    # Build aggregation expression
    if is_aov:
        revenue_col = m_col if m_col else _pick_best_revenue_col(table_cols_list)
        count_key = None
        if isinstance(aov_count_key_raw, str) and aov_count_key_raw.strip():
            raw = aov_count_key_raw.strip().lower()
            if raw in table_cols:
                count_key = raw
        if not count_key:
            count_key = _pick_best_count_key(table_cols_list)
        if not revenue_col or not count_key:
            return None
        agg_expr = (
            f'SUM("{revenue_col}") / '
            f'NULLIF(COUNT(DISTINCT "{count_key}"), 0)'
        )
    elif is_count:
        id_col = _pick_best_count_key(table_cols_list)
        agg_expr = f'COUNT(DISTINCT "{id_col}")' if id_col else "COUNT(*)"
    elif is_avg:
        agg_expr = f'AVG("{m_col}")'
    else:
        agg_expr = f'SUM("{m_col}")'

    # Build date filter using the epoch-aware cast expression
    date_filter = f'{date_cast} >= ? AND {date_cast} < ?{filter_clause}'

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
        limit_clause = "" if disable_limit else "\nLIMIT ?"
        return (
            f'SELECT "{e_col}" AS name,\n'
            f'       {agg_expr} AS value\n'
            f'FROM "{table}"\n'
            f'WHERE {date_filter}\n'
            f'GROUP BY "{e_col}"\n'
            f'ORDER BY value {direction}\n'
            f'{limit_clause}'
        )

    return None