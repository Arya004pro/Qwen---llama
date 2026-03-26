"""Schema prompt builder for SQL generation.

Key addition: _detect_business_rules() scans the live schema for well-known
'filter' columns (is_cancelled, is_deleted, status …) and injects mandatory
WHERE-clause hints so the LLM never includes cancelled / inactive rows in
revenue / count calculations.
"""

from db.duckdb_connection import get_read_connection

_STATIC_FALLBACK = """
Tables
------
states         : state_id (PK), state_name
cities         : city_id (PK), city_name, state_id (FK->states)
customers      : customer_id (PK), customer_name, gender, age, region_id, city_id (FK->cities)
categories     : category_id (PK), category_name
products       : product_id (PK), product_name, category_id (FK->categories), price DOUBLE
orders         : order_id (PK), customer_id (FK->customers), order_date DATE, total_amount DOUBLE
order_items    : order_item_id (PK), order_id (FK->orders), product_id (FK->products),
                 quantity INT, item_price DOUBLE

Aggregation rules
-----------------
product/category revenue    = SUM(oi.quantity * oi.item_price)
customer/city/state revenue = SUM(o.total_amount)
quantity sold               = SUM(oi.quantity)
order count                 = COUNT(DISTINCT o.order_id)

Date filter: always use o.order_date BETWEEN ? AND ?
Use ? placeholders for all params.
"""

# ── Column patterns that signal "exclude this row from metrics" ───────────────
# Maps column_name → the SQL condition that keeps VALID rows only.
_FILTER_COLUMN_RULES: dict[str, str] = {
    "is_cancelled":  "{col} = 0",
    "is_deleted":    "{col} = 0",
    "cancelled":     "{col} = 0",
    "is_active":     "{col} = 1",
    "active":        "{col} = 1",
    "is_refunded":   "{col} = 0",
    "refunded":      "{col} = 0",
    "is_void":       "{col} = 0",
    "is_fraud":      "{col} = 0",
    "is_test":       "{col} = 0",
}

# "status" columns need a value-inspection step — these strings mark BAD rows.
_STATUS_BAD_VALUES: set[str] = {
    "cancelled", "canceled", "refunded", "void", "failed",
    "rejected", "returned", "closed", "inactive", "deleted",
}


def _detect_business_rules(conn, tables: list[str]) -> list[str]:
    """
    Scan each table for well-known boolean / status filter columns.

    Returns a list of human-readable rule strings ready to embed into the
    SQL-generation prompt, e.g.:
      "zomato_master_final.is_cancelled = 0  (exclude cancelled rows)"
    """
    rules: list[str] = []

    for table in tables:
        try:
            cols = {c[0].lower(): c[1].upper() for c in
                    conn.execute(f'DESCRIBE "{table}"').fetchall()}
        except Exception:
            continue

        for col_name, dtype in cols.items():
            # Boolean / integer flag columns
            rule_tmpl = _FILTER_COLUMN_RULES.get(col_name)
            if rule_tmpl:
                condition = rule_tmpl.format(col=col_name)
                rules.append(
                    f'  "{table}": always add WHERE {condition}'
                    f'  -- exclude {"inactive" if "active" in col_name else "cancelled/invalid"} rows'
                )
                continue

            # "status" / "state" text columns — inspect distinct values
            if col_name in ("status", "order_status", "ride_status",
                            "payment_status", "state"):
                try:
                    rows = conn.execute(
                        f'SELECT DISTINCT "{col_name}" FROM "{table}" LIMIT 30'
                    ).fetchall()
                    values = {str(r[0]).lower() for r in rows if r[0] is not None}
                    bad    = values & _STATUS_BAD_VALUES
                    good   = values - _STATUS_BAD_VALUES - {""}
                    if bad and good:
                        good_list = ", ".join(f"'{v}'" for v in sorted(good)[:6])
                        rules.append(
                            f'  "{table}".{col_name}: only include rows where '
                            f'{col_name} IN ({good_list})'
                            f'  -- exclude {sorted(bad)}'
                        )
                except Exception:
                    pass

    return rules


def _detect_metric_columns(conn, tables: list[str]) -> list[str]:
    """
    Auto-detect likely monetary / count metric columns and suggest
    canonical names for the LLM to use.

    Returns lines like:
      "revenue / sales / earnings → SUM(final_price)  [table zomato_master_final]"
    """
    hints: list[str] = []

    # Keyword → column-name fragments (checked as substrings)
    MONETARY_KEYWORDS   = ("price", "fare", "amount", "earning", "revenue",
                           "commission", "fee", "cost", "sale", "payment", "total")
    QUANTITY_KEYWORDS   = ("quantity", "qty", "count", "units", "volume")
    DISTANCE_KEYWORDS   = ("distance", "km", "mile")
    DURATION_KEYWORDS   = ("duration", "minute", "min", "second", "hour")

    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in
                    conn.execute(f'DESCRIBE "{table}"').fetchall()]
        except Exception:
            continue

        monetary_cols  = [c for c, t in cols if any(k in c.lower() for k in MONETARY_KEYWORDS)
                          and any(x in t for x in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "INT"))]
        quantity_cols  = [c for c, t in cols if any(k in c.lower() for k in QUANTITY_KEYWORDS)
                          and any(x in t for x in ("INT", "FLOAT", "DOUBLE"))]
        distance_cols  = [c for c, t in cols if any(k in c.lower() for k in DISTANCE_KEYWORDS)]
        duration_cols  = [c for c, t in cols if any(k in c.lower() for k in DURATION_KEYWORDS)]

        if monetary_cols:
            # Prefer "final_price" > "total_fare" > "total_amount" > first found
            preferred = next(
                (c for c in monetary_cols if "final" in c.lower()),
                next((c for c in monetary_cols if "total" in c.lower()), monetary_cols[0])
            )
            hints.append(
                f'  revenue/sales/earnings in "{table}" → SUM({preferred})'
                f'  (all monetary columns: {", ".join(monetary_cols)})'
            )

        if quantity_cols:
            hints.append(
                f'  quantity/units in "{table}" → SUM({quantity_cols[0]})'
            )

        if distance_cols:
            hints.append(f'  distance in "{table}" → SUM({distance_cols[0]})')

        if duration_cols:
            hints.append(f'  duration in "{table}" → SUM({duration_cols[0]})')

    return hints


def _detect_date_columns(conn, tables: list[str]) -> list[str]:
    """Return lines telling the LLM which column to use for date filtering."""
    hints: list[str] = []
    DATE_KEYWORDS = ("date", "time", "created", "updated", "at", "on", "when")

    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in
                    conn.execute(f'DESCRIBE "{table}"').fetchall()]
        except Exception:
            continue

        date_cols = [c for c, t in cols
                     if any(k in c.lower() for k in DATE_KEYWORDS)
                     and any(x in t for x in ("DATE", "TIMESTAMP", "VARCHAR"))]
        if date_cols:
            hints.append(
                f'  date filter for "{table}": use {date_cols[0]} >= ? AND {date_cols[0]} < ?  (exclusive end — pass end+1day)'
            )

    return hints


def _live_schema_prompt() -> str:
    conn = get_read_connection()
    try:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_%'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        if not tables:
            return ""

        # ── Column listing ─────────────────────────────────────────────────────
        lines = ["Tables", "------"]
        for t in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            col_s = ", ".join(f"{c[0]} {c[1]}" for c in cols)
            lines.append(f"{t:<14}: {col_s}")

        # ── Auto-detected metric mappings ──────────────────────────────────────
        metric_hints = _detect_metric_columns(conn, tables)
        if metric_hints:
            lines += ["", "Metric column mappings (USE THESE EXACT COLUMN NAMES)",
                      "-----------------------------------------------------------"]
            lines += metric_hints

        # ── Auto-detected date columns ─────────────────────────────────────────
        date_hints = _detect_date_columns(conn, tables)
        if date_hints:
            lines += ["", "Date filter columns", "--------------------"]
            lines += date_hints

        # ── Auto-detected business / validity filters ──────────────────────────
        biz_rules = _detect_business_rules(conn, tables)
        if biz_rules:
            lines += [
                "",
                "MANDATORY business-validity filters (ALWAYS apply — never omit)",
                "-------------------------------------------------------------------",
                "These filters MUST appear in every query that touches these tables.",
                "Omitting them will include cancelled, deleted, or fraudulent rows.",
            ]
            lines += biz_rules

        # ── General SQL rules ──────────────────────────────────────────────────
        lines += [
            "",
            "SQL rules",
            "---------",
            "- SELECT only; no DML",
            "- Alias group key as name and metric as value",
            "- Use ? placeholders for ALL params (dates, limits, thresholds)",
            "- For ranked queries: ORDER BY value DESC/ASC LIMIT ?",
            "- For aggregate queries: single scalar column aliased 'value', no GROUP BY",
            "- APPLY all mandatory business-validity filters listed above",
        ]

        return "\n".join(lines)

    finally:
        conn.close()


def get_schema_prompt() -> str:
    try:
        live = _live_schema_prompt().strip()
        if live:
            return live
    except Exception:
        pass
    return _STATIC_FALLBACK.strip()