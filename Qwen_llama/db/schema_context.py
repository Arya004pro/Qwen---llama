"""Schema prompt builder for SQL generation.

Key addition: _detect_business_rules() scans the live schema for well-known
'filter' columns (is_cancelled, is_deleted, status …) and injects mandatory
WHERE-clause hints so the LLM never includes cancelled / inactive rows in
revenue / count calculations.
"""

from db.duckdb_connection import get_read_connection
from db.semantic_layer import render_semantic_layer_lines

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

_ENTITY_NAME_HINTS: tuple[str, ...] = (
    "name", "title", "label", "code", "id", "location", "warehouse",
    "store", "branch", "driver", "brand", "vendor", "customer",
    "employee", "agent", "partner", "city", "state", "region",
)

_NON_ENTITY_NAME_HINTS: tuple[str, ...] = (
    "date", "time", "timestamp", "created", "updated", "deleted",
    "status", "type", "description", "comment", "note", "month",
    "year", "week", "day",
)


def _is_text_type(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))


def _is_numeric_type(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"))


def _looks_like_date_column(col_name: str) -> bool:
    n = col_name.lower()
    return any(k in n for k in ("date", "time", "timestamp", "month", "year", "week", "day"))


def _looks_like_entity_column(col_name: str) -> bool:
    n = col_name.lower()
    if _looks_like_date_column(n):
        return False
    if any(k in n for k in _NON_ENTITY_NAME_HINTS):
        return False
    if n == "id" or n.endswith("_id"):
        return True
    return any(k in n for k in _ENTITY_NAME_HINTS)


def _detect_entities(conn, tables: list[str]) -> list[str]:
    """
    Detect likely business entities from high-cardinality text columns.
    """
    hints: list[str] = []

    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in conn.execute(f'DESCRIBE "{table}"').fetchall()]
        except Exception:
            continue

        candidates: list[tuple[float, str, int, int, float]] = []
        for col_name, dtype in cols:
            if not _is_text_type(dtype):
                continue
            if _looks_like_date_column(col_name):
                continue
            if not _looks_like_entity_column(col_name):
                continue

            try:
                non_null, distinct_cnt = conn.execute(
                    f'''
                    SELECT
                        COUNT("{col_name}") AS non_null_count,
                        COUNT(DISTINCT "{col_name}") AS distinct_count
                    FROM "{table}"
                    '''
                ).fetchone()
            except Exception:
                continue

            non_null = int(non_null or 0)
            distinct_cnt = int(distinct_cnt or 0)
            if non_null == 0:
                continue

            ratio = distinct_cnt / non_null
            id_like = col_name.lower() == "id" or col_name.lower().endswith("_id")
            high_card = (
                (distinct_cnt >= 10 and ratio >= 0.30) or
                (distinct_cnt >= 50 and ratio >= 0.15) or
                (id_like and distinct_cnt >= 10 and ratio >= 0.60)
            )
            if not high_card:
                continue

            score = ratio
            n = col_name.lower()
            if "name" in n:
                score += 0.40
            elif id_like:
                score += 0.30
            elif any(k in n for k in ("location", "store", "warehouse", "branch", "code")):
                score += 0.25
            if distinct_cnt >= 100:
                score += 0.10

            candidates.append((score, col_name, distinct_cnt, non_null, ratio))

        if not candidates:
            continue

        candidates.sort(reverse=True)
        top = candidates[:2]
        col_bits = ", ".join(
            f'{col} (distinct={distinct_cnt}/{non_null}, ratio={ratio:.2f})'
            for _, col, distinct_cnt, non_null, ratio in top
        )
        hints.append(f'  "{table}" -> {col_bits}')

    return hints


def _detect_uniqueness_profile(conn, tables: list[str]) -> tuple[list[str], list[str]]:
    """
    Build a uniqueness profile and safe grouping-key hints.

    Returns:
      - profile_lines: human-readable uniqueness lines per table
      - grouping_lines: preferred display->key mapping for collision-safe grouping
    """
    profile_lines: list[str] = []
    grouping_lines: list[str] = []

    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in conn.execute(f'DESCRIBE "{table}"').fetchall()]
        except Exception:
            continue

        if not cols:
            continue

        col_stats: dict[str, tuple[int, int, float]] = {}
        unique_like: list[str] = []
        label_non_unique: list[str] = []

        for col_name, dtype in cols:
            # Keep cardinality checks focused on text/numeric columns.
            if not (_is_text_type(dtype) or _is_numeric_type(dtype)):
                continue
            if _looks_like_date_column(col_name):
                continue

            try:
                non_null, distinct_cnt = conn.execute(
                    f'''
                    SELECT
                        COUNT("{col_name}") AS non_null_count,
                        COUNT(DISTINCT "{col_name}") AS distinct_count
                    FROM "{table}"
                    '''
                ).fetchone()
            except Exception:
                continue

            non_null = int(non_null or 0)
            distinct_cnt = int(distinct_cnt or 0)
            if non_null == 0:
                continue

            ratio = distinct_cnt / non_null
            col_stats[col_name.lower()] = (distinct_cnt, non_null, ratio)

            n = col_name.lower()
            id_like = (
                n == "id" or n.endswith("_id") or n.endswith("_uuid") or n.endswith("_key")
                or "email" in n or "phone" in n or "mobile" in n or n.endswith("_code")
            )
            if distinct_cnt >= 2 and ratio >= 0.98 and id_like:
                unique_like.append(col_name)

            if (n == "name" or n.endswith("_name") or "title" in n) and distinct_cnt >= 2 and ratio < 0.98:
                label_non_unique.append(f"{col_name} ({distinct_cnt}/{non_null}, ratio={ratio:.2f})")

        if unique_like:
            profile_lines.append(f'  "{table}" unique-like identifiers: {", ".join(unique_like[:6])}')
        if label_non_unique:
            profile_lines.append(f'  "{table}" non-unique labels: {", ".join(label_non_unique[:4])}')

        # Build safe grouping map: <base>_name -> <base>_id/<base>_code when key is unique-like.
        for raw_col, _ in cols:
            c = raw_col.lower()
            if c == "name" or c.endswith("_name"):
                base = c[:-5] if c.endswith("_name") else c
                key_candidates = [f"{base}_id", f"{base}_code", f"{base}_key", f"{base}_uuid", "id"]
                chosen = None
                for k in key_candidates:
                    stats = col_stats.get(k)
                    if not stats:
                        continue
                    _, non_null, ratio = stats
                    if non_null >= 2 and ratio >= 0.98:
                        chosen = k
                        break
                if chosen:
                    grouping_lines.append(f'  "{table}": display "{c}" -> group by "{chosen}" + "{c}"')

    return profile_lines, grouping_lines


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


def _build_schema_examples(conn, tables: list[str]) -> list[str]:
    """
    Build dynamic few-shot SQL examples using detected columns from the live schema.
    The examples are generated from actual table/column names so they stay domain-agnostic.
    """
    metric_keywords = (
        "final", "total", "amount", "revenue", "sales", "price",
        "fare", "earning", "commission", "quantity", "count",
    )
    date_keywords = ("date", "time", "created", "updated", "at")

    best: tuple[int, str, str, str, str] | None = None
    # score, table, entity_col, metric_col, date_col

    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in conn.execute(f'DESCRIBE "{table}"').fetchall()]
        except Exception:
            continue

        entity_cols = [
            c for c, t in cols
            if _is_text_type(t) and _looks_like_entity_column(c)
        ]
        if not entity_cols:
            continue

        metric_cols = [
            c for c, t in cols
            if any(x in t for x in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"))
            and not c.lower().endswith("_id")
        ]
        if not metric_cols:
            continue

        date_cols = [
            c for c, t in cols
            if (any(k in c.lower() for k in date_keywords) or any(x in t for x in ("DATE", "TIMESTAMP")))
        ]
        if not date_cols:
            continue

        entity_col = next((c for c in entity_cols if "name" in c.lower()), entity_cols[0])
        metric_col = next(
            (c for c in metric_cols if any(k in c.lower() for k in metric_keywords)),
            metric_cols[0],
        )
        date_col = date_cols[0]

        score = 0
        if "name" in entity_col.lower():
            score += 3
        if any(k in metric_col.lower() for k in ("revenue", "sales", "amount", "final", "total", "price")):
            score += 3
        if "date" in date_col.lower():
            score += 2
        score += min(len(metric_cols), 3)

        if best is None or score > best[0]:
            best = (score, table, entity_col, metric_col, date_col)

    if best is None:
        return []

    _, table, entity_col, metric_col, date_col = best

    return [
        f"  Example 1 (Top-N by metric):",
        f'    User: "Top 10 {entity_col} by {metric_col} in a time range"',
        f'    SQL:  SELECT "{entity_col}" AS name, SUM("{metric_col}") AS value',
        f'          FROM "{table}"',
        f'          WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?',
        f'          GROUP BY 1 ORDER BY value DESC LIMIT ?',
        "",
        f"  Example 2 (Aggregate total):",
        f'    User: "Total {metric_col} in a time range"',
        f'    SQL:  SELECT SUM("{metric_col}") AS value',
        f'          FROM "{table}"',
        f'          WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?',
        "",
        f"  Example 3 (Monthly trend):",
        f'    User: "Monthly trend of {metric_col}"',
        f"""    SQL:  SELECT DATE_TRUNC('month', CAST("{date_col}" AS DATE)) AS name, SUM("{metric_col}") AS value""",
        f'          FROM "{table}"',
        f'          WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?',
        "          GROUP BY 1 ORDER BY 1",
    ]


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

        entity_hints = _detect_entities(conn, tables)
        lines += ["", "Detected Entities", "-----------------"]
        lines += [
            "Use these as preferred grouping dimensions for entity questions.",
            "Map user terms like Drivers/Warehouses/Stores to the closest column below.",
        ]
        if entity_hints:
            lines += entity_hints
        else:
            lines += ["  (No high-cardinality entity-like text columns detected from current data.)"]

        uniqueness_lines, grouping_lines = _detect_uniqueness_profile(conn, tables)
        lines += ["", "Uniqueness Profile", "------------------"]
        if uniqueness_lines:
            lines += uniqueness_lines
        else:
            lines += ["  (No uniqueness signals detected.)"]
        lines += ["", "Safe Grouping Keys", "------------------"]
        if grouping_lines:
            lines += [
                "When display labels are not unique, group by stable key + label to avoid merged entities.",
            ]
            lines += grouping_lines
        else:
            lines += ["  (No explicit display->key pairing detected.)"]

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

        examples = _build_schema_examples(conn, tables)
        lines += ["", "Schema-Grounded Examples", "------------------------"]
        if examples:
            lines += [
                "Use these examples as patterns. Replace table/column names only when needed.",
                "Keep aliases exactly as: name, value.",
            ]
            lines += examples
        else:
            lines += [
                "  (No complete table profile found with entity + metric + date columns.)",
            ]

        # ── Auto-detected business / validity filters ──────────────────────────

        lines += [""]
        lines += render_semantic_layer_lines(conn, tables)
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
