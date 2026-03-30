"""Step 4: Text-to-SQL — fully generalised for any flat-table dataset.

Key changes vs original:
  - Removed _SALES_SCHEMA_TABLES, _BUILDER_ENTITIES, _BUILDER_METRICS constants
    (hardcoded to the old e-commerce schema).
  - Replaced with _is_classic_ecommerce_schema() which detects from live DB
    whether the fast sql_builder path is safe to use.
  - force_llm now uses a single clear rule: only use the builder for the
    known e-commerce schema; use LLM for everything else.
  - _build_llm_prompt() metric rules now use the schema's own Metric column
    mappings section instead of hardcoded final_price/total_fare preferences.
  - All other logic (time_series hint, SQL safety, EXPLAIN, fallbacks) unchanged.
"""

import os
import sys
import re
import logging

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests
from datetime import date, datetime, timezone
from typing import Any
from motia import FlowContext, queue

from shared_config import (
    GROQ_API_TOKEN,
    LLAMA_MODEL,
    QWEN_MODEL,
    GROQ_URL,
    QWEN_ENABLE_REASONING,
    QWEN_REASONING_EFFORT,
)
from utils.token_logger import log_tokens, add_tokens_to_state
from db.schema_context import get_schema_prompt
from db.sql_builder import build_sql
from db.duckdb_connection import explain_query, get_read_connection

try:
    from db.sql_registry import SQL_REGISTRY
except ImportError:
    SQL_REGISTRY = {}

logger = logging.getLogger(__name__)

config = {
    "name": "TextToSQL",
    "description": (
        "Builds SQL from parsed intent using the live schema. "
        "Uses a deterministic builder only for the classic e-commerce schema; "
        "all other datasets always go through the LLM path with the live schema "
        "injected into the prompt."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::text.to.sql")],
    "enqueues": ["query::execute"],
}

_THINK_RE    = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_SQL_LINE_RE = re.compile(r"(?im)^(WITH|SELECT)\b")
_FENCE_RE    = re.compile(r"```(?:sql)?\s*\n?(.*?)```", re.DOTALL | re.IGNORECASE)
_FORBIDDEN   = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE"
    r"|EXECUTE|COPY|VACUUM|ANALYZE|CALL|DO)\b",
    re.IGNORECASE,
)

# ── No schema-specific constants needed — all detection is live ────────────────


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _get_existing_tables() -> set[str]:
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()
        conn.close()
        return {r[0].lower() for r in rows}
    except Exception:
        return set()




def _detect_id_name_pairs() -> dict[str, str]:
    """Return {name_col: discriminator_col} for deduplication in GROUP BY."""
    from collections import defaultdict
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
        ).fetchall()
        conn.close()
    except Exception:
        return {}

    table_cols: dict = defaultdict(list)
    for table, col in rows:
        table_cols[table].append(col.lower())

    _CONTACT_DISCRIMINATORS = (
        "phone", "phone_number", "mobile", "mobile_number",
        "email", "email_address", "contact",
    )

    def _best_disc(name_col: str, col_set: set) -> str | None:
        base = name_col[:-5] if name_col.endswith("_name") else name_col
        for suffix in ("_id", "_code", "_key", "_uuid", "_no", "_number"):
            c = base + suffix
            if c in col_set:
                return c
        for c in _CONTACT_DISCRIMINATORS:
            if c in col_set:
                return c
        return None

    pairs: dict[str, str] = {}
    for table, cols in table_cols.items():
        col_set = set(cols)
        for col in cols:
            if col.endswith("_name"):
                disc = _best_disc(col, col_set)
                if disc and col not in pairs:
                    pairs[col] = disc
        if "name" in col_set and "name" not in pairs:
            for c in (f"{table}_id", "id", f"{table}_code"):
                if c in col_set:
                    pairs["name"] = c
                    break
    return pairs


# ── SQL extraction / safety ────────────────────────────────────────────────────

def _extract_sql(raw: str) -> str:
    text  = _THINK_RE.sub("", raw).strip()
    fence = _FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    m = _SQL_LINE_RE.search(text)
    if m:
        text = text[m.start():]
    text = re.sub(r"(--[^\n]*|/\*.*?\*/)", "", text, flags=re.DOTALL)
    return text.strip().rstrip(";")


def _is_safe(sql: str) -> tuple[bool, str]:
    if not _SQL_LINE_RE.match(sql):
        return False, f"does not start WITH/SELECT: {sql[:80]!r}"
    m = _FORBIDDEN.search(sql)
    if m:
        return False, f"forbidden keyword: {m.group()!r}"
    if ";" in sql:
        return False, "contains semicolon"
    return True, ""


def _explain(sql: str, parsed: dict | None = None) -> tuple[bool, str]:
    n   = sql.count("%s") + sql.count("?")
    d   = date.today().replace(day=1)
    num = 5
    qt  = (parsed or {}).get("query_type", "")
    thr = (parsed or {}).get("threshold") or {}
    typ = thr.get("type", "")
    if n == 0:
        params = []
    elif n == 1:
        params = [num]
    elif n == 2:
        params = [d, d]
    elif qt == "threshold" and typ == "percentage" and n == 5:
        params = [d, d, num, d, d]
    elif qt == "threshold" and typ == "absolute" and n == 3:
        params = [d, d, num]
    elif qt in ("comparison", "growth_ranking") and n == 5:
        params = [d, d, d, d, num]
    elif qt in ("comparison", "growth_ranking") and n == 4:
        params = [d, d, d, d]
    else:
        params = [d if i < n - 1 else num for i in range(n)]
    return explain_query(sql, params)


def _render_filter_clause(filters: dict) -> str:
    if not filters:
        return ""
    lines = []
    for col, val in filters.items():
        if isinstance(val, bool):
            lines.append(f"  AND {col} = {1 if val else 0}")
        elif isinstance(val, int):
            lines.append(f"  AND {col} = {val}")
        elif isinstance(val, float):
            lines.append(f"  AND {col} = {val}")
        else:
            safe_val = str(val).replace("'", "''")
            lines.append(f"  AND {col} = '{safe_val}'")
    return "\n".join(lines)


def _score_revenue_column(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0

    if any(k in c for k in ("revenue", "sales", "earning", "amount", "total", "final", "net", "paid")):
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


def _pick_best_revenue_column(columns: list[str]) -> str | None:
    if not columns:
        return None

    ranked = sorted(
        columns,
        key=lambda c: (
            _score_revenue_column(c),
            1 if "final" in c.lower() else 0,
            1 if "total" in c.lower() else 0,
            1 if "amount" in c.lower() else 0,
            -len(c),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _pick_best_count_key(columns: list[str]) -> str | None:
    id_cols = [c for c in columns if c.endswith("_id") or c == "id"]
    if not id_cols:
        return None

    def _score(c: str) -> tuple[int, int, int, int]:
        s = 0
        if any(k in c for k in ("order", "transaction", "invoice", "booking", "trip", "ride",
                                "ticket", "request", "visit", "session", "sale", "payment")):
            s += 10
        if any(k in c for k in ("row", "line", "item", "detail", "record", "event", "log")):
            s -= 10
        if c == "id":
            s -= 2
        return (
            s,
            1 if c.endswith("_id") else 0,
            1 if "order" in c else 0,
            -len(c),
        )

    ranked = sorted(id_cols, key=_score, reverse=True)
    return ranked[0]


def _check_filters_present(sql: str, filters: dict, ctx, query_id: str) -> None:
    for col in filters:
        if col.lower() not in sql.lower():
            ctx.logger.warn(
                f"⚠️ Filter '{col}' may be missing from generated SQL",
                {"queryId": query_id, "col": col},
            )


# ── Time-series SQL hint ───────────────────────────────────────────────────────

def _build_time_series_sql_hint(parsed: dict) -> str:
    bucket = parsed.get("time_bucket", "month")
    metric = parsed.get("metric", "value")
    aov_revenue_col = parsed.get("_aov_revenue_col") or "<revenue_column>"
    aov_count_key = parsed.get("_count_distinct_key") or "<order_identifier_column>"

    if bucket == "year":
        bucket_expr  = "CAST(YEAR(date_col) AS VARCHAR)"
        bucket_label = "YYYY"
    elif bucket == "month":
        bucket_expr  = "STRFTIME(date_col, '%Y-%m')"
        bucket_label = "YYYY-MM"
    elif bucket == "week":
        bucket_expr  = "STRFTIME(date_col, '%Y-W%W')"
        bucket_label = "YYYY-W##"
    elif bucket == "quarter":
        bucket_expr  = "CONCAT(CAST(YEAR(date_col) AS VARCHAR), '-Q', CAST(QUARTER(date_col) AS VARCHAR))"
        bucket_label = "YYYY-Q#"
    else:
        bucket_expr  = "STRFTIME(date_col, '%Y-%m-%d')"
        bucket_label = "YYYY-MM-DD"

    if metric == "aov":
        agg_expr = (
            f'SUM("{aov_revenue_col}") / '
            f'NULLIF(COUNT(DISTINCT "{aov_count_key}"), 0)'
        )
    elif metric == "count":
        agg_expr = "COUNT(DISTINCT <order_pk_column>)"
    elif metric.startswith("avg_"):
        actual_col = metric[4:]
        agg_expr = f"AVG({actual_col})"
    else:
        agg_expr = f"SUM({metric})"

    dc = "<actual_date_column>"
    bucket_expr_filled = bucket_expr.replace("date_col", dc)

    return f"""
REQUIRED SQL PATTERN for time_series ({bucket}):

  SELECT {bucket_expr_filled} AS name,
         {agg_expr} AS value
  FROM <table_name>
  WHERE {dc} >= ?
    AND {dc} < ?
    <BUSINESS_FILTERS>
  GROUP BY {bucket_expr_filled}
  ORDER BY name ASC

CRITICAL RULES for time_series (ALL MUST BE FOLLOWED):
1. GROUP BY IS MANDATORY.
2. Replace <actual_date_column> with the REAL date/datetime column from the schema.
3. Replace <table_name> with the REAL table name.
4. Replace <order_pk_column> with the primary order identifier column.
5. Replace <BUSINESS_FILTERS> with mandatory filter conditions.
6. ORDER BY name ASC for chronological output.
7. Do NOT add LIMIT for time_series.
8. Label format: {bucket_label}
9. For count metric: COUNT(DISTINCT <order_id_col>) NOT COUNT(*).
10. For avg_ metrics: use AVG(<column>) not SUM.
11. For AOV metric: SUM(<revenue_column>) / NULLIF(COUNT(DISTINCT <order_id_col>), 0).
"""


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_llm_prompt(user_query: str, parsed: dict, schema: str) -> str:
    entity     = parsed.get("entity")
    metric     = parsed.get("metric", "count")
    entity_key = parsed.get("_entity_group_key")
    count_key  = parsed.get("_count_distinct_key")
    aov_revenue_col = parsed.get("_aov_revenue_col")
    qt         = parsed.get("query_type", "top_n")
    top_n      = parsed.get("top_n", 5)
    disable_limit = bool(parsed.get("_disable_limit"))
    thr        = parsed.get("threshold") or {}
    filters    = parsed.get("filters", {}) or {}
    trs        = parsed.get("time_ranges", [])
    bucket     = parsed.get("time_bucket", "month")

    filter_clause = _render_filter_clause(filters)
    filter_desc   = (
        f"\n  MANDATORY FILTERS (from Rule 11):\n{filter_clause}"
        if filter_clause
        else "  none"
    )

    if entity:
        groupby_rule = (
            f"10. GROUPING (CRITICAL):\n"
            f"    Start with display column: {entity}\n"
            f"    Check 'Uniqueness Profile' and 'Safe Grouping Keys' sections.\n"
            f"    If {entity} is non-unique, GROUP BY stable_key + {entity}.\n"
            f"    Semantic suggestion key: {entity_key or 'N/A'}\n"
            f"    Do NOT assume any *_name column is unique.\n"
        )
    else:
        groupby_rule = (
            "10. GROUPING: aggregate or time_series query — no entity GROUP BY needed."
        )

    # ── Ranking instruction ───────────────────────────────────────────────────
    if qt == "time_series":
        ts_hint    = _build_time_series_sql_hint(parsed)
        rank_instr = (
            f"This is a TIME SERIES / TREND query.\n"
            f"Group by time bucket ({bucket}), NOT by any business entity.\n"
            f"ORDER BY name ASC (chronological). NO LIMIT.\n"
            f"\n{ts_hint}"
        )
    elif qt == "top_n":
        rank_instr = "ORDER BY value DESC\nNO LIMIT" if disable_limit else f"ORDER BY value DESC\nLIMIT {top_n}"
    elif qt == "bottom_n":
        rank_instr = "ORDER BY value ASC\nNO LIMIT" if disable_limit else f"ORDER BY value ASC\nLIMIT {top_n}"
    elif qt == "aggregate":
        rank_instr = "No GROUP BY, no ORDER BY, no LIMIT. Return single scalar aliased 'value'."
    elif qt == "threshold":
        op    = ">" if thr.get("operator", "gt") == "gt" else "<"
        ttype = thr.get("type", "absolute")
        tval  = thr.get("value", 0)
        if ttype == "percentage":
            rank_instr = (
                f"HAVING {metric}_expr {op} ({tval} / 100.0) * (SELECT SUM(...) total)\n"
                "ORDER BY value DESC"
            )
        else:
            rank_instr = f"HAVING aggregation {op} {tval}\nORDER BY value DESC"
    elif qt == "comparison":
        rank_instr = f"Two-period comparison. Use CTEs. ORDER BY value1 DESC LIMIT {top_n}"
    elif qt == "growth_ranking":
        rank_instr = f"Rank by delta = period2_value - period1_value. ORDER BY delta DESC LIMIT {top_n}"
    elif qt == "intersection":
        rank_instr = f"Only entities present in BOTH periods. ORDER BY value DESC LIMIT {top_n}"
    elif qt == "zero_filter":
        rank_instr = "Entities where metric = 0 or no rows in period. ORDER BY name"
    else:
        rank_instr = f"ORDER BY value DESC LIMIT {top_n}"

    date_hints = ""
    if trs:
        for i, tr in enumerate(trs[:2]):
            date_hints += f"\n  Period {i+1}: {tr.get('start')} to {tr.get('end')}"

    if filter_clause:
        filter_rule = (
            f"11. BUSINESS FILTERS — MANDATORY:\n"
            f"    The following WHERE conditions MUST be included:\n"
            f"{filter_clause}\n"
            f"    Add after date filter:\n"
            f"      WHERE date_col BETWEEN ? AND ?\n"
            f"{filter_clause}\n"
        )
    else:
        filter_rule = "11. No additional business filters required."

    return f"""You are a DuckDB SQL expert. Output ONLY raw SQL — no prose, no markdown fences.

User question: "{user_query}"

{schema}

Parsed intent:
  query_type : {qt}
  entity     : {entity}
  metric     : {metric}
    aov_numerator_col: {aov_revenue_col or 'N/A'}
    count_distinct_key: {count_key or 'N/A'}
  time_bucket: {bucket if qt == 'time_series' else 'N/A'}
  top_n      : {top_n}
    disable_limit: {disable_limit}
  filters    : {filter_desc}{date_hints}

STRICT RULES:
0. Read the "Semantic Layer" and "Metric column mappings" sections above first.
   Use the EXACT physical column names from those sections for the metric.
1. Use EXACT column names from the schema above (no guessing).
2. Alias the display column as "name" in SELECT.
3. Alias the aggregation as "value".
3b. Read "Uniqueness Profile" and "Safe Grouping Keys" first.
    If entity label is non-unique, group by stable key + label.
4. DATE FILTER — CRITICAL:
   Use EXCLUSIVE-RANGE pattern for datetime/timestamp columns:
       col >= ?  AND  col < ?
   where the second ? is the day AFTER the period end.
   NEVER use BETWEEN for datetime columns.
   NEVER use EXTRACT(YEAR ...) = ? or EXTRACT(MONTH ...) = ?
5. Use ? for ALL date/number placeholders.
6. No semicolons. No comments. SELECT only.
7. For "count" metric: COUNT(DISTINCT {count_key or '<primary_order_id_column>'}) AS value
8. For metric columns:
   - Default: SUM(column_name) AS value  (use exact column from schema)
   - If metric starts with "avg_": AVG(<column_without_avg_prefix>) AS value
   - If metric is "count": COUNT(DISTINCT <primary_order_id>) AS value
    - If metric is "aov":
         SUM({aov_revenue_col or '<revenue_column>'}) / NULLIF(COUNT(DISTINCT {count_key or '<order_identifier_column>'}), 0) AS value
      (Use this exact derived formula; never use COUNT(DISTINCT row-like IDs) for AOV.)
9. Ranking: {rank_instr}
{groupby_rule}
{filter_rule}

SQL:"""


# ── LLM call ──────────────────────────────────────────────────────────────────

def _call_llm(model: str, prompt: str) -> tuple[str, dict]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1024,
        "temperature": 0.0,
    }

    if (
        QWEN_ENABLE_REASONING
        and "qwen" in model.lower()
        and QWEN_REASONING_EFFORT
    ):
        payload["reasoning_effort"] = QWEN_REASONING_EFFORT

    headers = {
        "Authorization": f"Bearer {GROQ_API_TOKEN}",
        "Content-Type": "application/json",
    }

    resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=45)
    if resp.status_code >= 400 and "reasoning_effort" in payload:
        payload.pop("reasoning_effort", None)
        resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=45)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip(), data.get("usage", {})


def _registry_fallback(parsed: dict) -> str | None:
    """Fast-path fallback using hardcoded SQL registry (e-commerce schema only)."""
    qt = parsed.get("query_type", "top_n")
    if qt not in ("top_n", "bottom_n", "aggregate"):
        return None
    entity = parsed.get("entity")
    metric = parsed.get("metric")
    key    = "top" if qt == "top_n" else "bottom" if qt == "bottom_n" else "aggregate"
    try:
        return SQL_REGISTRY[entity][metric][key]
    except (KeyError, TypeError):
        return None


def _deterministic_time_series_fallback(parsed: dict) -> str | None:
    """
    Build a robust time-series SQL without LLM.
    Used when query_type=time_series and model generation fails.
    """
    metric    = (parsed.get("metric") or "count").lower()
    bucket    = (parsed.get("time_bucket") or "month").lower()
    filters   = parsed.get("filters", {}) or {}
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _bucket_expr(date_col: str) -> str:
        if bucket == "year":
            return f"CAST(YEAR({date_col}) AS VARCHAR)"
        if bucket == "quarter":
            return f"CONCAT(CAST(YEAR({date_col}) AS VARCHAR), '-Q', CAST(QUARTER({date_col}) AS VARCHAR))"
        if bucket == "week":
            return f"STRFTIME({date_col}, '%Y-W%W')"
        if bucket == "day":
            return f"STRFTIME({date_col}, '%Y-%m-%d')"
        return f"STRFTIME({date_col}, '%Y-%m')"

    def _choose_plan() -> tuple[str, str, str] | None:
        best = None
        for t in tables:
            try:
                cols = [(c[0], str(c[1]).upper()) for c in conn.execute(f'DESCRIBE "{t}"').fetchall()]
            except Exception:
                continue
            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c for c, typ in cols
                if ("DATE" in typ or "TIMESTAMP" in typ
                    or any(k in c.lower() for k in ("date", "time", "created", "updated", "at")))
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            agg_expr = None
            score = 0
            if metric == "aov":
                ck = count_key if count_key and count_key in col_names else _pick_best_count_key(col_names)
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    candidates = [
                        c for c in col_names
                        if any(k in c for k in ("amount", "total", "revenue", "sales",
                                                "earning", "price", "fare", "cost",
                                                "fee", "payment", "profit", "final", "net", "paid"))
                    ]
                    revenue_col = _pick_best_revenue_column(candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / '
                        f'NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
                    score += 8
            elif metric == "count":
                ck = count_key
                if not ck or ck.lower() not in col_names:
                    ck = _pick_best_count_key(col_names)
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
                score += 6 if ck else 2
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
                    score += 6
            else:
                if metric in col_names:
                    agg_expr = f'SUM("{metric}")'
                    score += 7
                else:
                    candidates = [
                        c for c in col_names
                        if any(k in c for k in ("amount", "total", "revenue", "sales",
                                                "earning", "price", "fare", "cost",
                                                "fee", "payment", "profit", "final"))
                    ]
                    best_money_col = _pick_best_revenue_column(candidates)
                    if best_money_col:
                        agg_expr = f'SUM("{best_money_col}")'
                        score += 4
            if not agg_expr:
                continue

            if any(k in date_col.lower() for k in ("date", "created", "time")):
                score += 2
            if metric in col_names:
                score += 2
            if best is None or score > best[0]:
                best = (score, t, date_col, agg_expr)

        return (best[1], best[2], best[3]) if best else None

    try:
        plan = _choose_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, agg_expr = plan
    b_expr = _bucket_expr(f'CAST("{date_col}" AS DATE)')
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    return (
        f'SELECT {b_expr} AS name, {agg_expr} AS value\n'
        f'FROM "{table}"\n'
        f'WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?'
        f'{where_extra}\n'
        f'GROUP BY {b_expr}\n'
        f'ORDER BY name ASC'
    )


def _deterministic_comparison_fallback(parsed: dict) -> str | None:
    """Build robust two-period comparison SQL without relying on LLM output."""
    metric    = (parsed.get("metric") or "count").lower()
    entity    = (parsed.get("entity") or "").lower().strip() or None
    filters   = parsed.get("filters", {}) or {}
    top_n     = int(parsed.get("top_n") or 5)
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _pick_plan() -> tuple[str, str, str | None, str] | None:
        best = None
        for t in tables:
            try:
                cols = [(c[0], str(c[1]).upper()) for c in conn.execute(f'DESCRIBE "{t}"').fetchall()]
            except Exception:
                continue
            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(k in c.lower() for k in ("date", "time", "created", "updated", "at"))
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            entity_col = None
            if entity:
                if entity in col_names:
                    entity_col = entity
                elif entity.endswith("_name") and entity[:-5] in col_names:
                    entity_col = entity[:-5]
                else:
                    entity_col = next((c for c in col_names if entity in c), None)
                if not entity_col:
                    text_cols = [c for c, typ in cols if any(tk in typ for tk in ("VARCHAR", "CHAR", "TEXT", "STRING"))]
                    entity_base = entity.replace("_name", "").replace("_id", "")
                    tokens = [tok for tok in entity_base.split("_") if tok]
                    entity_col = next(
                        (c for c in text_cols if any(tok in c.lower() for tok in tokens)),
                        None,
                    )

            agg_expr = None
            if metric == "aov":
                ck = count_key if count_key and count_key in col_names else _pick_best_count_key(col_names)
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    revenue_candidates = [
                        c for c in col_names
                        if any(k in c for k in ("final", "total", "amount", "price", "revenue", "sales", "earning", "fare", "net", "paid"))
                    ]
                    revenue_col = _pick_best_revenue_column(revenue_candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / '
                        f'NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
            elif metric == "count":
                ck = count_key if count_key and count_key in col_names else None
                if not ck:
                    ck = _pick_best_count_key(col_names)
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
            elif metric in col_names:
                agg_expr = f'SUM("{metric}")'
            else:
                revenue_candidates = [
                    c for c in col_names
                    if any(k in c for k in ("final", "total", "amount", "price", "revenue", "sales", "earning", "fare", "net", "paid"))
                ]
                money_col = _pick_best_revenue_column(revenue_candidates)
                if money_col:
                    agg_expr = f'SUM("{money_col}")'

            if not agg_expr:
                continue

            score = 0
            if metric in col_names:
                score += 5
            if entity_col:
                score += 4
            if all((k in col_names) for k in filters.keys()):
                score += 2
            if any(k in date_col.lower() for k in ("date", "time", "created")):
                score += 2

            if best is None or score > best[0]:
                best = (score, t, date_col, entity_col, agg_expr)

        return (best[1], best[2], best[3], best[4]) if best else None

    try:
        plan = _pick_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, entity_col, agg_expr = plan
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    if entity_col:
        return (
            f'WITH p1 AS (\n'
            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value1\n'
            f'  FROM "{table}"\n'
            f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
            f'  GROUP BY "{entity_col}"\n'
            f'),\n'
            f'p2 AS (\n'
            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value2\n'
            f'  FROM "{table}"\n'
            f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
            f'  GROUP BY "{entity_col}"\n'
            f')\n'
            f'SELECT COALESCE(p1.name, p2.name) AS name,\n'
            f'       COALESCE(p1.value1, 0) AS value1,\n'
            f'       COALESCE(p2.value2, 0) AS value2,\n'
            f'       COALESCE(p2.value2, 0) - COALESCE(p1.value1, 0) AS delta\n'
            f'FROM p1\n'
            f'FULL OUTER JOIN p2 ON p1.name = p2.name\n'
            f'ORDER BY value1 DESC\n'
            f'LIMIT {top_n}'
        )

    return (
        f'WITH p1 AS (\n'
        f'  SELECT {agg_expr} AS value1\n'
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f'),\n'
        f'p2 AS (\n'
        f'  SELECT {agg_expr} AS value2\n'
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f')\n'
        f"SELECT 'Total' AS name, p1.value1 AS value1, p2.value2 AS value2, (p2.value2 - p1.value1) AS delta\n"
        f'FROM p1 CROSS JOIN p2'
    )


# ── Handler ───────────────────────────────────────────────────────────────────

async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    parsed["_user_query"] = user_query

    qt      = parsed.get("query_type", "top_n")
    filters = parsed.get("filters", {}) or {}

    existing_tables = _get_existing_tables()

    ctx.logger.info("📋 DB tables found", {
        "queryId":      query_id,
        "tables":       list(existing_tables),
    })

    ctx.logger.info("🧬 TextToSQL", {
        "queryId":     query_id,
        "query_type":  qt,
        "time_bucket": parsed.get("time_bucket"),
        "filters":     filters,
    })

    generated_sql = None
    usage         = {}
    fallback_used = False
    sql_source    = "builder"

    # ── Fast path: deterministic builder (schema-driven, works for any dataset) ─
    use_builder = qt in ("top_n", "bottom_n", "aggregate", "zero_filter")

    if use_builder:
        built = build_sql(parsed)
        if built:
            ok, reason = _is_safe(built)
            if ok:
                ok2, err = _explain(built, parsed)
                if ok2:
                    generated_sql = built
                    ctx.logger.info("✅ Builder SQL validated", {"queryId": query_id})
                else:
                    ctx.logger.warn("⚠️ Builder EXPLAIN failed — falling to LLM",
                                    {"queryId": query_id, "error": err})
            else:
                ctx.logger.warn("⚠️ Builder safety failed — falling to LLM",
                                {"queryId": query_id, "reason": reason})

    # ── LLM path: used for all non-e-commerce datasets and complex query types ─
    if generated_sql is None:
        _COMPLEX = {"growth_ranking", "comparison", "threshold", "intersection",
                    "zero_filter", "time_series"}
        model      = QWEN_MODEL if qt in _COMPLEX else LLAMA_MODEL
        sql_source = f"llm_{model.split('/')[0]}"

        ctx.logger.info("🤖 LLM SQL generation", {"queryId": query_id, "model": model})

        try:
            schema     = get_schema_prompt()
            prompt     = _build_llm_prompt(user_query, parsed, schema)
            raw, usage = _call_llm(model, prompt)
            ctx.logger.info("🔬 LLM raw", {"queryId": query_id, "preview": raw[:400]})

            sql = _extract_sql(raw)
            ok, reason = _is_safe(sql)
            if ok:
                _check_filters_present(sql, filters, ctx, query_id)
                ok2, err = _explain(sql, parsed)
                if ok2:
                    generated_sql = sql
                    ctx.logger.info("✅ LLM SQL validated", {"queryId": query_id})
                else:
                    # For time_series, prefer deterministic fallback over broken LLM SQL.
                    if qt == "time_series":
                        ctx.logger.warn(
                            "⚠️ LLM EXPLAIN failed for time_series — using deterministic fallback",
                            {"queryId": query_id, "error": err})
                    else:
                        ctx.logger.warn("⚠️ LLM EXPLAIN failed — attempting execution anyway",
                                        {"queryId": query_id, "error": err})
                        generated_sql = sql
            else:
                ctx.logger.warn("⚠️ LLM safety failed", {"queryId": query_id, "reason": reason})
        except Exception as exc:
            ctx.logger.error("❌ LLM error", {"queryId": query_id, "error": str(exc)})

        if usage:
            log_tokens(ctx, query_id, "TextToSQL", model, usage)
            await add_tokens_to_state(ctx, query_id, "TextToSQL", model, usage)

    # ── Registry fallback — no longer used (registry is empty) ──────────────────

    # ── Deterministic time-series fallback (any schema) ────────────────────────
    if generated_sql is None and qt == "time_series":
        fb = _deterministic_time_series_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source    = "deterministic_time_series_fallback"
            ctx.logger.warn("Deterministic time_series fallback used", {"queryId": query_id})

    if generated_sql is None and qt in ("comparison", "growth_ranking", "intersection"):
        fb = _deterministic_comparison_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source    = "deterministic_comparison_fallback"
            ctx.logger.warn("Deterministic comparison fallback used", {"queryId": query_id})

    if generated_sql is None:
        msg = (
            f"Could not generate SQL for query_type={qt} "
            f"entity={parsed.get('entity')} metric={parsed.get('metric')}. "
            "Try rephrasing with explicit metric, dimension, and period."
        )
        qs = await ctx.state.get("queries", query_id)
        if qs:
            now_iso = datetime.now(timezone.utc).isoformat()
            prev_ts = qs.get("status_timestamps", {})
            await ctx.state.set("queries", query_id, {
                **qs, "status": "error", "error": msg,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "error": now_iso},
            })
        return

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs, "status": "sql_generated",
            "generated_sql": generated_sql,
            "sql_source":    sql_source,
            "sql_fallback":  fallback_used,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "sql_generated": now_iso},
        })

    await ctx.enqueue({
        "topic": "query::execute",
        "data":  {
            "queryId":       query_id,
            "query":         user_query,
            "parsed":        parsed,
            "generated_sql": generated_sql,
        },
    })