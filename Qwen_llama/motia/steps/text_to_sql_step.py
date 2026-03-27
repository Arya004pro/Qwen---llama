"""Step 4: Text-to-SQL — fully generalised for any flat-table dataset.

Key changes:
  - _build_llm_prompt() now receives `filters` from parsed intent and renders
    them as an explicit MANDATORY WHERE clause in the prompt.
  - Added Rule 11: "BUSINESS FILTERS — apply ALL parsed filters as WHERE conditions".
  - time_series query type support (unchanged from previous version).
  - Validates generated SQL contains expected filter conditions and warns if missing.
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

from shared_config import GROQ_API_TOKEN, LLAMA_MODEL, QWEN_MODEL, GROQ_URL
from utils.token_logger import log_tokens, add_tokens_to_state
from db.schema_context import get_schema_prompt
from db.sql_builder import build_sql
from db.duckdb_connection import explain_query, get_read_connection

try:
    from db.sql_registry import SQL_REGISTRY
except ImportError:
    SQL_REGISTRY = {}

logger = logging.getLogger(__name__)

_SALES_SCHEMA_TABLES = {
    "orders", "customers", "cities", "products",
    "categories", "states", "order_items",
}
_BUILDER_ENTITIES = {"product", "customer", "city", "category", "state"}
_BUILDER_METRICS  = {"revenue", "quantity", "order_count"}

config = {
    "name": "TextToSQL",
    "description": (
        "Builds SQL from parsed intent using the live schema. "
        "Injects business-validity filters (e.g. is_cancelled=0) into every query. "
        "Handles time_series (trend) queries grouped by month/week/quarter/day."
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

_BUCKET_FMT = {
    "month":   "%Y-%m",
    "week":    "%Y-W%W",
    "quarter": "%Y-Q",
    "day":     "%Y-%m-%d",
}


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


def _sales_schema_available(existing: set[str]) -> bool:
    return _SALES_SCHEMA_TABLES.issubset(existing)


def _detect_id_name_pairs() -> dict[str, str]:
    """
    Scan the live schema and return {name_col: best_discriminator_col}.

    Priority per *_name column:
      1. <base>_id / <base>_code / <base>_key / <base>_uuid  (surrogate key)
      2. phone / phone_number / mobile / email               (contact ID)
      3. <base>_no / <base>_number                           (alternate key)

    Handles datasets with no surrogate key (e.g. Zomato customers identified
    by phone, not by customer_id). The discriminator is used to GROUP BY so
    that two entities sharing the same display name are never collapsed.
    """
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
    elif qt == "threshold" and typ == "percentage" and n == 5:
        params = [d, d, num, d, d]
    elif qt == "threshold" and typ == "absolute" and n == 3:
        params = [d, d, num]
    elif qt in ("comparison", "growth_ranking") and n == 5:
        params = [d, d, d, d, num]
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


def _check_filters_present(sql: str, filters: dict, ctx, query_id: str) -> None:
    for col, val in filters.items():
        if col.lower() not in sql.lower():
            ctx.logger.warn(
                f"⚠️ Filter '{col}' may be missing from generated SQL",
                {"queryId": query_id, "col": col, "expected_val": val},
            )


def _build_time_series_sql_hint(parsed: dict) -> str:
    bucket = parsed.get("time_bucket", "month")
    metric = parsed.get("metric", "total_fare")

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

    if metric == "count":
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
1. GROUP BY IS MANDATORY — always include it. Without GROUP BY the query WILL FAIL.
2. Replace <actual_date_column> with the REAL date/datetime column from the schema.
3. Replace <table_name> with the REAL table name.
4. Replace <order_pk_column> with the primary order identifier column (e.g. order_id, ride_id).
5. Replace <BUSINESS_FILTERS> with the mandatory filter conditions from Rule 11.
6. ORDER BY name ASC ensures chronological output.
7. Do NOT add LIMIT for time_series.
8. Label format: {bucket_label}
9. For count metric: COUNT(DISTINCT <order_id_col>) NOT COUNT(*).
   One order may have many rows (items), so COUNT(*) overcounts transactions.
10. For avg_ metrics: use AVG(<column>) not SUM.
"""


def _build_llm_prompt(user_query: str, parsed: dict, schema: str) -> str:
    entity   = parsed.get("entity")
    metric   = parsed.get("metric", "count")
    qt       = parsed.get("query_type", "top_n")
    top_n    = parsed.get("top_n", 5)
    thr      = parsed.get("threshold") or {}
    filters  = parsed.get("filters", {}) or {}
    trs      = parsed.get("time_ranges", [])
    bucket   = parsed.get("time_bucket", "month")

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
            f"    Then check the schema sections 'Uniqueness Profile' and 'Safe Grouping Keys'.\n"
            f"    If {entity} is non-unique or has a mapped key, GROUP BY stable_key + {entity}\n"
            f"    (for example customer_id + customer_name) to avoid merging distinct entities.\n"
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
        rank_instr = f"ORDER BY value DESC\nLIMIT {top_n}"
    elif qt == "bottom_n":
        rank_instr = f"ORDER BY value ASC\nLIMIT {top_n}"
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
        rank_instr = f"Two-period comparison. Use CTEs for each period. ORDER BY value1 DESC LIMIT {top_n}"
    elif qt == "growth_ranking":
        rank_instr = f"Rank by delta = period2_value - period1_value. ORDER BY delta DESC LIMIT {top_n}"
    elif qt == "intersection":
        rank_instr = f"Only entities present in BOTH periods. ORDER BY value DESC LIMIT {top_n}"
    elif qt == "zero_filter":
        rank_instr = "Entities where metric = 0 or entity has no rows in period. ORDER BY name"
    else:
        rank_instr = f"ORDER BY value DESC LIMIT {top_n}"

    date_hints = ""
    if trs:
        for i, tr in enumerate(trs[:2]):
            date_hints += f"\n  Period {i+1}: {tr.get('start')} to {tr.get('end')}"

    if filter_clause:
        filter_rule = (
            f"11. BUSINESS FILTERS — MANDATORY (add these to EVERY query):\n"
            f"    The following WHERE conditions MUST be included.\n"
            f"    They exclude cancelled, deleted, or fraudulent rows and are\n"
            f"    CRITICAL for correct revenue / count figures:\n"
            f"{filter_clause}\n"
            f"    Add them after the date filter:\n"
            f"      WHERE date_col BETWEEN ? AND ?\n"
            f"{filter_clause}\n"
        )
    else:
        filter_rule = "11. No additional business filters required."

    return f"""You are a DuckDB SQL expert. Output ONLY raw SQL — no prose, no markdown fences, no explanation.

User question: "{user_query}"

{schema}

Parsed intent:
  query_type : {qt}
  entity     : {entity}
  metric     : {metric}
  time_bucket: {bucket if qt == 'time_series' else 'N/A'}
  top_n      : {top_n}
  filters    : {filter_desc}{date_hints}

STRICT RULES:
1. Use the EXACT column names from the schema above.
2. Alias the display column as "name" in SELECT.
3. Alias the aggregation as "value".
3b. Use schema uniqueness guidance:
   - Read "Uniqueness Profile" and "Safe Grouping Keys" first.
   - If entity label is non-unique, group using stable key + label.
   - Never collapse different IDs into one row only because names match.
4. DATE FILTER — CRITICAL:
   Use the EXCLUSIVE-RANGE pattern for datetime/timestamp columns:
       col >= ?  AND  col < ?
   where the second ? is the day AFTER the period end.
   Example for "year 2023":
       order_datetime >= ?   -- bound to 2023-01-01
       AND order_datetime < ?  -- bound to 2024-01-01  (exclusive)
   NEVER use BETWEEN for datetime columns — it silently drops the last day.
   NEVER use EXTRACT(YEAR ...) = ? or EXTRACT(MONTH ...) = ?
5. Use ? for ALL date/number placeholders — never hard-code values.
6. No semicolons. No comments. SELECT only.
7. For "count" metric: use COUNT(*) AS value
8. For metric columns:
   - Default: SUM(column_name) AS value
   - If metric starts with "avg_" (e.g. avg_final_price, avg_total_fare):
     use AVG(<column_without_avg_prefix>) AS value
     Example: metric=avg_final_price → AVG(final_price) AS value
   - If metric is "count": use COUNT(DISTINCT <primary_order_id_column>) AS value
     NOT COUNT(*). The table stores order items — each order appears on multiple
     rows. Use the unique order identifier (order_id, ride_id, trip_id, etc.).
9. Ranking: {rank_instr}
{groupby_rule}
{filter_rule}

SQL:"""


def _call_llm(model: str, prompt: str) -> tuple[str, dict]:
    resp = requests.post(
        GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_TOKEN}",
                 "Content-Type": "application/json"},
        json={
            "model":    model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1024, "temperature": 0.0,
        },
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip(), data.get("usage", {})


def _registry_fallback(parsed: dict) -> str | None:
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


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    parsed["_user_query"] = user_query

    qt      = parsed.get("query_type", "top_n")
    filters = parsed.get("filters", {}) or {}

    existing_tables = _get_existing_tables()
    sales_available = _sales_schema_available(existing_tables)
    ctx.logger.info("📋 DB tables found", {
        "queryId":      query_id,
        "tables":       list(existing_tables),
        "sales_schema": sales_available,
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

    force_llm = (
        bool(filters)
        or not sales_available
        or qt == "time_series"
        or parsed.get("entity") not in _BUILDER_ENTITIES
        or parsed.get("metric") not in _BUILDER_METRICS
    )

    if not force_llm:
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

    if generated_sql is None:
        COMPLEX = {"growth_ranking", "comparison", "threshold", "intersection",
                   "zero_filter", "time_series"}
        model      = QWEN_MODEL if qt in COMPLEX else LLAMA_MODEL
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

    if generated_sql is None and sales_available:
        fb = _registry_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source    = "registry_fallback"
            ctx.logger.warn("⚠️ Registry fallback used", {"queryId": query_id})

    if generated_sql is None:
        msg = (
            f"Could not generate SQL for query_type={qt} "
            f"entity={parsed.get('entity')} metric={parsed.get('metric')}. "
            "Try rephrasing — e.g. 'Top 5 restaurants by revenue in 2023'."
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
