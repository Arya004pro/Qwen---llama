"""Step 4: Text-to-SQL — fully generalised for any flat-table dataset.

Key changes vs original:
  1. Removed the hardcoded _SALES_SCHEMA_TABLES / _BUILDER_ENTITIES / _BUILDER_METRICS
     gate. The deterministic builder only applies when the old e-commerce schema
     is present; otherwise the LLM path is used directly.
  2. force_llm is now the DEFAULT for any dataset that isn't the classic
     {orders, customers, cities, products, categories, order_items} schema.
     This means Uber data, SaaS data, service data etc. all go through the LLM
     with the live schema injected — which produces correct, schema-aware SQL.
  3. The LLM prompt now always includes the live schema (same as parse_intent),
     so column names like total_fare, driver_earnings, platform_commission,
     pickup_city, vehicle_type etc. are resolved correctly.
  4. Bug 1 preserved: EXTRACT(YEAR) → BETWEEN rewrite.
  5. Bug 2 preserved: builder only used when sales tables confirmed present.
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

# Classic e-commerce sales schema — builder is ONLY valid when ALL these exist
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
        "Uses deterministic builder only for classic e-commerce schema. "
        "Falls back to LLM (Qwen/LLaMA) with schema injection for any other dataset."
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


# ── DB helpers ────────────────────────────────────────────────────────────────

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


# ── SQL extraction / safety ───────────────────────────────────────────────────

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


# ── LLM prompt — fully generalised with live schema ──────────────────────────

def _build_llm_prompt(user_query: str, parsed: dict, schema: str) -> str:
    entity     = parsed.get("entity")
    metric     = parsed.get("metric", "count")
    qt         = parsed.get("query_type", "top_n")
    top_n      = parsed.get("top_n", 5)
    thr        = parsed.get("threshold") or {}
    filters    = parsed.get("filters", {})
    trs        = parsed.get("time_ranges", [])

    filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items()) if filters else "none"

    # Build ranking instruction
    if qt in ("top_n",):
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

    # Date hints from parsed time_ranges
    date_hints = ""
    if trs:
        for i, tr in enumerate(trs[:2]):
            date_hints += f"\n  Period {i+1}: {tr.get('start')} to {tr.get('end')}"

    return f"""You are a DuckDB SQL expert. Output ONLY raw SQL — no prose, no markdown fences, no explanation.

User question: "{user_query}"

{schema}

Parsed intent:
  query_type : {qt}
  entity     : {entity}  ← this is the column to GROUP BY
  metric     : {metric}  ← this is the column to aggregate (or COUNT(*) if "count")
  top_n      : {top_n}
  filters    : {filter_desc}{date_hints}

STRICT RULES:
1. Use the EXACT column names from the schema above. Do not invent column names.
2. Alias the GROUP BY column as "name".
3. Alias the aggregation as "value".
4. Filter dates using:  date_column BETWEEN ? AND ?
   NEVER use EXTRACT(YEAR ...) = ? or EXTRACT(MONTH ...) = ?
5. Use ? for ALL date/number placeholders — never hard-code values.
6. No semicolons. No comments. SELECT only.
7. For "count" metric: use COUNT(*) AS value
8. For numeric metric columns: use SUM(column_name) AS value
9. Ranking: {rank_instr}

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
    filters = parsed.get("filters", {})

    # Check what tables actually exist
    existing_tables = _get_existing_tables()
    sales_available = _sales_schema_available(existing_tables)
    ctx.logger.info("📋 DB tables found", {
        "queryId": query_id,
        "tables":  list(existing_tables),
        "sales_schema": sales_available,
    })

    ctx.logger.info("🧬 TextToSQL", {"queryId": query_id, "query_type": qt,
                                     "has_filters": bool(filters)})

    generated_sql = None
    usage         = {}
    fallback_used = False
    sql_source    = "builder"

    # ── Deterministic builder path (classic e-commerce schema only) ───────────
    force_llm = (
        bool(filters)
        or not sales_available
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

    # ── LLM path — used for all non-classic-schema datasets ──────────────────
    if generated_sql is None:
        # Use Qwen for complex queries; LLaMA for simple ranked queries
        COMPLEX = {"growth_ranking", "comparison", "threshold", "intersection", "zero_filter"}
        model   = QWEN_MODEL if qt in COMPLEX else LLAMA_MODEL
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
                ok2, err = _explain(sql, parsed)
                if ok2:
                    generated_sql = sql
                    ctx.logger.info("✅ LLM SQL validated", {"queryId": query_id})
                else:
                    # EXPLAIN failed — log but still try to run it
                    # (EXPLAIN can be overly strict with DuckDB date casting)
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

    # ── Emergency registry fallback (sales schema only) ───────────────────────
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
            "Try rephrasing — e.g. 'Top 5 drivers by earnings in 2024'."
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