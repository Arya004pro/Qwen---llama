"""Step 4: Text-to-SQL

Primary path: db/sql_builder.py generates deterministic parameterized SQL
              from Qwen's structured intent for all standard query types.
LLM path:     Only when intent has custom filters (gender, age, state name
              in WHERE, region_id) that require SQL the builder can't produce.

This eliminates LLM SQL hallucination for standard analytics queries.
"""

import os, sys, re, logging

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
from db.duckdb_connection import explain_query

try:
    from db.sql_registry import SQL_REGISTRY
except ImportError:
    SQL_REGISTRY = {}

logger = logging.getLogger(__name__)

_BUILDER_ENTITIES = {"product", "customer", "city", "category", "state"}
_BUILDER_METRICS = {"revenue", "quantity", "order_count"}

config = {
    "name": "TextToSQL",
    "description": (
        "Builds SQL deterministically from Qwen intent for all standard queries. "
        "Uses LLM only for custom-filter queries (gender, age, state conditions)."
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
    n = sql.count("%s") + sql.count("?")
    if n == 0:
        params: list = []
    else:
        d   = date.today().replace(day=1)
        num = 5
        qt  = (parsed or {}).get("query_type","")
        thr = (parsed or {}).get("threshold") or {}
        typ = thr.get("type","")
        if qt == "threshold" and typ == "percentage" and n == 5:
            params = [d, d, num, d, d]      # start,end,pct_val,start,end
        elif qt == "threshold" and typ == "absolute" and n == 3:
            params = [d, d, num]             # start,end,threshold_val
        elif qt in ("comparison","growth_ranking") and n == 5:
            params = [d, d, d, d, num]       # start1,end1,start2,end2,limit
        else:
            params = [d if i < n - 1 else num for i in range(n)]
    return explain_query(sql, params)


def _build_llm_prompt(user_query: str, parsed: dict) -> str:
    """Used when deterministic builder is not suitable."""
    filters = parsed.get("filters", {})
    filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items()) if filters else "none"
    return f"""You are a DuckDB SQL expert. Output ONLY raw SQL — no prose, no markdown.

User question: "{user_query}"

{get_schema_prompt()}

Intent: {parsed}

Custom filters to apply: {filter_desc}

Build a SELECT query that:
- Aliases the primary group-by column as "name"
- Aliases the metric aggregation as "value"
- Applies the custom filters in the WHERE clause
- Uses ? for ALL date/number placeholders
- No semicolons, no comments

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
    """Emergency fallback for SIMPLE queries only (top/bottom/aggregate).
    Never returns SQL for complex types — those must use the builder."""
    qt = parsed.get("query_type", "top_n")
    if qt not in ("top_n", "bottom_n", "aggregate"):
        return None   # complex queries must not fall back to wrong SQL
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

    # Stash user query in parsed so builder can check sort direction
    parsed["_user_query"] = user_query

    qt      = parsed.get("query_type", "top_n")
    filters = parsed.get("filters", {})

    ctx.logger.info("🧬 TextToSQL", {"queryId": query_id, "query_type": qt,
                                      "has_filters": bool(filters)})

    generated_sql = None
    usage         = {}
    fallback_used = False
    sql_source    = "builder"

    # ── Primary path: deterministic builder ───────────────────────────────────
    force_llm = (
        bool(filters)
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
                    ctx.logger.warn("⚠️ Builder EXPLAIN failed", {"queryId": query_id, "error": err})
            else:
                ctx.logger.warn("⚠️ Builder safety failed", {"queryId": query_id, "reason": reason})

    # ── LLM path: custom filters OR builder failed ─────────────────────────────
    if generated_sql is None:
        # Complex queries → Qwen; simple → LLaMA
        COMPLEX = {"growth_ranking", "comparison", "threshold", "intersection", "zero_filter"}
        model   = QWEN_MODEL if qt in COMPLEX else LLAMA_MODEL
        sql_source = f"llm_{model.split('/')[0]}"

        ctx.logger.info("🤖 LLM SQL generation", {"queryId": query_id, "model": model})

        try:
            prompt      = _build_llm_prompt(user_query, parsed)
            raw, usage  = _call_llm(model, prompt)
            ctx.logger.info("🔬 LLM raw", {"queryId": query_id, "preview": raw[:400]})

            sql = _extract_sql(raw)
            ok, reason = _is_safe(sql)
            if ok:
                ok2, err = _explain(sql, parsed)
                if ok2:
                    generated_sql = sql
                    ctx.logger.info("✅ LLM SQL validated", {"queryId": query_id})
                else:
                    ctx.logger.warn("⚠️ LLM EXPLAIN failed", {"queryId": query_id, "error": err})
            else:
                ctx.logger.warn("⚠️ LLM safety failed", {"queryId": query_id, "reason": reason})
        except Exception as exc:
            ctx.logger.error("❌ LLM error", {"queryId": query_id, "error": str(exc)})

        if usage:
            log_tokens(ctx, query_id, "TextToSQL", model, usage)
            await add_tokens_to_state(ctx, query_id, "TextToSQL", model, usage)

    # ── Emergency registry fallback (simple queries only) ─────────────────────
    if generated_sql is None:
        fb = _registry_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source    = "registry_fallback"
            ctx.logger.warn("⚠️ Registry fallback used", {"queryId": query_id})
        else:
            msg = (
                f"Could not generate SQL for query_type={qt} "
                f"entity={parsed.get('entity')} metric={parsed.get('metric')}."
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
