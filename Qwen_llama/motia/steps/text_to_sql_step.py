"""Step 4: Text-to-SQL — fully self-contained, no project imports needed.

All logic is inlined: schema definition, SQL extraction, safety check, EXPLAIN,
Qwen/LLaMA routing. The only external imports are motia, requests, psycopg2,
and shared_config (for credentials only).

Hot-reloads automatically — no Docker rebuild needed.
"""

import os, sys, re, logging

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests
import psycopg2
from datetime import date
from typing import Any
from motia import FlowContext, queue

from shared_config import GROQ_API_TOKEN, LLAMA_MODEL, QWEN_MODEL, GROQ_URL, POSTGRES
from utils.token_logger import log_tokens, add_tokens_to_state

# SQL_REGISTRY import — only used for non-threshold fallback
try:
    from db.sql_registry import SQL_REGISTRY
except ImportError:
    SQL_REGISTRY = {}

logger = logging.getLogger(__name__)

config = {
    "name": "TextToSQL",
    "description": (
        "Generates a validated parameterized SQL query from intent using an LLM. "
        "Threshold queries use Qwen; all others use LLaMA with SQL_REGISTRY fallback."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::text.to.sql")],
    "enqueues": ["query::execute"],
}

# ── Inlined DB schema (no db/schema_context.py import needed) ─────────────────
_SCHEMA = """
Database schema
---------------
orders        : order_id, customer_id, order_date DATE, total_amount NUMERIC
order_items   : order_item_id, order_id, product_id, quantity INT, item_price NUMERIC
products      : product_id, product_name, category_id
categories    : category_id, category_name
customers     : customer_id, customer_name, city_id
cities        : city_id, city_name

Revenue rules
-------------
Product/category revenue = SUM(oi.quantity * oi.item_price)
Customer/city revenue    = SUM(o.total_amount)
Quantity                 = SUM(oi.quantity)
Always filter: o.order_date BETWEEN %s AND %s

Param order: ranked=(start, end, limit::int)  aggregate/threshold=(start, end)
Use %s for ALL placeholders — never hard-code dates or numbers.
LIMIT clause must be written as: LIMIT %s
"""

# ── Regex ──────────────────────────────────────────────────────────────────────
_THINK_RE    = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_SQL_LINE_RE = re.compile(r"(?im)^(WITH|SELECT)\b")
_FENCE_RE    = re.compile(r"```(?:sql)?\s*\n?(.*?)```", re.DOTALL | re.IGNORECASE)
_FORBIDDEN   = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|COPY|VACUUM|ANALYZE|CALL|DO)\b",
    re.IGNORECASE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _extract_sql(raw: str) -> str:
    text = _THINK_RE.sub("", raw).strip()          # strip <think> FIRST
    fence = _FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    m = _SQL_LINE_RE.search(text)                  # find first line-start WITH/SELECT
    if m:
        text = text[m.start():]
    text = re.sub(r"(--[^\n]*|/\*.*?\*/)", "", text, flags=re.DOTALL)
    return text.strip().rstrip(";")


def _is_safe(sql: str) -> tuple[bool, str]:
    if not _SQL_LINE_RE.match(sql):
        return False, f"no WITH/SELECT at line start — got: {sql[:80]!r}"
    m = _FORBIDDEN.search(sql)
    if m:
        return False, f"forbidden keyword: {m.group()!r}"
    if ";" in sql:
        return False, "contains semicolon"
    return True, ""


def _explain(sql: str, ranking: str) -> tuple[bool, str]:
    if ranking == "threshold":
        return True, ""  # param count varies in subqueries — checked at runtime
    d0, d1, lim = date(2024, 1, 1), date(2024, 1, 31), 5
    n = sql.count("%s")
    if n == 0:
        params: tuple = ()
    elif n == 3 and ranking in ("top", "bottom"):
        params = (d0, d1, lim)
    else:
        slots = []
        for i in range(n):
            if i == n - 1 and ranking in ("top", "bottom") and n % 2 == 1:
                slots.append(lim)
            else:
                slots.append(d0 if i % 2 == 0 else d1)
        params = tuple(slots)
    try:
        conn = psycopg2.connect(**POSTGRES)
        cur  = conn.cursor()
        cur.execute(f"EXPLAIN {sql}", params)
        cur.close()
        conn.close()
        return True, ""
    except Exception as exc:
        return False, str(exc).split("\n")[0]


def _build_prompt(entity, metric, ranking, top_n, raw_query) -> str:
    rank_instr = {
        "top":       f"Return top {top_n} rows.\nEnd with: ORDER BY value DESC\nLIMIT %s",
        "bottom":    f"Return bottom {top_n} rows.\nEnd with: ORDER BY value ASC\nLIMIT %s",
        "aggregate": "Return ONE scalar row, one column aliased 'value'. No GROUP BY, ORDER BY, or LIMIT.",
        "threshold": (
            "Use a HAVING clause from the user question.\n"
            "For '>10% of total revenue' write:\n"
            "  HAVING SUM(...) > 0.10 * (\n"
            "    SELECT SUM(oi2.quantity * oi2.item_price)\n"
            "    FROM order_items oi2\n"
            "    JOIN orders o2 ON oi2.order_id = o2.order_id\n"
            "    WHERE o2.order_date BETWEEN %s AND %s\n"
            "  )\n"
            "Alias group-by col as 'name', metric as 'value'. End ORDER BY value DESC. NO LIMIT."
        ),
    }.get(ranking, f"Return top {top_n} rows ORDER BY value DESC LIMIT %s")

    user_line = f'User question: "{raw_query}"\n' if raw_query else ""

    return (
        "You are a PostgreSQL expert. Output ONLY raw SQL — no prose, no markdown, no explanation.\n"
        f"{user_line}\n"
        f"{_SCHEMA}\n"
        f"Task: entity={entity}  metric={metric}  ranking={ranking}\n\n"
        "Rules:\n"
        "- Alias group-by column as  name  and aggregation as  value\n"
        "- Use %s for ALL date/limit placeholders\n"
        f"- {rank_instr}\n\n"
        "SQL:"
    )


def _call_llm(model: str, prompt_text: str) -> tuple[str, dict]:
    resp = requests.post(
        GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_TOKEN}", "Content-Type": "application/json"},
        json={"model": model, "messages": [{"role": "user", "content": prompt_text}],
              "max_tokens": 1024, "temperature": 0.0},
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip(), data.get("usage", {})


# ── Handler ────────────────────────────────────────────────────────────────────
async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    entity  = parsed.get("entity")
    metric  = parsed.get("metric")
    ranking = parsed.get("ranking") or "top"
    top_n   = parsed.get("top_n", 5)

    model = QWEN_MODEL if ranking == "threshold" else LLAMA_MODEL

    ctx.logger.info("🧬 TextToSQL", {
        "queryId": query_id, "model": model,
        "entity": entity, "metric": metric, "ranking": ranking,
    })

    prompt_text   = _build_prompt(entity, metric, ranking, top_n, user_query)
    generated_sql = None
    usage         = {}

    try:
        raw, usage = _call_llm(model, prompt_text)

        ctx.logger.info("🔬 LLM raw", {"queryId": query_id, "preview": raw[:400]})

        sql = _extract_sql(raw)
        ctx.logger.info("🔬 Extracted SQL", {"queryId": query_id, "sql": sql})

        ok, reason = _is_safe(sql)
        if not ok:
            ctx.logger.warn("⚠️ Safety FAILED", {"queryId": query_id, "reason": reason})
        else:
            ok2, err = _explain(sql, ranking)
            if not ok2:
                ctx.logger.warn("⚠️ EXPLAIN FAILED", {"queryId": query_id, "error": err, "sql": sql})
            else:
                generated_sql = sql
                ctx.logger.info("✅ SQL validated", {"queryId": query_id})

    except Exception as exc:
        ctx.logger.error("❌ LLM call error", {"queryId": query_id, "error": str(exc)})

    if usage:
        log_tokens(ctx, query_id, "TextToSQL", model, usage)
        await add_tokens_to_state(ctx, query_id, "TextToSQL", model, usage)

    # ── Fallback / error path ──────────────────────────────────────────────────
    used_fallback = False
    if generated_sql is None:
        if ranking == "threshold":
            msg = (
                f"Threshold SQL generation failed (entity={entity}, metric={metric}). "
                "Open http://localhost:3113/logs and search your queryId to see "
                "the LLM raw output and exact rejection reason."
            )
            qs = await ctx.state.get("queries", query_id)
            if qs:
                await ctx.state.set("queries", query_id, {**qs, "status": "error", "error": msg})
            return

        try:
            _ = SQL_REGISTRY[entity][metric][ranking]
            used_fallback = True
            ctx.logger.warn("⚠️ Registry fallback", {"queryId": query_id})
        except (KeyError, TypeError):
            msg = f"No SQL for entity={entity} metric={metric} ranking={ranking}"
            qs = await ctx.state.get("queries", query_id)
            if qs:
                await ctx.state.set("queries", query_id, {**qs, "status": "error", "error": msg})
            return

    qs = await ctx.state.get("queries", query_id)
    if qs:
        await ctx.state.set("queries", query_id, {
            **qs,
            "status":        "sql_generated",
            "generated_sql": generated_sql,
            "sql_fallback":  used_fallback,
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