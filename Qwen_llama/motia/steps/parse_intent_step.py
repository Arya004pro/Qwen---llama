"""Step 2: Parse Intent — Qwen extracts full structured intent as JSON.

No keyword matching in Python. Qwen understands the natural language and
returns a structured JSON covering ALL query types:
  top_n, bottom_n, aggregate, threshold (absolute + percentage),
  comparison, growth_ranking, intersection, zero_filter.

Intent JSON schema
------------------
{
  "entity":                  "product|customer|city|category",
  "metric":                  "revenue|quantity|order_count",
  "query_type":              "top_n|bottom_n|aggregate|threshold|comparison|
                              growth_ranking|intersection|zero_filter",
  "top_n":                   5,
  "time_ranges": [
    {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "label": "human label"}
  ],
  "threshold": {"value": 500, "type": "absolute|percentage", "operator": "gt|lt"} | null,
  "is_complete":             true | false,
  "clarification_question":  null | "string"
}
"""

import os, sys, re, json
from datetime import datetime, timezone

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests
from typing import Any
from motia import FlowContext, queue
from shared_config import GROQ_API_TOKEN, QWEN_MODEL, GROQ_URL
from utils.token_logger import log_tokens, add_tokens_to_state

config = {
    "name": "ParseIntent",
    "description": (
        "Qwen extracts full structured intent JSON from user query. "
        "Handles all query types including order_count, growth_ranking, "
        "intersection, zero_filter, and percentage thresholds."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}

_SYSTEM = """You are an analytics query parser for a sales database.
Extract the query intent and return ONLY valid JSON — no prose, no markdown.

JSON schema (all fields required):
{
  "entity":       one of [product, customer, city, category, state],
  "metric":       one of [revenue, quantity, order_count],
  "query_type":   one of [top_n, bottom_n, aggregate, threshold, comparison, growth_ranking, intersection, zero_filter],
  "top_n":        integer (default 5, use 1 for "highest/best/worst single"),
  "time_ranges":  array of {start:YYYY-MM-DD, end:YYYY-MM-DD, label:string},
  "threshold":    {value:number, type:absolute|percentage, operator:gt|lt} or null,
  "filters":      {} or {"gender":"Male|Female","age_min":int,"age_max":int,"state":"name","region_id":int} — optional demographic/geographic filters,
  "is_complete":  true or false,
  "clarification_question": null or a single short question
}

Rules:
- entity: infer from context (product/customer/city/category); default product
- metric:
    revenue      → money (SUM of amounts)
    quantity     → units sold (SUM of quantities)
    order_count  → number of orders placed (COUNT of orders) — use when query says "orders placed", "most orders", "number of orders"
    Note: gender/age are filter dimensions on customers, not separate metrics.
    For "revenue from female customers" → entity=customer, metric=revenue, and the SQL WHERE should include customers.gender = 'Female'.
    Capture such filters in a "filters" field: {"gender":"Female"} or {"age_min":25,"age_max":40}
- query_type:
    top_n         → highest N values
    bottom_n      → lowest N values
    aggregate     → single scalar total
    threshold     → HAVING filter by value or % of total
    comparison    → two periods side-by-side with delta shown
    growth_ranking→ rank entities BY growth delta between two periods
    intersection  → entities present in BOTH of two periods
    zero_filter   → entities with zero sales/activity in period
- time_ranges:
    Single period → 1 entry
    comparison / growth_ranking / intersection → 2 entries
    Q1=Jan 1–Mar 31, Q2=Apr 1–Jun 30, Q3=Jul 1–Sep 30, Q4=Oct 1–Dec 31
    "first half" = Jan 1–Jun 30, "second half" = Jul 1–Dec 31
    Named month → first day to last day of that month
- threshold.value: extract the actual number from the query (500, 20, etc.)
- is_complete: false only when entity or time period is genuinely unknown
- clarification_question: ask only the SINGLE most critical missing field
"""

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _call_qwen(user_query: str) -> tuple[dict, dict]:
    resp = requests.post(
        GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_TOKEN}",
                 "Content-Type": "application/json"},
        json={
            "model": QWEN_MODEL,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": user_query},
            ],
            "max_tokens": 600, "temperature": 0.0,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data  = resp.json()
    raw   = data["choices"][0]["message"]["content"].strip()
    raw   = _THINK_RE.sub("", raw).strip()
    raw   = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw   = re.sub(r"\s*```$", "", raw).strip()
    return json.loads(raw), data.get("usage", {})


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

    if merged_parsed:
        ctx.logger.info("🔄 Clarification path", {"queryId": query_id})
        parsed = merged_parsed
    else:
        ctx.logger.info("🔍 Qwen intent extraction", {"queryId": query_id, "query": user_query})
        try:
            parsed, usage = _call_qwen(user_query)
            log_tokens(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            await add_tokens_to_state(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            ctx.logger.info("✅ Intent", {"queryId": query_id, "parsed": parsed})
        except Exception as exc:
            ctx.logger.error("❌ Intent parse failed", {"error": str(exc), "queryId": query_id})
            parsed = {
                "entity": None, "metric": "revenue", "query_type": "top_n",
                "top_n": 5, "time_ranges": [], "threshold": None,
                "is_complete": False,
                "clarification_question": "Could you rephrase your question?",
            }

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs, "status": "intent_parsed", "parsed": parsed,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "intent_parsed": now_iso},
        })

    await ctx.enqueue({
        "topic": "query::ambiguity.check",
        "data":  {"queryId": query_id, "query": user_query, "parsed": parsed},
    })
