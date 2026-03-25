"""Step 2: Parse Intent — Qwen extracts full structured intent as JSON.

Fix applied (Bug 3):
  - System prompt now explicitly states: for query_type=aggregate,
    entity is optional and is_complete=true when metric + time range known.
  - Inline city/state/driver mentions go into filters{}, not entity grouping.
  - ambiguity_check fallback also guards: aggregate + metric + time = complete.
"""

import os
import sys
import re
import json
from datetime import datetime, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import requests
from motia import FlowContext, queue
from shared_config import GROQ_API_TOKEN, QWEN_MODEL, GROQ_URL
from utils.token_logger import log_tokens, add_tokens_to_state

config = {
    "name": "ParseIntent",
    "description": (
        "Qwen extracts full structured intent JSON. "
        "Handles aggregate-without-entity, filter-embedded locations, "
        "and all query types correctly."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}

# ── System prompt (Bug 3 fix) ─────────────────────────────────────────────────
_SYSTEM = """You are an analytics query parser for business data.
Extract the query intent and return ONLY valid JSON — no prose, no markdown.

JSON schema (all fields required):
{
  "entity":       string or null,
  "metric":       string (e.g. revenue/total_fare/driver_earnings/quantity/order_count/rides/bookings),
  "query_type":   one of [top_n, bottom_n, aggregate, threshold, comparison, growth_ranking, intersection, zero_filter],
  "top_n":        integer (default 5, use 1 for "highest/best/worst single entity"),
  "time_ranges":  array of {start:YYYY-MM-DD, end:YYYY-MM-DD, label:string},
  "threshold":    {value:number, type:absolute|percentage, operator:gt|lt} or null,
  "filters":      object (key-value filters extracted from the query),
  "is_complete":  true or false,
  "clarification_question": null or a single short question
}

ENTITY RULES:
- entity = the PRIMARY dimension to GROUP BY (e.g. driver, customer, city, state, vehicle_type).
- For query_type=aggregate (scalar total): entity MUST be null. Do not ask for entity.
- If the user names a specific city/state/driver/vehicle inline (e.g. "in Mumbai",
  "for Gujarat", "SUV rides", "by Sedan"), put it in filters:{} — do NOT use it as entity.
  Example: "How many rides in Mumbai in 2023?" → entity=null, query_type=aggregate,
  filters:{"pickup_city":"Mumbai"}, is_complete=true (metric + time known).
- Only set is_complete=false when the grouping dimension for a ranked query is unknown,
  OR when the time period is genuinely missing.

COMPLETENESS RULES:
- query_type=aggregate + metric known + time_ranges known → is_complete=true (entity not needed)
- query_type=top_n/bottom_n + entity unknown → is_complete=false, ask for entity
- Any query_type + time_ranges empty → is_complete=false, ask for time period
- NEVER ask for entity when query_type=aggregate. NEVER.

QUERY TYPE RULES:
- top_n         → highest N values, one entity group
- bottom_n      → lowest N values
- aggregate     → single scalar total/average/count (no grouping)
- threshold     → HAVING filter by value or % of total
- comparison    → two periods side-by-side with delta
- growth_ranking→ rank entities BY growth delta between two periods
- intersection  → entities present in BOTH of two periods
- zero_filter   → entities with zero activity in period

TIME RANGES:
- Single period → 1 entry
- comparison / growth_ranking / intersection → 2 entries
- For year-only queries (e.g. "in 2024"): start=2024-01-01, end=2024-12-31
- For quarter queries (e.g. "Q1 2024"): start=2024-01-01, end=2024-03-31

METRIC INFERENCE:
- "earnings", "driver earnings" → driver_earnings
- "fare", "total fare", "revenue", "amount" → total_fare (or revenue for sales data)
- "commission", "platform commission" → platform_commission
- "rides", "trips", "bookings", "number of rides", "count" → order_count
- "quantity", "units" → quantity

clarification_question: ask ONLY the single most critical missing field.
For aggregate queries, NEVER ask about entity — only ask about missing time period.
"""

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_TOPN_RE  = re.compile(r"\b(top|bottom)\s+(\d+)\b", re.IGNORECASE)


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:       esc = False
            elif ch == "\\": esc = True
            elif ch == '"':  in_str = False
            continue
        if ch == '"':   in_str = True; continue
        if ch == "{":   depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return ""


def _default_clarification(user_query: str, parsed: dict) -> str:
    qt = parsed.get("query_type", "top_n")
    if qt == "aggregate":
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("entity"):
        return "Which group should I analyze (for example customer, city, driver, or vehicle type)?"
    if not parsed.get("time_ranges"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("metric"):
        return "What metric should I use? (for example revenue, rides, or driver earnings)"
    return "Please clarify: which entity, metric, and time period do you want?"


def _fallback_parse(user_query: str) -> dict:
    q  = user_query.lower()
    qt = "top_n"
    entity = None
    top_n  = 5

    if any(x in q for x in ["driver", "drivers"]):       entity = "driver"
    elif any(x in q for x in ["customer", "customers"]):  entity = "customer"
    elif any(x in q for x in ["city", "cities"]):         entity = "city"
    elif any(x in q for x in ["state", "states"]):        entity = "state"
    elif any(x in q for x in ["product", "products"]):    entity = "product"
    elif any(x in q for x in ["category", "categories"]): entity = "category"

    metric = "revenue"
    if any(x in q for x in ["ride", "rides", "trip", "trips", "booking", "count"]):
        metric = "order_count"
    elif any(x in q for x in ["earnings", "driver earn"]):
        metric = "driver_earnings"
    elif any(x in q for x in ["commission"]):
        metric = "platform_commission"
    elif any(x in q for x in ["fare", "total fare"]):
        metric = "total_fare"

    # If no entity and question sounds like aggregate
    if entity is None and any(x in q for x in ["total", "how much", "how many", "average", "what was"]):
        qt = "aggregate"

    m = _TOPN_RE.search(q)
    if m:
        qt    = "bottom_n" if m.group(1).lower() == "bottom" else "top_n"
        top_n = int(m.group(2))
    elif any(x in q for x in ["lowest", "least", "bottom"]):
        qt = "bottom_n"

    is_complete = bool(metric) and qt == "aggregate"
    parsed = {
        "entity": entity, "metric": metric, "query_type": qt,
        "top_n": top_n, "time_ranges": [], "threshold": None,
        "filters": {}, "is_complete": is_complete,
        "clarification_question": "",
    }
    if not is_complete:
        parsed["clarification_question"] = _default_clarification(user_query, parsed)
    return parsed


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
            "max_tokens": 700, "temperature": 0.0,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    raw  = data["choices"][0]["message"]["content"].strip()
    raw  = _THINK_RE.sub("", raw).strip()
    raw  = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw  = re.sub(r"\s*```$", "", raw).strip()
    try:
        return json.loads(raw), data.get("usage", {})
    except Exception:
        obj = _extract_json_object(raw)
        if not obj:
            raise
        return json.loads(obj), data.get("usage", {})


def _post_process(parsed: dict) -> dict:
    """
    Safety net: enforce completeness rules that Qwen might miss.
    Bug 3 guard: aggregate + metric + time_ranges → always complete.
    """
    qt = parsed.get("query_type", "top_n")
    tr = parsed.get("time_ranges", [])
    m  = parsed.get("metric")

    if qt == "aggregate" and m and tr:
        parsed["is_complete"]            = True
        parsed["clarification_question"] = None
        parsed["entity"]                 = parsed.get("entity")  # keep null if null

    return parsed


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

    if merged_parsed:
        ctx.logger.info("Clarification path", {"queryId": query_id})
        parsed = merged_parsed
    else:
        ctx.logger.info("Qwen intent extraction", {"queryId": query_id, "query": user_query})
        try:
            parsed, usage = _call_qwen(user_query)
            parsed = _post_process(parsed)
            log_tokens(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            await add_tokens_to_state(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            ctx.logger.info("Intent parsed", {"queryId": query_id, "parsed": parsed})
        except Exception as exc:
            ctx.logger.error("Intent parse failed", {"error": str(exc), "queryId": query_id})
            parsed = _fallback_parse(user_query)

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