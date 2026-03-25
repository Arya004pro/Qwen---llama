"""Step 2: Parse Intent - Qwen extracts full structured intent as JSON."""

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
        "Qwen extracts full structured intent JSON from user query. "
        "Handles all query types including order_count, growth_ranking, "
        "intersection, zero_filter, and percentage thresholds."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}

_SYSTEM = """You are an analytics query parser for business sales data.
Extract the query intent and return ONLY valid JSON - no prose, no markdown.

JSON schema (all fields required):
{
  "entity":       string (grouping dimension, e.g. product/customer/city/doctor/hotel/driver/restaurant),
  "metric":       string (e.g. revenue/sales/amount/gmv/quantity/order_count/bookings/rides),
  "query_type":   one of [top_n, bottom_n, aggregate, threshold, comparison, growth_ranking, intersection, zero_filter],
  "top_n":        integer (default 5, use 1 for "highest/best/worst single"),
  "time_ranges":  array of {start:YYYY-MM-DD, end:YYYY-MM-DD, label:string},
  "threshold":    {value:number, type:absolute|percentage, operator:gt|lt} or null,
  "filters":      object (optional key-value filters inferred from the user query),
  "is_complete":  true or false,
  "clarification_question": null or a single short question
}

Rules:
- entity: infer the primary grouping dimension from query wording.
- metric: infer target business measure from wording; use "revenue" if clearly money-based.
- query_type:
    top_n         -> highest N values
    bottom_n      -> lowest N values
    aggregate     -> single scalar total
    threshold     -> HAVING filter by value or % of total
    comparison    -> two periods side-by-side with delta shown
    growth_ranking-> rank entities BY growth delta between two periods
    intersection  -> entities present in BOTH of two periods
    zero_filter   -> entities with zero sales/activity in period
- time_ranges:
    Single period -> 1 entry
    comparison / growth_ranking / intersection -> 2 entries
- threshold.value: extract the actual number from the query (500, 20, etc.)
- is_complete: false only when grouping dimension or time period is genuinely unknown
- clarification_question: ask only the SINGLE most critical missing field
"""

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_TOPN_RE = re.compile(r"\b(top|bottom)\s+(\d+)\b", re.IGNORECASE)


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return ""


def _default_clarification(user_query: str, parsed: dict) -> str:
    q = user_query.lower()
    if not parsed.get("entity"):
        return "Which group should I analyze (for example customer, city, driver, or ride type)?"
    if not parsed.get("time_ranges"):
        return "What time period should I use? (for example Jan 2024, last 30 days, or 2024)"
    if not parsed.get("metric"):
        if "ride" in q or "trip" in q:
            return "Should I rank by number of rides or total fare?"
        return "What metric should I use? (for example revenue, quantity, or count)"
    return "Please clarify your question with entity, metric, and time period."


def _fallback_parse(user_query: str) -> dict:
    q = user_query.lower()
    entity = ""
    if any(x in q for x in ["customer", "customers", "rider", "riders"]):
        entity = "customer"
    elif any(x in q for x in ["city", "cities"]):
        entity = "city"
    elif any(x in q for x in ["driver", "drivers"]):
        entity = "driver"
    elif any(x in q for x in ["category", "categories"]):
        entity = "category"
    elif any(x in q for x in ["product", "products"]):
        entity = "product"

    metric = "revenue"
    if any(x in q for x in ["ride", "rides", "trip", "trips", "booking", "bookings", "count", "number of"]):
        metric = "order_count"
    elif any(x in q for x in ["quantity", "units", "volume"]):
        metric = "quantity"

    query_type = "top_n"
    top_n = 5
    m = _TOPN_RE.search(q)
    if m:
        query_type = "bottom_n" if m.group(1).lower() == "bottom" else "top_n"
        top_n = int(m.group(2))
    elif any(x in q for x in ["lowest", "least", "bottom"]):
        query_type = "bottom_n"

    parsed = {
        "entity": entity,
        "metric": metric,
        "query_type": query_type,
        "top_n": top_n,
        "time_ranges": [],
        "threshold": None,
        "filters": {},
        "is_complete": False,
        "clarification_question": "",
    }
    parsed["clarification_question"] = _default_clarification(user_query, parsed)
    return parsed


def _call_qwen(user_query: str) -> tuple[dict, dict]:
    resp = requests.post(
        GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_TOKEN}", "Content-Type": "application/json"},
        json={
            "model": QWEN_MODEL,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user_query},
            ],
            "max_tokens": 600,
            "temperature": 0.0,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    raw = data["choices"][0]["message"]["content"].strip()
    raw = _THINK_RE.sub("", raw).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        return json.loads(raw), data.get("usage", {})
    except Exception:
        obj = _extract_json_object(raw)
        if not obj:
            raise
        return json.loads(obj), data.get("usage", {})


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

    if merged_parsed:
        ctx.logger.info("Clarification path", {"queryId": query_id})
        parsed = merged_parsed
    else:
        ctx.logger.info("Qwen intent extraction", {"queryId": query_id, "query": user_query})
        try:
            parsed, usage = _call_qwen(user_query)
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
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "intent_parsed",
                "parsed": parsed,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "intent_parsed": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::ambiguity.check",
            "data": {"queryId": query_id, "query": user_query, "parsed": parsed},
        }
    )
