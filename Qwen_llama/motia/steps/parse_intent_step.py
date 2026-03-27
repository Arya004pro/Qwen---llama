"""Step 2: Parse Intent — Qwen extracts full structured intent as JSON.

Generalised to work on ANY flat-table dataset (Uber, Zomato, e-commerce, SaaS, etc.).

Key changes vs original:
  - Injected live schema into system prompt so Qwen sees EXACT column names.
  - Added time_series query_type for trend / month-wise / weekly queries.
  - METRIC RULES section updated: maps user words → exact DB column names,
    prefers post-discount revenue columns (final_price > total_fare > total_amount).
  - BUSINESS FILTER awareness: Qwen now knows to set filters:{is_cancelled:0}
    when a cancellation column is present — so SQL generation can apply it.
  - _post_process() injects mandatory filters detected from live schema.
"""

import os
import sys
import re
import json
import calendar
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
from utils.time_parser import parse_time_ranges_from_query
from db.schema_context import get_schema_prompt
from db.duckdb_connection import get_read_connection
from db.semantic_layer import resolve_intent_with_semantic_layer

config = {
    "name": "ParseIntent",
    "description": (
        "Qwen extracts full structured intent JSON from any flat-table dataset. "
        "Schema columns are injected so entity/metric names match actual DB columns. "
        "Supports time_series (trend) queries grouped by month/week/quarter/day. "
        "Auto-injects mandatory business filters (e.g. is_cancelled=0)."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}

# ── System prompt ─────────────────────────────────────────────────────────────
_SYSTEM_TEMPLATE = """You are an analytics query parser for business data stored in a database.

The database has the following schema:
{schema}

Your job: extract the user's query intent and return ONLY valid JSON — no prose, no markdown fences.

JSON schema (all fields required):
{{
  "entity":       string or null,
  "metric":       string (semantic metric or physical column; resolver will map to real column),
  "query_type":   one of [top_n, bottom_n, aggregate, threshold, comparison, growth_ranking, intersection, zero_filter, time_series],
  "time_bucket":  one of [month, week, quarter, day] or null  (ONLY for time_series queries),
  "top_n":        integer (default 5; use 1 for "highest/best/worst/which single entity"),
  "time_ranges":  array of {{start:YYYY-MM-DD, end:YYYY-MM-DD, label:string}},
  "threshold":    {{value:number, type:absolute|percentage, operator:gt|lt}} or null,
  "filters":      object of key-value pairs for any WHERE conditions mentioned,
  "is_complete":  true or false,
  "clarification_question": null or a single short question string
}}

SEMANTIC LAYER RULES (FOUNDATION):
- The schema includes a Semantic Layer section with:
    - Dimensions (business name -> physical label/key columns)
    - Metrics (business metric -> SQL aggregation on physical columns)
- Prefer semantic business terms first (e.g. "revenue", "customers", "warehouses").
- You may output semantic entity/metric names; post-processing will resolve them.
- If both semantic and physical names are available, prefer semantic meaning that best matches user intent.

TIME SERIES RULES (CRITICAL):
- Use query_type=time_series when the user asks for a TREND or TIME-BUCKETED breakdown:
    "month-wise revenue", "monthly trend", "weekly sales", "quarterly earnings",
    "how did revenue change over time", "revenue by month", "per month breakdown",
    "day-wise rides", "quarterly comparison over year"
- For time_series: entity MUST be null (replaced by the time bucket in SQL).
- For time_series: set time_bucket to: month | week | quarter | day
- For time_series: is_complete=true as long as metric and time_ranges are known.
- NEVER ask for a business dimension (driver, city, etc.) for time_series queries.

ENTITY RULES:
- entity = the DISPLAY column to show in results — ALWAYS use the human-readable
  NAME column, NEVER a raw ID column.
  Good: driver_name, customer_name, product_name, pickup_city, vehicle_type, city, restaurant_name, item_name
  Bad:  driver_id, customer_id, product_id, order_id
- For query_type=aggregate or time_series: entity MUST be null.
- If the user names a specific filter value inline (e.g. "in Mumbai"),
  put it in filters:{{column_name: value}} — do NOT use it as entity.

METRIC RULES — map user language to EXACT column names from the schema above:
- "revenue", "sales", "earnings", "income", "money", "amount"
    → prefer final_price if it exists (post-discount actual revenue)
    → else total_fare, driver_earnings, total_amount, revenue — in that preference order
    → NEVER use unit_price alone (that's the pre-discount list price)
- "average order value", "average fare", "average revenue", "avg order", "mean order"
    → metric = "avg_final_price"  (use AVG(final_price) in SQL — NOT SUM)
- "average fare", "avg fare", "mean fare"
    → metric = "avg_total_fare"   (use AVG(total_fare) in SQL)
- "average earnings", "avg earnings"
    → metric = "avg_driver_earnings" (use AVG(driver_earnings) in SQL)
- "driver earnings", "driver income"  → driver_earnings
- "platform commission", "commission" → platform_commission
- "fare", "trip cost", "ride cost"    → total_fare  (else final_price)
- "discount"                          → discount
- "rides", "trips", "bookings", "orders", "count of", "number of", "how many"
    → metric = "count"
    → IMPORTANT: use COUNT(DISTINCT <order_id_column>) NOT COUNT(*)
      because each row may be an ORDER ITEM (one order has many rows).
      Look for the primary order identifier column (e.g. order_id, ride_id)
      and use COUNT(DISTINCT that_column) AS value
- "quantity", "units sold", "items"   → quantity  (SUM(quantity))
- "distance"                          → distance_km  (else the nearest distance column)
- "duration", "time taken"            → duration_min  (else nearest duration column)
- When in doubt for a food/delivery dataset: default metric = final_price

BUSINESS FILTER RULES — CRITICAL for accuracy:
- If the schema has an is_cancelled column, always include it in filters:
    filters: {{"is_cancelled": 0}}
  This ensures cancelled orders are EXCLUDED from revenue / count totals.
- If the schema has an is_deleted, cancelled, is_refunded column, add them to filters too.
- If there is a status/order_status column, set filters to keep only completed/delivered rows.
- These filters are MANDATORY — without them the revenue figures will be inflated.

COMPLETENESS RULES:
- query_type=aggregate + metric known + time_ranges known → is_complete=true
- query_type=time_series + metric known + time_ranges known → is_complete=true (NO entity needed)
- query_type=top_n/bottom_n + entity unknown → is_complete=false, ask for entity
- Any query_type + time_ranges empty → is_complete=false, ask for time period
- NEVER ask for entity when query_type=aggregate or time_series. NEVER.

QUERY TYPE RULES:
- top_n         → highest N values for a grouped entity
- bottom_n      → lowest N values
- aggregate     → single scalar total/average/count (no grouping)
- time_series   → metric grouped by time bucket (month/week/quarter/day) — trend queries
- threshold     → HAVING filter by absolute value or % of total
- comparison    → two periods side-by-side with delta
- growth_ranking→ rank entities BY growth delta between two periods
- intersection  → entities present in BOTH of two periods
- zero_filter   → entities with zero activity in period

TIME RANGES:
- Single period → 1 entry. Two-period queries (comparison/growth/intersection) → 2 entries.
- Year-only (e.g. "in 2024"): start=2024-01-01, end=2024-12-31
- Quarter (e.g. "Q1 2024"):   start=2024-01-01, end=2024-03-31
- Month (e.g. "March 2024"):  start=2024-03-01, end=2024-03-31
- Relative periods supported: "this month", "last month", "this year", "last year", "last 30 days"
- YoY queries ("yoy", "year over year", "year-on-year") should produce two time ranges.

clarification_question: ask ONLY the single most critical missing field.
For aggregate and time_series queries, NEVER ask about entity.
"""

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_TOPN_RE  = re.compile(r"\b(top|bottom)\s+(\d+)\b", re.IGNORECASE)

_TREND_KEYWORDS = {
    "month-wise", "monthly", "week-wise", "weekly", "day-wise", "daily",
    "quarterly", "quarter-wise", "by month", "per month", "by week", "per week",
    "by day", "per day", "by quarter", "per quarter", "over time", "trend",
    "time series", "breakdown by month", "breakdown by week",
    "revenue trend", "fare trend", "earnings trend", "how did", "how has",
    "yearly", "year-wise", "per year", "annual", "annually", "by year",
    "year over year", "yoy", "each year", "every year", "year-on-year",
}
_ALL_TIME_YEARLY_HINTS = {
    "each year", "every year", "yearly", "annual", "annually", "by year", "per year"
}

# ── Auto-detect mandatory filters from live schema ────────────────────────────
_MANDATORY_FILTER_COLS: dict[str, dict] = {
    "is_cancelled":  {"is_cancelled": 0},
    "is_deleted":    {"is_deleted":   0},
    "cancelled":     {"cancelled":    0},
    "is_active":     {"is_active":    1},
    "is_refunded":   {"is_refunded":  0},
    "is_void":       {"is_void":      0},
    "is_fraud":      {"is_fraud":     0},
    "is_test":       {"is_test":      0},
}


def _get_mandatory_filters() -> dict:
    """Scan the live DuckDB schema and return filters that MUST always be applied."""
    mandatory: dict = {}
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        for (table,) in rows:
            cols = {c[0].lower() for c in conn.execute(f'DESCRIBE "{table}"').fetchall()}
            for col_name, filter_dict in _MANDATORY_FILTER_COLS.items():
                if col_name in cols:
                    mandatory.update(filter_dict)
        conn.close()
    except Exception:
        pass
    return mandatory


def _looks_like_all_time_trend(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _ALL_TIME_YEARLY_HINTS)


def _infer_dataset_time_range() -> list[dict]:
    """
    Infer a default time range from dataset min/max date values.
    Used for trend queries like 'monthly revenue trend for each year'.
    """
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        best = None  # (start_date, end_date)
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for c in cols:
                col = c[0]
                typ = str(c[1]).upper()
                low = col.lower()
                if not (
                    "DATE" in typ or "TIMESTAMP" in typ or
                    any(k in low for k in ("date", "time", "created", "updated", "at"))
                ):
                    continue
                try:
                    r = conn.execute(
                        f'SELECT MIN(CAST("{col}" AS DATE)), MAX(CAST("{col}" AS DATE)) '
                        f'FROM "{table}" WHERE "{col}" IS NOT NULL'
                    ).fetchone()
                    if not r or not r[0] or not r[1]:
                        continue
                    s, e = r[0], r[1]
                    if best is None or (s < best[0] or e > best[1]):
                        best = (s, e)
                except Exception:
                    continue
        conn.close()
        if best:
            s, e = best
            label = (
                f"All data ({s.year})"
                if s.year == e.year
                else f"All data ({s.year}-{e.year})"
            )
            return [{"start": s.isoformat(), "end": e.isoformat(), "label": label}]
    except Exception:
        pass
    return []


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:           esc = False
            elif ch == "\\":  esc = True
            elif ch == '"':   in_str = False
            continue
        if ch == '"':   in_str = True; continue
        if ch == "{":   depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return ""


def _default_clarification(parsed: dict) -> str:
    qt = parsed.get("query_type", "top_n")
    if qt in ("aggregate", "time_series"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("entity"):
        return "Which dimension should I group by? (e.g. driver, city, vehicle type, state, customer, restaurant)"
    if not parsed.get("time_ranges"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("metric"):
        return "What metric should I measure? (e.g. revenue, driver earnings, number of orders, quantity)"
    return "Please clarify: which dimension, metric, and time period do you want?"


def _detect_time_bucket(query: str) -> str:
    q = query.lower()
    # Prefer finer granularity if explicitly requested.
    # Example: "monthly revenue trend for each year" should stay monthly, not yearly.
    if any(x in q for x in ["month", "monthly", "month-wise", "by month", "per month"]):
        return "month"
    if any(x in q for x in ["quarter", "q1", "q2", "q3", "q4", "quarterly",
                              "per quarter", "quarter-wise"]):
        return "quarter"
    if any(x in q for x in ["week", "weekly", "week-wise", "per week"]):
        return "week"
    if any(x in q for x in ["day", "daily", "day-wise", "per day"]):
        return "day"
    if any(x in q for x in ["per year", "by year", "each year", "yearly",
                              "year-wise", "annual", "annually",
                              "year over year", "yoy", "every year",
                              "year-on-year", "per annum"]):
        return "year"
    return "month"

def _is_trend_query(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in _TREND_KEYWORDS)


def _has_ranking_cue(query: str) -> bool:
    q = f" {query.lower()} "
    if _TOPN_RE.search(q):
        return True
    return any(x in q for x in [
        " top ", " bottom ", "highest", "lowest", "least", "most",
        "best", "worst", "top-", "bottom-",
    ])


def _fallback_parse(user_query: str) -> dict:
    q  = user_query.lower()

    if _is_trend_query(user_query):
        metric = "final_price"
        for kw, met in [
            ("average order", "avg_final_price"), ("avg order", "avg_final_price"),
            ("average fare", "avg_total_fare"), ("avg fare", "avg_total_fare"),
            ("average earning", "avg_driver_earnings"),
            ("earning", "driver_earnings"), ("commission", "platform_commission"),
            ("final", "final_price"), ("fare", "total_fare"),
            ("revenue", "final_price"), ("ride", "count"),
            ("trip", "count"), ("booking", "count"), ("order", "count"),
            ("quantity", "quantity"),
        ]:
            if kw in q:
                metric = met
                break
        bucket = _detect_time_bucket(user_query)
        return {
            "entity": None, "metric": metric, "query_type": "time_series",
            "time_bucket": bucket, "top_n": 5, "time_ranges": [],
            "threshold": None, "filters": {}, "is_complete": False,
            "clarification_question": "What time period should I use? (e.g. 2024, Q1 2024)",
        }

    qt = "top_n"
    entity = None
    top_n  = 5
    for kw, ent in [
        ("restaurant", "restaurant_name"), ("item", "item_name"),
        ("category", "category"), ("city", "city"),
        ("customer", "customer_name"), ("driver", "driver_name"),
        ("pickup city", "pickup_city"), ("drop city", "drop_city"),
        ("state", "state"), ("vehicle type", "vehicle_type"),
        ("vehicle", "vehicle_type"), ("product", "product_name"),
        ("payment", "payment_mode"),
    ]:
        if kw in q:
            entity = ent
            break

    # Prefer post-discount revenue columns
    metric = "final_price"
    for kw, met in [
        ("earning", "driver_earnings"), ("commission", "platform_commission"),
        ("final", "final_price"), ("fare", "total_fare"),
        ("revenue", "final_price"), ("sales", "final_price"),
        ("amount", "total_amount"), ("ride", "count"), ("trip", "count"),
        ("order", "count"), ("booking", "count"), ("quantity", "quantity"),
    ]:
        if kw in q:
            metric = met
            break

    if entity is None and any(x in q for x in ["total", "how much", "how many", "average"]):
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
        "time_bucket": None, "top_n": top_n, "time_ranges": [],
        "threshold": None, "filters": {}, "is_complete": is_complete,
        "clarification_question": "",
    }
    if not is_complete:
        parsed["clarification_question"] = _default_clarification(parsed)
    return parsed


def _extract_time_ranges_from_text(text: str) -> list[dict]:
    ranges, _ = parse_time_ranges_from_query(text)
    return ranges


def _post_process(parsed: dict, user_query: str = "",
                  mandatory_filters: dict | None = None) -> dict:
    """
    Clean up Qwen's output and inject auto-detected mandatory filters.
    """
    qt = parsed.get("query_type", "top_n")
    tr = parsed.get("time_ranges", [])
    m  = parsed.get("metric")

    # ── Detect trend query that Qwen may have misclassified ───────────────────
    ranking_cue = _has_ranking_cue(user_query)
    if _is_trend_query(user_query) and not ranking_cue and qt not in ("time_series",):
        parsed["query_type"] = "time_series"
        parsed["entity"]     = None
        qt = "time_series"
        if not parsed.get("time_bucket"):
            parsed["time_bucket"] = _detect_time_bucket(user_query)

    if qt == "time_series" and ranking_cue:
        mtop = _TOPN_RE.search(user_query)
        parsed["query_type"] = "top_n"
        if mtop:
            parsed["query_type"] = "bottom_n" if mtop.group(1).lower() == "bottom" else "top_n"
            parsed["top_n"] = int(mtop.group(2))
        elif any(x in user_query.lower() for x in ["lowest", "least", "bottom", "worst"]):
            parsed["query_type"] = "bottom_n"
        parsed["time_bucket"] = None
        qt = parsed["query_type"]

    if qt == "time_series" and not parsed.get("time_bucket"):
        parsed["time_bucket"] = _detect_time_bucket(user_query)

    # ── Extract missing time ranges from raw text ─────────────────────────────
    if not tr and user_query:
        extracted, suggested_qt = parse_time_ranges_from_query(user_query)
        if extracted:
            parsed["time_ranges"] = extracted
            tr = extracted
            if (suggested_qt == "comparison" and
                    parsed.get("query_type") not in ("comparison", "growth_ranking", "intersection")):
                parsed["query_type"] = "comparison"
                qt = "comparison"
        elif qt == "time_series" and _looks_like_all_time_trend(user_query):
            inferred = _infer_dataset_time_range()
            if inferred:
                parsed["time_ranges"] = inferred
                tr = inferred

    # ── Guard: swap ID columns → name columns ─────────────────────────────────
    entity = parsed.get("entity", "") or ""
    if entity.endswith("_id"):
        parsed["entity"] = entity[:-3] + "_name"

    # Resolve semantic aliases to concrete schema columns.
    parsed = resolve_intent_with_semantic_layer(parsed, user_query)

    # ── Inject mandatory business filters ─────────────────────────────────────
    if mandatory_filters:
        existing_filters = dict(parsed.get("filters") or {})
        # Only inject a filter if the user hasn't explicitly overridden it
        for k, v in mandatory_filters.items():
            existing_filters.setdefault(k, v)
        parsed["filters"] = existing_filters

    # ── Completeness guards ───────────────────────────────────────────────────
    if qt in ("aggregate", "time_series") and m and tr:
        parsed["is_complete"]            = True
        parsed["clarification_question"] = None

    if qt == "comparison" and m and len(tr) >= 2:
        parsed["is_complete"]            = True
        parsed["clarification_question"] = None

    if qt in ("top_n", "bottom_n", "threshold", "growth_ranking", "zero_filter"):
        if parsed.get("entity") and tr:
            parsed["is_complete"]            = True
            parsed["clarification_question"] = None

    return parsed


def _call_qwen(user_query: str, schema: str) -> tuple[dict, dict]:
    system = _SYSTEM_TEMPLATE.format(schema=schema)
    resp = requests.post(
        GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_TOKEN}",
                 "Content-Type": "application/json"},
        json={
            "model": QWEN_MODEL,
            "messages": [
                {"role": "system", "content": system},
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


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

    # Pre-compute mandatory filters once (fast — no LLM call)
    mandatory_filters = _get_mandatory_filters()
    if mandatory_filters:
        ctx.logger.info("🔒 Mandatory filters detected", {
            "queryId": query_id, "filters": mandatory_filters
        })

    if merged_parsed:
        ctx.logger.info("Clarification path", {"queryId": query_id})
        parsed = _post_process(merged_parsed, user_query, mandatory_filters)
    else:
        ctx.logger.info("Qwen intent extraction", {"queryId": query_id, "query": user_query})
        try:
            schema = get_schema_prompt()
            parsed, usage = _call_qwen(user_query, schema)
            parsed = _post_process(parsed, user_query, mandatory_filters)
            log_tokens(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            await add_tokens_to_state(ctx, query_id, "ParseIntent", QWEN_MODEL, usage)
            ctx.logger.info("Intent parsed", {"queryId": query_id, "parsed": parsed})
        except Exception as exc:
            ctx.logger.error("Intent parse failed", {"error": str(exc), "queryId": query_id})
            parsed = _post_process(_fallback_parse(user_query), user_query, mandatory_filters)

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
