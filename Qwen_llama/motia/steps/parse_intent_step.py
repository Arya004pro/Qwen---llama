"""Step 2: Parse Intent — Qwen extracts full structured intent as JSON.

Changes vs original:
  - System prompt METRIC RULES section no longer hardcodes specific column names
    (final_price, total_fare, driver_earnings). It now tells the LLM to read the
    live "Metric column mappings" section from the schema instead.
  - _fallback_parse() no longer hardcodes entity/metric keyword→column maps
    for Uber/Zomato. It now reads the live schema to build those maps dynamically.
  - All other logic (time_series detection, post_process, mandatory filters) unchanged.
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
from shared_config import (
    GROQ_API_TOKEN,
    QWEN_MODEL,
    GROQ_URL,
    QWEN_ENABLE_REASONING,
    QWEN_REASONING_EFFORT,
)
from utils.token_logger import log_tokens, add_tokens_to_state
from utils.time_parser import parse_time_ranges_from_query
from db.schema_context import get_schema_prompt
from db.duckdb_connection import get_read_connection
from db.semantic_layer import resolve_intent_with_semantic_layer

config = {
    "name": "ParseIntent",
    "description": (
        "Qwen extracts full structured intent JSON from any dataset. "
        "Schema injected live — no hardcoded column/domain names. "
        "Fallback parser uses live schema for entity/metric detection. "
        "Supports time_series queries and auto-injects mandatory business filters."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}

# ── System prompt ─────────────────────────────────────────────────────────────
# NOTE: Metric rules no longer hardcode specific column names.
# The LLM is told to read the "Metric column mappings" section from the
# injected schema, which is built dynamically from the live DB.
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
- The schema includes a Semantic Layer section with Dimensions and Metrics.
- Prefer semantic business terms first; post-processing resolves them to physical columns.
- Output the semantic entity/metric name that best matches user intent.

TIME SERIES RULES (CRITICAL):
- Use query_type=time_series when the user asks for a TREND or TIME-BUCKETED breakdown:
    "month-wise revenue", "monthly trend", "weekly sales", "quarterly earnings",
    "how did revenue change over time", "revenue by month", "per month breakdown"
- For time_series: entity MUST be null.
- For time_series: set time_bucket to: month | week | quarter | day
- For time_series: is_complete=true as long as metric and time_ranges are known.
- NEVER ask for a business dimension for time_series queries.

ENTITY RULES:
- entity = the DISPLAY column to show in results — ALWAYS use the human-readable
  NAME column, NEVER a raw ID column.
    Good: customer_name, product_name, category_name, city, segment_name
    Bad:  customer_id, product_id, record_id, transaction_id
- For query_type=aggregate or time_series: entity MUST be null.
- If the user names a specific filter value (e.g. "in Mumbai"), put it in
  filters: {{column_name: value}} — do NOT use it as entity.

METRIC RULES — read the "Metric column mappings" section from the schema above:
- Use those exact physical column names for metric selection.
- For "revenue", "sales", "earnings": prefer the post-discount / final amount column
  (highest specificity — read the schema to find it).
- For "average order value" / "AOV": set metric="aov".
    SQL generation will compute: SUM(revenue_column) / COUNT(DISTINCT order_identifier).
- For "average order value", "average fare", "avg X": metric = "avg_<column>"
  (signals SQL generation to use AVG not SUM).
- For "rides", "trips", "bookings", "orders", "count of", "number of", "how many":
  metric = "count"
  IMPORTANT: use COUNT(DISTINCT <order_id_column>) NOT COUNT(*).
- For "quantity", "units sold", "items": use the quantity column from the schema.
- When in doubt: pick the primary revenue/amount column from the schema.

BUSINESS FILTER RULES — CRITICAL for accuracy:
- If the schema has an is_cancelled column, always include: filters: {{"is_cancelled": 0}}
- If the schema has is_deleted, cancelled, is_refunded: add them to filters.
- If there is a status/order_status column, set filters to keep only completed rows.
- These filters are MANDATORY — without them revenue figures will be inflated.

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
- time_series   → metric grouped by time bucket — trend queries
- threshold     → HAVING filter by absolute value or % of total
- comparison    → two periods side-by-side with delta
- growth_ranking→ rank entities BY growth delta between two periods
- intersection  → entities present in BOTH of two periods
- zero_filter   → entities with zero activity in period

TIME RANGES:
- Single period → 1 entry. Two-period queries → 2 entries.
- Year-only: start=YYYY-01-01, end=YYYY-12-31
- Quarter Q1 2024: start=2024-01-01, end=2024-03-31
- Month March 2024: start=2024-03-01, end=2024-03-31
- Relative: "this month", "last month", "this year", "last year", "last 30 days"
- YoY queries produce two time ranges.

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
    "each year", "every year", "yearly", "annual", "annually",
    "by year", "per year",
}

_REVENUE_QUERY_HINTS = (
    "revenue", "sales", "earning", "earnings", "income",
    "turnover", "gmv", "gross merchandise", "net sales",
)

_REVENUE_STRONG_COL_HINTS = (
    "revenue", "sales", "earning", "amount", "total",
    "final", "net", "paid", "gmv",
)

_REVENUE_WEAK_COL_HINTS = (
    "unit", "base", "list", "mrp", "msrp", "catalog", "original",
    "cost", "tax", "discount", "coupon", "shipping", "commission",
    "refund", "refunded", "before_",
)

_AOV_QUERY_HINTS = (
    "aov", "average order value", "avg order value",
    "average basket value", "average transaction value", "average ticket size",
)

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


# ── Schema-driven fallback builder ────────────────────────────────────────────

def _build_schema_keyword_maps() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Read the live DuckDB schema and return two keyword→column maps:
      entity_map : [(keyword, column_name), ...]  for entity (text) columns
      metric_map : [(keyword, column_name), ...]  for metric (numeric) columns

    These replace the hardcoded lists that were previously in _fallback_parse().
    """
    entity_map: list[tuple[str, str]] = []
    metric_map: list[tuple[str, str]] = []

    _TEXT_TYPES   = ("VARCHAR", "CHAR", "TEXT", "STRING")
    _NUM_TYPES    = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    _DATE_HINTS   = ("date", "time", "created", "updated", "timestamp")
    _ID_HINTS     = ("_id", "_key", "_uuid")

    _MONETARY = ("price", "fare", "amount", "earning", "revenue",
                 "commission", "fee", "cost", "sale", "total", "final", "profit")
    _COUNT_TRIGGERS = ("order", "ride", "trip", "booking", "transaction", "visit")

    try:
        conn = get_read_connection()
        try:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
            ]

            for table in tables:
                cols = conn.execute(f'DESCRIBE "{table}"').fetchall()

                # Find primary-key-like column for count metric
                id_col = next(
                    (c[0] for c in cols if c[0].lower().endswith("_id")),
                    None,
                )

                for col, dtype, *_ in cols:
                    col_l = dtype.upper()
                    cname = col.lower()

                    # Skip date-like columns
                    if any(k in cname for k in _DATE_HINTS):
                        continue

                    if any(t in col_l for t in _TEXT_TYPES):
                        # Entity candidate
                        if any(k in cname for k in _ID_HINTS):
                            continue  # Skip raw id columns
                        # Build keyword from column name by stripping suffixes
                        base = cname
                        for sfx in ("_name", "_type", "_mode", "_status"):
                            if base.endswith(sfx):
                                base = base[:-len(sfx)]
                                break
                        base_words = base.replace("_", " ").strip()
                        if base_words:
                            entity_map.append((base_words, col))
                        # Also add the raw column name as keyword
                        if cname != base_words:
                            entity_map.append((cname.replace("_", " "), col))

                    elif any(t in col_l for t in _NUM_TYPES):
                        # Metric candidate — skip id-like columns
                        if any(k in cname for k in _ID_HINTS):
                            continue
                        if cname in ("year", "month", "day", "week", "quarter"):
                            continue

                        # Map natural language keywords → this column
                        base_words = cname.replace("_", " ")

                        # Monetary column → revenue/earnings/sales all map here
                        if any(k in cname for k in _MONETARY):
                            metric_map.append((base_words, col))
                            # Add revenue aliases only for strong revenue columns.
                            if any(k in cname for k in ("revenue", "sales", "amount", "earning", "total", "final", "net", "paid")):
                                for alias in ("revenue", "sales", "income", "earnings", "money"):
                                    metric_map.append((alias, col))

                        # Count trigger columns (order_id, ride_id etc.)
                        if id_col and any(k in cname for k in _COUNT_TRIGGERS):
                            for alias in ("count", "how many", "number of", "rides", "trips",
                                          "orders", "bookings", "visits"):
                                metric_map.append((alias, "count"))

                        # Quantity
                        if "quantity" in cname or "qty" in cname:
                            for alias in ("quantity", "units", "items", "pieces",
                                          "how many items", "units sold"):
                                metric_map.append((alias, col))

                        # Distance / duration
                        if "distance" in cname or "km" in cname:
                            metric_map.append(("distance", col))
                            metric_map.append(("km", col))
                        if "duration" in cname or "minute" in cname:
                            metric_map.append(("duration", col))
                            metric_map.append(("time taken", col))

        finally:
            conn.close()

    except Exception:
        pass  # Return whatever we have (may be empty — caller handles it)

    # Deduplicate while preserving order
    seen_e: set = set()
    seen_m: set = set()
    entity_map = [(k, v) for k, v in entity_map if (k, v) not in seen_e and not seen_e.add((k, v))]  # type: ignore[func-returns-value]
    metric_map = [(k, v) for k, v in metric_map if (k, v) not in seen_m and not seen_m.add((k, v))]  # type: ignore[func-returns-value]

    return entity_map, metric_map


# ── Mandatory filters ─────────────────────────────────────────────────────────

def _get_mandatory_filters() -> dict:
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


def _should_skip_mandatory_filter(col_name: str, user_query: str, chosen_entity: str | None = None) -> bool:
    ql = (user_query or "").lower()
    c = (col_name or "").lower().strip()
    if not c:
        return False
    if chosen_entity and c == str(chosen_entity).lower():
        return True

    normalized = c.replace("is_", "")
    tokens = [t for t in normalized.split("_") if len(t) >= 3]
    if any(tok in ql for tok in tokens):
        return True
    return False


def _find_split_dimension_for_query(user_query: str) -> str | None:
    ql = (user_query or "").lower()
    split_cue = any(x in ql for x in (" vs ", " versus ", " compared to ", " against "))
    if not split_cue:
        return None

    # Keep this generic: identify low-cardinality columns likely used for categorical splits.
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()

        best: tuple[int, str] | None = None
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for col, dtype, *_ in cols:
                c = col.lower()
                d = str(dtype).upper()

                if any(k in c for k in ("date", "time", "created", "updated", "timestamp")):
                    continue
                if c.endswith("_id"):
                    continue
                if any(k in c for k in (
                    "price", "amount", "revenue", "sales", "earning", "total",
                    "cost", "discount", "qty", "quantity", "distance", "duration",
                    "score", "rate", "percent", "ratio",
                )):
                    continue
                is_text = any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))
                is_numeric = any(t in d for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"))
                if not (is_text or is_numeric):
                    continue

                try:
                    distinct_cnt, non_null = conn.execute(
                        f'SELECT COUNT(DISTINCT "{col}"), COUNT("{col}") FROM "{table}"'
                    ).fetchone()
                    distinct_cnt = int(distinct_cnt or 0)
                    non_null = int(non_null or 0)
                except Exception:
                    continue

                if non_null == 0 or distinct_cnt < 2 or distinct_cnt > 12:
                    continue

                score = 0
                name_tokens = [t for t in c.replace("is_", "").split("_") if len(t) >= 3]
                if any(tok in ql for tok in name_tokens):
                    score += 10
                if c.startswith("is_"):
                    score += 4
                if distinct_cnt <= 6:
                    score += 3
                if any(w in ql for w in ("count", "number of", "how many", "total")):
                    score += 2

                if score > 0 and (best is None or score > best[0]):
                    best = (score, col)

        conn.close()
        return best[1] if best else None
    except Exception:
        return None


def _infer_boolean_flag_filters(user_query: str, split_cue: bool) -> dict[str, int]:
    if split_cue:
        return {}

    ql = (user_query or "").lower()

    def _word(term: str) -> bool:
        return bool(re.search(rf"\b{re.escape(term)}\b", ql))

    inferred: dict[str, int] = {}
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for col, dtype, *_ in cols:
                c = str(col).lower()
                d = str(dtype).upper()
                if not any(t in d for t in ("INT", "BIGINT", "SMALLINT", "TINYINT", "BOOLEAN")):
                    continue

                try:
                    vals = conn.execute(
                        f'SELECT DISTINCT "{col}" FROM "{table}" '
                        f'WHERE "{col}" IS NOT NULL LIMIT 3'
                    ).fetchall()
                except Exception:
                    continue
                norm_vals = {str(v[0]).strip() for v in vals}
                if not norm_vals.issubset({"0", "1", "0.0", "1.0", "False", "True", "false", "true"}):
                    continue

                base = c[3:] if c.startswith("is_") else c
                base_words = base.replace("_", " ")

                positive_hit = _word(base) or _word(base_words) or _word(c)
                negative_hit = any(
                    phrase in ql for phrase in (
                        f"not {base_words}",
                        f"non {base_words}",
                        f"non-{base_words}",
                        f"without {base_words}",
                        f"no {base_words}",
                    )
                )
                if base == "active" and _word("inactive"):
                    negative_hit = True
                if base.endswith("ed") and _word(f"un{base}"):
                    negative_hit = True

                if negative_hit:
                    inferred[c] = 0
                elif positive_hit:
                    inferred[c] = 1

        conn.close()
    except Exception:
        return inferred

    return inferred


# ── Time-range helpers ────────────────────────────────────────────────────────

def _looks_like_all_time_trend(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _ALL_TIME_YEARLY_HINTS)


def _infer_dataset_time_range() -> list[dict]:
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        best = None
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for c in cols:
                col = c[0]
                typ = str(c[1]).upper()
                low = col.lower()
                if not (
                    "DATE" in typ or "TIMESTAMP" in typ
                    or any(k in low for k in ("date", "time", "created", "updated", "at"))
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


# ── JSON helpers ──────────────────────────────────────────────────────────────

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


def _is_revenue_intent(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _REVENUE_QUERY_HINTS)


def _revenue_col_score(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0

    if any(k in c for k in _REVENUE_STRONG_COL_HINTS):
        score += 10
    if "final" in c or "net" in c or "paid" in c:
        score += 8
    if "total" in c:
        score += 6
    if "price" in c or "fare" in c:
        score += 3
    if any(k in c for k in _REVENUE_WEAK_COL_HINTS):
        score -= 7

    return score


def _select_primary_revenue_column(metric_map: list[tuple[str, str]]) -> str | None:
    candidates: dict[str, int] = {}
    for _, col in metric_map:
        c = (col or "").lower().strip()
        if not c or c == "count" or c.startswith("avg_"):
            continue
        score = _revenue_col_score(c)
        prev = candidates.get(c)
        if prev is None or score > prev:
            candidates[c] = score

    if not candidates:
        return None

    ranked = sorted(
        candidates.items(),
        key=lambda kv: (
            kv[1],
            1 if "final" in kv[0] else 0,
            1 if "total" in kv[0] else 0,
            1 if "amount" in kv[0] else 0,
            -len(kv[0]),
        ),
        reverse=True,
    )
    return ranked[0][0]


def _select_primary_count_key() -> str | None:
    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        best: tuple[int, str] | None = None
        for (table,) in table_rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            id_cols = [c[0].lower() for c in cols if c[0].lower().endswith("_id") or c[0].lower() == "id"]
            for col in id_cols:
                score = 0
                if any(k in col for k in ("order", "transaction", "invoice", "booking", "trip", "ride",
                                          "ticket", "request", "visit", "session", "sale", "payment")):
                    score += 10
                if any(k in col for k in ("row", "line", "item", "detail", "record", "event", "log")):
                    score -= 10
                if col == "id":
                    score -= 2

                try:
                    non_null, distinct_cnt = conn.execute(
                        f'SELECT COUNT("{col}"), COUNT(DISTINCT "{col}") FROM "{table}"'
                    ).fetchone()
                    non_null = int(non_null or 0)
                    distinct_cnt = int(distinct_cnt or 0)
                    if non_null > 0:
                        ratio = distinct_cnt / non_null
                        if 0.4 <= ratio < 0.995:
                            score += 6
                        elif ratio >= 0.995:
                            score -= 6
                        elif ratio < 0.05:
                            score -= 4
                except Exception:
                    pass

                if best is None or score > best[0]:
                    best = (score, col)
        conn.close()
        return best[1] if best else None
    except Exception:
        return None


def _is_aov_intent(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _AOV_QUERY_HINTS)


def _default_clarification(parsed: dict) -> str:
    qt = parsed.get("query_type", "top_n")
    if qt in ("aggregate", "time_series"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("entity"):
        return "Which dimension should I group by? (e.g. customer, product, region, channel, category)"
    if not parsed.get("time_ranges"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("metric"):
        return "What metric should I measure? (e.g. revenue, quantity, record count)"
    return "Please clarify: which dimension, metric, and time period do you want?"


def _detect_time_bucket(query: str) -> str:
    q = query.lower()
    if any(x in q for x in ["month", "monthly", "month-wise", "by month", "per month"]):
        return "month"
    if any(x in q for x in ["quarter", "q1", "q2", "q3", "q4", "quarterly"]):
        return "quarter"
    if any(x in q for x in ["week", "weekly", "week-wise", "per week"]):
        return "week"
    if any(x in q for x in ["day", "daily", "day-wise", "per day"]):
        return "day"
    if any(x in q for x in ["per year", "by year", "each year", "yearly",
                              "year-wise", "annual", "annually", "yoy"]):
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


def _has_metric_cue(query: str) -> bool:
    q = (query or "").lower()
    metric_cues = (
        "revenue", "sales", "earning", "income", "turnover", "gmv",
        "count", "how many", "number of", "quantity", "units",
        "average", "avg", "price", "amount", "fare", "cost",
        "discount", "profit", "margin", "rate", "score",
    )
    return any(c in q for c in metric_cues)


def _infer_entity_from_grouping_cue(
    user_query: str,
    entity_map: list[tuple[str, str]],
) -> str | None:
    q = f" {(user_query or '').lower()} "
    targets = []
    for m in re.finditer(r"\b(?:per|by|for each|each)\s+([a-z][a-z0-9]*(?:\s+[a-z][a-z0-9]*){0,2})", q):
        phrase = (m.group(1) or "").strip()
        if not phrase:
            continue
        first = phrase.split()[0]
        if first in {"year", "month", "week", "day", "quarter", "date", "time"}:
            continue
        targets.append(phrase)

    best: tuple[int, str] | None = None

    for kw, col in entity_map:
        k = (kw or "").lower().strip()
        c = (col or "").lower().replace("_", " ").strip()
        if not k:
            continue
        variants = {k, k.rstrip("s"), c, c.rstrip("s")}
        k_tokens = [t for t in re.split(r"\s+", k.replace("_", " ")) if t]
        c_tokens = [t for t in re.split(r"\s+", c) if t]
        for t in k_tokens + c_tokens:
            if len(t) >= 3 and t not in {"name", "type", "status", "code", "id"}:
                variants.add(t)

        score = 0
        for v in variants:
            if len(v) < 3:
                continue
            if f" per {v} " in q:
                score += 12
            if f" by {v} " in q:
                score += 10
            if f" each {v} " in q or f" for each {v} " in q:
                score += 10
            for tgt in targets:
                if tgt == v:
                    score += 10
                elif tgt in v or v in tgt:
                    score += 6
        if score > 0 and (best is None or score > best[0]):
            best = (score, col)

    return best[1] if best else None


# ── Schema-aware fallback parser ──────────────────────────────────────────────

def _fallback_parse(user_query: str) -> dict:
    """
    Build a best-effort intent from user query WITHOUT calling the LLM.
    Uses the LIVE SCHEMA to detect entity and metric columns instead of
    hardcoded keyword lists.
    """
    q = user_query.lower()

    # ── Time series ───────────────────────────────────────────────────────────
    if _is_trend_query(user_query):
        if _is_aov_intent(user_query):
            return {
                "entity": None,
                "metric": "aov",
                "semantic_metric": "aov",
                "_aov_revenue_col": _select_primary_revenue_column(_build_schema_keyword_maps()[1]),
                "_count_distinct_key": _select_primary_count_key(),
                "query_type": "time_series",
                "time_bucket": _detect_time_bucket(user_query),
                "top_n": 5,
                "time_ranges": [],
                "threshold": None,
                "filters": {},
                "is_complete": False,
                "clarification_question": "What time period should I use? (e.g. 2024, Q1 2024)",
            }

        # Try to identify the metric from live schema
        _, metric_map = _build_schema_keyword_maps()
        metric = None
        for kw, col in metric_map:
            if kw in q:
                metric = col
                break

        if _is_revenue_intent(user_query):
            revenue_col = _select_primary_revenue_column(metric_map)
            if revenue_col and (
                metric is None or _revenue_col_score(metric) < _revenue_col_score(revenue_col)
            ):
                metric = revenue_col

        if metric is None:
            metric = "count"

        bucket = _detect_time_bucket(user_query)
        return {
            "entity": None, "metric": metric, "query_type": "time_series",
            "time_bucket": bucket, "top_n": 5, "time_ranges": [],
            "threshold": None, "filters": {}, "is_complete": False,
            "clarification_question": "What time period should I use? (e.g. 2024, Q1 2024)",
        }

    # ── Ranked / aggregate queries ────────────────────────────────────────────
    entity_map, metric_map = _build_schema_keyword_maps()

    qt    = "top_n"
    entity: str | None = None
    top_n = 5

    # Detect entity from live schema keyword map
    for kw, col in entity_map:
        if kw in q:
            entity = col
            break

    # If still no entity and query looks aggregate, set accordingly
    if entity is None and any(x in q for x in ["total", "how much", "how many", "average"]):
        qt = "aggregate"

    # Detect metric from live schema keyword map
    metric = None
    is_aov = _is_aov_intent(user_query)
    if is_aov:
        metric = "aov"

    if not is_aov:
        for kw, col in metric_map:
            if kw in q:
                metric = col
                break

    if not is_aov and _is_revenue_intent(user_query):
        revenue_col = _select_primary_revenue_column(metric_map)
        if revenue_col and (
            metric is None or _revenue_col_score(metric) < _revenue_col_score(revenue_col)
        ):
            metric = revenue_col

    if metric is None:
        # Default to first monetary column found, or "count"
        for kw, col in metric_map:
            if col != "count":
                metric = col
                break
        if metric is None:
            metric = "count"

    # Detect ranking direction
    m = _TOPN_RE.search(q)
    if m:
        qt    = "bottom_n" if m.group(1).lower() == "bottom" else "top_n"
        top_n = int(m.group(2))
    elif any(x in q for x in ["lowest", "least", "bottom", "worst"]):
        qt = "bottom_n"
    elif any(x in q for x in ["highest", "most", "best", "top", "largest"]):
        qt = "top_n"

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


# ── Post-processing ───────────────────────────────────────────────────────────

def _post_process(
    parsed: dict,
    user_query: str = "",
    mandatory_filters: dict | None = None,
) -> dict:
    ql = (user_query or "").lower()
    qt = parsed.get("query_type", "top_n")
    tr = parsed.get("time_ranges", [])
    m  = parsed.get("metric")

    def _period_key(period: dict | None) -> tuple[str, str]:
        period = period or {}
        return (str(period.get("start") or ""), str(period.get("end") or ""))

    count_cues = (
        "how many", "number of", "count", "orders", "records",
        "transactions", "number of records",
    )
    avg_cues = ("average", "avg ", "aov", "average order value")
    aggregate_cues = (
        "total", "overall", "sum", "average", "avg", "how many", "number of",
    )
    growth_cues = ("growth", "grew", "increase", "decrease", "delta", "change")
    split_cue = any(x in ql for x in (" vs ", " versus ", " compared to ", " against "))
    grouping_cue = bool(re.search(r"\b(per|by|for each|each)\b", ql))

    if _is_aov_intent(user_query):
        parsed["metric"] = "aov"
        parsed["semantic_metric"] = "aov"
        m = "aov"
    elif any(c in ql for c in count_cues):
        parsed["metric"] = "count"
        m = "count"
    elif m and isinstance(m, str) and not m.startswith("avg_") and any(c in ql for c in avg_cues):
        if m != "count":
            parsed["metric"] = f"avg_{m}"
            m = parsed["metric"]

    ranking_cue = _has_ranking_cue(user_query)

    if not parsed.get("entity") and grouping_cue:
        entity_map_for_grouping, _ = _build_schema_keyword_maps()
        inferred_entity = _infer_entity_from_grouping_cue(user_query, entity_map_for_grouping)
        if inferred_entity:
            parsed["entity"] = inferred_entity

    if split_cue and any(c in ql for c in count_cues):
        split_entity = _find_split_dimension_for_query(user_query)
        if split_entity:
            parsed["query_type"] = "top_n"
            parsed["entity"] = split_entity
            parsed["metric"] = "count"
            parsed["semantic_metric"] = "count"
            parsed["top_n"] = max(int(parsed.get("top_n") or 5), 20)
            current_filters = dict(parsed.get("filters") or {})
            current_filters.pop(split_entity, None)
            parsed["filters"] = current_filters
            qt = "top_n"
            m = "count"

            if _is_trend_query(user_query):
                parsed["is_complete"] = False
                parsed["_force_clarification"] = True
                parsed["clarification_question"] = (
                    "Do you want a year-wise trend for one status, or an overall paid vs refunded split?"
                )

    inferred_flag_filters = _infer_boolean_flag_filters(user_query, split_cue)
    if inferred_flag_filters:
        current_filters = dict(parsed.get("filters") or {})
        for fk, fv in inferred_flag_filters.items():
            current_filters[fk] = fv
        parsed["filters"] = current_filters

    if (parsed.get("metric") == "aov"
            and qt in ("top_n", "bottom_n")
            and not ranking_cue
            and not parsed.get("entity")):
        parsed["query_type"] = "aggregate"
        parsed["entity"] = None
        qt = "aggregate"

    if any(c in ql for c in growth_cues) and not _is_trend_query(user_query):
        entity_growth_phrase = bool(parsed.get("entity")) or " by " in f" {ql} "
        if entity_growth_phrase and qt != "intersection":
            parsed["query_type"] = "growth_ranking"
            qt = "growth_ranking"

    if qt in ("top_n", "bottom_n") and not ranking_cue and not split_cue and any(c in ql for c in aggregate_cues) and not (grouping_cue and parsed.get("entity")):
        parsed["query_type"] = "aggregate"
        parsed["entity"] = None
        qt = "aggregate"

    if qt == "aggregate" and grouping_cue and parsed.get("entity") and any(c in ql for c in aggregate_cues):
        parsed["query_type"] = "top_n"
        parsed["_disable_limit"] = True
        qt = "top_n"

    if qt in ("top_n", "bottom_n") and grouping_cue and parsed.get("entity") and not ranking_cue:
        parsed["query_type"] = "top_n"
        parsed["_disable_limit"] = True
        qt = "top_n"

    if qt in ("top_n", "bottom_n") and not parsed.get("entity"):
        entity_map, _ = _build_schema_keyword_maps()
        inferred_entity = _infer_entity_from_grouping_cue(user_query, entity_map)
        if inferred_entity:
            parsed["entity"] = inferred_entity
        preferred_tokens = {
            "product": "product",
            "customer": "customer",
            "region": "region",
            "segment": "segment",
            "channel": "channel",
            "course": "course",
            "student": "student",
            "platform": "platform",
            "category": "category",
            "city": "city",
            "coupon": "coupon",
            "payment": "payment",
        }
        if not parsed.get("entity"):
            for tok, needle in preferred_tokens.items():
                if tok not in ql:
                    continue
                match_col = next((col for _, col in entity_map if needle in col.lower()), None)
                if match_col:
                    parsed["entity"] = match_col
                    break

    if (
        _is_trend_query(user_query)
        and not ranking_cue
        and qt not in ("time_series",)
        and not (split_cue and parsed.get("entity"))
    ):
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

    if not tr and user_query:
        extracted, suggested_qt = parse_time_ranges_from_query(user_query)
        if extracted:
            parsed["time_ranges"] = extracted
            tr = extracted
            if (suggested_qt == "comparison"
                    and parsed.get("query_type") not in ("comparison", "growth_ranking", "intersection")):
                parsed["query_type"] = "comparison"
                qt = "comparison"
        elif qt == "time_series" and _looks_like_all_time_trend(user_query):
            inferred = _infer_dataset_time_range()
            if inferred:
                parsed["time_ranges"] = inferred
                tr = inferred

    if not tr and qt not in ("comparison", "intersection"):
        inferred = _infer_dataset_time_range()
        if inferred:
            parsed["time_ranges"] = inferred
            tr = inferred

    # Guard: swap ID columns → name columns
    entity = parsed.get("entity", "") or ""
    if entity.endswith("_id"):
        parsed["entity"] = entity[:-3] + "_name"

    # Resolve semantic aliases to concrete schema columns
    parsed = resolve_intent_with_semantic_layer(parsed, user_query)

    if (parsed.get("metric") or "").lower() == "aov":
        _, metric_map = _build_schema_keyword_maps()
        parsed.setdefault("_aov_revenue_col", _select_primary_revenue_column(metric_map))
        parsed.setdefault("_count_distinct_key", _select_primary_count_key())
        parsed["semantic_metric"] = "aov"

    # Revenue safety: if user asked for revenue-like metrics, prefer the
    # strongest net/final monetary column from live schema over list/base price.
    if _is_revenue_intent(user_query):
        _, metric_map = _build_schema_keyword_maps()
        preferred_revenue = _select_primary_revenue_column(metric_map)
        if preferred_revenue:
            current_metric = (parsed.get("metric") or "").lower().strip()
            avg_mode = bool(current_metric.startswith("avg_"))
            current_base = current_metric[4:] if avg_mode else current_metric
            preferred_metric = f"avg_{preferred_revenue}" if avg_mode else preferred_revenue
            if (
                not current_metric
                or current_metric == "count"
                or _revenue_col_score(current_base) < _revenue_col_score(preferred_revenue)
            ):
                parsed["metric"] = preferred_metric
            parsed["semantic_metric"] = "avg_revenue" if avg_mode else "revenue"

    # Inject mandatory business filters
    if mandatory_filters:
        existing = dict(parsed.get("filters") or {})
        for k, v in mandatory_filters.items():
            if _should_skip_mandatory_filter(k, user_query, parsed.get("entity")):
                continue
            existing.setdefault(k, v)
        parsed["filters"] = existing

    # Late guard: if ranking-style query still has no entity, infer one from
    # user phrasing + live schema column names.
    if parsed.get("query_type") in ("top_n", "bottom_n", "threshold", "growth_ranking", "zero_filter") and not parsed.get("entity"):
        entity_map, _ = _build_schema_keyword_maps()
        hint_groups = [
            (("product", "products", "item", "items"), ("product", "item")),
            (("customer", "customers", "client", "clients", "user", "users"), ("customer", "client", "user")),
            (("region", "regions", "state", "states"), ("region", "state")),
            (("segment", "segments"), ("segment",)),
            (("channel", "channels"), ("channel",)),
            (("platform", "platforms", "channel", "channels"), ("platform", "channel")),
            (("city", "cities", "town", "towns"), ("city",)),
            (("course", "courses"), ("course",)),
            (("student", "students", "learner", "learners"), ("student", "learner")),
            (("category", "categories"), ("category",)),
            (("coupon", "coupons", "promo", "promocode", "coupon code"), ("coupon", "promo")),
            (("payment", "payment mode", "payment method"), ("payment",)),
        ]
        for terms, needles in hint_groups:
            if not any(t in ql for t in terms):
                continue
            match_col = next(
                (
                    col for _, col in entity_map
                    if any(n in col.lower() for n in needles)
                ),
                None,
            )
            if match_col:
                parsed["entity"] = match_col
                break

    # Refresh derived fields in case semantic resolution or previous guards updated intent.
    qt = parsed.get("query_type", qt)
    tr = parsed.get("time_ranges", tr)
    m = parsed.get("metric", m)

    if parsed.get("_force_clarification"):
        parsed["is_complete"] = False
        parsed.setdefault(
            "clarification_question",
            "Please clarify your request so I can avoid an incorrect comparison.",
        )
        return parsed

    # Completeness guards
    if (parsed.get("metric") or "").lower() == "aov":
        if not parsed.get("_aov_revenue_col") or not parsed.get("_count_distinct_key"):
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "I need one revenue column and one order identifier column to compute AOV. "
                "Which should I use?"
            )
            return parsed

    if qt in ("aggregate", "time_series") and m and tr:
        parsed["is_complete"]            = True
        parsed["clarification_question"] = None
    if qt in ("comparison", "intersection"):
        if m and len(tr) >= 2 and _period_key(tr[0]) != _period_key(tr[1]):
            parsed["is_complete"] = True
            parsed["clarification_question"] = None
        else:
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Please provide two different time periods to compare "
                "(for example: 2022 vs 2023)."
            )
    if qt in ("top_n", "bottom_n", "threshold", "zero_filter"):
        if parsed.get("entity") and tr:
            parsed["is_complete"]            = True
            parsed["clarification_question"] = None
    if qt == "growth_ranking":
        if not _has_metric_cue(user_query):
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Which metric should I use for growth ranking "
                "(e.g. revenue, quantity, record count)?"
            )
        elif parsed.get("entity") and len(tr) >= 2 and _period_key(tr[0]) != _period_key(tr[1]):
            parsed["is_complete"] = True
            parsed["clarification_question"] = None
        else:
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Please provide an entity and two different time periods for growth ranking "
                "(for example: product growth in 2022 vs 2023)."
            )

    return parsed


# ── Qwen API call ─────────────────────────────────────────────────────────────

def _call_qwen(user_query: str, schema: str) -> tuple[dict, dict]:
    system = _SYSTEM_TEMPLATE.format(schema=schema)
    payload = {
        "model": QWEN_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_query},
        ],
        "max_tokens": 700,
        "temperature": 0.0,
    }

    if QWEN_ENABLE_REASONING and "qwen" in QWEN_MODEL.lower() and QWEN_REASONING_EFFORT:
        payload["reasoning_effort"] = QWEN_REASONING_EFFORT

    headers = {
        "Authorization": f"Bearer {GROQ_API_TOKEN}",
        "Content-Type": "application/json",
    }

    resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
    if resp.status_code >= 400 and "reasoning_effort" in payload:
        # Backward-safe fallback for providers/models that don't support this field.
        payload.pop("reasoning_effort", None)
        resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
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


# ── Handler ───────────────────────────────────────────────────────────────────

async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

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