"""Step 3: Ambiguity Check — generalised for any dataset.

Changes vs original:
  - Clarification questions now pull dimension/metric examples from the live
    DuckDB schema instead of hardcoding Uber/Zomato column names.
  - time_series query type is handled: only metric + time_ranges needed.
    Entity is NEVER required for time_series.
  - All other routing logic unchanged.
"""

import os
import sys
from datetime import datetime, timezone

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "AmbiguityCheck",
    "description": (
        "Routes to SQL generation or saves clarification. "
        "Clarification examples are drawn from the live schema — no hardcoded "
        "column/domain names. Handles time_series (trend) queries."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::ambiguity.check")],
    "enqueues": ["query::text.to.sql"],
}


# ── Live-schema helpers ────────────────────────────────────────────────────────

def _live_entity_examples(max_items: int = 5) -> str:
    """
    Return a comma-separated list of actual entity (grouping) column names
    from the live DuckDB schema.  Falls back to a generic example if the
    schema cannot be read.
    """
    try:
        from db.duckdb_connection import get_read_connection

        conn = get_read_connection()
        try:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
            ]

            _TEXT_TYPES   = ("VARCHAR", "CHAR", "TEXT", "STRING")
            _DATE_HINTS   = ("date", "time", "created", "updated", "timestamp")
            _ENTITY_HINTS = ("name", "id", "city", "state", "type", "code",
                             "driver", "customer", "category", "product",
                             "location", "store", "region", "brand")

            candidates = []
            seen: set[str] = set()

            for table in tables:
                cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
                for col, dtype, *_ in cols:
                    col_l = col.lower()
                    dtype_u = dtype.upper()

                    # Skip non-text, date-like, and already-seen columns
                    if not any(t in dtype_u for t in _TEXT_TYPES):
                        continue
                    if any(k in col_l for k in _DATE_HINTS):
                        continue
                    if col_l in seen:
                        continue
                    if not any(k in col_l for k in _ENTITY_HINTS):
                        continue

                    # Prefer *_name columns; deprioritise raw id columns
                    priority = 0
                    if col_l.endswith("_name") or col_l == "name":
                        priority = 3
                    elif any(k in col_l for k in ("city", "state", "type", "category")):
                        priority = 2
                    elif col_l.endswith("_id") or col_l == "id":
                        priority = 0
                    else:
                        priority = 1

                    candidates.append((priority, col))
                    seen.add(col_l)

        finally:
            conn.close()

        if not candidates:
            return "e.g. customer, product, region, category, channel"

        candidates.sort(key=lambda x: -x[0])
        # Format: strip _name suffix for readability
        formatted = [
            c.replace("_name", "").replace("_", " ")
            for _, c in candidates[:max_items]
        ]
        return ", ".join(formatted)

    except Exception:
        return "e.g. customer, product, region, category, channel"


def _live_metric_examples(max_items: int = 5) -> str:
    """
    Return a comma-separated list of actual numeric metric column names
    from the live DuckDB schema.  Falls back to a generic example.
    """
    try:
        from db.duckdb_connection import get_read_connection

        conn = get_read_connection()
        try:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
            ]

            _NUM_TYPES  = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
            _SKIP_HINTS = ("_id", "_key", "_code", "year", "month", "day",
                           "week", "quarter", "flag", "bool", "is_")
            _METRIC_HINTS = ("price", "fare", "amount", "earning", "revenue",
                             "commission", "quantity", "qty", "count",
                             "total", "fee", "cost", "distance", "duration",
                             "discount", "sales", "profit", "margin")

            candidates = []
            seen: set[str] = set()

            for table in tables:
                cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
                for col, dtype, *_ in cols:
                    col_l = col.lower()
                    dtype_u = dtype.upper()

                    if not any(t in dtype_u for t in _NUM_TYPES):
                        continue
                    if any(k in col_l for k in _SKIP_HINTS):
                        continue
                    if col_l in seen:
                        continue
                    if not any(k in col_l for k in _METRIC_HINTS):
                        continue

                    # Score by specificity
                    score = sum(1 for k in _METRIC_HINTS if k in col_l)
                    candidates.append((score, col))
                    seen.add(col_l)

        finally:
            conn.close()

        if not candidates:
            return "e.g. revenue, quantity, discount, margin, record count"

        candidates.sort(key=lambda x: -x[0])
        formatted = [c.replace("_", " ") for _, c in candidates[:max_items]]
        # Always add a generic count option if not already present.
        if not any("count" in f or "number" in f for f in formatted):
            formatted.append("record count")
        return ", ".join(formatted[:max_items])

    except Exception:
        return "e.g. revenue, quantity, discount, margin, record count"


# ── Completeness check ─────────────────────────────────────────────────────────

def _is_actually_complete(parsed: dict) -> tuple[bool, str | None]:
    """Returns (complete, clarification_question_or_None)."""
    qt  = parsed.get("query_type", "top_n")
    tr  = parsed.get("time_ranges", [])
    m   = parsed.get("metric")
    ent = parsed.get("entity")
    cq  = parsed.get("clarification_question")
    if parsed.get("_force_clarification") and cq:
        return False, str(cq)
    if (m or "").lower() == "aov":
        if not parsed.get("_aov_revenue_col") or not parsed.get("_count_distinct_key"):
            return False, (
                cq or
                "I need one revenue column and one order identifier column to compute AOV. Which should I use?"
            )

    # ── time_series (trend) ────────────────────────────────────────────────────
    if qt == "time_series":
        if m and tr:
            return True, None
        if not tr:
            return False, (
                "What time period should I use? "
                "(e.g. all of 2024, Q1 2025, January to June 2025)"
            )
        if not m:
            return False, (
                "What metric should I measure? "
                f"({_live_metric_examples()})"
            )
        return True, None

    if qt == "forecast":
        if m and tr:
            return True, None
        if not tr:
            return False, "What historical period should I use to train the forecast? (e.g. all of 2024, last 2 years)"
        if not m:
            return False, f"What metric should I forecast? ({_live_metric_examples()})"
        return True, None

    # ── aggregate ─────────────────────────────────────────────────────────────
    if qt == "aggregate":
        if m and tr:
            return True, None
        if not tr:
            return False, "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
        if not m:
            return False, (
                f"What metric should I measure? ({_live_metric_examples()})"
            )
        return True, None

    # ── ranked queries ────────────────────────────────────────────────────────
    if qt in ("top_n", "bottom_n", "threshold", "zero_filter"):
        if not ent:
            return False, (
                cq or
                f"Which dimension should I group by? ({_live_entity_examples()})"
            )
        if not tr:
            return False, "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
        return True, None

    # ── comparison / intersection ─────────────────────────────────────────────
    if qt in ("comparison", "intersection"):
        if not tr or len(tr) < 2:
            return False, (
                cq or
                "Please specify two time periods to compare "
                "(e.g. Q1 2024 vs Q2 2024, or January vs February 2024)"
            )
        p1 = tr[0] or {}
        p2 = tr[1] or {}
        if (p1.get("start"), p1.get("end")) == (p2.get("start"), p2.get("end")):
            return False, (
                cq or
                "Please specify two different time periods to compare "
                "(e.g. 2023 vs 2024)."
            )
        return True, None

    if qt == "growth_ranking":
        if cq and "metric should i use for growth ranking" in cq.lower():
            return False, cq
        if not ent:
            return False, (
                cq or
                f"Which dimension should I group by? ({_live_entity_examples()})"
            )
        if not tr or len(tr) < 2:
            return False, (
                cq or
                "Please specify two time periods to compare "
                "(e.g. Q1 2024 vs Q2 2024, or January vs February 2024)"
            )
        p1 = tr[0] or {}
        p2 = tr[1] or {}
        if (p1.get("start"), p1.get("end")) == (p2.get("start"), p2.get("end")):
            return False, (
                cq or
                "Please specify two different time periods to compare "
                "(e.g. 2023 vs 2024)."
            )
        return True, None

    # Default — trust Qwen's is_complete flag
    is_complete   = parsed.get("is_complete", True)
    clarification = parsed.get("clarification_question") if not is_complete else None
    return is_complete, clarification


# ── Handler ───────────────────────────────────────────────────────────────────

async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    is_complete, clarification = _is_actually_complete(parsed)

    ctx.logger.info("🔎 Ambiguity check", {
        "queryId":       query_id,
        "is_complete":   is_complete,
        "clarification": clarification,
        "query_type":    parsed.get("query_type"),
        "time_bucket":   parsed.get("time_bucket"),
    })

    qs = await ctx.state.get("queries", query_id)

    if not is_complete and clarification:
        ctx.logger.warn("⚠️ Needs clarification", {
            "queryId": query_id, "question": clarification
        })
        if qs:
            now_iso = datetime.now(timezone.utc).isoformat()
            prev_ts = qs.get("status_timestamps", {})
            await ctx.state.set("queries", query_id, {
                **qs,
                "status":        "needs_clarification",
                "clarification": clarification,
                "parsed":        parsed,
                "updatedAt":     now_iso,
                "status_timestamps": {**prev_ts, "needs_clarification": now_iso},
            })
        return

    # Complete — forward to SQL generation
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs, "status": "ambiguity_checked",
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "ambiguity_checked": now_iso},
        })

    await ctx.enqueue({
        "topic": "query::text.to.sql",
        "data":  {"queryId": query_id, "query": user_query, "parsed": parsed},
    })