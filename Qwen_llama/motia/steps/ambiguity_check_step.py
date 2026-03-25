"""Step 3: Ambiguity Check — generalised for any dataset.

Changes vs original:
  - Clarification questions use generic language (dimension, metric)
    rather than hard-coding entity types or metric names.
  - aggregate + metric + time = always complete (Bug 3 guard preserved).
  - All query type routing logic unchanged.
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
    "description": "Routes to SQL generation or saves clarification. Generalised for any dataset.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::ambiguity.check")],
    "enqueues": ["query::text.to.sql"],
}


def _is_actually_complete(parsed: dict) -> tuple[bool, str | None]:
    """
    Determine true completeness. Overrides Qwen's is_complete for edge cases.
    Returns (complete, clarification_question_or_None).
    """
    qt  = parsed.get("query_type", "top_n")
    tr  = parsed.get("time_ranges", [])
    m   = parsed.get("metric")
    ent = parsed.get("entity")
    cq  = parsed.get("clarification_question")

    # Bug 3 fix: aggregate queries NEVER need entity
    if qt == "aggregate":
        if m and tr:
            return True, None
        if not tr:
            return False, "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
        if not m:
            return False, "What metric should I measure? (e.g. total fare, driver earnings, revenue, number of rides)"
        return True, None

    # Ranked queries need an entity/dimension
    if qt in ("top_n", "bottom_n", "threshold", "growth_ranking", "zero_filter"):
        if not ent:
            return False, (
                cq or
                "Which dimension should I group by? "
                "(e.g. driver, city, state, vehicle type, customer, product, category)"
            )
        if not tr:
            return False, "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
        return True, None

    # Comparison / intersection need 2 time ranges
    if qt in ("comparison", "intersection"):
        if not tr or len(tr) < 2:
            return False, (
                cq or
                "Please specify two time periods to compare "
                "(e.g. Q1 2024 vs Q2 2024, or January vs February 2024)"
            )
        return True, None

    # Default: trust Qwen's is_complete flag
    is_complete   = parsed.get("is_complete", True)
    clarification = parsed.get("clarification_question") if not is_complete else None
    return is_complete, clarification


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    is_complete, clarification = _is_actually_complete(parsed)

    ctx.logger.info("🔎 Ambiguity check", {
        "queryId":        query_id,
        "is_complete":    is_complete,
        "clarification":  clarification,
        "query_type":     parsed.get("query_type"),
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