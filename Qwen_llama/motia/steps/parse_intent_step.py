"""Step 2: Parse Intent

Extracts structured intent from the user query and forwards it downstream.
Supports multi-turn clarification via mergedParsed.
"""

import os, sys

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from typing import Any
from motia import FlowContext, queue
from state.conversation_state import ConversationState

config = {
    "name": "ParseIntent",
    "description": (
        "Parses user query into structured intent: entity, metric, time range, "
        "ranking, comparison/intersection/growth flags, and threshold details."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::ambiguity.check"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")

    if merged_parsed:
        ctx.logger.info("🔄 Clarification path — using merged parsed state",
                        {"queryId": query_id})
        parsed = merged_parsed
    else:
        ctx.logger.info("🔍 Parsing intent", {"queryId": query_id, "query": user_query})
        s = ConversationState()
        s.update_from_user(user_query)
        parsed = {
            "entity":            s.entity,
            "metric":            s.metric,
            "time_range":        s.time_range,
            "raw_time_text":     s.raw_time_text or user_query,
            "ranking":           s.ranking,
            "top_n":             s.top_n,
            "is_comparison":     s.is_comparison,
            "is_intersection":   s.is_intersection,
            "is_growth_ranking": s.is_growth_ranking,
            "threshold_value":   s.threshold_value,
            "threshold_type":    s.threshold_type,
        }

    ctx.logger.info("✅ Intent parsed", {
        "queryId":           query_id,
        "entity":            parsed.get("entity"),
        "metric":            parsed.get("metric"),
        "ranking":           parsed.get("ranking"),
        "is_comparison":     parsed.get("is_comparison"),
        "is_growth_ranking": parsed.get("is_growth_ranking"),
        "is_intersection":   parsed.get("is_intersection"),
        "threshold_type":    parsed.get("threshold_type"),
        "threshold_value":   parsed.get("threshold_value"),
    })

    qs = await ctx.state.get("queries", query_id)
    if qs:
        await ctx.state.set("queries", query_id, {
            **qs, "status": "intent_parsed", "parsed": parsed,
        })

    await ctx.enqueue({
        "topic": "query::ambiguity.check",
        "data":  {"queryId": query_id, "query": user_query, "parsed": parsed},
    })