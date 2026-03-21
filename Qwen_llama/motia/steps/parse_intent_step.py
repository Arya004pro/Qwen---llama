"""Step 2: Parse Intent — Extracts entity, metric, time_range, ranking, and
comparison intent from the user query."""

import os
import sys

# ── Fix imports FIRST before anything else ──────────────────────────────────
_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)

for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ────────────────────────────────────────────────────────────────────────────

from typing import Any
from motia import FlowContext, queue

from state.conversation_state import ConversationState

config = {
    "name": "ParseIntent",
    "description": "Interprets the user question into structured intent fields (entity, metric, time range, ranking, comparison flag)",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::intent.parse"),
    ],
    "enqueues": ["query::ambiguity.check"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")

    ctx.logger.info("🔍 Parsing intent", {"queryId": query_id, "query": user_query})

    state = ConversationState()
    state.update_from_user(user_query)

    parsed = {
        "entity": state.entity,
        "metric": state.metric,
        "time_range": state.time_range,
        "raw_time_text": state.raw_time_text or user_query,
        "ranking": state.ranking,
        "top_n": state.top_n,
        "is_comparison": state.is_comparison,   # ← NEW
    }

    ctx.logger.info("✅ Intent parsed", {
        "queryId": query_id,
        "entity": parsed["entity"],
        "metric": parsed["metric"],
        "time_range": parsed["time_range"],
        "ranking": parsed["ranking"],
        "top_n": parsed["top_n"],
        "is_comparison": parsed["is_comparison"],
    })

    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "intent_parsed",
            "parsed": parsed,
        })

    await ctx.enqueue({
        "topic": "query::ambiguity.check",
        "data": {
            "queryId": query_id,
            "query": user_query,
            "parsed": parsed,
        },
    })