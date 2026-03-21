"""Step 2: Parse Intent — Extracts entity, metric, time_range, and ranking from the query.

Uses the existing ConversationState class to parse the user's natural
language query into structured fields (entity, metric, time_range, ranking).

Trigger: Queue (query::intent.parse)
Emits:   query::ambiguity.check
Flow:    sales-analytics-flow
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import re
from typing import Any
from motia import FlowContext, queue

# ── Import existing project modules ──
from shared_config import PROJECT_ROOT
import sys
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from state.conversation_state import ConversationState

config = {
    "name": "ParseIntent",
    "description": "Extracts entity, metric, time_range, and ranking from the user query",
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

    # Use the existing ConversationState to parse
    state = ConversationState()
    state.update_from_user(user_query)

    parsed = {
        "entity": state.entity,
        "metric": state.metric,
        "time_range": state.time_range,
        "raw_time_text": state.raw_time_text or user_query,
        "ranking": state.ranking,
        "top_n": state.top_n,
    }

    ctx.logger.info("✅ Intent parsed", {
        "queryId": query_id,
        "entity": parsed["entity"],
        "metric": parsed["metric"],
        "time_range": parsed["time_range"],
        "ranking": parsed["ranking"],
        "top_n": parsed["top_n"],
    })

    # Update query state
    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "intent_parsed",
            "parsed": parsed,
        })

    # Emit to ambiguity check
    await ctx.enqueue({
        "topic": "query::ambiguity.check",
        "data": {
            "queryId": query_id,
            "query": user_query,
            "parsed": parsed,
        },
    })
