"""Step 3: Ambiguity Check — reads is_complete from Qwen's parsed intent.

Qwen already determined if the query is complete and what to ask.
This step either routes to SQL generation or saves the clarification question.
No second LLM call needed unless Qwen failed to parse.
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

config = {
    "name": "AmbiguityCheck",
    "description": "Routes to SQL generation or saves clarification question from Qwen intent.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::ambiguity.check")],
    "enqueues": ["query::text.to.sql"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    is_complete = parsed.get("is_complete", True)
    clarification = parsed.get("clarification_question")

    ctx.logger.info("🔎 Ambiguity check", {
        "queryId": query_id,
        "is_complete": is_complete,
        "clarification": clarification,
    })

    qs = await ctx.state.get("queries", query_id)

    if not is_complete and clarification:
        ctx.logger.warn("⚠️ Needs clarification", {"queryId": query_id, "question": clarification})
        if qs:
            await ctx.state.set("queries", query_id, {
                **qs,
                "status":        "needs_clarification",
                "clarification": clarification,
                "parsed":        parsed,
            })
        return

    # Complete — forward to SQL generation
    if qs:
        await ctx.state.set("queries", query_id, {
            **qs, "status": "ambiguity_checked",
        })

    await ctx.enqueue({
        "topic": "query::text.to.sql",
        "data":  {"queryId": query_id, "query": user_query, "parsed": parsed},
    })