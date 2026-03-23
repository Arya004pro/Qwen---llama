"""Step 1: Receive Query — HTTP endpoint that accepts analytics queries.

Now supports multi-turn clarification:
  - On first query, creates a new session.
  - If a ``sessionId`` is passed in the body (follow-up clarification), the
    step loads the previous parsed state and merges the new text into it using
    ConversationState.merge_clarification().  The existing queryId is re-used
    so the pipeline continues where it left off.

Trigger: HTTP POST /query
Body:
  { "query": "Which city had highest revenue in Q1?",
    "sessionId": "optional — pass the queryId from the previous needs_clarification response" }
"""

from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http
import uuid
import sys
import os

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from datetime import datetime, timezone
from state.conversation_state import ConversationState

config = {
    "name": "ReceiveQuery",
    "description": (
        "Entry point: accepts a natural-language analytics question. "
        "Supports multi-turn clarification via optional sessionId field."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [
        http("POST", "/query"),
    ],
    "enqueues": ["query::intent.parse"],
}


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body       = request.body or {}
    user_query = body.get("query", "").strip()
    session_id = body.get("sessionId", "").strip()   # optional

    if not user_query:
        return ApiResponse(status=400, body={"error": "Missing 'query' field in request body"})

    now = datetime.now(timezone.utc)

    # ── Multi-turn clarification path ─────────────────────────────────────────
    if session_id:
        previous = await ctx.state.get("queries", session_id)
        if previous and previous.get("status") == "needs_clarification":
            ctx.logger.info("🔄 Clarification received for existing session", {
                "sessionId": session_id,
                "query":     user_query,
            })

            # Rebuild state from what was already parsed and merge the new text
            prev_parsed = previous.get("parsed", {})
            state = ConversationState()
            state.entity        = prev_parsed.get("entity")
            state.metric        = prev_parsed.get("metric")
            state.time_range    = prev_parsed.get("time_range")
            state.raw_time_text = prev_parsed.get("raw_time_text")
            state.ranking       = prev_parsed.get("ranking")
            state.top_n         = prev_parsed.get("top_n", 5)
            state.is_comparison = prev_parsed.get("is_comparison", False)

            state.merge_clarification(user_query)

            merged_parsed = {
                "entity":        state.entity,
                "metric":        state.metric,
                "time_range":    state.time_range,
                "raw_time_text": state.raw_time_text or user_query,
                "ranking":       state.ranking,
                "top_n":         state.top_n,
                "is_comparison": state.is_comparison,
            }

            # Reuse the same session_id as the query_id so state tracking is continuous
            query_id = session_id

            await ctx.state.set("queries", query_id, {
                **previous,
                "status":          "received",
                "lastQuery":       user_query,
                "updatedAt":       now.isoformat(),
                "parsed":          merged_parsed,
                "clarificationOf": session_id,
            })

            await ctx.enqueue({
                "topic": "query::intent.parse",
                "data":  {
                    "queryId":       query_id,
                    "query":         user_query,
                    "mergedParsed":  merged_parsed,   # parse_intent_step skips re-parse
                },
            })

            return ApiResponse(status=200, body={
                "queryId":   query_id,
                "sessionId": session_id,
                "status":    "processing",
                "message":   "Clarification accepted — pipeline resumed",
            })

    # ── New query path ────────────────────────────────────────────────────────
    query_id = f"Q-{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:6]}"

    ctx.logger.info("📥 Query received", {
        "queryId": query_id,
        "query":   user_query,
    })

    await ctx.state.set("queries", query_id, {
        "id":        query_id,
        "query":     user_query,
        "status":    "received",
        "createdAt": now.isoformat(),
    })

    await ctx.enqueue({
        "topic": "query::intent.parse",
        "data":  {
            "queryId": query_id,
            "query":   user_query,
        },
    })

    return ApiResponse(status=200, body={
        "queryId":   query_id,
        "sessionId": query_id,   # echo back so client can pass it on clarification
        "status":    "processing",
        "message":   "Query accepted — pipeline started",
    })