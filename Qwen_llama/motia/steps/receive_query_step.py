"""Step 1: Receive Query — HTTP entry point.

Supports multi-turn clarification: if sessionId is passed and the previous
response was needs_clarification, merges the clarification into the existing
parsed intent and resumes the pipeline.
"""

import os, sys, uuid
_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from datetime import datetime, timezone
from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "ReceiveQuery",
    "description": "Entry point. Accepts natural-language query. Supports clarification via sessionId.",
    "flows": ["sales-analytics-flow"],
    "triggers": [http("POST", "/query")],
    "enqueues": ["query::intent.parse"],
}


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body       = request.body or {}
    user_query = body.get("query", "").strip()
    session_id = body.get("sessionId", "").strip()

    if not user_query:
        return ApiResponse(status=400, body={"error": "Missing 'query' field"})

    now = datetime.now(timezone.utc)

    # ── Clarification reply ────────────────────────────────────────────────────
    if session_id:
        previous = await ctx.state.get("queries", session_id)
        if previous and previous.get("status") == "needs_clarification":
            ctx.logger.info("🔄 Clarification reply", {"sessionId": session_id, "query": user_query})

            # Merge clarification text into previous parsed intent.
            # Re-send the combined query so Qwen can re-parse with full context.
            prev_parsed = previous.get("parsed", {})
            combined_query = (
                f"{previous.get('query', '')}. "
                f"To clarify: {user_query}"
            )

            query_id = session_id
            await ctx.state.set("queries", query_id, {
                **previous,
                "status":    "received",
                "lastQuery": user_query,
                "updatedAt": now.isoformat(),
            })

            await ctx.enqueue({
                "topic": "query::intent.parse",
                "data":  {"queryId": query_id, "query": combined_query},
            })

            return ApiResponse(status=200, body={
                "queryId": query_id, "sessionId": session_id,
                "status": "processing", "message": "Clarification accepted",
            })

    # ── New query ──────────────────────────────────────────────────────────────
    query_id = f"Q-{int(now.timestamp()*1000)}-{uuid.uuid4().hex[:6]}"
    ctx.logger.info("📥 New query", {"queryId": query_id, "query": user_query})

    await ctx.state.set("queries", query_id, {
        "id": query_id, "query": user_query,
        "status": "received", "createdAt": now.isoformat(),
    })

    await ctx.enqueue({
        "topic": "query::intent.parse",
        "data":  {"queryId": query_id, "query": user_query},
    })

    return ApiResponse(status=200, body={
        "queryId": query_id, "sessionId": query_id,
        "status": "processing", "message": "Query accepted",
    })