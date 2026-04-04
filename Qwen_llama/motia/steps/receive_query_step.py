"""Step 1: Receive Query - HTTP entry point.

Supports:
- Clarification replies inside an existing session.
- Follow-up turns that reuse prior parsed intent context (session-aware).
"""

import os
import sys
import uuid

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from datetime import datetime, timezone
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "QueryIntake",
    "description": "Receives user query and starts or resumes the analytics workflow.",
    "flows": ["sales-analytics-flow"],
    "triggers": [http("POST", "/query")],
    "enqueues": ["query::intent.parse"],
}

_SESSIONS_NS = "query_sessions"


def _new_query_id(now: datetime) -> str:
    return f"Q-{int(now.timestamp()*1000)}-{uuid.uuid4().hex[:6]}"


async def _resolve_session_context(
    ctx: FlowContext[Any],
    session_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not session_id:
        return None, None

    session_state = await ctx.state.get(_SESSIONS_NS, session_id)
    previous = None

    if session_state and session_state.get("last_query_id"):
        previous = await ctx.state.get("queries", session_state.get("last_query_id"))

    # Backward compatibility: older clients may send prior queryId as sessionId.
    if not previous:
        previous = await ctx.state.get("queries", session_id)

    return session_state, previous


async def _upsert_session(
    ctx: FlowContext[Any],
    session_id: str,
    query_id: str,
    now_iso: str,
) -> None:
    existing = await ctx.state.get(_SESSIONS_NS, session_id) or {}
    query_ids = list(existing.get("query_ids") or [])
    if not query_ids or query_ids[-1] != query_id:
        query_ids.append(query_id)
    query_ids = query_ids[-30:]

    await ctx.state.set(_SESSIONS_NS, session_id, {
        **existing,
        "id": session_id,
        "createdAt": existing.get("createdAt", now_iso),
        "updatedAt": now_iso,
        "last_query_id": query_id,
        "query_ids": query_ids,
    })


def _build_followup_context(
    session_id: str,
    previous: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not previous:
        return None

    prev_parsed = previous.get("parsed") or {}
    prev_query = str(previous.get("query") or "").strip()
    prev_query_id = str(previous.get("id") or "").strip()

    if not prev_parsed and not prev_query:
        return None

    return {
        "sessionId": session_id,
        "previousQueryId": prev_query_id,
        "previousQuery": prev_query,
        "previousParsed": prev_parsed,
    }


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body = request.body or {}
    user_query = body.get("query", "").strip()
    session_id = body.get("sessionId", "").strip()

    if not user_query:
        return ApiResponse(status=400, body={"error": "Missing 'query' field"})

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # No session provided -> create a new session anchored to the first query id.
    if not session_id:
        query_id = _new_query_id(now)
        session_id = query_id
        ctx.logger.info("New query (new session)", {
            "queryId": query_id,
            "sessionId": session_id,
            "query": user_query,
        })

        await ctx.state.set("queries", query_id, {
            "id": query_id,
            "sessionId": session_id,
            "query": user_query,
            "status": "received",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "status_timestamps": {"received": now_iso},
        })
        await _upsert_session(ctx, session_id, query_id, now_iso)

        await ctx.enqueue({
            "topic": "query::intent.parse",
            "data": {"queryId": query_id, "query": user_query},
        })

        return ApiResponse(status=200, body={
            "queryId": query_id,
            "sessionId": session_id,
            "status": "processing",
            "message": "Query accepted",
        })

    # Existing session turn (clarification or follow-up).
    _session_state, previous = await _resolve_session_context(ctx, session_id)

    # Clarification reply path: mutate the same query id waiting for clarification.
    if previous and previous.get("status") == "needs_clarification":
        query_id = str(previous.get("id") or session_id)
        ctx.logger.info("Clarification reply", {
            "sessionId": session_id,
            "queryId": query_id,
            "query": user_query,
        })

        combined_query = f"{previous.get('query', '')}. To clarify: {user_query}"
        prev_ts = (previous or {}).get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **previous,
            "sessionId": session_id,
            "status": "received",
            "lastQuery": user_query,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "received": now_iso},
        })
        await _upsert_session(ctx, session_id, query_id, now_iso)

        await ctx.enqueue({
            "topic": "query::intent.parse",
            "data": {"queryId": query_id, "query": combined_query},
        })

        return ApiResponse(status=200, body={
            "queryId": query_id,
            "sessionId": session_id,
            "status": "processing",
            "message": "Clarification accepted",
        })

    # Normal follow-up/new turn in existing session: create a fresh query id and
    # pass previous parsed intent as context for referential queries.
    query_id = _new_query_id(now)
    followup_context = _build_followup_context(session_id, previous)
    previous_query_id = (followup_context or {}).get("previousQueryId")

    ctx.logger.info("Session query", {
        "queryId": query_id,
        "sessionId": session_id,
        "query": user_query,
        "has_followup_context": bool(followup_context),
    })

    await ctx.state.set("queries", query_id, {
        "id": query_id,
        "sessionId": session_id,
        "previousQueryId": previous_query_id,
        "query": user_query,
        "status": "received",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "status_timestamps": {"received": now_iso},
    })
    await _upsert_session(ctx, session_id, query_id, now_iso)

    enqueue_data: dict[str, Any] = {"queryId": query_id, "query": user_query}
    if followup_context:
        enqueue_data["followupContext"] = followup_context

    await ctx.enqueue({
        "topic": "query::intent.parse",
        "data": enqueue_data,
    })

    return ApiResponse(status=200, body={
        "queryId": query_id,
        "sessionId": session_id,
        "status": "processing",
        "message": "Query accepted",
    })
