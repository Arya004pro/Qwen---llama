"""Step 1: Receive Query — HTTP endpoint that accepts analytics queries.

This is the entry point of the workflow. Users POST a natural language
query (e.g. "top 5 products by revenue in March 2024") and the step
kicks off the entire analytics pipeline.

Trigger: HTTP POST /query
Emits:   query::intent.parse
Flow:    sales-analytics-flow
"""

from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http
import uuid
from datetime import datetime, timezone

config = {
    "name": "ReceiveQuery",
    "description": "Accepts analytics queries via HTTP and starts the workflow pipeline",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        http("POST", "/query"),
    ],
    "enqueues": ["query::intent.parse"],
}


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body = request.body or {}
    user_query = body.get("query", "").strip()

    if not user_query:
        return ApiResponse(status=400, body={"error": "Missing 'query' field in request body"})

    # Generate a unique query ID for tracing through the pipeline
    query_id = f"Q-{int(datetime.now(timezone.utc).timestamp() * 1000)}-{uuid.uuid4().hex[:6]}"

    ctx.logger.info("📥 Query received", {
        "queryId": query_id,
        "query": user_query,
    })

    # Store the initial query in state
    await ctx.state.set("queries", query_id, {
        "id": query_id,
        "query": user_query,
        "status": "received",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    })

    # Enqueue for intent parsing
    await ctx.enqueue({
        "topic": "query::intent.parse",
        "data": {
            "queryId": query_id,
            "query": user_query,
        },
    })

    return ApiResponse(status=200, body={
        "queryId": query_id,
        "status": "processing",
        "message": "Query accepted — pipeline started",
    })
