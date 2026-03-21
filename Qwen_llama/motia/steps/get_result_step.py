"""Get Query Result — HTTP endpoint to retrieve processed query results.

This is a utility step that allows users to check the status and
results of a previously submitted query using its query ID.

Trigger: HTTP GET /query/:queryId
"""

from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "GetQueryResult",
    "description": "Utility endpoint: returns status and results for a previously submitted query id",
    "flows": ["sales-analytics-utilities"],
    "triggers": [
        http("GET", "/query/:queryId"),
    ],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    query_id = request.path_params.get("queryId", "")

    if not query_id:
        return ApiResponse(status=400, body={"error": "Missing queryId parameter"})

    ctx.logger.info("📋 Fetching query result", {"queryId": query_id})

    query_state = await ctx.state.get("queries", query_id)

    if not query_state:
        return ApiResponse(status=404, body={"error": f"Query {query_id} not found"})

    return ApiResponse(status=200, body=query_state)
