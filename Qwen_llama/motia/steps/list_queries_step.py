"""List All Queries — HTTP endpoint to show all processed queries.

A utility step to list all queries that have been processed through
the workflow, showing their status and results.

Trigger: HTTP GET /queries
Flow:    sales-analytics-flow
"""

from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "ListQueries",
    "description": "Returns all queries that have been processed through the workflow",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        http("GET", "/queries"),
    ],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    _ = request
    queries = await ctx.state.list("queries")

    ctx.logger.info("📋 Listing all queries", {"count": len(queries)})

    return ApiResponse(status=200, body={
        "queries": queries,
        "count": len(queries),
    })
