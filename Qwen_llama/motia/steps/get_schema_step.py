"""GET /schema - return current ingested schema information."""

import os
import sys
from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.duckdb_connection import get_read_connection


config = {
    "name": "GetSchema",
    "description": "Utility endpoint: returns current DuckDB schema and relationships",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/schema")],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    _ = request
    schema_state = await ctx.state.get("schema_registry", "current")

    conn = get_read_connection()
    try:
        tables = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
            ORDER BY table_name
            """
        ).fetchall()
        table_schema = []
        for (t,) in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            table_schema.append(
                {
                    "table": t,
                    "columns": [{"name": c[0], "type": c[1]} for c in cols],
                }
            )
    finally:
        conn.close()

    return ApiResponse(
        status=200,
        body={
            "tables": table_schema,
            "relationships": (schema_state or {}).get("relationships", []),
            "registry": schema_state or {},
        },
    )
