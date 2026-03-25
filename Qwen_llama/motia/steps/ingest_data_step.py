"""POST /ingest - ingest uploaded data files into DuckDB tables."""

import os
import sys
from datetime import datetime, timezone
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.data_ingester import ingest_files


config = {
    "name": "IngestData",
    "description": "Ingest one or more uploaded files into DuckDB",
    "flows": ["sales-analytics-ingest"],
    "triggers": [http("POST", "/ingest")],
    "enqueues": [],
}


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body = request.body or {}
    files = body.get("files") or []
    reset_db = bool(body.get("reset_db", False))
    use_llm_grouping = bool(body.get("use_llm_grouping", False))

    if not isinstance(files, list) or not files:
        return ApiResponse(status=400, body={"error": "files[] is required"})

    try:
        result = ingest_files(files, reset_db=reset_db)
    except Exception as exc:
        ctx.logger.error("Ingest failed", {"error": str(exc)})
        return ApiResponse(status=500, body={"error": str(exc)})

    now_iso = datetime.now(timezone.utc).isoformat()
    await ctx.state.set(
        "schema_registry",
        "current",
        {
            "updatedAt": now_iso,
            "source": "upload",
            "use_llm_grouping": use_llm_grouping,
            **result,
        },
    )
    return ApiResponse(status=200, body={"ok": True, **result})
