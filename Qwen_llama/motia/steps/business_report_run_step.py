"""Manual trigger endpoint for business digest reports."""

import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "BusinessReportRun",
    "description": "HTTP endpoint to trigger weekly/monthly business digest generation.",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("POST", "/reports/run")],
    "enqueues": ["report::generate"],
}


def _normalize_period(raw: str) -> str:
    p = (raw or "").strip().lower()
    if p in {"weekly", "monthly", "both"}:
        return p
    return "weekly"


async def handler(request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    body = request.body or {}
    period = _normalize_period(str(body.get("period") or "weekly"))
    requested_by = str(body.get("requestedBy") or "manual")
    run_id = f"RPT-{int(datetime.now(timezone.utc).timestamp() * 1000)}-{uuid.uuid4().hex[:6]}"

    periods = ["weekly", "monthly"] if period == "both" else [period]
    for p in periods:
        await ctx.enqueue({
            "topic": "report::generate",
            "data": {
                "period": p,
                "trigger": "http",
                "runId": run_id,
                "requestedBy": requested_by,
            },
        })

    return ApiResponse(status=202, body={
        "status": "accepted",
        "runId": run_id,
        "periods": periods,
        "message": "Business digest generation queued",
    })

