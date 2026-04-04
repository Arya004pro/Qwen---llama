"""Utility endpoint to fetch latest auto-generated business digest report."""

import os
import sys
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "BusinessReportGet",
    "description": "Returns the latest weekly/monthly business digest report.",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/reports/latest")],
    "enqueues": [],
}


def _normalize_period(raw: str) -> str:
    p = (raw or "").strip().lower()
    if p in {"weekly", "monthly"}:
        return p
    return ""


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    q = getattr(request, "query_params", None)
    if not isinstance(q, dict):
        q = {}
    period = _normalize_period(str(q.get("period", "")))
    key = f"latest_{period}" if period else "latest"

    report = await ctx.state.get("business_reports", key)
    if not report:
        return ApiResponse(status=404, body={
            "error": "No business report found yet",
            "period": period or "latest",
        })

    return ApiResponse(status=200, body=report)
