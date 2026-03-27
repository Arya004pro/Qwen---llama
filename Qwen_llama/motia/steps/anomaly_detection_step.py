"""Step: Anomaly Detection.

Detects statistical outliers from query result rows using simple z-score logic
before formatting. This is schema-agnostic and works on any numeric result
column (prefers value/delta when present).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from motia import FlowContext, queue

config = {
    "name": "AnomalyDetection",
    "description": (
        "Detects statistical outliers in executed query results using z-score "
        "and forwards anomaly metadata to formatter."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::detect.anomalies")],
    "enqueues": ["query::format.result"],
}


def _to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _pick_numeric_key(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    preferred = ("value", "delta", "raw_value", "value2", "value1")
    for key in preferred:
        if any(_to_float(r.get(key)) is not None for r in rows):
            return key
    for key in rows[0].keys():
        if any(_to_float(r.get(key)) is not None for r in rows):
            return key
    return None


def _label_for_row(row: dict[str, Any], idx: int) -> str:
    for k in ("name", "period", "label", "entity"):
        if row.get(k):
            return str(row[k])
    return f"row_{idx + 1}"


def _detect_anomalies(rows: list[dict[str, Any]], query_type: str) -> dict[str, Any]:
    if not rows:
        return {"items": []}

    value_key = _pick_numeric_key(rows)
    if not value_key:
        return {"items": []}

    points: list[tuple[str, float]] = []
    for i, r in enumerate(rows):
        v = _to_float(r.get(value_key))
        if v is None:
            continue
        points.append((_label_for_row(r, i), v))

    if len(points) < 6:
        return {"items": []}

    vals = [v for _, v in points]
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(var)
    if std <= 0:
        return {"items": []}

    flagged = []
    for label, v in points:
        z = (v - mean) / std
        ratio = (v / mean) if mean else None
        if abs(z) >= 2.5 or (mean > 0 and ratio is not None and ratio >= 3.0):
            flagged.append(
                {
                    "label": label,
                    "value": v,
                    "z_score": round(z, 3),
                    "ratio_to_mean": round(ratio, 3) if ratio is not None else None,
                }
            )

    flagged.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    return {
        "query_type": query_type,
        "value_key": value_key,
        "count": len(vals),
        "mean": mean,
        "std": std,
        "items": flagged[:5],
    }


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    parsed = input_data.get("parsed", {}) or {}
    results = input_data.get("results", []) or []

    qt = parsed.get("query_type", "")
    anomalies = _detect_anomalies(results, qt)
    flagged_cnt = len(anomalies.get("items", []))
    ctx.logger.info(
        "Anomaly detection complete",
        {"queryId": query_id, "flagged": flagged_cnt, "value_key": anomalies.get("value_key")},
    )

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "anomalies": anomalies,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "anomaly_detected": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::format.result",
            "data": {
                **input_data,
                "anomalies": anomalies,
            },
        }
    )

