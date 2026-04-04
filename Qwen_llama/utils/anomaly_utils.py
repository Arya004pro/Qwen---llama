"""Shared anomaly detection helpers."""

from __future__ import annotations

import math
from typing import Any


def to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def pick_numeric_key(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    preferred = ("value", "delta", "raw_value", "value2", "value1")
    for key in preferred:
        if any(to_float(r.get(key)) is not None for r in rows):
            return key
    for key in rows[0].keys():
        if any(to_float(r.get(key)) is not None for r in rows):
            return key
    return None


def label_for_row(row: dict[str, Any], idx: int) -> str:
    for k in ("name", "period", "label", "entity"):
        if row.get(k):
            return str(row[k])
    return f"row_{idx + 1}"


def detect_anomalies(rows: list[dict[str, Any]], query_type: str = "") -> dict[str, Any]:
    if not rows:
        return {"items": []}

    value_key = pick_numeric_key(rows)
    if not value_key:
        return {"items": []}

    points: list[tuple[str, float]] = []
    for i, r in enumerate(rows):
        v = to_float(r.get(value_key))
        if v is None:
            continue
        points.append((label_for_row(r, i), v))

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

