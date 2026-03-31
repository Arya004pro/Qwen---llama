"""Step: Generate Insights.

Uses a configurable model to generate 2-3 plain-English business insights from
query results and anomaly flags before final formatting.
Includes adaptive skip logic to reduce token usage on simple result shapes.
Pipeline: execute -> detect.anomalies -> generate.insights -> format.result
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

import requests
from motia import FlowContext, queue

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_config import (
    GROQ_API_TOKEN,
    GROQ_URL,
    INSIGHTS_MODEL,
    AI_INSIGHTS_MODE,
    AI_INSIGHTS_ROW_THRESHOLD,
)
from utils.token_logger import add_tokens_to_state, log_tokens

config = {
    "name": "GenerateInsights",
    "description": "Generates plain-English business insights using adaptive model routing from result rows and anomalies.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::generate.insights")],
    "enqueues": ["query::format.result"],
}

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _compress_rows(rows: list[dict[str, Any]], limit: int = 24) -> list[dict[str, Any]]:
    if not rows:
        return []
    if len(rows) <= limit:
        return rows
    head = max(3, limit // 2)
    tail = max(3, limit - head)
    return rows[:head] + rows[-tail:]


def _should_generate_ai_insights(parsed: dict[str, Any], rows: list[dict[str, Any]], anomalies: dict[str, Any]) -> bool:
    mode = (AI_INSIGHTS_MODE or "adaptive").lower()
    if mode == "off":
        return False
    if mode == "always":
        return bool(rows)

    # Adaptive mode: spend tokens only when extra synthesis is likely valuable.
    if not rows:
        return False

    if (anomalies or {}).get("items"):
        return True

    qt = (parsed or {}).get("query_type", "")
    if (parsed or {}).get("_rank_within_time"):
        return True

    if qt in {"comparison", "growth_ranking", "intersection", "threshold", "time_series"}:
        return True

    if len(rows) >= max(1, int(AI_INSIGHTS_ROW_THRESHOLD or 8)):
        return True

    if qt in {"aggregate", "zero_filter"}:
        return False

    if (parsed or {}).get("entity", "").startswith("is_") and len(rows) <= 2:
        return False

    return False


def _call_ai_insights(user_query: str, parsed: dict[str, Any], rows: list[dict[str, Any]], anomalies: dict[str, Any]) -> tuple[list[str], dict]:
    if not GROQ_API_TOKEN:
        return [], {}

    compact_rows = _compress_rows(rows, limit=24)
    anomaly_items = (anomalies or {}).get("items", [])[:5]
    prompt = (
        "You are a business analytics assistant. "
        "Write 2-3 concise plain-English insights from the data. "
        "Reference anomaly signals when relevant. "
        "Do not invent causes as facts; use careful wording like 'may indicate'. "
        "Return ONLY JSON in this shape: {\"insights\":[\"...\",\"...\"]}.\n\n"
        f"User query: {user_query}\n"
        f"Parsed intent: {json.dumps(parsed, ensure_ascii=False)}\n"
        f"Sample rows: {json.dumps(compact_rows, ensure_ascii=False)}\n"
        f"Anomalies: {json.dumps(anomaly_items, ensure_ascii=False)}\n"
    )

    resp = requests.post(
        GROQ_URL,
        headers={
            "Authorization": f"Bearer {GROQ_API_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "model": INSIGHTS_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 280,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    usage = data.get("usage", {})
    raw = (data["choices"][0]["message"]["content"] or "").strip()
    raw = _THINK_RE.sub("", raw).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        obj = json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not m:
            return [], usage
        obj = json.loads(m.group(0))
    lines = [str(x).strip() for x in (obj.get("insights") or []) if str(x).strip()]
    return lines[:3], usage


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "") or ""
    parsed = input_data.get("parsed", {}) or {}
    results = input_data.get("results", []) or []
    anomalies = input_data.get("anomalies", {}) or {}

    insights: list[str] = []
    usage: dict[str, Any] = {}
    try:
        if _should_generate_ai_insights(parsed, results, anomalies):
            insights, usage = _call_ai_insights(user_query, parsed, results, anomalies)
            if usage:
                log_tokens(ctx, query_id, "GenerateInsights", INSIGHTS_MODEL, usage)
                await add_tokens_to_state(ctx, query_id, "GenerateInsights", INSIGHTS_MODEL, usage)
        else:
            log_tokens(ctx, query_id, "GenerateInsightsSkipped", "adaptive_rule", {})
            await add_tokens_to_state(ctx, query_id, "GenerateInsightsSkipped", "adaptive_rule", {})
    except Exception as exc:
        ctx.logger.warn("GenerateInsights failed; continuing without AI insights", {"queryId": query_id, "error": str(exc)})

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "auto_insights": insights,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "insights_generated": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::format.result",
            "data": {
                **input_data,
                "auto_insights": insights,
            },
        }
    )

