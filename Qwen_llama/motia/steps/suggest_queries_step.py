"""Step: Query Suggestions — GET /suggestions

Returns a list of ready-to-run example analytics questions tailored to the
live database schema.  Results are cached in Motia state for 30 minutes so
repeated page loads don't hammer the LLM.

Endpoints
---------
GET  /suggestions          → return cached suggestions (generate if cache is cold)
GET  /suggestions?refresh=1 → force-regenerate even if cache is warm

Response body
-------------
{
  "suggestions": ["Top 5 drivers ...", "Monthly revenue trend ...", ...],
  "generated_at": "<iso-timestamp>",
  "cached": true | false
}
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import ApiRequest, ApiResponse, FlowContext, http

from shared_config import GROQ_API_TOKEN, QWEN_MODEL, GROQ_URL
from db.duckdb_connection import get_read_connection
from utils.llm_client import clean_model_text, post_chat_completion

# ── Config ─────────────────────────────────────────────────────────────────────

CACHE_KEY      = "suggestions"          # key inside the "query_suggestions" state namespace
CACHE_TTL_SECS = 30 * 60               # 30 minutes
NUM_SUGGESTIONS = 8

config = {
    "name": "QuerySuggestions",
    "description": (
        "Returns 8 schema-aware example analytics questions. "
        "Results are LLM-generated and cached for 30 minutes. "
        "Pass ?refresh=1 to force regeneration."
    ),
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/suggestions")],
    "enqueues": [],
}

# ── Schema snapshot ────────────────────────────────────────────────────────────

def _build_schema_snapshot() -> str:
    """
    Build a compact, human-readable schema description for the LLM prompt.
    Includes table names, columns (with types), sample distinct values for
    low-cardinality dimensions, and date ranges.
    """
    lines: list[str] = []

    _TEXT  = ("VARCHAR", "CHAR", "TEXT", "STRING")
    _NUM   = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    _DATE  = ("DATE", "TIMESTAMP")
    _DATE_HINTS = ("date", "time", "created", "updated", "at", "on")

    try:
        conn = get_read_connection()
        tables = [
            r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' ORDER BY table_name"
            ).fetchall()
        ]

        if not tables:
            return "No tables loaded yet."

        for table in tables:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            col_info: list[str] = []

            date_col: str | None = None
            date_min = date_max = None

            for col, dtype, *_ in cols:
                col_l = col.lower()
                dtype_u = str(dtype).upper()

                # Track first date column for range info
                if date_col is None and (
                    any(t in dtype_u for t in _DATE)
                    or any(k in col_l for k in _DATE_HINTS)
                ):
                    date_col = col
                    try:
                        r = conn.execute(
                            f'SELECT MIN(CAST("{col}" AS DATE)), MAX(CAST("{col}" AS DATE)) '
                            f'FROM "{table}"'
                        ).fetchone()
                        if r and r[0]:
                            date_min, date_max = r[0], r[1]
                    except Exception:
                        pass

                # Collect sample values for low-cardinality text columns
                if any(t in dtype_u for t in _TEXT) and not col_l.endswith("_id"):
                    try:
                        distinct = conn.execute(
                            f'SELECT COUNT(DISTINCT "{col}") FROM "{table}"'
                        ).fetchone()[0]
                        if 2 <= distinct <= 12:
                            vals = [
                                str(r[0]) for r in conn.execute(
                                    f'SELECT DISTINCT "{col}" FROM "{table}" '
                                    f'WHERE "{col}" IS NOT NULL LIMIT 8'
                                ).fetchall()
                            ]
                            col_info.append(f'{col} [{", ".join(vals)}]')
                        else:
                            col_info.append(f"{col} (text, {distinct} values)")
                    except Exception:
                        col_info.append(col)

                elif any(t in dtype_u for t in _NUM) and not col_l.endswith("_id"):
                    col_info.append(f"{col} (numeric)")

            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            lines.append(f"Table: {table}  ({row_count:,} rows)")
            if date_min and date_max:
                lines.append(f"  Date range: {date_min} → {date_max}")
            for part in col_info:
                lines.append(f"  • {part}")
            lines.append("")

        conn.close()
    except Exception as exc:
        return f"Schema unavailable: {exc}"

    return "\n".join(lines)


# ── LLM call ───────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = f"""\
You are an analytics assistant helping users explore their data.
Given a database schema, generate exactly {NUM_SUGGESTIONS} diverse, specific,
ready-to-type analytics questions that a business analyst would ask.

Rules:
- Use REAL column names, dimension values, and date ranges from the schema.
- Cover a variety of query types: top-N rankings, aggregates, monthly/quarterly \
trends, comparisons across two time periods, threshold filters, and growth rankings.
- Each question must be self-contained — no follow-up needed.
- Keep each question under 20 words.
- DO NOT number the questions.
- Return ONLY a JSON array of strings, e.g.:
  ["question 1", "question 2", ...]
- No prose, no markdown, no extra keys.
"""


def _call_llm(schema: str) -> list[str]:
    user_msg = f"Schema:\n{schema}\n\nGenerate {NUM_SUGGESTIONS} analytics questions."
    payload = {
        "model": QWEN_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "max_tokens": 512,
        "temperature": 0.7,   # slight creativity for diverse suggestions
    }
    data = post_chat_completion(
        api_url=GROQ_URL,
        api_token=GROQ_API_TOKEN,
        payload=payload,
        timeout=30,
        retry_without_reasoning_effort=False,
    )
    raw = clean_model_text(data["choices"][0]["message"]["content"], strip_fences=True)

    suggestions: list[str] = json.loads(raw)
    if not isinstance(suggestions, list):
        raise ValueError(f"LLM returned non-list: {raw[:200]}")

    # Sanitise: keep only non-empty strings, cap at NUM_SUGGESTIONS
    return [str(s).strip() for s in suggestions if str(s).strip()][:NUM_SUGGESTIONS]


# ── Fallback suggestions (used when LLM fails or no API key) ──────────────────

def _fallback_suggestions(schema: str) -> list[str]:
    """
    Rule-based suggestions built directly from the schema snapshot.
    Guarantees a useful response even without an LLM.
    """
    generic = [
        "Top 5 cities by total revenue in 2024",
        "Monthly revenue trend in 2024",
        "Top 10 items by quantity sold in Q1 2024",
        "Compare revenue in 2023 vs 2024",
        "Bottom 5 performers by earnings last year",
        "Total revenue by payment method in 2024",
        "Which categories had the most orders in 2024?",
        "Top 5 drivers by earnings in 2024",
    ]

    # Tailor based on detected table names
    if "uber_rides" in schema and "zomato_orders" in schema:
        return [
            "Top 5 drivers by total earnings in 2024",
            "Monthly fare revenue trend in 2024",
            "Revenue comparison: 2023 vs 2024 for Uber rides",
            "Top 3 cities by ride count in Q1 2025",
            "Top 5 food categories by revenue in 2024",
            "Which restaurant earned the most in Mumbai in 2024?",
            "Monthly Zomato order trend in 2024",
            "Compare Zomato revenue: 2023 vs 2024",
        ]
    return generic


# ── Cache helpers ──────────────────────────────────────────────────────────────

async def _load_cache(ctx: FlowContext[Any]) -> dict | None:
    cached = await ctx.state.get("query_suggestions", CACHE_KEY)
    if not cached:
        return None
    age = time.time() - cached.get("generated_epoch", 0)
    if age > CACHE_TTL_SECS:
        return None           # cache expired
    return cached


async def _save_cache(ctx: FlowContext[Any], suggestions: list[str]) -> None:
    now = datetime.now(timezone.utc)
    await ctx.state.set("query_suggestions", CACHE_KEY, {
        "suggestions":     suggestions,
        "generated_at":    now.isoformat(),
        "generated_epoch": time.time(),
    })


# ── Handler ────────────────────────────────────────────────────────────────────

async def handler(
    request: ApiRequest[Any],
    ctx: FlowContext[Any],
) -> ApiResponse[Any]:
    # Support both ?refresh=1 and ?refresh=true
    force_refresh = str(
        (request.query or {}).get("refresh", "") or ""
    ).lower() in ("1", "true", "yes")

    # Try cache first (unless force refresh)
    if not force_refresh:
        cached = await _load_cache(ctx)
        if cached:
            ctx.logger.info("💡 Serving cached suggestions",
                            {"count": len(cached["suggestions"])})
            return ApiResponse(status=200, body={
                "suggestions": cached["suggestions"],
                "generated_at": cached["generated_at"],
                "cached": True,
            })

    ctx.logger.info("💡 Generating fresh suggestions")

    schema = _build_schema_snapshot()
    suggestions: list[str] = []
    used_llm = False

    if GROQ_API_TOKEN:
        try:
            suggestions = _call_llm(schema)
            used_llm = True
            ctx.logger.info("✅ LLM suggestions generated",
                            {"count": len(suggestions)})
        except Exception as exc:
            ctx.logger.warn("⚠️ LLM failed — using fallback suggestions",
                            {"error": str(exc)})

    if not suggestions:
        suggestions = _fallback_suggestions(schema)
        ctx.logger.info("📋 Using fallback suggestions",
                        {"count": len(suggestions)})

    await _save_cache(ctx, suggestions)

    now_iso = datetime.now(timezone.utc).isoformat()
    return ApiResponse(status=200, body={
        "suggestions": suggestions,
        "generated_at": now_iso,
        "cached": False,
        "source": "llm" if used_llm else "fallback",
    })
