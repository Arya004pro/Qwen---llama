"""llm/sql_generator.py

Generates a parameterized DuckDB SELECT statement from parsed query intent
using an LLM, then validates it:

  1. Strip Qwen3 <think> blocks  — must happen BEFORE any SQL extraction
  2. Extract the SQL block        — find first line starting WITH/SELECT
  3. Static safety check          — no DML, starts with WITH or SELECT
  4. EXPLAIN syntax check         — skipped for threshold (subquery param count varies)

Public API
----------
  generate_sql(intent, groq_url, api_token, model, schema_prompt)
      -> (sql | None, usage_dict)
"""

from __future__ import annotations

import re
import logging
from datetime import date
from typing import Any

import requests
from db.duckdb_connection import explain_query

logger = logging.getLogger(__name__)

# ─── Regex constants ──────────────────────────────────────────────────────────

# Must start with WITH or SELECT at the beginning of a line
_SQL_START_RE = re.compile(r"(?im)^(WITH|SELECT)\b")

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE"
    r"|EXECUTE|COPY|VACUUM|ANALYZE|CALL|DO)\b",
    re.IGNORECASE,
)
_SEMICOLON = re.compile(r";")
_COMMENT   = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)
_THINK_TAG = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


# ─── Extraction helpers ───────────────────────────────────────────────────────

def _extract_sql(raw: str) -> str:
    """
    Pull the actual SQL out of a raw LLM response.

    Steps (each is a fallback for the previous):
      1. Strip Qwen3 <think>...</think> reasoning blocks entirely.
      2. Pull content from ```sql ... ``` or ``` ... ``` fences if present.
      3. Find the first LINE that starts with WITH or SELECT (line-anchored).
         Line-anchored means we do NOT match 'SELECT' inside a sentence of prose.
      4. Clean: strip SQL comments, trailing semicolons, surrounding whitespace.
    """
    # Step 1 — strip thinking blocks (Qwen3 extended reasoning)
    text = _THINK_TAG.sub("", raw).strip()

    # Step 2 — extract from markdown fences
    fence = re.search(r"```(?:sql)?\s*\n?(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fence:
        text = fence.group(1).strip()

    # Step 3 — find first line starting with WITH or SELECT
    m = _SQL_START_RE.search(text)
    if m:
        text = text[m.start():]

    # Step 4 — clean
    text = _COMMENT.sub("", text)
    text = text.strip().rstrip(";")
    return text


# ─── Safety check ─────────────────────────────────────────────────────────────

def _is_safe(sql: str) -> tuple[bool, str]:
    """Return (ok, reason). reason is empty when ok=True."""
    if not _SQL_START_RE.match(sql):
        return False, f"does not start with WITH/SELECT — starts with: {sql[:80]!r}"
    m = _FORBIDDEN.search(sql)
    if m:
        return False, f"contains forbidden keyword '{m.group()}'"
    if _SEMICOLON.search(sql):
        return False, "contains a semicolon"
    return True, ""


# ─── EXPLAIN syntax check ─────────────────────────────────────────────────────

def _validate_syntax(sql: str, ranking: str) -> tuple[bool, str]:
    """
    Run EXPLAIN to verify syntax against the live schema.
    Skipped for 'threshold' — subquery param count varies and is checked at runtime.
    """
    if ranking == "threshold":
        return True, ""

    dummy_start = date(2024, 1, 1)
    dummy_end   = date(2024, 1, 31)
    dummy_limit = 5

    count = sql.count("%s") + sql.count("?")
    if count == 0:
        params: list = []
    elif count == 3 and ranking in ("top", "bottom"):
        params = [dummy_start, dummy_end, dummy_limit]
    else:
        slots = []
        for i in range(count):
            if i == count - 1 and ranking in ("top", "bottom") and count % 2 == 1:
                slots.append(dummy_limit)
            elif i % 2 == 0:
                slots.append(dummy_start)
            else:
                slots.append(dummy_end)
        params = slots
    return explain_query(sql, params)


# ─── Prompt ───────────────────────────────────────────────────────────────────

def _build_prompt(intent: dict[str, Any], schema_prompt: str) -> str:
    entity    = intent.get("entity",         "(auto-detect from schema)")
    metric    = intent.get("metric",         "(auto-detect from schema)")
    ranking   = intent.get("ranking",        "top")
    top_n     = intent.get("top_n",          5)
    raw_query = intent.get("raw_user_query", "")

    rank_instruction = {
        "top": (
            f"Return the top {top_n} rows.\n"
            "End the query with: ORDER BY value DESC\nLIMIT ?"
        ),
        "bottom": (
            f"Return the bottom {top_n} rows.\n"
            "End the query with: ORDER BY value ASC\nLIMIT ?"
        ),
        "aggregate": (
            "Return a single scalar total — one row, one column aliased 'value'.\n"
            "No GROUP BY, no ORDER BY, no LIMIT."
        ),
        "threshold": (
            "Use a HAVING clause derived from the user's question.\n"
            "For 'more than 10% of total', write:\n"
            "  HAVING SUM(...) > 0.10 * (SELECT SUM(...) FROM ... WHERE <date_col> BETWEEN ? AND ?)\n"
            "Alias the group-by column as 'name' and the metric as 'value'.\n"
            "End with ORDER BY value DESC. No LIMIT clause."
        ),
    }.get(ranking, f"Return top {top_n} rows ORDER BY value DESC LIMIT ?")

    user_line = f'\nUser question: "{raw_query}"\n' if raw_query else ""

    return f"""You are a DuckDB SQL expert. Output ONLY a raw SQL query — no prose, no explanation, no markdown.
{user_line}
{schema_prompt}

Task
----
Entity  : {entity}
Metric  : {metric}
Ranking : {ranking}

Requirements
------------
- Read the "Semantic Layer" section first to map business terms to physical columns.
- Alias the group-by column as  name  and the aggregation as  value
- Use ? placeholders for every date and numeric limit (never hard-code values)
- Read "Uniqueness Profile" and "Safe Grouping Keys" from schema first.
- If entity labels are non-unique, group by stable key + label to avoid merged entities.
- {rank_instruction}

SQL:"""


# ─── Public API ───────────────────────────────────────────────────────────────

def generate_sql(
    intent:          dict[str, Any],
    groq_url:        str,
    api_token:       str,
    model:           str,
    schema_prompt:   str,
) -> tuple[str | None, dict]:
    """
    Generate and validate a parameterized SQL query.
    Returns (sql, usage) where sql is None if generation or validation failed.
    """
    ranking = intent.get("ranking", "top")
    prompt  = _build_prompt(intent, schema_prompt)

    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    payload = {
        "model":      model,
        "messages":   [{"role": "user", "content": prompt}],
        "max_tokens": 1024,   # enough for Qwen3 thinking + full SQL
        "temperature": 0.0,
    }

    try:
        resp = requests.post(groq_url, headers=headers, json=payload, timeout=45)
        resp.raise_for_status()
        result = resp.json()
        usage  = result.get("usage", {})
        raw    = result["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.error("LLM call failed: %s", exc)
        return None, {}

    logger.info("LLM raw response (len=%d): %.400s", len(raw), raw)

    sql = _extract_sql(raw)

    logger.info("Extracted SQL:\n%s", sql)

    # Safety check
    ok, reason = _is_safe(sql)
    if not ok:
        logger.warning("Safety FAILED: %s", reason)
        return None, usage

    # EXPLAIN (skipped for threshold)
    ok, err = _validate_syntax(sql, ranking)
    if not ok:
        logger.warning("EXPLAIN FAILED: %s\nSQL was:\n%s", err, sql)
        return None, usage

    logger.info("SQL OK (ranking=%s)", ranking)
    return sql, usage
