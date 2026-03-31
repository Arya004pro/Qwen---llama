"""Step 5: Execute Query — runs SQL against DuckDB.

Changes vs original:
  - _DATETIME_COL_HINTS: removed domain-specific names "order_date",
    "order_datetime", "ride_date" — these are already matched by the
    generic "date" substring, so they were redundant and misleading when
    working with other dataset schemas.
  - All execution, rewrite, and repair logic unchanged.
"""

import os
import sys
import calendar
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, queue
from db.duckdb_connection import run_query as _run_sql_raw, get_read_connection
from db.query_validator import validate_query

config = {
    "name": "ExecuteQuery",
    "description": (
        "Executes LLM-generated SQL against DuckDB. "
        "Uses exclusive end-date (>= start AND < end+1day). "
        "Auto-repairs missing GROUP BY for time-series queries. "
        "Auto-adds ID column to GROUP BY when only a *_name column is present."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::execute")],
    "enqueues": ["query::detect.anomalies"],
}

# ── Datetime column hints ──────────────────────────────────────────────────────
# Generic substrings only — no domain-specific column names.
# Any column whose name contains one of these tokens is treated as a
# datetime column and gets the BETWEEN → ">= AND <" rewrite applied.
_DATETIME_COL_HINTS = {
    "date",       # matches order_date, ride_date, created_date, etc.
    "time",       # matches datetime, timestamp, order_time, etc.
    "timestamp",
    "created",
    "updated",
    "at",         # matches created_at, updated_at, etc.
    "on",         # matches ordered_on, etc.
    "when",
}

# ── SQL rewrite patterns ──────────────────────────────────────────────────────

_EXTRACT_YEAR_PAT = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(\w+)\s*\)\s*=\s*\?", re.IGNORECASE)
_EXTRACT_MONTH_PAT = re.compile(
    r"\s*AND\s+EXTRACT\s*\(\s*MONTH\s+FROM\s+\w+\s*\)\s+BETWEEN\s+\d+\s+AND\s+\d+",
    re.IGNORECASE)
_EXTRACT_ANY_PAT = re.compile(
    r"EXTRACT\s*\(\s*(YEAR|MONTH|DAY|QUARTER)\s+FROM", re.IGNORECASE)
_BETWEEN_PAT = re.compile(r"(\w+)\s+BETWEEN\s+\?\s+AND\s+\?", re.IGNORECASE)
_GROUP_BY_CLAUSE_RE = re.compile(
    r"(?is)\bGROUP\s+BY\b(?P<group_by>.*?)(?=(\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|$))")
_NAME_EXPR_RE = re.compile(r"(?is)\bSELECT\s+(?P<expr>.*?)\s+AS\s+name\b")
_MISSING_GB_COL_RE = re.compile(
    r'column\s+"([^"]+)"\s+must appear in the GROUP BY', re.IGNORECASE)
_HAS_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _get_id_name_pairs() -> dict[str, str]:
    """Return {name_col: best_discriminator_col} for deduplication."""
    from collections import defaultdict
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
        ).fetchall()
        conn.close()
    except Exception:
        return {}

    table_cols: dict = defaultdict(list)
    for table, col in rows:
        table_cols[table].append(col.lower())

    _CONTACT_DISCRIMINATORS = (
        "phone", "phone_number", "mobile", "mobile_number",
        "email", "email_address", "contact",
    )

    def _best_disc(name_col: str, col_set: set) -> str | None:
        base = name_col[:-5] if name_col.endswith("_name") else name_col
        for suffix in ("_id", "_code", "_key", "_uuid", "_no", "_number"):
            if (base + suffix) in col_set:
                return base + suffix
        for c in _CONTACT_DISCRIMINATORS:
            if c in col_set:
                return c
        return None

    pairs: dict[str, str] = {}
    for table, cols in table_cols.items():
        col_set = set(cols)
        for col in cols:
            if col.endswith("_name"):
                disc = _best_disc(col, col_set)
                if disc and col not in pairs:
                    pairs[col] = disc
        if "name" in col_set and "name" not in pairs:
            for c in (f"{table}_id", "id", f"{table}_code"):
                if c in col_set:
                    pairs["name"] = c
                    break
    return pairs


# ── Rewrite: add ID to GROUP BY ────────────────────────────────────────────────

def _repair_group_by_add_id(sql: str) -> tuple[str, bool]:
    """Prepend discriminator key to GROUP BY to prevent same-name entity merging."""
    if not _HAS_GROUP_BY_RE.search(sql):
        return sql, False

    id_name_pairs = _get_id_name_pairs()
    if not id_name_pairs:
        return sql, False

    gb_match = _GROUP_BY_CLAUSE_RE.search(sql)
    if not gb_match:
        return sql, False

    gb_raw = gb_match.group("group_by").strip()

    def _strip_alias(token: str) -> str:
        t = token.strip().strip('"').strip('`')
        if "." in t:
            t = t.split(".")[-1]
        return t.lower()

    gb_tokens_raw  = [t.strip() for t in gb_raw.split(",") if t.strip()]
    gb_tokens_bare = [_strip_alias(t) for t in gb_tokens_raw]

    changed = False
    new_tokens_raw = list(gb_tokens_raw)

    for name_col, id_col in id_name_pairs.items():
        if name_col in gb_tokens_bare and id_col not in gb_tokens_bare:
            idx = gb_tokens_bare.index(name_col)
            new_tokens_raw.insert(idx, id_col)
            gb_tokens_bare.insert(idx, id_col)
            changed = True

    if not changed:
        return sql, False

    new_gb_str = ", ".join(new_tokens_raw)
    start, end = gb_match.span("group_by")
    return sql[:start] + " " + new_gb_str + " " + sql[end:], True


# ── Existing helpers ──────────────────────────────────────────────────────────

def _exclusive_end(d: date) -> date:
    return d + timedelta(days=1)


def _is_datetime_col(col_name: str) -> bool:
    low = col_name.lower()
    return any(hint in low for hint in _DATETIME_COL_HINTS)


def _rewrite_extract_to_range(sql: str) -> tuple[str, bool]:
    if not _EXTRACT_YEAR_PAT.search(sql):
        return sql, False
    new_sql = _EXTRACT_YEAR_PAT.sub(r"\1 >= ? AND \1 < ?", sql)
    new_sql = _EXTRACT_MONTH_PAT.sub("", new_sql)
    new_sql = re.sub(r"\s{2,}", " ", new_sql).strip()
    return new_sql, True


def _rewrite_between_to_range(sql: str) -> tuple[str, bool]:
    changed = False

    def _replacer(m: re.Match) -> str:
        nonlocal changed
        col = m.group(1)
        if _is_datetime_col(col):
            changed = True
            return f"{col} >= ? AND {col} < ?"
        return m.group(0)

    return _BETWEEN_PAT.sub(_replacer, sql), changed


def _has_extract(sql: str) -> bool:
    return bool(_EXTRACT_ANY_PAT.search(sql))


def _repair_group_by_missing_name(sql: str, error_text: str) -> str | None:
    err_m  = _MISSING_GB_COL_RE.search(error_text or "")
    gb_m   = _GROUP_BY_CLAUSE_RE.search(sql or "")
    name_m = _NAME_EXPR_RE.search(sql or "")
    if not (err_m and gb_m and name_m):
        return None
    missing_col  = err_m.group(1).strip()
    name_expr    = name_m.group("expr").strip()
    if not name_expr:
        return None
    group_by_raw = gb_m.group("group_by")
    if (missing_col.lower() in group_by_raw.lower()
            or name_expr.lower() in group_by_raw.lower()):
        return None
    fixed = f"{group_by_raw.strip()}, {name_expr}"
    s, e  = gb_m.span("group_by")
    return sql[:s] + " " + fixed + " " + sql[e:]


def _repair_add_missing_group_by(sql: str, error_text: str) -> str | None:
    if _HAS_GROUP_BY_RE.search(sql):
        return None
    name_m = _NAME_EXPR_RE.search(sql or "")
    if not name_m:
        return None
    name_expr  = name_m.group("expr").strip()
    expr_upper = name_expr.upper()
    is_time_bucket = any(kw in expr_upper for kw in [
        "EXTRACT(", "STRFTIME(", "DATE_TRUNC(", "YEAR(", "MONTH(",
        "QUARTER(", "WEEK(", "DAY(",
    ])
    if not is_time_bucket:
        return None
    order_m = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    limit_m = re.search(r"\bLIMIT\b", sql, re.IGNORECASE)
    group_by_clause = f"\nGROUP BY {name_expr}"
    if order_m:
        pos = order_m.start()
        return sql[:pos].rstrip() + group_by_clause + "\n" + sql[pos:]
    elif limit_m:
        pos = limit_m.start()
        return sql[:pos].rstrip() + group_by_clause + "\n" + sql[pos:]
    else:
        return sql.rstrip() + group_by_clause


def _run_sql(sql: str, params: tuple) -> list:
    sql_duck = sql.replace("%s", "?")
    return _run_sql_raw(sql_duck, list(params))


def _rows_to_dicts(rows: list, parsed: dict | None = None) -> list[dict]:
    rank_within_time = bool((parsed or {}).get("_rank_within_time"))
    result = []
    for row in rows:
        if len(row) == 1:
            result.append({"value": float(row[0]) if row[0] is not None else None})
        elif len(row) == 2:
            result.append({"name":  str(row[0]),
                           "value": float(row[1]) if row[1] is not None else 0.0})
        elif len(row) == 3:
            if rank_within_time:
                result.append({
                    "period": str(row[0]),
                    "name": str(row[1]),
                    "value": float(row[2]) if row[2] is not None else 0.0,
                })
            else:
                result.append({"name":   str(row[0]),
                               "value1": float(row[1]) if row[1] is not None else 0.0,
                               "value2": float(row[2]) if row[2] is not None else 0.0})
        elif len(row) >= 4:
            result.append({"name":   str(row[0]),
                           "value1": float(row[1]) if row[1] is not None else 0.0,
                           "value2": float(row[2]) if row[2] is not None else 0.0,
                           "delta":  float(row[3]) if row[3] is not None else 0.0})
    return result


def _build_params(sql: str, parsed: dict) -> tuple:
    n     = sql.count("%s") + sql.count("?")
    trs   = parsed.get("time_ranges", [])
    qt    = parsed.get("query_type", "top_n")
    top_n = parsed.get("top_n", 5) or 5
    thr   = parsed.get("threshold") or {}

    if n == 0:
        return ()

    def _d(s: str) -> date:
        return date.fromisoformat(s)

    uses_exclusive = ">=" in sql and "< ?" in sql

    def _end(end_str: str) -> date:
        d = _d(end_str)
        return _exclusive_end(d) if uses_exclusive else d

    if qt in ("comparison", "growth_ranking", "intersection") and len(trs) >= 2:
        s1 = _d(trs[0]["start"]);  e1 = _end(trs[0]["end"])
        s2 = _d(trs[1]["start"]);  e2 = _end(trs[1]["end"])
        if n == 4:  return (s1, e1, s2, e2)
        if n == 5:  return (s1, e1, s2, e2, top_n)
        base = [s1, e1, s2, e2]
        while len(base) < n - 1:
            base += [s1, e1]
        if n > len(base):
            base.append(top_n)
        return tuple(base[:n])

    if trs:
        s = _d(trs[0]["start"])
        e = _end(trs[0]["end"])
    else:
        today = date.today()
        s     = today.replace(month=1, day=1)
        e     = _exclusive_end(date(today.year, 12, 31))

    if n == 2:  return (s, e)
    if n == 3:
        if qt == "threshold" and thr.get("type") == "absolute":
            return (s, e, thr.get("value"))
        return (s, e, top_n)
    if n == 4 and qt == "threshold":
        return (s, e, s, e)
    if n == 5 and qt == "threshold" and thr.get("type") == "percentage":
        return (s, e, thr.get("value"), s, e)

    slots = []
    for i in range(n):
        if i == n - 1:
            if qt == "threshold" and thr.get("type") == "absolute":
                slots.append(thr.get("value"))
            else:
                slots.append(top_n)
        elif i % 2 == 0:
            slots.append(s)
        else:
            slots.append(e)
    return tuple(slots)


def _period_label(start_str: str, end_str: str) -> str:
    s = date.fromisoformat(start_str)
    e = date.fromisoformat(end_str)
    if s.month == e.month and s.year == e.year:
        return f"{calendar.month_name[s.month]} {s.year}"
    if s.day == 1 and e.day >= 28:
        return f"{calendar.month_abbr[s.month]}-{calendar.month_abbr[e.month]} {s.year}"
    return f"{s} to {e}"


async def _error(ctx, qs, query_id, msg):
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs, "status": "error", "error": msg,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "error": now_iso},
        })


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    parsed        = input_data.get("parsed", {})
    generated_sql = input_data.get("generated_sql")

    if not generated_sql:
        qs = await ctx.state.get("queries", query_id)
        await _error(ctx, qs, query_id, "No SQL was generated.")
        return

    qt  = parsed.get("query_type", "top_n")
    trs = parsed.get("time_ranges", [])

    # ── Rewrite 1: EXTRACT(YEAR) → >= AND < ──────────────────────────────────
    generated_sql, was_rewritten = _rewrite_extract_to_range(generated_sql)
    if was_rewritten:
        ctx.logger.info("Rewrote EXTRACT->range", {"queryId": query_id})
    if _has_extract(generated_sql):
        ctx.logger.warn("SQL still contains EXTRACT", {"queryId": query_id})

    # ── Rewrite 2: BETWEEN → >= AND < (datetime columns only) ────────────────
    generated_sql, between_rewritten = _rewrite_between_to_range(generated_sql)
    if between_rewritten:
        ctx.logger.info("Rewrote BETWEEN->range for datetime col", {"queryId": query_id})

    # ── Rewrite 3: Add missing GROUP BY for time-bucket SELECT expressions ────
    if qt == "time_series" and not _HAS_GROUP_BY_RE.search(generated_sql):
        repaired = _repair_add_missing_group_by(generated_sql, "")
        if repaired:
            ctx.logger.info("Auto-added missing GROUP BY for time_series", {"queryId": query_id})
            generated_sql = repaired

    ctx.logger.info("Executing SQL", {"queryId": query_id, "query_type": qt})

    params = _build_params(generated_sql, parsed)
    ctx.logger.info("SQL params", {"queryId": query_id, "params": str(params)})

    # ── Validator layer ────────────────────────────────────────────────────────
    ok, errors, warnings = validate_query(generated_sql, params)
    if warnings:
        ctx.logger.warn("SQL validation warnings", {"queryId": query_id, "warnings": warnings})
    if not ok:
        qs = await ctx.state.get("queries", query_id)
        msg = "SQL validation failed: " + " | ".join(errors)
        await _error(ctx, qs, query_id, msg)
        return

    try:
        rows    = _run_sql(generated_sql, params)
        results = _rows_to_dicts(rows, parsed)
    except Exception as exc:
        err_str = str(exc)
        repaired_sql = _repair_add_missing_group_by(generated_sql, err_str)
        if not repaired_sql:
            repaired_sql = _repair_group_by_missing_name(generated_sql, err_str)
        if not repaired_sql:
            qs = await ctx.state.get("queries", query_id)
            await _error(ctx, qs, query_id, f"SQL execution failed: {exc}")
            return
        try:
            rows    = _run_sql(repaired_sql, params)
            results = _rows_to_dicts(rows, parsed)
            generated_sql = repaired_sql
            ctx.logger.warn("Repaired GROUP BY and retried", {"queryId": query_id})
        except Exception as exc2:
            qs = await ctx.state.get("queries", query_id)
            await _error(ctx, qs, query_id, f"SQL execution failed: {exc2}")
            return

    ranked_types = {
        "top_n", "bottom_n", "threshold", "intersection",
        "zero_filter", "growth_ranking", "comparison",
    }
    if qt in ranked_types and results and "name" not in results[0]:
        qs = await ctx.state.get("queries", query_id)
        await _error(ctx, qs, query_id,
                     f"SQL returned scalar instead of rows for query_type={qt}.")
        return

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs,
            "status":        "executed",
            "results":       results,
            "generated_sql": generated_sql,
            "updatedAt":     now_iso,
            "status_timestamps": {**prev_ts, "executed": now_iso},
        })

    period_labels = [_period_label(t["start"], t["end"]) for t in trs]
    start_date    = trs[0]["start"] if trs else ""
    end_date      = trs[-1]["end"]  if trs else ""

    await ctx.enqueue({"topic": "query::detect.anomalies", "data": {
        "queryId":       query_id,
        "query":         user_query,
        "parsed":        parsed,
        "results":       results,
        "period_labels": period_labels,
        "startDate":     start_date,
        "endDate":       end_date,
    }})