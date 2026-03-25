"""Step 5: Execute Query - runs SQL against DuckDB.

Includes two runtime repairs for LLM SQL:
1) EXTRACT(YEAR FROM date_col)=? -> date_col BETWEEN ? AND ?
2) GROUP BY mismatch when SELECT <name_expr> AS name but GROUP BY omits it
"""

import os
import sys
import calendar
import re
from datetime import date, datetime, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, queue
from db.duckdb_connection import run_query as _run_sql_raw

config = {
    "name": "ExecuteQuery",
    "description": "Executes LLM-generated SQL against DuckDB with small SQL repairs.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::execute")],
    "enqueues": ["query::format.result"],
}

_EXTRACT_YEAR_PAT = re.compile(r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(\w+)\s*\)\s*=\s*\?", re.IGNORECASE)
_EXTRACT_MONTH_PAT = re.compile(r"\s*AND\s+EXTRACT\s*\(\s*MONTH\s+FROM\s+\w+\s*\)\s+BETWEEN\s+\d+\s+AND\s+\d+", re.IGNORECASE)
_EXTRACT_ANY_PAT = re.compile(r"EXTRACT\s*\(\s*(YEAR|MONTH|DAY|QUARTER)\s+FROM", re.IGNORECASE)
_GROUP_BY_CLAUSE_RE = re.compile(r"(?is)\bGROUP\s+BY\b(?P<group_by>.*?)(?=(\bORDER\s+BY\b|\bLIMIT\b|$))")
_NAME_EXPR_RE = re.compile(r"(?is)\bSELECT\s+(?P<expr>.*?)\s+AS\s+name\b")
_MISSING_GB_COL_RE = re.compile(r'column\s+"([^"]+)"\s+must appear in the GROUP BY', re.IGNORECASE)


def _rewrite_extract_to_between(sql: str) -> tuple[str, bool]:
    if not _EXTRACT_YEAR_PAT.search(sql):
        return sql, False
    new_sql = _EXTRACT_YEAR_PAT.sub(r"\1 BETWEEN ? AND ?", sql)
    new_sql = _EXTRACT_MONTH_PAT.sub("", new_sql)
    new_sql = re.sub(r"\s{2,}", " ", new_sql).strip()
    return new_sql, True


def _has_extract(sql: str) -> bool:
    return bool(_EXTRACT_ANY_PAT.search(sql))


def _repair_group_by_missing_name(sql: str, error_text: str) -> str | None:
    err_m = _MISSING_GB_COL_RE.search(error_text or "")
    gb_m = _GROUP_BY_CLAUSE_RE.search(sql or "")
    name_m = _NAME_EXPR_RE.search(sql or "")
    if not (err_m and gb_m and name_m):
        return None
    missing_col = err_m.group(1).strip()
    name_expr = name_m.group("expr").strip()
    if not name_expr:
        return None
    group_by_raw = gb_m.group("group_by")
    if missing_col.lower() in group_by_raw.lower() or name_expr.lower() in group_by_raw.lower():
        return None
    fixed_group_by = f"{group_by_raw.strip()}, {name_expr}"
    s, e = gb_m.span("group_by")
    return sql[:s] + " " + fixed_group_by + " " + sql[e:]


def _run_sql(sql: str, params: tuple) -> list:
    sql_duck = sql.replace("%s", "?")
    return _run_sql_raw(sql_duck, list(params))


def _rows_to_dicts(rows: list) -> list[dict]:
    result = []
    for row in rows:
        if len(row) == 1:
            result.append({"value": float(row[0]) if row[0] is not None else None})
        elif len(row) == 2:
            result.append({"name": str(row[0]), "value": float(row[1]) if row[1] is not None else 0.0})
        elif len(row) == 3:
            result.append({
                "name": str(row[0]),
                "value1": float(row[1]) if row[1] is not None else 0.0,
                "value2": float(row[2]) if row[2] is not None else 0.0,
            })
        elif len(row) >= 4:
            result.append({
                "name": str(row[0]),
                "value1": float(row[1]) if row[1] is not None else 0.0,
                "value2": float(row[2]) if row[2] is not None else 0.0,
                "delta": float(row[3]) if row[3] is not None else 0.0,
            })
    return result


def _build_params(sql: str, parsed: dict) -> tuple:
    n = sql.count("%s") + sql.count("?")
    trs = parsed.get("time_ranges", [])
    qt = parsed.get("query_type", "top_n")
    top_n = parsed.get("top_n", 5) or 5
    thr = parsed.get("threshold") or {}

    if n == 0:
        return ()

    def _d(s: str) -> date:
        return date.fromisoformat(s)

    if qt in ("comparison", "growth_ranking", "intersection") and len(trs) >= 2:
        s1, e1 = _d(trs[0]["start"]), _d(trs[0]["end"])
        s2, e2 = _d(trs[1]["start"]), _d(trs[1]["end"])
        if n == 4:
            return (s1, e1, s2, e2)
        if n == 5:
            return (s1, e1, s2, e2, top_n)
        base = [s1, e1, s2, e2]
        while len(base) < n - 1:
            base += [s1, e1]
        if n > len(base):
            base.append(top_n)
        return tuple(base[:n])

    if trs:
        s, e = _d(trs[0]["start"]), _d(trs[0]["end"])
    else:
        today = date.today()
        s, e = today.replace(month=1, day=1), today.replace(month=12, day=31)

    if n == 2:
        return (s, e)
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
            **qs,
            "status": "error",
            "error": msg,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "error": now_iso},
        })


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})
    generated_sql = input_data.get("generated_sql")

    if not generated_sql:
        qs = await ctx.state.get("queries", query_id)
        await _error(ctx, qs, query_id, "No SQL was generated.")
        return

    qt = parsed.get("query_type", "top_n")
    trs = parsed.get("time_ranges", [])

    generated_sql, was_rewritten = _rewrite_extract_to_between(generated_sql)
    if was_rewritten:
        ctx.logger.info("Rewrote EXTRACT->BETWEEN", {"queryId": query_id})
    if _has_extract(generated_sql):
        ctx.logger.warn("SQL still contains EXTRACT", {"queryId": query_id})

    ctx.logger.info("Executing SQL", {"queryId": query_id, "query_type": qt})

    params = _build_params(generated_sql, parsed)
    try:
        rows = _run_sql(generated_sql, params)
        results = _rows_to_dicts(rows)
    except Exception as exc:
        repaired_sql = _repair_group_by_missing_name(generated_sql, str(exc))
        if not repaired_sql:
            qs = await ctx.state.get("queries", query_id)
            await _error(ctx, qs, query_id, f"SQL execution failed: {exc}")
            return
        try:
            rows = _run_sql(repaired_sql, params)
            results = _rows_to_dicts(rows)
            generated_sql = repaired_sql
            ctx.logger.warn("Repaired GROUP BY mismatch and retried", {"queryId": query_id})
        except Exception as exc2:
            qs = await ctx.state.get("queries", query_id)
            await _error(ctx, qs, query_id, f"SQL execution failed: {exc2}")
            return

    ranked_types = {"top_n", "bottom_n", "threshold", "intersection", "zero_filter", "growth_ranking", "comparison"}
    if qt in ranked_types and results and "name" not in results[0]:
        qs = await ctx.state.get("queries", query_id)
        await _error(ctx, qs, query_id, f"SQL returned a scalar instead of per-entity rows for query_type={qt}.")
        return

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs,
            "status": "executed",
            "results": results,
            "generated_sql": generated_sql,
            "updatedAt": now_iso,
            "status_timestamps": {**prev_ts, "executed": now_iso},
        })

    period_labels = [_period_label(t["start"], t["end"]) for t in trs]
    start_date = trs[0]["start"] if trs else ""
    end_date = trs[-1]["end"] if trs else ""

    await ctx.enqueue({"topic": "query::format.result", "data": {
        "queryId": query_id,
        "query": user_query,
        "parsed": parsed,
        "results": results,
        "period_labels": period_labels,
        "startDate": start_date,
        "endDate": end_date,
    }})
