"""Step 5: Execute Query — Runs SQL against PostgreSQL.

Supports:
  • top N    — highest performers (ORDER BY DESC)
  • bottom N — worst performers  (ORDER BY ASC)
  • aggregate — total / sum
  • comparison — runs two queries for two separate time periods
"""

import os
import sys
import calendar

# ── Fix imports FIRST before anything else ──────────────────────────────────
_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)

for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ────────────────────────────────────────────────────────────────────────────

import psycopg2
from typing import Any
from motia import FlowContext, queue

from shared_config import POSTGRES
from db.sql_registry import SQL_REGISTRY
from utils.date_parser import parse_date_range, parse_comparison_date_ranges

config = {
    "name": "ExecuteQuery",
    "description": "Builds SQL inputs from intent, runs the query on PostgreSQL, and captures raw analytics results",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::execute"),
    ],
    "enqueues": ["query::format.result"],
}


def _run_sql(sql, params):
    """Execute a SQL query and return rows as list of dicts."""
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def _rows_to_results(rows, ranking):
    results = []
    for row in rows:
        if ranking == "aggregate":
            results.append({"value": float(row[0]) if row[0] is not None else None})
        else:
            results.append({
                "name": str(row[0]),
                "value": float(row[1]) if row[1] is not None else None,
            })
    return results


def _period_label(start_date, end_date):
    """Human-readable label like 'March 2024' or 'Mar–Jun 2024'."""
    if start_date.month == end_date.month and start_date.year == end_date.year:
        return f"{calendar.month_name[start_date.month]} {start_date.year}"
    return (
        f"{calendar.month_abbr[start_date.month]}–"
        f"{calendar.month_abbr[end_date.month]} {start_date.year}"
    )


async def _set_error(ctx, query_state, query_id, msg):
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "error",
            "error": msg,
        })


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id    = input_data.get("queryId")
    user_query  = input_data.get("query", "")
    parsed      = input_data.get("parsed", {})

    entity       = parsed.get("entity")
    metric       = parsed.get("metric")
    time_range   = parsed.get("time_range")
    raw_time_text = parsed.get("raw_time_text", user_query)
    ranking      = parsed.get("ranking")
    top_n        = parsed.get("top_n", 5)
    is_comparison = parsed.get("is_comparison", False)

    ctx.logger.info("🗄️ Executing SQL query", {
        "queryId": query_id,
        "entity": entity,
        "metric": metric,
        "ranking": ranking,
        "is_comparison": is_comparison,
    })

    # ── Default ranking ──────────────────────────────────────────
    if ranking is None:
        ranking = "aggregate" if is_comparison else "top"
    if ranking in ("top", "bottom") and (top_n is None or top_n <= 0):
        top_n = 5

    # ── Look up SQL template ──────────────────────────────────────
    try:
        sql = SQL_REGISTRY[entity][metric][ranking]
    except KeyError:
        msg = f"No SQL template for entity={entity}, metric={metric}, ranking={ranking}"
        ctx.logger.error("❌ No SQL template found", {"queryId": query_id, "error": msg})
        query_state = await ctx.state.get("queries", query_id)
        await _set_error(ctx, query_state, query_id, msg)
        return

    query_state = await ctx.state.get("queries", query_id)

    # ════════════════════════════════════════════════════════════
    # COMPARISON PATH
    # ════════════════════════════════════════════════════════════
    if is_comparison:
        try:
            (start1, end1), (start2, end2) = parse_comparison_date_ranges(raw_time_text)
        except (ValueError, Exception) as e:
            msg = f"Could not parse comparison date ranges: {e}"
            ctx.logger.error("❌ Comparison date parsing failed", {
                "queryId": query_id, "error": msg
            })
            await _set_error(ctx, query_state, query_id, msg)
            return

        ctx.logger.info("📅 Comparison date ranges", {
            "queryId": query_id,
            "period1": f"{start1} → {end1}",
            "period2": f"{start2} → {end2}",
        })

        try:
            if ranking == "aggregate":
                rows1 = _run_sql(sql, (start1, end1))
                rows2 = _run_sql(sql, (start2, end2))
            else:
                rows1 = _run_sql(sql, (start1, end1, top_n))
                rows2 = _run_sql(sql, (start2, end2, top_n))

            results_1 = _rows_to_results(rows1, ranking)
            results_2 = _rows_to_results(rows2, ranking)

            ctx.logger.info("✅ Comparison SQL executed", {
                "queryId": query_id,
                "rows_period1": len(results_1),
                "rows_period2": len(results_2),
            })
        except Exception as e:
            msg = f"Comparison database query failed: {e}"
            ctx.logger.error("❌ SQL execution failed", {"queryId": query_id, "error": str(e)})
            await _set_error(ctx, query_state, query_id, msg)
            return

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status": "executed",
                "results_1": results_1,
                "results_2": results_2,
            })

        await ctx.enqueue({
            "topic": "query::format.result",
            "data": {
                "queryId": query_id,
                "query": user_query,
                "parsed": parsed,
                "is_comparison": True,
                "results_1": results_1,
                "results_2": results_2,
                "period1_label": _period_label(start1, end1),
                "period2_label": _period_label(start2, end2),
                "ranking": ranking,
                "topN": top_n,
            },
        })
        return

    # ════════════════════════════════════════════════════════════
    # NORMAL (single period) PATH
    # ════════════════════════════════════════════════════════════
    try:
        start_date, end_date = parse_date_range(time_range, raw_time_text)
        ctx.logger.info("📅 Date range parsed", {
            "queryId": query_id,
            "start": str(start_date),
            "end": str(end_date),
        })
    except (ValueError, Exception) as e:
        msg = f"Could not parse date range: {e}"
        ctx.logger.error("❌ Date parsing failed", {
            "queryId": query_id, "error": msg, "raw_time_text": raw_time_text
        })
        await _set_error(ctx, query_state, query_id, msg)
        return

    try:
        if ranking == "aggregate":
            rows = _run_sql(sql, (start_date, end_date))
        else:
            rows = _run_sql(sql, (start_date, end_date, top_n))

        results = _rows_to_results(rows, ranking)

        ctx.logger.info("✅ SQL executed successfully", {
            "queryId": query_id,
            "rowCount": len(results),
        })
    except Exception as e:
        msg = f"Database query failed: {e}"
        ctx.logger.error("❌ SQL execution failed", {"queryId": query_id, "error": str(e)})
        await _set_error(ctx, query_state, query_id, msg)
        return

    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "executed",
            "results": results,
        })

    await ctx.enqueue({
        "topic": "query::format.result",
        "data": {
            "queryId": query_id,
            "query": user_query,
            "parsed": parsed,
            "is_comparison": False,
            "results": results,
            "startDate": str(start_date),
            "endDate": str(end_date),
            "ranking": ranking,
            "topN": top_n,
        },
    })