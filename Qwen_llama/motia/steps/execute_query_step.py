"""Step 5: Execute Query — Runs SQL against PostgreSQL.

Uses LLM-generated SQL from text_to_sql_step when available,
falls back to SQL_REGISTRY for standard queries.
"""

import os, sys, calendar

_STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR    = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import psycopg2
from typing import Any
from motia import FlowContext, queue

from shared_config import POSTGRES
from db.sql_registry import SQL_REGISTRY
from utils.date_parser import parse_date_range, parse_comparison_date_ranges

config = {
    "name": "ExecuteQuery",
    "description": "Runs the resolved SQL (generated or registry) against PostgreSQL.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::execute")],
    "enqueues": ["query::format.result"],
}


def _run_sql(sql, params):
    conn = psycopg2.connect(**POSTGRES)
    cur  = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def _rows_to_results(rows, ranking):
    results = []
    for row in rows:
        if ranking in ("aggregate", "threshold") and len(row) == 1:
            results.append({"value": float(row[0]) if row[0] is not None else None})
        else:
            results.append({
                "name":  str(row[0]),
                "value": float(row[1]) if row[1] is not None else None,
            })
    return results


def _build_params(sql, ranking, start_date, end_date, top_n):
    """Count %s placeholders and build the right param tuple."""
    count = sql.count("%s")
    if count == 0:
        return ()
    if count == 3 and ranking in ("top", "bottom"):
        return (start_date, end_date, top_n)
    slots = []
    for i in range(count):
        if i == count - 1 and ranking in ("top", "bottom") and count % 2 == 1:
            slots.append(top_n)
        else:
            slots.append(start_date if i % 2 == 0 else end_date)
    return tuple(slots)


def _period_label(start_date, end_date):
    if start_date.month == end_date.month and start_date.year == end_date.year:
        return f"{calendar.month_name[start_date.month]} {start_date.year}"
    return (f"{calendar.month_abbr[start_date.month]}–"
            f"{calendar.month_abbr[end_date.month]} {start_date.year}")


def _resolve_sql(generated_sql, entity, metric, ranking):
    if generated_sql is not None:
        return generated_sql, "generated"
    try:
        return SQL_REGISTRY[entity][metric][ranking], "registry"
    except KeyError:
        return None, "none"


async def _set_error(ctx, query_state, query_id, msg):
    if query_state:
        await ctx.state.set("queries", query_id, {**query_state, "status": "error", "error": msg})


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    parsed        = input_data.get("parsed", {})
    generated_sql = input_data.get("generated_sql")

    entity        = parsed.get("entity")
    metric        = parsed.get("metric")
    time_range    = parsed.get("time_range")
    raw_time_text = parsed.get("raw_time_text", user_query)
    ranking       = parsed.get("ranking")
    top_n         = parsed.get("top_n", 5)
    is_comparison = parsed.get("is_comparison", False)

    if ranking is None:
        ranking = "aggregate" if is_comparison else "top"
    if ranking in ("top", "bottom") and (top_n is None or top_n <= 0):
        top_n = 5

    sql, sql_source = _resolve_sql(generated_sql, entity, metric, ranking)

    ctx.logger.info("🗄️ SQL resolved", {
        "queryId": query_id, "source": sql_source,
        "entity": entity, "metric": metric, "ranking": ranking,
    })

    if sql is None:
        msg = f"No SQL available for entity={entity} metric={metric} ranking={ranking}"
        ctx.logger.error("❌ No SQL", {"queryId": query_id})
        qs = await ctx.state.get("queries", query_id)
        await _set_error(ctx, qs, query_id, msg)
        return

    query_state = await ctx.state.get("queries", query_id)

    # ── COMPARISON PATH ───────────────────────────────────────────────────────
    if is_comparison:
        try:
            (start1, end1), (start2, end2) = parse_comparison_date_ranges(raw_time_text)
        except Exception as e:
            await _set_error(ctx, query_state, query_id, f"Could not parse comparison dates: {e}")
            return

        try:
            rows1 = _run_sql(sql, _build_params(sql, ranking, start1, end1, top_n))
            rows2 = _run_sql(sql, _build_params(sql, ranking, start2, end2, top_n))
            results_1 = _rows_to_results(rows1, ranking)
            results_2 = _rows_to_results(rows2, ranking)
        except Exception as e:
            await _set_error(ctx, query_state, query_id, f"Comparison SQL failed: {e}")
            return

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state, "status": "executed", "sql_source": sql_source,
                "results_1": results_1, "results_2": results_2,
            })

        await ctx.enqueue({"topic": "query::format.result", "data": {
            "queryId": query_id, "query": user_query, "parsed": parsed,
            "is_comparison": True, "results_1": results_1, "results_2": results_2,
            "period1_label": _period_label(start1, end1),
            "period2_label": _period_label(start2, end2),
            "ranking": ranking, "topN": top_n,
        }})
        return

    # ── NORMAL PATH ───────────────────────────────────────────────────────────
    try:
        start_date, end_date = parse_date_range(time_range, raw_time_text)
    except Exception as e:
        await _set_error(ctx, query_state, query_id, f"Could not parse date range: {e}")
        return

    try:
        params  = _build_params(sql, ranking, start_date, end_date, top_n)
        rows    = _run_sql(sql, params)
        results = _rows_to_results(rows, ranking)
        ctx.logger.info("✅ SQL executed", {"queryId": query_id, "rows": len(results), "source": sql_source})
    except Exception as e:
        # Runtime fallback for generated SQL — try registry once
        if sql_source == "generated":
            ctx.logger.warn("⚠️ Generated SQL failed, trying registry", {"queryId": query_id, "error": str(e)})
            try:
                fallback = SQL_REGISTRY[entity][metric][ranking]
                params   = _build_params(fallback, ranking, start_date, end_date, top_n)
                rows     = _run_sql(fallback, params)
                results  = _rows_to_results(rows, ranking)
                sql_source = "registry-runtime-fallback"
            except Exception as e2:
                await _set_error(ctx, query_state, query_id, f"All SQL paths failed: {e2}")
                return
        else:
            if ranking == "threshold":
                ctx.logger.error("❌ Threshold SQL execution failed", {
                    "queryId": query_id, "error": str(e), "sql": sql[:500], "params": str(params)
                })
            await _set_error(ctx, query_state, query_id, f"SQL failed: {e}")
            return

    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state, "status": "executed",
            "sql_source": sql_source, "results": results,
        })

    await ctx.enqueue({"topic": "query::format.result", "data": {
        "queryId": query_id, "query": user_query, "parsed": parsed,
        "is_comparison": False, "results": results,
        "startDate": str(start_date), "endDate": str(end_date),
        "ranking": ranking, "topN": top_n,
    }})