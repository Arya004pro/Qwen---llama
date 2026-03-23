"""Step 5: Execute Query — Runs SQL against PostgreSQL.

Modes handled
-------------
  normal          : single period, top / bottom / aggregate
  threshold       : absolute  → SQL_REGISTRY threshold_absolute (no LLM)
                    percentage → LLM-generated SQL (Qwen, passed as generated_sql)
  zero_filter     : NOT EXISTS query from SQL_REGISTRY
  comparison      : two periods, side-by-side (vs / compare)
  intersection    : "both X and Y" — entities appearing in BOTH periods
  top_growth      : comparison ranked by revenue/quantity DELTA per entity

No values are hardcoded — threshold_value, top_n, dates all come from parsed intent.
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
from utils.date_parser import (
    parse_date_range,
    parse_comparison_date_ranges,
    parse_both_date_ranges,
)

config = {
    "name": "ExecuteQuery",
    "description": (
        "Executes SQL against PostgreSQL for all query modes: normal, "
        "threshold (absolute & percentage), zero_filter, comparison, "
        "intersection, and top_growth (growth ranking)."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::execute")],
    "enqueues": ["query::format.result"],
}


# ── low-level helpers ──────────────────────────────────────────────────────────

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
        if ranking in ("aggregate", "threshold", "zero_filter") and len(row) == 1:
            results.append({"value": float(row[0]) if row[0] is not None else None})
        else:
            results.append({
                "name":  str(row[0]),
                "value": float(row[1]) if len(row) > 1 and row[1] is not None else 0.0,
            })
    return results


def _build_params(sql, ranking, start_date, end_date, top_n,
                  threshold_value=None):
    """
    Build the parameter tuple for the SQL query.

    Param count logic:
      top / bottom                → (start, end, top_n)
      aggregate / zero_filter     → (start, end)
      threshold_absolute          → (start, end, threshold_value)
      LLM-generated threshold     → detected from %s count
    """
    count = sql.count("%s")
    if count == 0:
        return ()

    if ranking == "threshold" and threshold_value is not None:
        # Absolute threshold SQL: always (start, end, threshold_value)
        if count == 3:
            return (start_date, end_date, threshold_value)

    if count == 3 and ranking in ("top", "bottom", "top_growth"):
        return (start_date, end_date, top_n)

    if count == 2:
        return (start_date, end_date)

    # Generic fallback: fill even slots with start, odd with end,
    # last slot with top_n/threshold if needed
    slots = []
    for i in range(count):
        if i == count - 1 and ranking in ("top", "bottom") and count % 2 == 1:
            slots.append(top_n)
        elif i == count - 1 and ranking == "threshold" and count % 2 == 1:
            slots.append(threshold_value or 0)
        else:
            slots.append(start_date if i % 2 == 0 else end_date)
    return tuple(slots)


def _period_label(start_date, end_date):
    if start_date.month == end_date.month and start_date.year == end_date.year:
        return f"{calendar.month_name[start_date.month]} {start_date.year}"
    return (f"{calendar.month_abbr[start_date.month]}–"
            f"{calendar.month_abbr[end_date.month]} {start_date.year}")


def _resolve_sql(generated_sql, entity, metric, ranking, threshold_type=None):
    """
    Resolve which SQL to use.

    For threshold:
      - absolute  → SQL_REGISTRY[entity][metric]["threshold_absolute"]
      - percentage → LLM-generated (passed as generated_sql)
    For all others:
      - generated_sql if present (LLM)
      - SQL_REGISTRY fallback
    """
    if ranking == "threshold" and threshold_type == "absolute":
        try:
            return SQL_REGISTRY[entity][metric]["threshold_absolute"], "registry_absolute"
        except KeyError:
            return None, "none"

    if generated_sql is not None:
        return generated_sql, "generated"

    try:
        return SQL_REGISTRY[entity][metric][ranking], "registry"
    except KeyError:
        return None, "none"


async def _set_error(ctx, query_state, query_id, msg):
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state, "status": "error", "error": msg,
        })


# ── growth ranking helper ──────────────────────────────────────────────────────

def _run_growth_ranking(entity, metric, start1, end1, start2, end2, top_n, ascending=False):
    """
    Fetch per-entity values for two periods, compute delta, rank by delta.
    Returns list of dicts: {name, value1, value2, delta}
    """
    BIG_LIMIT = 9999
    top_sql = SQL_REGISTRY[entity][metric]["top"]

    rows1 = _run_sql(top_sql, (start1, end1, BIG_LIMIT))
    rows2 = _run_sql(top_sql, (start2, end2, BIG_LIMIT))

    dict1 = {str(r[0]): float(r[1]) for r in rows1 if r[1] is not None}
    dict2 = {str(r[0]): float(r[1]) for r in rows2 if r[1] is not None}

    # Guard: if either period has no data at all, raise a clear error
    if not dict1 and not dict2:
        raise ValueError("No data found for either period.")
    if not dict1:
        raise ValueError(
            f"No {entity} data found for the first period ({start1.strftime('%b %Y')} "
            f"to {end1.strftime('%b %Y')}). The dataset may not cover this time range."
        )
    if not dict2:
        raise ValueError(
            f"No {entity} data found for the second period ({start2.strftime('%b %Y')} "
            f"to {end2.strftime('%b %Y')}). The dataset may not cover this time range."
        )

    all_names = set(dict1) | set(dict2)
    deltas = []
    for name in all_names:
        v1    = dict1.get(name, 0.0)
        v2    = dict2.get(name, 0.0)
        delta = v2 - v1
        deltas.append({"name": name, "value1": v1, "value2": v2, "delta": delta})

    deltas.sort(key=lambda x: x["delta"], reverse=not ascending)
    return deltas[:top_n]


# ── intersection helper ────────────────────────────────────────────────────────

def _run_intersection(entity, metric, start1, end1, start2, end2, top_n, ascending=False):
    BIG_LIMIT = 9999
    top_sql   = SQL_REGISTRY[entity][metric]["top"]
    rows1 = _run_sql(top_sql, (start1, end1, BIG_LIMIT))
    rows2 = _run_sql(top_sql, (start2, end2, BIG_LIMIT))
    dict1 = {str(r[0]): float(r[1]) for r in rows1 if r[1] is not None}
    dict2 = {str(r[0]): float(r[1]) for r in rows2 if r[1] is not None}
    common = set(dict1) & set(dict2)
    if not common:
        return []
    combined = sorted(
        [{"name": n, "value": dict1[n] + dict2[n]} for n in common],
        key=lambda x: x["value"], reverse=not ascending,
    )
    return combined[:top_n]


# ── main handler ───────────────────────────────────────────────────────────────

async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    parsed        = input_data.get("parsed", {})
    generated_sql = input_data.get("generated_sql")

    entity           = parsed.get("entity")
    metric           = parsed.get("metric")
    time_range       = parsed.get("time_range")
    raw_time_text    = parsed.get("raw_time_text", user_query)
    ranking          = parsed.get("ranking")
    top_n            = parsed.get("top_n", 5) or 5
    is_comparison    = parsed.get("is_comparison", False)
    is_intersection  = parsed.get("is_intersection", False)
    is_growth_ranking= parsed.get("is_growth_ranking", False)
    threshold_value  = parsed.get("threshold_value")
    threshold_type   = parsed.get("threshold_type")

    if ranking is None:
        ranking = "aggregate" if is_comparison else "top"
    if ranking in ("top", "bottom") and top_n <= 0:
        top_n = 5

    query_state = await ctx.state.get("queries", query_id)

    # ── TOP GROWTH (comparison ranked by delta) ────────────────────────────────
    if is_growth_ranking and ranking == "top_growth":
        try:
            (s1, e1), (s2, e2) = parse_comparison_date_ranges(raw_time_text)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Could not parse comparison dates: {exc}")
            return

        ascending = any(w in (raw_time_text or "").lower()
                        for w in ["lowest", "worst", "smallest", "minimum", "least"])
        try:
            results = _run_growth_ranking(entity, metric, s1, e1, s2, e2,
                                          top_n, ascending=ascending)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Growth ranking failed: {exc}")
            return

        p1_label = _period_label(s1, e1)
        p2_label = _period_label(s2, e2)

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state, "status": "executed",
                "results_growth": results,
            })
        await ctx.enqueue({"topic": "query::format.result", "data": {
            "queryId": query_id, "query": user_query, "parsed": parsed,
            "is_growth_ranking": True,
            "results_growth":    results,
            "period1_label":     p1_label,
            "period2_label":     p2_label,
            "ranking":           ranking, "topN": top_n,
        }})
        return

    # ── INTERSECTION ──────────────────────────────────────────────────────────
    if is_intersection:
        try:
            (s1, e1), (s2, e2) = parse_both_date_ranges(raw_time_text)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Could not parse 'both' date ranges: {exc}")
            return
        ascending = (ranking == "bottom")
        try:
            results = _run_intersection(entity, metric, s1, e1, s2, e2, top_n, ascending=ascending)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Intersection SQL failed: {exc}")
            return

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state, "status": "executed", "results": results,
            })
        await ctx.enqueue({"topic": "query::format.result", "data": {
            "queryId": query_id, "query": user_query, "parsed": parsed,
            "is_intersection":       True,
            "intersection_period1":  _period_label(s1, e1),
            "intersection_period2":  _period_label(s2, e2),
            "results": results, "startDate": str(s1), "endDate": str(e2),
            "ranking": "top", "topN": top_n,
        }})
        return

    # ── COMPARISON ────────────────────────────────────────────────────────────
    if is_comparison:
        sql, sql_source = _resolve_sql(generated_sql, entity, metric, ranking)
        if sql is None:
            await _set_error(ctx, query_state, query_id,
                             f"No SQL for {entity}/{metric}/{ranking}")
            return
        try:
            (s1, e1), (s2, e2) = parse_comparison_date_ranges(raw_time_text)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Could not parse comparison dates: {exc}")
            return
        try:
            r1 = _rows_to_results(_run_sql(sql, _build_params(sql, ranking, s1, e1, top_n)), ranking)
            r2 = _rows_to_results(_run_sql(sql, _build_params(sql, ranking, s2, e2, top_n)), ranking)
        except Exception as exc:
            await _set_error(ctx, query_state, query_id,
                             f"Comparison SQL failed: {exc}")
            return
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state, "status": "executed", "sql_source": sql_source,
                "results_1": r1, "results_2": r2,
            })
        await ctx.enqueue({"topic": "query::format.result", "data": {
            "queryId": query_id, "query": user_query, "parsed": parsed,
            "is_comparison": True, "results_1": r1, "results_2": r2,
            "period1_label": _period_label(s1, e1),
            "period2_label": _period_label(s2, e2),
            "ranking": ranking, "topN": top_n,
        }})
        return

    # ── NORMAL / THRESHOLD / ZERO_FILTER ──────────────────────────────────────
    try:
        start_date, end_date = parse_date_range(time_range, raw_time_text)
    except Exception as exc:
        await _set_error(ctx, query_state, query_id,
                         f"Could not parse date range: {exc}")
        return

    sql, sql_source = _resolve_sql(generated_sql, entity, metric, ranking, threshold_type)
    ctx.logger.info("🗄️ SQL resolved", {
        "queryId": query_id, "source": sql_source,
        "ranking": ranking, "threshold_type": threshold_type,
        "threshold_value": threshold_value,
    })

    if sql is None:
        msg = (
            f"No SQL available for entity={entity} metric={metric} "
            f"ranking={ranking} threshold_type={threshold_type}"
        )
        await _set_error(ctx, query_state, query_id, msg)
        return

    try:
        params  = _build_params(sql, ranking, start_date, end_date, top_n, threshold_value)
        rows    = _run_sql(sql, params)
        results = _rows_to_results(rows, ranking)
        ctx.logger.info("✅ SQL executed", {
            "queryId": query_id, "rows": len(results), "source": sql_source,
        })
    except Exception as exc:
        # Runtime fallback for LLM-generated SQL only
        if sql_source == "generated":
            ctx.logger.warn("⚠️ Generated SQL failed, trying registry",
                            {"queryId": query_id, "error": str(exc)})
            try:
                fallback_key = "threshold_absolute" if ranking == "threshold" and threshold_type == "absolute" else ranking
                fallback   = SQL_REGISTRY[entity][metric][fallback_key]
                params     = _build_params(fallback, ranking, start_date, end_date, top_n, threshold_value)
                rows       = _run_sql(fallback, params)
                results    = _rows_to_results(rows, ranking)
                sql_source = "registry-runtime-fallback"
            except Exception as exc2:
                await _set_error(ctx, query_state, query_id,
                                 f"All SQL paths failed: {exc2}")
                return
        else:
            await _set_error(ctx, query_state, query_id, f"SQL failed: {exc}")
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
        "threshold_value": threshold_value, "threshold_type": threshold_type,
    }})