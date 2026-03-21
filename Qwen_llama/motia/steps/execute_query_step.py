"""Step 5: Execute Query — Runs SQL against PostgreSQL.

Parses the date range from natural language, looks up the SQL template
from the registry, executes it against PostgreSQL, and passes the
raw results to the formatting step.

Trigger: Queue (query::execute)
Emits:   query::format.result
Flow:    sales-analytics-flow
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2
from typing import Any
from motia import FlowContext, queue

from shared_config import POSTGRES, PROJECT_ROOT
import sys
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.sql_registry import SQL_REGISTRY
from utils.date_parser import parse_date_range

config = {
    "name": "ExecuteQuery",
    "description": "Parses dates, looks up SQL from registry, executes against PostgreSQL",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::execute"),
    ],
    "enqueues": ["query::format.result"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})

    entity = parsed.get("entity")
    metric = parsed.get("metric")
    time_range = parsed.get("time_range")
    raw_time_text = parsed.get("raw_time_text", user_query)
    ranking = parsed.get("ranking")
    top_n = parsed.get("top_n", 5)

    ctx.logger.info("🗄️ Executing SQL query", {
        "queryId": query_id,
        "entity": entity,
        "metric": metric,
        "ranking": ranking,
    })

    # ── Parse date range ──
    try:
        start_date, end_date = parse_date_range(time_range, raw_time_text)
        ctx.logger.info("📅 Date range parsed", {
            "queryId": query_id,
            "start": str(start_date),
            "end": str(end_date),
        })
    except (ValueError, Exception) as e:
        ctx.logger.error("❌ Date parsing failed", {
            "queryId": query_id,
            "error": str(e),
            "raw_time_text": raw_time_text,
        })

        query_state = await ctx.state.get("queries", query_id)
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status": "error",
                "error": f"Could not parse date range: {str(e)}",
            })
        return

    # ── Default ranking ──
    if ranking is None:
        ranking = "top"
    if ranking == "top" and (top_n is None or top_n <= 0):
        top_n = 5

    # ── Look up SQL ──
    try:
        sql = SQL_REGISTRY[entity][metric][ranking]
    except KeyError:
        ctx.logger.error("❌ No SQL template found", {
            "queryId": query_id,
            "entity": entity,
            "metric": metric,
            "ranking": ranking,
        })

        query_state = await ctx.state.get("queries", query_id)
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status": "error",
                "error": f"No SQL template for entity={entity}, metric={metric}, ranking={ranking}",
            })
        return

    # ── Execute SQL ──
    try:
        conn = psycopg2.connect(**POSTGRES)
        cur = conn.cursor()

        if ranking == "aggregate":
            cur.execute(sql, (start_date, end_date))
        else:
            cur.execute(sql, (start_date, end_date, top_n))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Convert rows to serializable format
        results = []
        for row in rows:
            if ranking == "aggregate":
                results.append({"value": float(row[0]) if row[0] is not None else None})
            else:
                results.append({
                    "name": str(row[0]),
                    "value": float(row[1]) if row[1] is not None else None,
                })

        ctx.logger.info("✅ SQL executed successfully", {
            "queryId": query_id,
            "rowCount": len(results),
        })

    except Exception as e:
        ctx.logger.error("❌ SQL execution failed", {
            "queryId": query_id,
            "error": str(e),
        })

        query_state = await ctx.state.get("queries", query_id)
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status": "error",
                "error": f"Database query failed: {str(e)}",
            })
        return

    # ── Update state and emit ──
    query_state = await ctx.state.get("queries", query_id)
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
            "results": results,
            "startDate": str(start_date),
            "endDate": str(end_date),
            "ranking": ranking,
            "topN": top_n,
        },
    })
