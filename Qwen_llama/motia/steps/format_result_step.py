"""Step 6: Format Result — Formats query results into a human-readable response.

Takes the raw SQL results and formats them into a structured response
with proper labels, currency formatting, and summary text. The final
formatted result is stored in Motia state.

Trigger: Queue (query::format.result)
Emits:   (none — this is the terminal step)
Flow:    sales-analytics-flow
"""

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "FormatResult",
    "description": "Formats raw SQL results into human-readable text with proper formatting",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::format.result"),
    ],
    "enqueues": [],
}

ENTITY_LABELS = {
    "product": "products",
    "customer": "customers",
    "city": "cities",
    "category": "categories",
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})
    results = input_data.get("results", [])
    start_date = input_data.get("startDate", "")
    end_date = input_data.get("endDate", "")
    ranking = input_data.get("ranking", "top")
    top_n = input_data.get("topN", 5)

    entity = parsed.get("entity", "")
    metric = parsed.get("metric", "")

    ctx.logger.info("📝 Formatting results", {
        "queryId": query_id,
        "resultCount": len(results),
        "ranking": ranking,
    })

    prefix = "₹" if metric == "revenue" else ""
    entity_label = ENTITY_LABELS.get(entity, entity)

    # ── Build formatted output ──
    if not results or (len(results) == 1 and results[0].get("value") is None):
        formatted_text = "No data available for the selected period."
        formatted_items = []
    elif ranking == "aggregate":
        value = results[0].get("value", 0)
        formatted_text = (
            f"Total {metric} between {start_date} and {end_date} "
            f"is {prefix}{value:,.2f}"
        )
        formatted_items = [{"label": f"Total {metric}", "value": f"{prefix}{value:,.2f}"}]
    else:
        formatted_text = (
            f"Top {top_n} {entity_label} by {metric} "
            f"between {start_date} and {end_date}:"
        )
        formatted_items = []
        for i, row in enumerate(results, start=1):
            name = row.get("name", "Unknown")
            value = row.get("value", 0)
            item_text = f"{i}. {name} — {prefix}{value:,.2f}"
            formatted_text += f"\n{item_text}"
            formatted_items.append({
                "rank": i,
                "name": name,
                "value": f"{prefix}{value:,.2f}",
                "raw_value": value,
            })

    ctx.logger.info("✅ Result formatted", {
        "queryId": query_id,
        "summary": formatted_text[:200],
    })

    # ── Store final result in state ──
    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "completed",
            "formattedText": formatted_text,
            "formattedItems": formatted_items,
            "completedAt": __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc
            ).isoformat(),
        })

    ctx.logger.info("🏁 Pipeline complete!", {
        "queryId": query_id,
        "status": "completed",
    })
