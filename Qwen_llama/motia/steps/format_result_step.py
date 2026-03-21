"""Step 6: Format Result — formats results + appends token-usage summary to state."""

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "FormatResult",
    "description": "Transforms raw query output into the final user-facing answer and marks the run complete",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::format.result")],
    "enqueues": [],
}

ENTITY_LABELS = {
    "product": "products",
    "customer": "customers",
    "city": "cities",
    "category": "categories",
}


def _fmt(value, prefix):
    if value is None:
        return "N/A"
    return f"{prefix}{value:,.2f}"


def _delta_str(v1, v2, prefix):
    if v1 is None or v2 is None:
        return "N/A"
    delta = v2 - v1
    sign  = "+" if delta >= 0 else ""
    pct   = (delta / v1 * 100) if v1 != 0 else float("inf")
    return f"{sign}{prefix}{abs(delta):,.2f}  ({sign}{pct:.1f}%)"


def _format_comparison(parsed, results_1, results_2, period1, period2, ranking, top_n):
    entity  = parsed.get("entity", "")
    metric  = parsed.get("metric", "")
    prefix  = "₹" if metric == "revenue" else ""
    entity_label = ENTITY_LABELS.get(entity, entity)

    if ranking == "aggregate":
        v1 = results_1[0]["value"] if results_1 else None
        v2 = results_2[0]["value"] if results_2 else None
        delta_text = _delta_str(v1, v2, prefix)
        text = (
            f"📊 {metric.title()} Comparison: {period1} vs {period2}\n"
            f"{'─' * 48}\n"
            f"  {period1:<20} {_fmt(v1, prefix)}\n"
            f"  {period2:<20} {_fmt(v2, prefix)}\n"
            f"  Δ Change             {delta_text}"
        )
        items = [
            {"period": period1, "value": _fmt(v1, prefix), "raw_value": v1},
            {"period": period2, "value": _fmt(v2, prefix), "raw_value": v2},
            {"delta": delta_text},
        ]
        return text, items

    label = "Top" if ranking == "top" else "Bottom"
    header = f"📊 {label} {top_n} {entity_label} by {metric}: {period1} vs {period2}"
    dict1 = {r["name"]: r["value"] for r in results_1}
    dict2 = {r["name"]: r["value"] for r in results_2}
    all_names = list(dict.fromkeys(
        [r["name"] for r in results_1] + [r["name"] for r in results_2]
    ))
    col_w   = max((len(n) for n in all_names), default=20)
    col_w   = max(col_w, 20)
    sep     = "─" * (col_w + 44)
    hdr_row = f"  {'#':>3}  {'Name':<{col_w}}  {period1:>16}  {period2:>16}  {'Δ Change':>14}"
    rows_text = []
    formatted_items = []
    for i, name in enumerate(all_names, start=1):
        v1 = dict1.get(name)
        v2 = dict2.get(name)
        delta = _delta_str(v1, v2, prefix)
        rows_text.append(
            f"  {i:>3}. {name:<{col_w}}  {_fmt(v1, prefix):>16}  {_fmt(v2, prefix):>16}  {delta:>14}"
        )
        formatted_items.append({
            "rank": i, "name": name,
            "period1_value": _fmt(v1, prefix),
            "period2_value": _fmt(v2, prefix),
            "delta": delta,
        })
    text = "\n".join([header, sep, hdr_row, sep] + rows_text)
    return text, formatted_items


def _token_summary_text(token_usage: list, token_totals: dict) -> str:
    """Build a compact token-usage summary string to append to formatted output."""
    if not token_usage and not token_totals:
        return ""
    lines = ["\n\n─── Token Usage ───────────────────────────────"]
    for entry in token_usage:
        short_model = entry.get("model", "").split("/")[-1]
        lines.append(
            f"  {entry.get('step','?'):<20} {short_model:<28} "
            f"prompt={entry.get('prompt_tokens',0):>5}  "
            f"completion={entry.get('completion_tokens',0):>4}  "
            f"total={entry.get('total_tokens',0):>5}"
        )
    if token_totals:
        lines.append("  " + "─" * 70)
        lines.append(
            f"  {'TOTAL':<20} {'':28} "
            f"prompt={token_totals.get('prompt_tokens',0):>5}  "
            f"completion={token_totals.get('completion_tokens',0):>4}  "
            f"total={token_totals.get('total_tokens',0):>5}"
        )
    return "\n".join(lines)


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    import datetime as _dt

    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    parsed        = input_data.get("parsed", {})
    ranking       = input_data.get("ranking", "top")
    top_n         = input_data.get("topN", 5)
    is_comparison = input_data.get("is_comparison", False)

    entity  = parsed.get("entity", "")
    metric  = parsed.get("metric", "")
    prefix  = "₹" if metric == "revenue" else ""
    entity_label = ENTITY_LABELS.get(entity, entity)

    ctx.logger.info("📝 Formatting results", {
        "queryId": query_id, "ranking": ranking, "is_comparison": is_comparison
    })

    # ── Pull existing token data from state (accumulated by previous steps) ─
    query_state  = await ctx.state.get("queries", query_id)
    token_usage  = (query_state or {}).get("token_usage",  [])
    token_totals = (query_state or {}).get("token_totals", {})

    # ════════════════════════════════════════════════════════════
    # COMPARISON FORMAT
    # ════════════════════════════════════════════════════════════
    if is_comparison:
        results_1 = input_data.get("results_1", [])
        results_2 = input_data.get("results_2", [])
        period1   = input_data.get("period1_label", "Period 1")
        period2   = input_data.get("period2_label", "Period 2")

        if not results_1 and not results_2:
            formatted_text  = "No data available for the selected periods."
            formatted_items = []
        else:
            formatted_text, formatted_items = _format_comparison(
                parsed, results_1, results_2, period1, period2, ranking, top_n
            )

        formatted_text += _token_summary_text(token_usage, token_totals)
        ctx.logger.info("✅ Comparison result formatted",
                        {"queryId": query_id, "summary": formatted_text[:200]})

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status":         "completed",
                "formattedText":  formatted_text,
                "formattedItems": formatted_items,
                "token_usage":    token_usage,
                "token_totals":   token_totals,
                "completedAt":    _dt.datetime.now(_dt.timezone.utc).isoformat(),
            })
        ctx.logger.info("🏁 Pipeline complete (comparison)!", {"queryId": query_id})
        return

    # ════════════════════════════════════════════════════════════
    # NORMAL (single-period) FORMAT
    # ════════════════════════════════════════════════════════════
    results    = input_data.get("results", [])
    start_date = input_data.get("startDate", "")
    end_date   = input_data.get("endDate", "")

    if not results or (len(results) == 1 and results[0].get("value") is None):
        formatted_text  = "No data available for the selected period."
        formatted_items = []

    elif ranking == "aggregate":
        value = results[0].get("value", 0)
        formatted_text = (
            f"Total {metric} between {start_date} and {end_date} "
            f"is {prefix}{value:,.2f}"
        )
        formatted_items = [{"label": f"Total {metric}", "value": f"{prefix}{value:,.2f}"}]

    else:
        rank_label = "Top" if ranking == "top" else "Bottom"
        formatted_text = (
            f"{rank_label} {top_n} {entity_label} by {metric} "
            f"between {start_date} and {end_date}:"
        )
        formatted_items = []
        for i, row in enumerate(results, start=1):
            name  = row.get("name", "Unknown")
            value = row.get("value", 0)
            formatted_text += f"\n{i}. {name} — {prefix}{value:,.2f}"
            formatted_items.append({
                "rank": i, "name": name,
                "value": f"{prefix}{value:,.2f}", "raw_value": value,
            })

    formatted_text += _token_summary_text(token_usage, token_totals)

    ctx.logger.info("✅ Result formatted",
                    {"queryId": query_id, "summary": formatted_text[:200]})

    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status":         "completed",
            "formattedText":  formatted_text,
            "formattedItems": formatted_items,
            "token_usage":    token_usage,
            "token_totals":   token_totals,
            "completedAt":    _dt.datetime.now(_dt.timezone.utc).isoformat(),
        })

    ctx.logger.info("🏁 Pipeline complete!", {"queryId": query_id})