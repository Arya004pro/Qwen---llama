"""Step 6: Format Result — formats results + chart config + token-usage summary."""

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
    "product":  "products",
    "customer": "customers",
    "city":     "cities",
    "category": "categories",
}

_PALETTE = [
    "rgba(99,  179, 237, 0.85)",
    "rgba(104, 211, 145, 0.85)",
    "rgba(246, 173,  85, 0.85)",
    "rgba(252, 129, 129, 0.85)",
    "rgba(154, 117, 221, 0.85)",
    "rgba( 79, 209, 197, 0.85)",
    "rgba(246, 135, 179, 0.85)",
    "rgba(183, 148, 255, 0.85)",
]
_BORDER_PALETTE = [c.replace("0.85", "1") for c in _PALETTE]

_CHART_DEFAULTS = {
    "responsive": True,
    "maintainAspectRatio": True,
    "plugins": {
        "legend": {"display": False},
        "tooltip": {
            "backgroundColor": "#1e2130",
            "titleColor":      "#e2e8f0",
            "bodyColor":       "#94a3b8",
            "borderColor":     "#2d3148",
            "borderWidth":     1,
        },
    },
    "scales": {
        "x": {"ticks": {"color": "#94a3b8", "font": {"size": 11}},
              "grid":  {"color": "rgba(255,255,255,0.05)"}},
        "y": {"ticks": {"color": "#94a3b8", "font": {"size": 11}},
              "grid":  {"color": "rgba(255,255,255,0.05)"},
              "beginAtZero": True},
    },
}


def _build_bar_chart(labels, values, metric, title, subtitle):
    prefix  = "₹" if metric == "revenue" else ""
    colors  = [_PALETTE[i % len(_PALETTE)]        for i in range(len(labels))]
    borders = [_BORDER_PALETTE[i % len(_PALETTE)] for i in range(len(labels))]
    cfg = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [{"label": metric.title(), "data": values,
                          "backgroundColor": colors, "borderColor": borders,
                          "borderWidth": 1, "borderRadius": 4}],
        },
        "options": {
            **_CHART_DEFAULTS,
            "indexAxis": "y",
            "plugins": {
                **_CHART_DEFAULTS["plugins"],
                "legend": {"display": False},
                "tooltip": {
                    **_CHART_DEFAULTS["plugins"]["tooltip"],
                    "callbacks": {"label": f"function(c){{return ' {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"}}
            },
            "scales": {
                "x": {**_CHART_DEFAULTS["scales"]["x"],
                      "ticks": {**_CHART_DEFAULTS["scales"]["x"]["ticks"],
                                "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}},
                "y": _CHART_DEFAULTS["scales"]["y"],
            },
        },
    }
    return {"title": title, "subtitle": subtitle, "config": cfg}


def _build_comparison_chart(labels, values1, values2, period1, period2, metric, title, subtitle, ranking):
    prefix = "₹" if metric == "revenue" else ""
    if ranking == "aggregate":
        cfg = {
            "type": "bar",
            "data": {
                "labels": [period1, period2],
                "datasets": [{"label": metric.title(),
                              "data": [values1[0] if values1 else 0, values2[0] if values2 else 0],
                              "backgroundColor": [_PALETTE[0], _PALETTE[1]],
                              "borderColor": [_BORDER_PALETTE[0], _BORDER_PALETTE[1]],
                              "borderWidth": 1, "borderRadius": 4}],
            },
            "options": {**_CHART_DEFAULTS,
                        "plugins": {**_CHART_DEFAULTS["plugins"],
                                    "tooltip": {**_CHART_DEFAULTS["plugins"]["tooltip"],
                                                "callbacks": {"label": f"function(c){{return ' {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"}}},
                        "scales": {"x": _CHART_DEFAULTS["scales"]["x"],
                                   "y": {**_CHART_DEFAULTS["scales"]["y"],
                                         "ticks": {**_CHART_DEFAULTS["scales"]["y"]["ticks"],
                                                   "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}}}}
        }
    else:
        cfg = {
            "type": "bar",
            "data": {
                "labels": labels,
                "datasets": [
                    {"label": period1, "data": values1, "backgroundColor": _PALETTE[0],
                     "borderColor": _BORDER_PALETTE[0], "borderWidth": 1, "borderRadius": 3},
                    {"label": period2, "data": values2, "backgroundColor": _PALETTE[1],
                     "borderColor": _BORDER_PALETTE[1], "borderWidth": 1, "borderRadius": 3},
                ],
            },
            "options": {**_CHART_DEFAULTS, "indexAxis": "y",
                        "plugins": {**_CHART_DEFAULTS["plugins"],
                                    "legend": {"display": True, "labels": {"color": "#94a3b8", "font": {"size": 12}}},
                                    "tooltip": {**_CHART_DEFAULTS["plugins"]["tooltip"],
                                                "callbacks": {"label": f"function(c){{return ' '+c.dataset.label+': {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"}}},
                        "scales": {"x": {**_CHART_DEFAULTS["scales"]["x"],
                                         "ticks": {**_CHART_DEFAULTS["scales"]["x"]["ticks"],
                                                   "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}},
                                   "y": _CHART_DEFAULTS["scales"]["y"]}}
        }
    return {"title": title, "subtitle": subtitle, "config": cfg}


def _fmt(value, prefix):
    return f"{prefix}{value:,.2f}" if value is not None else "N/A"


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
        text = (f"📊 {metric.title()} Comparison: {period1} vs {period2}\n"
                f"{'─'*48}\n"
                f"  {period1:<20} {_fmt(v1, prefix)}\n"
                f"  {period2:<20} {_fmt(v2, prefix)}\n"
                f"  Δ Change             {_delta_str(v1, v2, prefix)}")
        items = [{"period": period1, "value": _fmt(v1, prefix), "raw_value": v1},
                 {"period": period2, "value": _fmt(v2, prefix), "raw_value": v2},
                 {"delta": _delta_str(v1, v2, prefix)}]
        return text, items

    label = "Top" if ranking == "top" else "Bottom"
    header = f"📊 {label} {top_n} {entity_label} by {metric}: {period1} vs {period2}"
    dict1 = {r["name"]: r["value"] for r in results_1}
    dict2 = {r["name"]: r["value"] for r in results_2}
    all_names = list(dict.fromkeys([r["name"] for r in results_1] + [r["name"] for r in results_2]))
    col_w = max((len(n) for n in all_names), default=20)
    col_w = max(col_w, 20)
    sep     = "─" * (col_w + 44)
    hdr_row = f"  {'#':>3}  {'Name':<{col_w}}  {period1:>16}  {period2:>16}  {'Δ Change':>14}"
    rows_text = []
    formatted_items = []
    for i, name in enumerate(all_names, start=1):
        v1 = dict1.get(name)
        v2 = dict2.get(name)
        delta = _delta_str(v1, v2, prefix)
        rows_text.append(f"  {i:>3}. {name:<{col_w}}  {_fmt(v1,prefix):>16}  {_fmt(v2,prefix):>16}  {delta:>14}")
        formatted_items.append({"rank": i, "name": name,
                                 "period1_value": _fmt(v1, prefix),
                                 "period2_value": _fmt(v2, prefix), "delta": delta})
    text = "\n".join([header, sep, hdr_row, sep] + rows_text)
    return text, formatted_items


def _token_summary_text(token_usage, token_totals):
    if not token_usage and not token_totals:
        return ""
    lines = ["\n\n─── Token Usage ───────────────────────────────"]
    for entry in token_usage:
        short_model = entry.get("model", "").split("/")[-1]
        lines.append(f"  {entry.get('step','?'):<20} {short_model:<28} "
                     f"prompt={entry.get('prompt_tokens',0):>5}  "
                     f"completion={entry.get('completion_tokens',0):>4}  "
                     f"total={entry.get('total_tokens',0):>5}")
    if token_totals:
        lines.append("  " + "─"*70)
        lines.append(f"  {'TOTAL':<20} {'':28} "
                     f"prompt={token_totals.get('prompt_tokens',0):>5}  "
                     f"completion={token_totals.get('completion_tokens',0):>4}  "
                     f"total={token_totals.get('total_tokens',0):>5}")
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

    query_state  = await ctx.state.get("queries", query_id)
    token_usage  = (query_state or {}).get("token_usage",  [])
    token_totals = (query_state or {}).get("token_totals", {})

    chart_config = None

    # ── COMPARISON ────────────────────────────────────────────────────────────
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
                parsed, results_1, results_2, period1, period2, ranking, top_n)

        formatted_text += _token_summary_text(token_usage, token_totals)

        if results_1 or results_2:
            chart_title    = f"{metric.title()} comparison: {period1} vs {period2}"
            chart_subtitle = f"Query: {user_query}"
            if ranking == "aggregate":
                v1 = [results_1[0]["value"]] if results_1 else [0]
                v2 = [results_2[0]["value"]] if results_2 else [0]
                chart_config = _build_comparison_chart([], v1, v2, period1, period2, metric, chart_title, chart_subtitle, "aggregate")
            else:
                dict1  = {r["name"]: r["value"] for r in results_1}
                dict2  = {r["name"]: r["value"] for r in results_2}
                names  = list(dict.fromkeys([r["name"] for r in results_1] + [r["name"] for r in results_2]))
                vals1  = [dict1.get(n, 0) or 0 for n in names]
                vals2  = [dict2.get(n, 0) or 0 for n in names]
                chart_config = _build_comparison_chart(names, vals1, vals2, period1, period2, metric, chart_title, chart_subtitle, ranking)

        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state, "status": "completed",
                "formattedText": formatted_text, "formattedItems": formatted_items,
                "chart_config": chart_config, "token_usage": token_usage,
                "token_totals": token_totals,
                "completedAt": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            })
        ctx.logger.info("🏁 Pipeline complete (comparison)!", {"queryId": query_id})
        return

    # ── NORMAL ────────────────────────────────────────────────────────────────
    results    = input_data.get("results", [])
    start_date = input_data.get("startDate", "")
    end_date   = input_data.get("endDate", "")

    if not results or (len(results) == 1 and results[0].get("value") is None):
        formatted_text  = "No data available for the selected period."
        formatted_items = []

    elif ranking == "aggregate":
        value = results[0].get("value", 0)
        formatted_text  = f"Total {metric} between {start_date} and {end_date} is {prefix}{value:,.2f}"
        formatted_items = [{"label": f"Total {metric}", "value": f"{prefix}{value:,.2f}"}]

    elif ranking == "threshold":
        # ── NEW: threshold display ────────────────────────────────────────────
        formatted_text = (
            f"{len(results)} {entity_label} matched your filter "
            f"between {start_date} and {end_date}:"
        )
        formatted_items = []
        labels, values = [], []
        for i, row in enumerate(results, start=1):
            name  = row.get("name", "Unknown")
            value = row.get("value", 0) or 0
            formatted_text += f"\n{i}. {name} — {prefix}{value:,.2f}"
            formatted_items.append({"rank": i, "name": name,
                                     "value": f"{prefix}{value:,.2f}", "raw_value": value})
            labels.append(name)
            values.append(value)
        chart_title    = f"{entity_label.title()} matching threshold filter"
        chart_subtitle = f"{start_date} to {end_date}"
        chart_config   = _build_bar_chart(labels, values, metric, chart_title, chart_subtitle)

    else:
        rank_label = "Top" if ranking == "top" else "Bottom"
        formatted_text = (f"{rank_label} {top_n} {entity_label} by {metric} "
                          f"between {start_date} and {end_date}:")
        formatted_items = []
        labels, values = [], []
        for i, row in enumerate(results, start=1):
            name  = row.get("name", "Unknown")
            value = row.get("value", 0) or 0
            formatted_text += f"\n{i}. {name} — {prefix}{value:,.2f}"
            formatted_items.append({"rank": i, "name": name,
                                     "value": f"{prefix}{value:,.2f}", "raw_value": value})
            labels.append(name)
            values.append(value)
        chart_title    = f"{rank_label} {top_n} {entity_label} by {metric}"
        chart_subtitle = f"{start_date} to {end_date}"
        chart_config   = _build_bar_chart(labels, values, metric, chart_title, chart_subtitle)

    formatted_text += _token_summary_text(token_usage, token_totals)

    ctx.logger.info("✅ Result formatted", {"queryId": query_id})

    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state, "status": "completed",
            "formattedText": formatted_text, "formattedItems": formatted_items,
            "chart_config": chart_config, "token_usage": token_usage,
            "token_totals": token_totals,
            "completedAt": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        })

    ctx.logger.info("🏁 Pipeline complete!", {"queryId": query_id})
    if chart_config:
        ctx.logger.info("📈 Chart config saved", {
            "queryId": query_id,
            "chart_type": chart_config.get("config", {}).get("type"),
            "chart_title": chart_config.get("title"),
        })