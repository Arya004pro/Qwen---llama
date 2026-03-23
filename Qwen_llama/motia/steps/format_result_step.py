"""Step 6: Format Result — formats results + chart config + token summary.

Handles all query modes:
  normal         top / bottom / aggregate
  threshold      absolute and percentage
  zero_filter    entities with no sales
  comparison     side-by-side two periods
  intersection   entities in BOTH periods
  top_growth     entities ranked by growth delta
"""

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "FormatResult",
    "description": (
        "Formats raw SQL results into user-facing text and Chart.js config "
        "for all query modes including growth ranking and zero-filter."
    ),
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

_CHART_BASE = {
    "responsive": True,
    "maintainAspectRatio": True,
    "plugins": {
        "legend": {"display": False},
        "tooltip": {
            "backgroundColor": "#1e2130", "titleColor": "#e2e8f0",
            "bodyColor": "#94a3b8", "borderColor": "#2d3148", "borderWidth": 1,
        },
    },
    "scales": {
        "x": {"ticks": {"color": "#94a3b8", "font": {"size": 11}},
              "grid":  {"color": "rgba(255,255,255,0.05)"}},
        "y": {"ticks": {"color": "#94a3b8", "font": {"size": 11}},
              "grid":  {"color": "rgba(255,255,255,0.05)"}, "beginAtZero": True},
    },
}


# ── chart builders ────────────────────────────────────────────────────────────

def _bar_chart(labels, values, metric, title, subtitle, horizontal=True):
    prefix  = "₹" if metric == "revenue" else ""
    colors  = [_PALETTE[i % len(_PALETTE)]         for i in range(len(labels))]
    borders = [_BORDER_PALETTE[i % len(_PALETTE)]  for i in range(len(labels))]
    opts    = {
        **_CHART_BASE,
        **({"indexAxis": "y"} if horizontal else {}),
        "plugins": {
            **_CHART_BASE["plugins"],
            "tooltip": {
                **_CHART_BASE["plugins"]["tooltip"],
                "callbacks": {"label": f"function(c){{return ' {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"},
            },
        },
        "scales": {
            "x": {**_CHART_BASE["scales"]["x"],
                  "ticks": {**_CHART_BASE["scales"]["x"]["ticks"],
                            "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}},
            "y": _CHART_BASE["scales"]["y"],
        },
    }
    return {
        "title": title, "subtitle": subtitle,
        "config": {
            "type": "bar",
            "data": {"labels": labels,
                     "datasets": [{"label": metric.title(), "data": values,
                                   "backgroundColor": colors, "borderColor": borders,
                                   "borderWidth": 1, "borderRadius": 4}]},
            "options": opts,
        },
    }


def _comparison_chart(labels, v1, v2, p1, p2, metric, title, subtitle, ranking):
    prefix = "₹" if metric == "revenue" else ""
    if ranking == "aggregate":
        cfg = {
            "type": "bar",
            "data": {"labels": [p1, p2],
                     "datasets": [{"label": metric.title(),
                                   "data": [v1[0] if v1 else 0, v2[0] if v2 else 0],
                                   "backgroundColor": [_PALETTE[0], _PALETTE[1]],
                                   "borderColor": [_BORDER_PALETTE[0], _BORDER_PALETTE[1]],
                                   "borderWidth": 1, "borderRadius": 4}]},
            "options": {
                **_CHART_BASE,
                "plugins": {**_CHART_BASE["plugins"],
                            "tooltip": {**_CHART_BASE["plugins"]["tooltip"],
                                        "callbacks": {"label": f"function(c){{return ' {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"}}},
                "scales": {"x": _CHART_BASE["scales"]["x"],
                           "y": {**_CHART_BASE["scales"]["y"],
                                 "ticks": {**_CHART_BASE["scales"]["y"]["ticks"],
                                           "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}}},
            },
        }
    else:
        cfg = {
            "type": "bar",
            "data": {"labels": labels,
                     "datasets": [
                         {"label": p1, "data": v1, "backgroundColor": _PALETTE[0],
                          "borderColor": _BORDER_PALETTE[0], "borderWidth": 1, "borderRadius": 3},
                         {"label": p2, "data": v2, "backgroundColor": _PALETTE[1],
                          "borderColor": _BORDER_PALETTE[1], "borderWidth": 1, "borderRadius": 3},
                     ]},
            "options": {
                **_CHART_BASE, "indexAxis": "y",
                "plugins": {**_CHART_BASE["plugins"],
                            "legend": {"display": True, "labels": {"color": "#94a3b8"}},
                            "tooltip": {**_CHART_BASE["plugins"]["tooltip"],
                                        "callbacks": {"label": f"function(c){{return ' '+c.dataset.label+': {prefix}'+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}})}}"}}},
                "scales": {"x": {**_CHART_BASE["scales"]["x"],
                                 "ticks": {**_CHART_BASE["scales"]["x"]["ticks"],
                                           "callback": f"function(v){{return '{prefix}'+v.toLocaleString('en-IN')}}"}},
                           "y": _CHART_BASE["scales"]["y"]},
            },
        }
    return {"title": title, "subtitle": subtitle, "config": cfg}


# ── text helpers ──────────────────────────────────────────────────────────────

def _fmt(value, prefix):
    return f"{prefix}{value:,.2f}" if value is not None else "N/A"


def _delta_str(v1, v2, prefix):
    if v1 is None or v2 is None:
        return "N/A"
    delta = v2 - v1
    sign  = "+" if delta >= 0 else ""
    pct   = (delta / v1 * 100) if v1 != 0 else float("inf")
    return f"{sign}{prefix}{abs(delta):,.2f}  ({sign}{pct:.1f}%)"


def _format_comparison(parsed, r1, r2, p1, p2, ranking, top_n):
    entity  = parsed.get("entity", "")
    metric  = parsed.get("metric", "")
    prefix  = "₹" if metric == "revenue" else ""
    label   = ENTITY_LABELS.get(entity, entity)

    if ranking == "aggregate":
        v1 = r1[0]["value"] if r1 else None
        v2 = r2[0]["value"] if r2 else None
        text = (f"📊 {metric.title()} Comparison: {p1} vs {p2}\n"
                f"{'─'*48}\n"
                f"  {p1:<22} {_fmt(v1, prefix)}\n"
                f"  {p2:<22} {_fmt(v2, prefix)}\n"
                f"  Δ Change             {_delta_str(v1, v2, prefix)}")
        items = [{"period": p1, "value": _fmt(v1, prefix), "raw_value": v1},
                 {"period": p2, "value": _fmt(v2, prefix), "raw_value": v2},
                 {"delta": _delta_str(v1, v2, prefix)}]
        return text, items

    rl = "Top" if ranking == "top" else "Bottom"
    d1 = {r["name"]: r["value"] for r in r1}
    d2 = {r["name"]: r["value"] for r in r2}
    names   = list(dict.fromkeys([r["name"] for r in r1] + [r["name"] for r in r2]))
    col_w   = max(max((len(n) for n in names), default=20), 20)
    hdr     = f"📊 {rl} {top_n} {label} by {metric}: {p1} vs {p2}"
    sep     = "─" * (col_w + 44)
    hdr_row = f"  {'#':>3}  {'Name':<{col_w}}  {p1:>16}  {p2:>16}  {'Δ Change':>14}"
    rows_t, items = [], []
    for i, name in enumerate(names, 1):
        vv1, vv2 = d1.get(name), d2.get(name)
        d = _delta_str(vv1, vv2, prefix)
        rows_t.append(f"  {i:>3}. {name:<{col_w}}  {_fmt(vv1,prefix):>16}  {_fmt(vv2,prefix):>16}  {d:>14}")
        items.append({"rank": i, "name": name,
                      "period1_value": _fmt(vv1, prefix),
                      "period2_value": _fmt(vv2, prefix), "delta": d})
    return "\n".join([hdr, sep, hdr_row, sep] + rows_t), items


def _token_summary(token_usage, token_totals):
    if not token_usage and not token_totals:
        return ""
    lines = ["\n\n─── Token Usage ───────────────────────────────"]
    for e in token_usage:
        short = e.get("model", "").split("/")[-1]
        lines.append(f"  {e.get('step','?'):<20} {short:<28} "
                     f"prompt={e.get('prompt_tokens',0):>5}  "
                     f"completion={e.get('completion_tokens',0):>4}  "
                     f"total={e.get('total_tokens',0):>5}")
    if token_totals:
        lines.append("  " + "─"*70)
        lines.append(f"  {'TOTAL':<20} {'':28} "
                     f"prompt={token_totals.get('prompt_tokens',0):>5}  "
                     f"completion={token_totals.get('completion_tokens',0):>4}  "
                     f"total={token_totals.get('total_tokens',0):>5}")
    return "\n".join(lines)


# ── handler ───────────────────────────────────────────────────────────────────

async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    import datetime as _dt

    query_id         = input_data.get("queryId")
    user_query       = input_data.get("query", "")
    parsed           = input_data.get("parsed", {})
    ranking          = input_data.get("ranking", "top")
    top_n            = input_data.get("topN", 5)
    is_comparison    = input_data.get("is_comparison", False)
    is_intersection  = input_data.get("is_intersection", False)
    is_growth_ranking= input_data.get("is_growth_ranking", False)
    threshold_value  = input_data.get("threshold_value")
    threshold_type   = input_data.get("threshold_type")

    entity  = parsed.get("entity", "")
    metric  = parsed.get("metric", "")
    prefix  = "₹" if metric == "revenue" else ""
    label   = ENTITY_LABELS.get(entity, entity)

    ctx.logger.info("📝 Formatting results", {
        "queryId": query_id, "ranking": ranking,
        "is_comparison": is_comparison,
        "is_growth_ranking": is_growth_ranking,
        "is_intersection": is_intersection,
    })

    qs           = await ctx.state.get("queries", query_id)
    token_usage  = (qs or {}).get("token_usage", [])
    token_totals = (qs or {}).get("token_totals", {})
    chart_config = None

    # ── GROWTH RANKING ────────────────────────────────────────────────────────
    if is_growth_ranking:
        results = input_data.get("results_growth", [])
        p1      = input_data.get("period1_label", "Period 1")
        p2      = input_data.get("period2_label", "Period 2")
        direction = "highest" if not any(
            w in user_query.lower()
            for w in ["lowest", "worst", "smallest", "minimum", "least"]
        ) else "lowest"

        if not results:
            formatted_text  = f"No {label} with {metric} data found for comparison."
            formatted_items = []
        else:
            header = (
                f"📈 {label.title()} with {direction} {metric} growth "
                f"({p1} → {p2}):"
            )
            formatted_items = []
            names, deltas = [], []
            for i, row in enumerate(results, 1):
                name  = row.get("name", "Unknown")
                v1    = row.get("value1", 0.0)
                v2    = row.get("value2", 0.0)
                delta = row.get("delta", 0.0)
                sign  = "+" if delta >= 0 else ""
                pct   = (delta / v1 * 100) if v1 != 0 else float("inf")
                pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new entry"
                header += (
                    f"\n{i}. {name}"
                    f"\n   {p1}: {prefix}{v1:,.2f}"
                    f"\n   {p2}: {prefix}{v2:,.2f}"
                    f"\n   Growth: {sign}{prefix}{abs(delta):,.2f} ({pct_s})"
                )
                formatted_items.append({
                    "rank": i, "name": name,
                    "value1": f"{prefix}{v1:,.2f}",
                    "value2": f"{prefix}{v2:,.2f}",
                    "delta":  f"{sign}{prefix}{abs(delta):,.2f} ({pct_s})",
                    "raw_delta": delta,
                })
                names.append(name)
                deltas.append(delta)

            formatted_text = header
            chart_config   = _bar_chart(
                names, deltas, metric,
                f"{label.title()} by {metric} growth: {p1} vs {p2}",
                f"Delta in {metric} ({p1} → {p2})",
                horizontal=True,
            )

        formatted_text += _token_summary(token_usage, token_totals)
        if qs:
            await qs_update(ctx, query_id, qs, formatted_text, formatted_items,
                            chart_config, token_usage, token_totals)
        ctx.logger.info("🏁 Pipeline complete (growth ranking)!", {"queryId": query_id})
        return

    # ── INTERSECTION ──────────────────────────────────────────────────────────
    if is_intersection:
        results  = input_data.get("results", [])
        p1       = input_data.get("intersection_period1", "Period 1")
        p2       = input_data.get("intersection_period2", "Period 2")
        if not results:
            formatted_text  = (
                f"No {label} placed orders in BOTH {p1} AND {p2}."
            )
            formatted_items = []
        else:
            header = (
                f"🔀 {label.title()} present in BOTH {p1} AND {p2} "
                f"(ranked by combined {metric}):"
            )
            formatted_items, names, values = [], [], []
            for i, row in enumerate(results, 1):
                name  = row.get("name", "Unknown")
                value = row.get("value", 0) or 0
                header += f"\n{i}. {name} — {prefix}{value:,.2f}"
                formatted_items.append({"rank": i, "name": name,
                                         "value": f"{prefix}{value:,.2f}",
                                         "raw_value": value})
                names.append(name)
                values.append(value)
            formatted_text = header
            chart_config   = _bar_chart(names, values, metric,
                                        f"Top {label} in both {p1} & {p2}",
                                        f"Combined {metric}", horizontal=True)
        formatted_text += _token_summary(token_usage, token_totals)
        if qs:
            await qs_update(ctx, query_id, qs, formatted_text, formatted_items,
                            chart_config, token_usage, token_totals)
        ctx.logger.info("🏁 Pipeline complete (intersection)!", {"queryId": query_id})
        return

    # ── COMPARISON ────────────────────────────────────────────────────────────
    if is_comparison:
        r1 = input_data.get("results_1", [])
        r2 = input_data.get("results_2", [])
        p1 = input_data.get("period1_label", "Period 1")
        p2 = input_data.get("period2_label", "Period 2")
        if not r1 and not r2:
            formatted_text, formatted_items = "No data available for the selected periods.", []
        else:
            formatted_text, formatted_items = _format_comparison(
                parsed, r1, r2, p1, p2, ranking, top_n)
        formatted_text += _token_summary(token_usage, token_totals)
        if r1 or r2:
            t = f"{metric.title()} comparison: {p1} vs {p2}"
            s = f"Query: {user_query}"
            if ranking == "aggregate":
                vv1 = [r1[0]["value"]] if r1 else [0]
                vv2 = [r2[0]["value"]] if r2 else [0]
                chart_config = _comparison_chart([], vv1, vv2, p1, p2, metric, t, s, "aggregate")
            else:
                d1 = {r["name"]: r["value"] for r in r1}
                d2 = {r["name"]: r["value"] for r in r2}
                names = list(dict.fromkeys([r["name"] for r in r1] + [r["name"] for r in r2]))
                chart_config = _comparison_chart(
                    names,
                    [d1.get(n, 0) or 0 for n in names],
                    [d2.get(n, 0) or 0 for n in names],
                    p1, p2, metric, t, s, ranking,
                )
        if qs:
            await qs_update(ctx, query_id, qs, formatted_text, formatted_items,
                            chart_config, token_usage, token_totals)
        ctx.logger.info("🏁 Pipeline complete (comparison)!", {"queryId": query_id})
        return

    # ── NORMAL / THRESHOLD / ZERO_FILTER ──────────────────────────────────────
    results    = input_data.get("results", [])
    start_date = input_data.get("startDate", "")
    end_date   = input_data.get("endDate", "")

    if not results or (len(results) == 1 and results[0].get("value") is None):
        # Informative message for empty threshold results
        if ranking == "threshold" and threshold_value is not None:
            unit = "%" if threshold_type == "percentage" else (
                " units" if metric == "quantity" else ""
            )
            threshold_display = f"{threshold_value:.0f}{unit}"
            formatted_text = (
                f"No {label} matched the filter "
                f"({metric} > {threshold_display}) "
                f"between {start_date} and {end_date}."
            )
        elif ranking == "zero_filter":
            formatted_text = (
                f"No {label} with zero {metric} found "
                f"between {start_date} and {end_date}. "
                f"All {label} had activity in this period."
            )
        else:
            formatted_text = "No data available for the selected period."
        formatted_items = []

    elif ranking == "aggregate":
        value = results[0].get("value", 0)
        formatted_text  = (
            f"Total {metric} between {start_date} and {end_date} "
            f"is {prefix}{value:,.2f}"
        )
        formatted_items = [{"label": f"Total {metric}",
                             "value": f"{prefix}{value:,.2f}"}]

    elif ranking == "zero_filter":
        formatted_text = (
            f"{len(results)} {label} had zero {metric} "
            f"between {start_date} and {end_date}:"
        )
        formatted_items = []
        for i, row in enumerate(results, 1):
            name = row.get("name", "Unknown")
            formatted_text  += f"\n{i}. {name}"
            formatted_items.append({"rank": i, "name": name, "value": "0"})
        # No chart for zero-filter (no meaningful values to plot)

    elif ranking == "threshold":
        unit = "%" if threshold_type == "percentage" else (
            " units" if metric == "quantity" else ""
        )
        threshold_display = (
            f"{threshold_value:.0f}{unit}" if threshold_value is not None else "threshold"
        )
        formatted_text = (
            f"{len(results)} {label} where {metric} exceeded "
            f"{threshold_display} between {start_date} and {end_date}:"
        )
        formatted_items, names, values = [], [], []
        for i, row in enumerate(results, 1):
            name  = row.get("name", "Unknown")
            value = row.get("value", 0) or 0
            formatted_text  += f"\n{i}. {name} — {prefix}{value:,.2f}"
            formatted_items.append({"rank": i, "name": name,
                                     "value": f"{prefix}{value:,.2f}",
                                     "raw_value": value})
            names.append(name)
            values.append(value)
        chart_config = _bar_chart(
            names, values, metric,
            f"{label.title()} exceeding {threshold_display}",
            f"{start_date} to {end_date}", horizontal=True,
        )

    else:
        rl = "Top" if ranking == "top" else "Bottom"
        formatted_text = (
            f"{rl} {top_n} {label} by {metric} "
            f"between {start_date} and {end_date}:"
        )
        formatted_items, names, values = [], [], []
        for i, row in enumerate(results, 1):
            name  = row.get("name", "Unknown")
            value = row.get("value", 0) or 0
            formatted_text  += f"\n{i}. {name} — {prefix}{value:,.2f}"
            formatted_items.append({"rank": i, "name": name,
                                     "value": f"{prefix}{value:,.2f}",
                                     "raw_value": value})
            names.append(name)
            values.append(value)
        chart_config = _bar_chart(
            names, values, metric,
            f"{rl} {top_n} {label} by {metric}",
            f"{start_date} to {end_date}", horizontal=True,
        )

    formatted_text += _token_summary(token_usage, token_totals)
    ctx.logger.info("✅ Result formatted", {"queryId": query_id})
    if qs:
        await qs_update(ctx, query_id, qs, formatted_text, formatted_items,
                        chart_config, token_usage, token_totals)
    ctx.logger.info("🏁 Pipeline complete!", {"queryId": query_id})


async def qs_update(ctx, query_id, qs, text, items, chart, usage, totals):
    import datetime as _dt
    await ctx.state.set("queries", query_id, {
        **qs, "status": "completed",
        "formattedText": text, "formattedItems": items,
        "chart_config": chart, "token_usage": usage,
        "token_totals": totals,
        "completedAt": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    })