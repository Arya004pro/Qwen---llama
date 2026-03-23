"""Step 6: Format Result — formats any result shape into text + chart.

Detects result shape from the data itself (no query-type switch needed):
  1 col  → aggregate scalar
  2 cols (name, value) → ranked / threshold / zero_filter
  3 cols (name, value1, value2) → comparison
  4 cols (name, value1, value2, delta) → growth_ranking
"""

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "FormatResult",
    "description": "Formats any result shape into user-facing text and Chart.js config.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::format.result")],
    "enqueues": [],
}

ENTITY_LABELS = {
    "product": "products", "customer": "customers",
    "city": "cities",      "category": "categories",
    "state": "states",
}

_PALETTE = [
    "rgba(99,179,237,0.85)",  "rgba(104,211,145,0.85)",
    "rgba(246,173,85,0.85)",  "rgba(252,129,129,0.85)",
    "rgba(154,117,221,0.85)", "rgba(79,209,197,0.85)",
    "rgba(246,135,179,0.85)", "rgba(183,148,255,0.85)",
]
_BORDERS = [c.replace("0.85","1") for c in _PALETTE]

_BASE = {
    "responsive": True, "maintainAspectRatio": True,
    "plugins": {
        "legend": {"display": False},
        "tooltip": {"backgroundColor":"#1e2130","titleColor":"#e2e8f0",
                    "bodyColor":"#94a3b8","borderColor":"#2d3148","borderWidth":1},
    },
    "scales": {
        "x": {"ticks":{"color":"#94a3b8","font":{"size":11}},
              "grid":{"color":"rgba(255,255,255,0.05)"}},
        "y": {"ticks":{"color":"#94a3b8","font":{"size":11}},
              "grid":{"color":"rgba(255,255,255,0.05)"},"beginAtZero":True},
    },
}


def _fmt(v, p): return f"{p}{v:,.2f}" if v is not None else "N/A"

def _delta_str(v1, v2, p):
    if v1 is None or v2 is None: return "N/A"
    d = v2 - v1; sign = "+" if d >= 0 else ""
    pct = (d/v1*100) if v1 != 0 else float("inf")
    pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new entry"
    return f"{sign}{p}{abs(d):,.2f}  ({pct_s})"

def _bar(labels, values, metric, title, subtitle):
    p = "₹" if metric == "revenue" else ""
    cfg = {
        "type":"bar",
        "data":{"labels":labels,"datasets":[{
            "label":metric.title(),"data":values,
            "backgroundColor":[_PALETTE[i%len(_PALETTE)] for i in range(len(labels))],
            "borderColor":[_BORDERS[i%len(_PALETTE)] for i in range(len(labels))],
            "borderWidth":1,"borderRadius":4,
        }]},
        "options":{**_BASE,"indexAxis":"y",
            "plugins":{**_BASE["plugins"],"tooltip":{**_BASE["plugins"]["tooltip"],
                "callbacks":{"label":f"@@FUNCTION@@function(c){{let v=c.raw;return ' {p}'+(typeof v==='number'?v.toLocaleString('en-IN',{{minimumFractionDigits:2}}):v);}}@@ENDFUNCTION@@"},
            }},
            "scales":{"x":{**_BASE["scales"]["x"],"ticks":{**_BASE["scales"]["x"]["ticks"],
                "callback":f"@@FUNCTION@@function(v){{return '{p}'+(typeof v==='number'?v.toLocaleString('en-IN'):v);}}@@ENDFUNCTION@@"}},
                "y":_BASE["scales"]["y"]},
        },
    }
    return {"title":title,"subtitle":subtitle,"config":cfg}

def _token_summary(usage, totals):
    if not usage and not totals: return ""
    lines = ["\n\n─── Token Usage ───────────────────────────────"]
    for e in usage:
        short = e.get("model","").split("/")[-1]
        lines.append(f"  {e.get('step','?'):<20} {short:<28} "
                     f"prompt={e.get('prompt_tokens',0):>5}  "
                     f"completion={e.get('completion_tokens',0):>4}  "
                     f"total={e.get('total_tokens',0):>5}")
    if totals:
        lines.append("  "+"─"*70)
        lines.append(f"  {'TOTAL':<20} {'':28} "
                     f"prompt={totals.get('prompt_tokens',0):>5}  "
                     f"completion={totals.get('completion_tokens',0):>4}  "
                     f"total={totals.get('total_tokens',0):>5}")
    return "\n".join(lines)


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    import datetime as _dt

    query_id     = input_data.get("queryId")
    user_query   = input_data.get("query","")
    parsed       = input_data.get("parsed",{})
    results      = input_data.get("results",[])
    period_labels= input_data.get("period_labels",[])
    start_date   = input_data.get("startDate","")
    end_date     = input_data.get("endDate","")

    qt     = parsed.get("query_type","top_n")
    entity = parsed.get("entity","product")
    metric = parsed.get("metric","revenue")
    top_n  = parsed.get("top_n",5)
    thr    = parsed.get("threshold")
    money  = metric == "revenue"
    p      = "₹" if money else ""  # no currency symbol for quantity/order_count
    elabel = ENTITY_LABELS.get(entity, entity)

    qs           = await ctx.state.get("queries", query_id)
    token_usage  = (qs or {}).get("token_usage", [])
    token_totals = (qs or {}).get("token_totals", {})
    chart_config = None

    ctx.logger.info("📝 Formatting", {"queryId":query_id,"query_type":qt,"rows":len(results)})

    # ── detect result shape ────────────────────────────────────────────────────
    has_delta  = results and "delta"  in results[0]
    has_value2 = results and "value2" in results[0] and not has_delta
    is_scalar  = results and len(results) == 1 and "name" not in results[0]
    is_empty   = not results or (is_scalar and results[0].get("value") is None)

    # ── EMPTY ──────────────────────────────────────────────────────────────────
    if is_empty:
        if qt == "zero_filter":
            text = (f"No {elabel} with zero {metric} found "
                    f"between {start_date} and {end_date}. "
                    f"All {elabel} had activity in this period.")
        elif qt == "threshold" and thr:
            unit = "%" if thr.get("type")=="percentage" else (f" {metric}")
            text = (f"No {elabel} matched the filter "
                    f"({metric} > {thr['value']}{unit}) "
                    f"between {start_date} and {end_date}.")
        elif period_labels and len(period_labels) >= 2:
            text = (f"No data found for {period_labels[1]}. "
                    f"The dataset may not cover this time range.")
        else:
            text = "No data available for the selected period."
        formatted_text = text + _token_summary(token_usage, token_totals)
        items = []

    # ── AGGREGATE scalar ────────────────────────────────────────────────────────
    elif is_scalar:
        v    = results[0]["value"]
        text = f"Total {metric} between {start_date} and {end_date} is {p}{v:,.2f}"
        formatted_text = text + _token_summary(token_usage, token_totals)
        items = [{"label": f"Total {metric}", "value": f"{p}{v:,.2f}"}]

    # ── GROWTH RANKING (4 cols: name, value1, value2, delta) ────────────────────
    elif has_delta:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        direction = "highest" if qt != "bottom_n" else "lowest"
        header = f"📈 {elabel.title()} with {direction} {metric} growth ({p1} → {p2}):"
        items = []
        for i, row in enumerate(results, 1):
            name = row.get("name","?")
            v1, v2, d = row.get("value1",0), row.get("value2",0), row.get("delta",0)
            sign  = "+" if d >= 0 else ""
            pct   = (d/v1*100) if v1 != 0 else float("inf")
            pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new entry"
            header += (f"\n{i}. {name}"
                       f"\n   {p1}: {p}{v1:,.2f}"
                       f"\n   {p2}: {p}{v2:,.2f}"
                       f"\n   Growth: {sign}{p}{abs(d):,.2f} ({pct_s})")
            items.append({"rank":i,"name":name,"delta":d})
        formatted_text = header + _token_summary(token_usage, token_totals)
        names   = [r.get("name","?") for r in results]
        deltas  = [r.get("delta",0)  for r in results]
        chart_config = _bar(names, deltas, metric,
                            f"{elabel.title()} by {metric} growth: {p1}→{p2}",
                            f"Delta in {metric}")

    # ── COMPARISON (3 cols: name, value1, value2) ────────────────────────────────
    elif has_value2:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        header = f"📊 Top {top_n} {elabel} by {metric}: {p1} vs {p2}"
        col_w  = max((len(r.get("name","")) for r in results), default=20)
        col_w  = max(col_w, 20)
        sep    = "─"*(col_w+44)
        hdr    = f"  {'#':>3}  {'Name':<{col_w}}  {p1:>16}  {p2:>16}  {'Δ Change':>14}"
        lines  = [header, sep, hdr, sep]
        items  = []
        for i, row in enumerate(results, 1):
            name = row.get("name","?")
            v1, v2 = row.get("value1"), row.get("value2")
            d = _delta_str(v1, v2, p)
            lines.append(f"  {i:>3}. {name:<{col_w}}  {_fmt(v1,p):>16}  {_fmt(v2,p):>16}  {d:>14}")
            items.append({"rank":i,"name":name,f"{p1}_value":_fmt(v1,p),f"{p2}_value":_fmt(v2,p),"delta":d})
        formatted_text = "\n".join(lines) + _token_summary(token_usage, token_totals)
        # grouped bar chart
        names  = [r.get("name","?") for r in results]
        vals1  = [r.get("value1",0) or 0 for r in results]
        vals2  = [r.get("value2",0) or 0 for r in results]
        cfg = {
            "type":"bar","data":{"labels":names,"datasets":[
                {"label":p1,"data":vals1,"backgroundColor":_PALETTE[0],"borderColor":_BORDERS[0],"borderWidth":1,"borderRadius":3},
                {"label":p2,"data":vals2,"backgroundColor":_PALETTE[1],"borderColor":_BORDERS[1],"borderWidth":1,"borderRadius":3},
            ]},
            "options":{**_BASE,"indexAxis":"y",
                "plugins":{**_BASE["plugins"],"legend":{"display":True,"labels":{"color":"#94a3b8"}},
                    "tooltip":{**_BASE["plugins"]["tooltip"],"callbacks":{"label":f"@@FUNCTION@@function(c){{let v=c.raw;return ' '+c.dataset.label+': {p}'+(typeof v==='number'?v.toLocaleString('en-IN',{{minimumFractionDigits:2}}):v);}}@@ENDFUNCTION@@"}}},
                "scales":{"x":{**_BASE["scales"]["x"],"ticks":{**_BASE["scales"]["x"]["ticks"],
                    "callback":f"@@FUNCTION@@function(v){{return '{p}'+(typeof v==='number'?v.toLocaleString('en-IN'):v);}}@@ENDFUNCTION@@"}},
                          "y":_BASE["scales"]["y"]},
            },
        }
        chart_config = {"title":f"{metric.title()} comparison: {p1} vs {p2}","subtitle":user_query,"config":cfg}

    # ── RANKED / THRESHOLD / INTERSECTION / ZERO_FILTER (2 cols: name, value) ──
    else:
        names, values = [], []
        if qt == "zero_filter":
            header = f"{len(results)} {elabel} had zero {metric} between {start_date} and {end_date}:"
            items  = []
            for i, row in enumerate(results, 1):
                name = row.get("name","?")
                header += f"\n{i}. {name}"
                items.append({"rank":i,"name":name,"value":"0"})
        else:
            # threshold, intersection, top_n, bottom_n
            if qt == "threshold" and thr:
                unit = "%" if thr.get("type")=="percentage" else (f" {metric}")
                header = (f"{len(results)} {elabel} where {metric} exceeded "
                          f"{thr['value']:.0f}{unit} between {start_date} and {end_date}:")
            elif qt == "intersection":
                p1 = period_labels[0] if len(period_labels)>0 else "Period 1"
                p2 = period_labels[1] if len(period_labels)>1 else "Period 2"
                header = f"🔀 {elabel.title()} present in BOTH {p1} AND {p2} (combined {metric}):"
            else:
                rl = "Top" if qt == "top_n" else "Bottom"
                header = f"{rl} {top_n} {elabel} by {metric} between {start_date} and {end_date}:"
            items = []
            for i, row in enumerate(results, 1):
                name  = row.get("name","?")
                value = row.get("value",0) or 0
                header += f"\n{i}. {name} — {p}{value:,.2f}"
                items.append({"rank":i,"name":name,"value":f"{p}{value:,.2f}","raw_value":value})
                names.append(name); values.append(value)

        formatted_text = header + _token_summary(token_usage, token_totals)
        if names:
            rl = "Top" if qt not in ("bottom_n",) else "Bottom"
            chart_config = _bar(names, values, metric,
                                f"{rl} {elabel} by {metric}",
                                f"{start_date} to {end_date}")

    ctx.logger.info("✅ Formatted", {"queryId": query_id})
    if qs:
        await ctx.state.set("queries", query_id, {
            **qs, "status": "completed",
            "formattedText": formatted_text, "formattedItems": items,
            "chart_config":  chart_config,
            "token_usage":   token_usage, "token_totals": token_totals,
            "completedAt":   _dt.datetime.now(_dt.timezone.utc).isoformat(),
        })
    ctx.logger.info("🏁 Pipeline complete!", {"queryId": query_id})