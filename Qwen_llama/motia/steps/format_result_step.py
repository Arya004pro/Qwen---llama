"""Step 6: Format Result — formats any result shape into text + chart.

Changes vs original:
  - Added time_series result handling:
      Renders a LINE CHART (Chart.js type: 'line') for trend data.
      Month labels like "2024-01" are converted to "Jan 2024" for readability.
      Text output shows the trend table with all time buckets.
  - All other formatting (comparison, ranked, aggregate, etc.) unchanged.
"""

from typing import Any
from motia import FlowContext, queue

config = {
    "name": "FormatResult",
    "description": (
        "Formats any result shape into user-facing text and Chart.js config. "
        "Time-series queries render as line charts with human-readable bucket labels."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::format.result")],
    "enqueues": [],
}

ENTITY_LABELS: dict[str, str] = {
    "product_name": "products", "customer_name": "customers",
    "city_name": "cities",      "category_name": "categories",
    "state_name": "states",     "product": "products",
    "customer": "customers",    "city": "cities",
    "category": "categories",   "state": "states",
    "driver_name": "drivers",   "pickup_city": "pickup cities",
    "drop_city": "drop cities", "vehicle_type": "vehicle types",
    "vehicle_model": "vehicle models", "payment_method": "payment methods",
    "ride_type": "ride types",
}

_PALETTE = [
    "rgba(99,179,237,0.85)",  "rgba(104,211,145,0.85)",
    "rgba(246,173,85,0.85)",  "rgba(252,129,129,0.85)",
    "rgba(154,117,221,0.85)", "rgba(79,209,197,0.85)",
    "rgba(246,135,179,0.85)", "rgba(183,148,255,0.85)",
]
_BORDERS = [c.replace("0.85", "1") for c in _PALETTE]

# Month abbreviations for label formatting
_MONTH_ABBR = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
}


def _format_bucket_label(raw_label: str, bucket: str) -> str:
    """Convert raw bucket label (e.g. '2024-01') to a human-readable string."""
    if bucket == "month" and len(raw_label) == 7 and "-" in raw_label:
        year, month = raw_label.split("-", 1)
        abbr = _MONTH_ABBR.get(month, month)
        return f"{abbr} {year}"
    if bucket == "quarter" and "Q" in raw_label:
        # e.g. "2024-Q1" stays as is — already readable
        return raw_label
    if bucket == "week":
        return raw_label.replace("-W", " W")
    return raw_label


def _infer_currency(metric: str, hint: str = "") -> str:
    m = (metric or "").lower()
    h = (hint or "").lower()
    if any(x in m or x in h for x in ["usd", "dollar", "$"]):
        return "$"
    if any(x in m for x in ["fare", "earnings", "commission", "revenue",
                              "amount", "price", "total", "salary", "sales", "profit"]):
        return "₹"
    if any(x in m for x in ["count", "quantity", "units", "distance", "duration", "rides"]):
        return ""
    return "₹"


def _metric_label(metric: str, currency: str) -> str:
    mapping = {
        "total_fare": "Total Fare", "driver_earnings": "Driver Earnings",
        "platform_commission": "Platform Commission", "revenue": "Revenue",
        "total_amount": "Total Amount", "quantity": "Quantity Sold",
        "order_count": "Order Count", "count": "Count",
        "avg_final_price": "Avg Order Value",
        "avg_total_fare": "Avg Fare",
        "avg_driver_earnings": "Avg Driver Earnings",
        "avg_unit_price": "Avg Unit Price",
        "final_price": "Revenue (Final Price)",
        "distance_km": "Distance (km)", "duration_min": "Duration (min)",
    }
    label = mapping.get(metric, metric.replace("_", " ").title())
    if currency:
        return f"{label} ({currency})"
    return label


def _entity_label(entity: str) -> str:
    if not entity:
        return ""
    if entity in ENTITY_LABELS:
        return ENTITY_LABELS[entity]
    base = entity
    for suffix in ("_name", "_id", "_type", "_code"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    words = base.replace("_", " ").strip()
    if words and not words.endswith("s"):
        words += "s"
    return words


def _tick_fn(metric: str, currency: str) -> str:
    pfx = currency.replace("₹", "\\u20b9").replace("$", "\\u0024")
    if currency in ("₹", "$"):
        return (
            f"function(v){{"
            f"if(typeof v!=='number')return v;"
            f"var a=Math.abs(v);"
            f"if(a>=10000000)return '{pfx}'+(v/10000000).toLocaleString('en-IN',{{maximumFractionDigits:2}})+'Cr';"
            f"if(a>=100000)return '{pfx}'+(v/100000).toLocaleString('en-IN',{{maximumFractionDigits:2}})+'L';"
            f"if(a>=1000)return '{pfx}'+(v/1000).toLocaleString('en-IN',{{maximumFractionDigits:1}})+'K';"
            f"return '{pfx}'+v.toLocaleString('en-IN');"
            f"}}"
        )
    else:
        return (
            "function(v){"
            "if(typeof v!=='number')return v;"
            "var a=Math.abs(v);"
            "if(a>=1000000)return (v/1000000).toLocaleString('en-IN',{maximumFractionDigits:2})+'M';"
            "if(a>=1000)return (v/1000).toLocaleString('en-IN',{maximumFractionDigits:1})+'K';"
            "return v.toLocaleString('en-IN');"
            "}"
        )


def _tooltip_fn(metric: str, currency: str) -> str:
    pfx = currency.replace("₹", "\\u20b9").replace("$", "\\u0024")
    if currency:
        return (
            f"function(c){{"
            f"var v=c.raw;"
            f"var s=typeof v==='number'"
            f"?v.toLocaleString('en-IN',{{minimumFractionDigits:2,maximumFractionDigits:2}})"
            f":String(v);"
            f"return ' {pfx}'+s;"
            f"}}"
        )
    else:
        return (
            "function(c){"
            "var v=c.raw;"
            "var s=typeof v==='number'"
            "?v.toLocaleString('en-IN',{maximumFractionDigits:0})"
            ":String(v);"
            "return ' '+s;"
            "}"
        )


def _cmp_tooltip_fn(metric: str, currency: str) -> str:
    dec = "{minimumFractionDigits:2,maximumFractionDigits:2}" if currency else "{maximumFractionDigits:0}"
    pfx = currency.replace("₹", "\\u20b9").replace("$", "\\u0024")
    return (
        f"function(c){{"
        f"var v=c.raw;"
        f"var s=typeof v==='number'?v.toLocaleString('en-IN',{dec}):String(v);"
        f"return ' '+c.dataset.label+': {pfx}'+s;"
        f"}}"
    )


def _make_base(metric: str, currency: str, entity: str,
               legend: bool = False, index_axis: str = "y") -> dict:
    metric_lbl = _metric_label(metric, currency)
    entity_lbl = _entity_label(entity)

    if index_axis == "y":
        x_title = metric_lbl
        y_title = entity_lbl
        x_tick  = _tick_fn(metric, currency)
        y_tick  = None
    else:
        x_title = entity_lbl
        y_title = metric_lbl
        x_tick  = None
        y_tick  = _tick_fn(metric, currency)

    x_ticks: dict = {"color": "#94a3b8", "font": {"size": 11}}
    if x_tick:
        x_ticks["callback"] = x_tick
    y_ticks: dict = {"color": "#94a3b8", "font": {"size": 11}}
    if y_tick:
        y_ticks["callback"] = y_tick

    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "indexAxis": index_axis,
        "interaction": {"mode": "nearest", "intersect": True},
        "plugins": {
            "legend": {
                "display": legend,
                "labels": {"color": "#94a3b8", "font": {"size": 12}},
            },
            "tooltip": {
                "enabled": True,
                "backgroundColor": "#1e2130",
                "titleColor": "#e2e8f0",
                "bodyColor": "#94a3b8",
                "borderColor": "#2d3148",
                "borderWidth": 1,
            },
        },
        "scales": {
            "x": {
                "title": {"display": bool(x_title), "text": x_title,
                          "color": "#94a3b8", "font": {"size": 11, "weight": "normal"}},
                "ticks": x_ticks,
                "grid":  {"color": "rgba(255,255,255,0.05)"},
            },
            "y": {
                "title": {"display": bool(y_title), "text": y_title,
                          "color": "#94a3b8", "font": {"size": 11, "weight": "normal"}},
                "ticks":       y_ticks,
                "grid":        {"color": "rgba(255,255,255,0.05)"},
                "beginAtZero": True,
            },
        },
    }


def _make_line_chart(labels: list, values: list, metric: str,
                     currency: str, title: str, subtitle: str,
                     bucket: str) -> dict:
    """Build a Chart.js line chart config for time_series results."""
    mlabel = _metric_label(metric, currency)
    tick_fn = _tick_fn(metric, currency)
    tip_fn  = _tooltip_fn(metric, currency)

    # x-axis label formatting: show rotated labels for month/day
    max_rotation = 45 if len(labels) > 6 else 0

    cfg = {
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": mlabel,
                "data":  values,
                "borderColor":           "rgba(99,179,237,1)",
                "backgroundColor":       "rgba(99,179,237,0.12)",
                "pointBackgroundColor":  "rgba(99,179,237,1)",
                "pointBorderColor":      "#1a1d27",
                "pointRadius":           4,
                "pointHoverRadius":      6,
                "borderWidth":           2,
                "fill":                  True,
                "tension":               0.35,
            }],
        },
        "options": {
            "responsive":         True,
            "maintainAspectRatio": True,
            "interaction": {"mode": "index", "intersect": False},
            "plugins": {
                "legend": {"display": False},
                "tooltip": {
                    "enabled":         True,
                    "backgroundColor": "#1e2130",
                    "titleColor":      "#e2e8f0",
                    "bodyColor":       "#94a3b8",
                    "borderColor":     "#2d3148",
                    "borderWidth":     1,
                    "callbacks":       {"label": tip_fn},
                },
            },
            "scales": {
                "x": {
                    "title": {"display": True, "text": bucket.capitalize(),
                              "color": "#94a3b8", "font": {"size": 11}},
                    "ticks": {
                        "color":       "#94a3b8",
                        "font":        {"size": 11},
                        "maxRotation": max_rotation,
                        "minRotation": max_rotation,
                    },
                    "grid": {"color": "rgba(255,255,255,0.05)"},
                },
                "y": {
                    "title": {"display": True, "text": mlabel,
                              "color": "#94a3b8", "font": {"size": 11}},
                    "ticks":       {"color": "#94a3b8", "font": {"size": 11},
                                    "callback": tick_fn},
                    "grid":        {"color": "rgba(255,255,255,0.05)"},
                    "beginAtZero": False,
                },
            },
        },
    }
    return {"title": title, "subtitle": subtitle, "prefix": currency, "config": cfg}


def _bar(labels, values, metric, currency, entity, title, subtitle):
    base    = _make_base(metric, currency, entity, index_axis="y")
    tooltip = _tooltip_fn(metric, currency)
    base["plugins"]["tooltip"]["callbacks"] = {"label": tooltip}
    cfg = {
        "type": "bar",
        "data": {"labels": labels, "datasets": [{
            "label": _metric_label(metric, currency),
            "data": values,
            "backgroundColor": [_PALETTE[i % len(_PALETTE)] for i in range(len(labels))],
            "borderColor":     [_BORDERS[i % len(_PALETTE)] for i in range(len(labels))],
            "borderWidth": 1, "borderRadius": 4,
        }]},
        "options": base,
    }
    return {"title": title, "subtitle": subtitle, "prefix": currency, "config": cfg}


def _token_summary(usage, totals):
    if not usage and not totals:
        return ""
    lines = ["\n\n─── Token Usage ───────────────────────────────"]
    for e in usage:
        short = e.get("model", "").split("/")[-1]
        lines.append(
            f"  {e.get('step','?'):<20} {short:<28} "
            f"prompt={e.get('prompt_tokens',0):>5}  "
            f"completion={e.get('completion_tokens',0):>4}  "
            f"total={e.get('total_tokens',0):>5}"
        )
    if totals:
        lines.append("  " + "─" * 70)
        lines.append(
            f"  {'TOTAL':<20} {'':28} "
            f"prompt={totals.get('prompt_tokens',0):>5}  "
            f"completion={totals.get('completion_tokens',0):>4}  "
            f"total={totals.get('total_tokens',0):>5}"
        )
    return "\n".join(lines)


def _fmt_indian(v: Any, currency: str, decimals: int = 2) -> str:
    if v is None:
        return "—"
    try:
        val = float(v)
    except Exception:
        return str(v)
    
    sign = "-" if val < 0 else ""
    abs_val = abs(val)
    
    # Format with requested decimals
    s = f"{abs_val:.{decimals}f}"
    parts = s.split('.')
    integer_part = parts[0]
    decimal_part = parts[1] if len(parts) > 1 else ""
    
    # Indian grouping: last 3, then every 2
    res = ""
    if len(integer_part) > 3:
        res = "," + integer_part[-3:]
        remaining = integer_part[:-3]
        while len(remaining) > 2:
            res = "," + remaining[-2:] + res
            remaining = remaining[:-2]
        if remaining:
            res = remaining + res
    else:
        res = integer_part
        
    formatted = f"{sign}{currency}{res}"
    if decimals > 0:
        formatted += f".{decimal_part}"
    return formatted


def _fmt(v, currency):
    return _fmt_indian(v, currency)


def _delta_str(v1, v2, currency):
    if v1 is None or v2 is None:
        return "N/A"
    delta = v2 - v1
    sign  = "+" if delta >= 0 else ""
    pct   = (delta / v1 * 100) if v1 != 0 else float("inf")
    pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new"
    return f"{sign}{_fmt_indian(abs(delta), currency)} ({pct_s})"


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    import datetime as _dt

    query_id      = input_data.get("queryId")
    user_query    = input_data.get("query", "")
    parsed        = input_data.get("parsed", {})
    results       = input_data.get("results", [])
    period_labels = input_data.get("period_labels", [])
    start_date    = input_data.get("startDate", "")
    end_date      = input_data.get("endDate", "")

    qt     = parsed.get("query_type", "top_n")
    entity = parsed.get("entity", "")
    metric = parsed.get("metric", "value")
    top_n  = parsed.get("top_n", 5)
    thr    = parsed.get("threshold")
    bucket = parsed.get("time_bucket", "month")

    currency = _infer_currency(metric)
    p        = currency
    elabel   = _entity_label(entity) or entity or "items"
    mlabel   = _metric_label(metric, "")

    _trs = parsed.get("time_ranges", [])
    if _trs and _trs[0].get("label"):
        _period_phrase = f"in {_trs[0]['label']}"
    elif start_date and end_date:
        _period_phrase = f"between {start_date} and {end_date}"
    else:
        _period_phrase = ""

    qs           = await ctx.state.get("queries", query_id)
    if not user_query and qs:
        user_query = qs.get("query", "")
    token_usage  = (qs or {}).get("token_usage", [])
    token_totals = (qs or {}).get("token_totals", {})
    chart_config = None

    ctx.logger.info("📝 Formatting", {"queryId": query_id, "query_type": qt, "rows": len(results)})

    has_delta  = results and "delta"  in results[0]
    has_value2 = results and "value2" in results[0] and not has_delta
    is_scalar  = results and len(results) == 1 and "name" not in results[0]
    is_empty   = not results or (is_scalar and results[0].get("value") is None)

    # ── EMPTY ──────────────────────────────────────────────────────────────────
    if is_empty:
        if qt == "time_series":
            text = (f"No {mlabel} data found {_period_phrase}. "
                    "The dataset may not cover this time range.")
        elif qt == "zero_filter":
            text = (f"No {elabel} with zero {metric} found "
                    f"between {start_date} and {end_date}.")
        elif qt == "threshold" and thr:
            unit = "%" if thr.get("type") == "percentage" else f" {metric}"
            text = (f"No {elabel} matched the filter "
                    f"({metric} > {thr['value']}{unit}) "
                    f"between {start_date} and {end_date}.")
        elif period_labels and len(period_labels) >= 2:
            text = (f"No data found for {period_labels[1]}.")
        else:
            text = "No data available for the selected period."
        formatted_text = text + _token_summary(token_usage, token_totals)
        items = []

    # ── TIME SERIES (trend) ────────────────────────────────────────────────────
    elif qt == "time_series":
        period_str = _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
        _bucket_map = {
            "year": "Yearly", "month": "Monthly", "quarter": "Quarterly",
            "week": "Weekly", "day": "Daily",
        }
        bucket_label = _bucket_map.get(bucket, bucket.capitalize()) 
        header = f"📈 {bucket_label} {mlabel} trend {period_str}:\n"
        header += f"\n  {'Period':<14}  {'Value':>16}"
        header += f"\n  {'─'*14}  {'─'*16}"

        raw_labels = [r.get("name", "?") for r in results]
        values     = [r.get("value", 0) or 0 for r in results]
        # Human-readable labels
        labels     = [_format_bucket_label(lbl, bucket) for lbl in raw_labels]

        items = []
        for i, (lbl, val) in enumerate(zip(labels, values)):
            val_s = _fmt_indian(val, p)
            header += f"\n  {lbl:<14}  {val_s:>16}"
            items.append({"period": lbl, "value": val_s, "raw_value": val})

        # Add a summary row
        if values:
            total   = sum(values)
            average = total / len(values)
            header += f"\n  {'─'*14}  {'─'*16}"
            header += f"\n  {'Total':<14}  {_fmt_indian(total, p):>16}"
            header += f"\n  {'Average':<14}  {_fmt_indian(average, p):>16}"

        formatted_text = header + _token_summary(token_usage, token_totals)

        if labels and values:
            chart_config = _make_line_chart(
                labels, values, metric, currency,
                user_query or f"{bucket_label} {mlabel} trend",
                period_str,
                bucket,
            )

    # ── AGGREGATE scalar ────────────────────────────────────────────────────────
    elif is_scalar:
        v = results[0]["value"]
        period_str = _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
        val_s = _fmt_indian(v, p)
        formatted_text = f"Total {mlabel} {period_str} is {val_s}"
        items = [{"label": f"Total {mlabel}", "value": val_s}]

    # ── GROWTH RANKING ──────────────────────────────────────────────────────────
    elif has_delta:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        direction = "highest" if qt != "bottom_n" else "lowest"
        header = f"📈 {elabel.title()} with {direction} {metric} growth ({p1} → {p2}):"
        items = []
        for i, row in enumerate(results, 1):
            name = row.get("name", "?")
            v1, v2, d = row.get("value1", 0), row.get("value2", 0), row.get("delta", 0)
            sign  = "+" if d >= 0 else ""
            pct   = (d / v1 * 100) if v1 != 0 else float("inf")
            pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new entry"
            header += (f"\n{i}. {name}"
                       f"\n   {p1}: {_fmt_indian(v1, p)}"
                       f"\n   {p2}: {_fmt_indian(v2, p)}"
                       f"\n   Growth: {sign}{_fmt_indian(abs(d), p)} ({pct_s})")
            items.append({"rank": i, "name": name, "delta": d})
        formatted_text = header + _token_summary(token_usage, token_totals)
        names  = [r.get("name", "?") for r in results]
        deltas = [r.get("delta", 0)   for r in results]
        chart_config = _bar(
            names, deltas, metric, currency, entity,
            user_query or f"{elabel.title()} by {metric} growth: {p1}→{p2}",
            f"Delta in {metric} ({p1} → {p2})",
        )

    # ── COMPARISON ──────────────────────────────────────────────────────────────
    elif has_value2:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        header = f"📊 Top {top_n} {elabel} by {metric}: {p1} vs {p2}"
        col_w  = max((len(r.get("name", "")) for r in results), default=20)
        col_w  = max(col_w, 20)
        sep    = "─" * (col_w + 44)
        hdr    = f"  {'#':>3}  {'Name':<{col_w}}  {p1:>16}  {p2:>16}  {'Δ Change':>14}"
        lines  = [header, sep, hdr, sep]
        items  = []
        for i, row in enumerate(results, 1):
            name = row.get("name", "?")
            v1, v2 = row.get("value1"), row.get("value2")
            d = _delta_str(v1, v2, p)
            lines.append(f"  {i:>3}. {name:<{col_w}}  {_fmt(v1,p):>16}  {_fmt(v2,p):>16}  {d:>14}")
            items.append({"rank": i, "name": name,
                          f"{p1}_value": _fmt(v1, p), f"{p2}_value": _fmt(v2, p), "delta": d})
        formatted_text = "\n".join(lines) + _token_summary(token_usage, token_totals)
        names = [r.get("name", "?")      for r in results]
        vals1 = [r.get("value1", 0) or 0 for r in results]
        vals2 = [r.get("value2", 0) or 0 for r in results]
        base_cmp = _make_base(metric, currency, entity, legend=True, index_axis="y")
        base_cmp["plugins"]["tooltip"]["callbacks"] = {"label": _cmp_tooltip_fn(metric, currency)}
        cmp_cfg = {
            "type": "bar",
            "data": {"labels": names, "datasets": [
                {"label": p1, "data": vals1,
                 "backgroundColor": _PALETTE[0], "borderColor": _BORDERS[0],
                 "borderWidth": 1, "borderRadius": 3},
                {"label": p2, "data": vals2,
                 "backgroundColor": _PALETTE[1], "borderColor": _BORDERS[1],
                 "borderWidth": 1, "borderRadius": 3},
            ]},
            "options": base_cmp,
        }
        chart_config = {
            "title":    user_query or f"{metric.title()} comparison: {p1} vs {p2}",
            "subtitle": f"{p1} vs {p2}",
            "prefix":   p,
            "config":   cmp_cfg,
        }

    # ── RANKED / THRESHOLD / INTERSECTION / ZERO_FILTER ────────────────────────
    else:
        names, values = [], []
        if qt == "zero_filter":
            header = (f"{len(results)} {elabel} had zero {metric} "
                      f"between {start_date} and {end_date}:")
            items = []
            for i, row in enumerate(results, 1):
                name = row.get("name", "?")
                header += f"\n{i}. {name}"
                items.append({"rank": i, "name": name, "value": "0"})
        else:
            if qt == "threshold" and thr:
                thr_op   = thr.get("operator", "gt")
                thr_type = thr.get("type", "absolute")
                thr_val  = thr.get("value", 0)
                direction = "less than" if thr_op == "lt" else "more than"
                thr_val_str = f"{thr_val:.0f}% of total" if thr_type == "percentage" else _fmt_indian(thr_val, p, decimals=0)
                header = (f"{len(results)} {elabel} where {metric} contributed "
                          f"{direction} {thr_val_str} "
                          f"between {start_date} and {end_date}:")
            elif qt == "intersection":
                p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
                p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
                header = f"🔀 {elabel.title()} present in BOTH {p1} AND {p2} (combined {metric}):"
            else:
                rl = "Top" if qt == "top_n" else "Bottom"
                period_str = _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
                header = f"{rl} {top_n} {elabel} by {mlabel} {period_str}:"
            items = []
            for i, row in enumerate(results, 1):
                name  = row.get("name", "?")
                value = row.get("value", 0) or 0
                val_s = _fmt_indian(value, p)
                header += f"\n{i}. {name} — {val_s}"
                items.append({"rank": i, "name": name,
                              "value": val_s, "raw_value": value})
                names.append(name)
                values.append(value)

        formatted_text = header + _token_summary(token_usage, token_totals)
        if names:
            rl = "Top" if qt != "bottom_n" else "Bottom"
            chart_config = _bar(
                names, values, metric, currency, entity,
                user_query or f"{rl} {elabel} by {metric}",
                f"{start_date} to {end_date}",
            )

    ctx.logger.info("✅ Formatted", {"queryId": query_id})
    if qs:
        now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set("queries", query_id, {
            **qs,
            "status":         "completed",
            "formattedText":  formatted_text,
            "formattedItems": items,
            "chart_config":   chart_config,
            "token_usage":    token_usage,
            "token_totals":   token_totals,
            "completedAt":    now_iso,
            "updatedAt":      now_iso,
            "status_timestamps": {**prev_ts, "completed": now_iso},
        })
    ctx.logger.info("🏁 Pipeline complete!", {"queryId": query_id})