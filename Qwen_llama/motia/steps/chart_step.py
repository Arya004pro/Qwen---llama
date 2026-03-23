"""Chart Step — GET /query/:queryId/chart

Returns a self-contained HTML page with a Chart.js visualisation of the
query results. Reads chart_config saved by format_result_step.

Trigger: HTTP GET /query/:queryId/chart
"""

from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

config = {
    "name": "ChartQuery",
    "description": "Returns a Chart.js HTML page for a completed query result",
    "flows": ["sales-analytics-utilities"],
    "triggers": [
        http("GET", "/query/:queryId/chart"),
    ],
    "enqueues": [],
}

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 32px 16px;
  }}
  .card {{
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 12px;
    padding: 28px 32px;
    width: 100%;
    max-width: 860px;
  }}
  h1 {{
    font-size: 18px;
    font-weight: 600;
    color: #f1f5f9;
    margin-bottom: 4px;
    line-height: 1.4;
  }}
  .subtitle {{
    font-size: 13px;
    color: #64748b;
    margin-bottom: 28px;
  }}
  .chart-wrap {{
    position: relative;
    width: 100%;
  }}
  .token-box {{
    margin-top: 24px;
    background: #12141e;
    border: 1px solid #2d3148;
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 12px;
    color: #64748b;
    display: flex;
    gap: 24px;
    flex-wrap: wrap;
  }}
  .token-box span {{ color: #94a3b8; }}
  .no-chart {{
    text-align: center;
    color: #64748b;
    padding: 48px 0;
    font-size: 14px;
  }}
</style>
</head>
<body>
<div class="card">
  <h1>{title}</h1>
  <p class="subtitle">{subtitle}</p>
  {body}
</div>
<script>
{script}
</script>
</body>
</html>"""


def _build_html(query_state: dict) -> str:
    chart_config = query_state.get("chart_config")
    token_totals = query_state.get("token_totals", {})
    parsed       = query_state.get("parsed", {})
    metric       = parsed.get("metric", "revenue")
    entity       = parsed.get("entity", "product")
    ranking      = query_state.get("schema", {}).get("ranking") or parsed.get("ranking", "top")

    # Build subtitle from token totals
    token_html = ""
    if token_totals.get("total_tokens"):
        token_html = (
            f'<div class="token-box">'
            f'Token usage — '
            f'<span>prompt: {token_totals.get("prompt_tokens", 0)}</span>'
            f'<span>completion: {token_totals.get("completion_tokens", 0)}</span>'
            f'<span>total: {token_totals.get("total_tokens", 0)}</span>'
            f'</div>'
        )

    if not chart_config:
        body   = '<div class="no-chart">No chart data available for this query type.</div>'
        script = ""
        title    = query_state.get("query", "Query result")
        subtitle = f"Query ID: {query_state.get('id', '')}"
        return _HTML_TEMPLATE.format(
            title=title, subtitle=subtitle,
            body=body + token_html, script=script
        )

    title    = chart_config.get("title", "Analytics chart")
    subtitle = chart_config.get("subtitle", f"Query ID: {query_state.get('id', '')}")

    body = f'<div class="chart-wrap"><canvas id="myChart"></canvas></div>{token_html}'

    import json
    import re
    cfg = chart_config.get("config", {})
    json_str = json.dumps(cfg, indent=2)
    # Strip function markers and surrounding quotes so ChartJS sees actual JS functions
    json_str = json_str.replace('"@@FUNCTION@@', '').replace('@@ENDFUNCTION@@"', '')
    # Fallback for old cached queries: match string literals starting with function(
    json_str = re.sub(r'"(function\([cv]\)\{[^"]+\})"', r'\1', json_str)
    
    script = f"""
const ctx = document.getElementById('myChart');
new Chart(ctx, {json_str});
"""
    return _HTML_TEMPLATE.format(
        title=title, subtitle=subtitle, body=body, script=script
    )


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    query_id = request.path_params.get("queryId", "")

    if not query_id:
        return ApiResponse(status=400, body={"error": "Missing queryId"})

    query_state = await ctx.state.get("queries", query_id)

    if not query_state:
        return ApiResponse(status=404, body={"error": f"Query {query_id} not found"})

    if query_state.get("status") != "completed":
        return ApiResponse(status=202, body={
            "error": "Query not yet completed",
            "status": query_state.get("status"),
        })

    html = _build_html(query_state)

    # Return raw HTML — Motia will pass through the Content-Type header
    return ApiResponse(
        status=200,
        body=html,
        headers={"Content-Type": "text/html; charset=utf-8"},
    )