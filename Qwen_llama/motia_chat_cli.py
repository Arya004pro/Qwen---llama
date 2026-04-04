"""motia_chat_cli.py — Terminal chat client with chart file generation.

Changes vs original:
  - Keeps a conversation sessionId across turns so follow-up prompts
    (e.g. "same for last year", "top 3 of that") reuse prior intent context.
  - Clarification replies continue to work in the same session.
  - Chart output and everything else is unchanged.
"""

import os
import json
import time
import re
import requests

API_BASE   = os.getenv("MOTIA_API_URL", "http://host.docker.internal:3121").rstrip("/")
SUBMIT_URL = f"{API_BASE}/query"

CHART_DIR = os.getenv("CHART_OUTPUT_DIR", os.path.join(os.getcwd(), "charts"))

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

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:32px 16px}}
.card{{background:#1a1d27;border:1px solid #2d3148;border-radius:12px;padding:28px 32px;width:100%;max-width:920px}}
h1{{font-size:18px;font-weight:600;color:#f1f5f9;margin-bottom:4px}}
.sub{{font-size:13px;color:#64748b;margin-bottom:28px}}
.tok{{margin-top:24px;background:#12141e;border:1px solid #2d3148;border-radius:8px;padding:12px 16px;font-size:12px;color:#64748b;display:flex;gap:24px;flex-wrap:wrap}}
.tok span{{color:#94a3b8}}
</style>
</head>
<body>
<div class="card">
<h1>{title}</h1>
<p class="sub">{subtitle}</p>
<canvas id="c"></canvas>
{token_html}
</div>
<script>
new Chart(document.getElementById('c'),{cfg});
</script>
</body>
</html>"""


def _border(c):
    return c.replace("0.85", "1")

def _x_tick(prefix):
    return f"function(v){{return '{prefix}'+v.toLocaleString('en-IN');}}"

def _tip(prefix):
    return (f"function(c){{return ' {prefix}'"
            f"+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}});}}")

def _tip2(prefix):
    return (f"function(c){{return ' '+c.dataset.label+': {prefix}'"
            f"+c.raw.toLocaleString('en-IN',{{minimumFractionDigits:2}});}}")

def _base(prefix, tip_fn=None, legend=False, index_axis=None):
    opts = {
        "responsive": True,
        "plugins": {
            "legend": {"display": legend,
                       "labels": {"color": "#94a3b8", "font": {"size": 12}}},
            "tooltip": {
                "backgroundColor": "#1e2130",
                "titleColor": "#e2e8f0",
                "bodyColor": "#94a3b8",
                "borderColor": "#2d3148",
                "borderWidth": 1,
                "callbacks": {"label": tip_fn or _tip(prefix)},
            },
        },
        "scales": {
            "x": {
                "ticks": {"color": "#94a3b8", "font": {"size": 11},
                          "callback": _x_tick(prefix)},
                "grid":  {"color": "rgba(255,255,255,0.05)"},
            },
            "y": {
                "ticks":       {"color": "#94a3b8", "font": {"size": 11}},
                "grid":        {"color": "rgba(255,255,255,0.05)"},
                "beginAtZero": True,
            },
        },
    }
    if index_axis:
        opts["indexAxis"] = index_axis
        opts["scales"]["x"]["ticks"].pop("callback", None)
        opts["scales"]["y"]["ticks"]["callback"] = _x_tick(prefix)
    return opts


def _cfg_from_state(result: dict):
    chart_config = result.get("chart_config")
    if not chart_config:
        return None, None, None
    cfg = chart_config.get("config")
    if not cfg:
        return None, None, None
    return cfg, chart_config.get("title"), chart_config.get("subtitle")


def _cfg_from_items(result: dict):
    parsed        = result.get("parsed", {})
    metric        = parsed.get("metric", "revenue")
    ranking       = parsed.get("ranking") or "top"
    is_comparison = parsed.get("is_comparison", False)
    prefix        = "₹" if metric == "revenue" else ""
    items         = result.get("formattedItems", [])

    if not items:
        return None

    if is_comparison:
        if ranking == "aggregate":
            period_items = [i for i in items if "period" in i]
            if len(period_items) < 2:
                return None
            labels = [i["period"]           for i in period_items]
            values = [i.get("raw_value", 0) for i in period_items]
            return {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": metric.title(),
                        "data":  values,
                        "backgroundColor": [_PALETTE[0], _PALETTE[1]],
                        "borderColor":     [_border(_PALETTE[0]), _border(_PALETTE[1])],
                        "borderWidth": 1, "borderRadius": 4,
                    }],
                },
                "options": {
                    **_base(prefix),
                    "scales": {
                        "x": {"ticks": {"color": "#94a3b8"},
                              "grid":  {"color": "rgba(255,255,255,0.05)"}},
                        "y": {"ticks": {"color": "#94a3b8",
                                        "callback": _x_tick(prefix)},
                              "grid":  {"color": "rgba(255,255,255,0.05)"},
                              "beginAtZero": True},
                    },
                },
            }
        else:
            names, v1s, v2s = [], [], []
            for i in items:
                if "name" not in i:
                    continue
                names.append(i["name"])
                for lst, key in [(v1s, "period1_value"), (v2s, "period2_value")]:
                    raw = i.get(key, "0").replace("₹", "").replace(",", "")
                    try:    lst.append(float(raw))
                    except: lst.append(0)

            p1, p2 = "Period 1", "Period 2"
            m = re.search(r":\s*(.+?)\s+vs\s+(.+?)$",
                          result.get("formattedText", ""), re.MULTILINE)
            if m:
                p1, p2 = m.group(1).strip(), m.group(2).strip()

            return {
                "type": "bar",
                "data": {
                    "labels": names,
                    "datasets": [
                        {"label": p1, "data": v1s,
                         "backgroundColor": _PALETTE[0],
                         "borderColor": _border(_PALETTE[0]),
                         "borderWidth": 1, "borderRadius": 3},
                        {"label": p2, "data": v2s,
                         "backgroundColor": _PALETTE[1],
                         "borderColor": _border(_PALETTE[1]),
                         "borderWidth": 1, "borderRadius": 3},
                    ],
                },
                "options": _base(prefix, tip_fn=_tip2(prefix),
                                  legend=True, index_axis="y"),
            }

    if ranking == "aggregate":
        return None

    labels  = [i.get("name", "")          for i in items]
    values  = [i.get("raw_value", 0) or 0 for i in items]
    colors  = [_PALETTE[i % len(_PALETTE)]         for i in range(len(labels))]
    borders = [_border(_PALETTE[i % len(_PALETTE)]) for i in range(len(labels))]

    return {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": metric.title(),
                "data":  values,
                "backgroundColor": colors,
                "borderColor":     borders,
                "borderWidth": 1, "borderRadius": 4,
            }],
        },
        "options": _base(prefix, index_axis="y"),
    }


def _write_chart(result: dict, query_id: str) -> str | None:
    cfg, title, subtitle = _cfg_from_state(result)

    if cfg is None:
        cfg = _cfg_from_items(result)
        text     = result.get("formattedText", "")
        title    = text.splitlines()[0].lstrip("📊 ").strip() if text else "Query result"
        subtitle = f"Query ID: {query_id}"

    if cfg is None:
        return None

    totals     = result.get("token_totals", {})
    token_html = ""
    if totals.get("total_tokens"):
        token_html = (
            f'<div class="tok">Token usage — '
            f'<span>prompt: {totals.get("prompt_tokens", 0)}</span>'
            f'<span>completion: {totals.get("completion_tokens", 0)}</span>'
            f'<span>total: {totals.get("total_tokens", 0)}</span></div>'
        )

    html = _HTML.format(
        title=title or "Query result",
        subtitle=subtitle or f"Query ID: {query_id}",
        cfg=json.dumps(cfg, indent=2),
        token_html=token_html,
    )

    try:
        os.makedirs(CHART_DIR, exist_ok=True)
        path = os.path.join(CHART_DIR, f"chart_{query_id}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        return path
    except Exception as e:
        print(f"  [chart] Write failed: {e}")
        return None


def fetch_result(query_id: str) -> dict:
    r = requests.get(f"{API_BASE}/query/{query_id}", timeout=15)
    r.raise_for_status()
    return r.json()


def print_final(result: dict, query_id: str) -> None:
    status = result.get("status")

    if status == "completed":
        text = result.get("formattedText")
        print(f"Assistant: {text}" if text else "Assistant: Query completed.")
        path = _write_chart(result, query_id)
        if path:
            win_path = path.replace("/charts/", "Qwen_llama\\charts\\")
            print(f"\n  📈 Chart ready — open in browser:")
            print(f"     {win_path}")
        return

    if status == "needs_clarification":
        print(f"Assistant: {result.get('clarification', 'Please clarify your query.')}")
    elif status == "error":
        print(f"Assistant: ⚠  {result.get('error', 'Unknown error.')}")
        if result.get("sql_source"):
            print(f"  [sql_source] {result.get('sql_source')}")
        print(f"  [check Motia logs at http://localhost:3113/logs for the full SQL]")
    else:
        print(f"Assistant: Query finished with status '{status}'.")


def submit_query(user_input: str, session_id: str | None = None) -> dict | None:
    """POST /query, optionally attaching a sessionId for conversational context."""
    body = {"query": user_input}
    if session_id:
        body["sessionId"] = session_id
    try:
        resp = requests.post(SUBMIT_URL, json=body, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"Assistant: Failed to submit query ({exc}).")
        return None


def poll_until_done(query_id: str) -> dict | None:
    """Poll /query/{id} until terminal status. Returns final state dict."""
    deadline    = time.time() + 180
    last_status = None
    while time.time() < deadline:
        try:
            result = fetch_result(query_id)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                time.sleep(0.7)
                continue
            print(f"Assistant: Failed to fetch result ({exc}).")
            return None
        except Exception as exc:
            print(f"Assistant: Failed to fetch result ({exc}).")
            return None

        status = result.get("status")
        if status != last_status:
            print(f"[status] {status}")
            last_status = status

        if status in {"completed", "error", "needs_clarification"}:
            return result

        time.sleep(0.7)

    print("Assistant: Timed out. Check Motia Logs/Traces UI.")
    return None


def main() -> None:
    print("Motia Live Chat (type 'exit' to quit)")
    print(f"API    : {API_BASE}")
    print(f"Charts : {CHART_DIR}\n")

    conversation_session_id: str | None = None

    while True:
        user_input = input("User: ").strip()
        if user_input.lower() in {"exit", "quit", "q"}:
            print("Assistant: Goodbye")
            return
        if not user_input:
            continue

        data = submit_query(user_input, session_id=conversation_session_id)

        if not data:
            continue

        query_id = data.get("queryId")
        conversation_session_id = data.get("sessionId") or conversation_session_id or query_id
        if not query_id:
            print("Assistant: No queryId returned.")
            continue

        print(f"Assistant: Accepted (queryId: {query_id}). Processing...")

        result = poll_until_done(query_id)
        if result is None:
            continue

        print_final(result, query_id)

        print()


if __name__ == "__main__":
    main()
