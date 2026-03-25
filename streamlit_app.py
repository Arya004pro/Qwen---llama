"""streamlit_app.py — Sales Analytics Pipeline Dashboard

Changes from original:
- Added a single direct "Download" PDF button.
- PDF is generated server-side with selectable text, plus chart + table.
"""

import streamlit as st
import streamlit.components.v1 as components
import requests
import time
from datetime import datetime
from pathlib import Path

def fetch_schema():
        try:
            r = requests.get(f"{API}/schema", timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

API           = "http://127.0.0.1:3121"
POLL_INTERVAL = 0.7
SHARED_UPLOAD_DIR = Path("Qwen_llama/motia/data/uploads")
CONTAINER_UPLOAD_DIR = "/app/motia/data/uploads"

STEPS = [
    {"label": "Receive",      "sub": "REST entry point",    "icon": "⤵"},
    {"label": "Parse intent", "sub": "Qwen 3-32B",          "icon": "◈"},
    {"label": "Ambiguity",    "sub": "Clarification check", "icon": "?"},
    {"label": "Text → SQL",   "sub": "Builder / LLaMA",     "icon": "{}"},
    {"label": "Execute SQL",  "sub": "DuckDB",               "icon": "▶"},
    {"label": "Format",       "sub": "Result + chart",      "icon": "✦"},
]

STATUS_MAP = {
    "received":            0,
    "intent_parsed":       1,
    "ambiguity_checked":   2,
    "needs_clarification": 2,
    "schema_mapped":       3,
    "sql_generated":       3,
    "executed":            4,
    "completed":           5,
    "error":               5,
}

STEP_STATUS_OPTIONS = {
    0: ["received"],
    1: ["intent_parsed"],
    2: ["ambiguity_checked", "needs_clarification"],
    3: ["sql_generated", "schema_mapped"],
    4: ["executed"],
    5: ["completed", "error"],
}

for key, default in {
    "query_id":        None,
    "polling":         False,
    "pending_session": None,
    "final_state":     None,
    "current_status":  "",
    "history":         [],
    "step_times":      {},
    "poll_start":      None,
    "last_completed":  -1,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

def _on_enter():
        val = st.session_state.get("query_input_field", "").strip()
        if not val or st.session_state.polling:
            return
        st.session_state.polling        = True
        st.session_state.final_state    = None
        st.session_state.current_status = ""
        st.session_state.step_times     = {}
        st.session_state.last_completed = -1
        st.session_state.poll_start     = time.time()
        try:
            resp = submit_query(val, session_id=st.session_state.pending_session)
            st.session_state.query_id        = resp["queryId"]
            st.session_state.pending_session = None
        except Exception as e:
            st.session_state.polling = False
            st.error(f"Failed to submit: {e}")
            
st.set_page_config(
    page_title="Sales Analytics Pipeline",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
.step-row { display:flex; align-items:flex-start; margin:8px 0 20px; }
.step-wrap { flex:1; display:flex; flex-direction:column; align-items:center; position:relative; }
.step-wrap:not(:last-child)::after {
    content:''; position:absolute; top:22px; left:50%; width:100%; height:2px;
    background:#2d3148; z-index:0;
}
.step-wrap.done:not(:last-child)::after   { background:#10b981; }
.step-wrap.active:not(:last-child)::after { background:linear-gradient(to right,#10b981,#2d3148); }
.step-circle {
    width:44px; height:44px; border-radius:50%;
    display:flex; align-items:center; justify-content:center;
    font-size:16px; font-weight:600; z-index:1; position:relative;
}
.step-circle.idle   { border:2px solid #2d3148; background:#0f1117; color:#64748b; }
.step-circle.active { border:2px solid #6366f1; background:rgba(99,102,241,.1); color:#818cf8;
                      animation:pulse 1.4s ease-in-out infinite; }
.step-circle.done   { border:2px solid #10b981; background:rgba(16,185,129,.1); color:#10b981; }
.step-circle.error  { border:2px solid #ef4444; background:rgba(239,68,68,.1); color:#ef4444; }
@keyframes pulse {
    0%,100%{box-shadow:0 0 0 4px rgba(99,102,241,.15);}
    50%{box-shadow:0 0 0 12px rgba(99,102,241,.30);}
}
.step-label { margin-top:8px; font-size:11px; font-weight:600; text-align:center;
              color:#64748b; max-width:80px; line-height:1.3; }
.step-sub   { font-size:10px; color:#475569; text-align:center; max-width:80px; line-height:1.3; margin-top:2px; }
.step-time  { font-size:10px; color:#10b981; text-align:center; margin-top:3px; }
.step-wrap.done   .step-label { color:#e2e8f0; }
.step-wrap.active .step-label { color:#e2e8f0; }
.step-wrap.active .step-sub   { color:#818cf8; }
.step-wrap.active .step-time  { color:#818cf8; }
.step-wrap.idle   .step-time  { display:none; }
.sql-box {
    background:#080a10; border:1px solid #2d3148; border-radius:8px;
    padding:12px 14px; font-family:"Cascadia Code",Consolas,monospace;
    font-size:12px; color:#93c5fd; white-space:pre-wrap; word-break:break-all;
    line-height:1.6; max-height:220px; overflow-y:auto;
}
.clarify-box {
    background:rgba(245,158,11,.07); border:1px solid rgba(245,158,11,.3);
    border-radius:10px; padding:14px 16px;
    color:#fbbf24; font-size:14px; line-height:1.5; margin:8px 0;
}
.result-box {
    background:#1a1d27; border:1px solid #2d3148; border-radius:10px;
    padding:16px 18px; font-size:14px; color:#e2e8f0;
    white-space:pre-wrap; line-height:1.75;
}
.result-error { border-color:rgba(239,68,68,.4); color:#fca5a5; }
.hist-item { background:#1a1d27; border:1px solid #2d3148; border-radius:8px;
             padding:10px 14px; margin-bottom:6px; font-size:12px; }
.hist-q { color:#94a3b8; margin-bottom:4px; }
.hist-a { color:#e2e8f0; white-space:pre-wrap; }
.section-lbl { font-size:10px; font-weight:700; color:#475569;
               text-transform:uppercase; letter-spacing:.1em; margin-bottom:10px; }
</style>
""", unsafe_allow_html=True)


# ── Export report builder (direct downloadable PDF) ──────────────────────────

def _safe_text(v) -> str:
    s = "" if v is None else str(v)
    return s.replace("₹", "Rs ").replace("—", "-")


def _pretty_col(k: str) -> str:
    return k.replace("_", " ").replace("period1", "Period 1").replace("period2", "Period 2").title()


def _to_num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _to_rl_color(s, fallback):
    from reportlab.lib import colors
    if not isinstance(s, str):
        return fallback
    s = s.strip()
    try:
        if s.startswith("#"):
            return colors.HexColor(s)
        if s.startswith("rgba("):
            vals = s[5:-1].split(",")
            r, g, b = [max(0, min(255, int(float(x.strip())))) for x in vals[:3]]
            return colors.Color(r / 255.0, g / 255.0, b / 255.0)
        if s.startswith("rgb("):
            vals = s[4:-1].split(",")
            r, g, b = [max(0, min(255, int(float(x.strip())))) for x in vals[:3]]
            return colors.Color(r / 255.0, g / 255.0, b / 255.0)
    except Exception:
        return fallback
    return fallback


def _build_pdf_chart(chart_config):
    if not chart_config:
        return None
    cfg = chart_config.get("config", {}) or {}
    data = cfg.get("data", {}) or {}
    labels = list(data.get("labels", []) or [])
    datasets = list(data.get("datasets", []) or [])
    if not labels or not datasets:
        return None

    series = []
    for ds in datasets:
        vals = ds.get("data", []) or []
        if len(vals) < len(labels):
            vals = vals + [0] * (len(labels) - len(vals))
        series.append([_to_num(v) for v in vals[: len(labels)]])
    if not series:
        return None

    from reportlab.lib import colors
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import HorizontalBarChart

    max_label = max((len(str(x)) for x in labels), default=10)
    left_pad = 120 if max_label <= 14 else min(210, 120 + (max_label - 14) * 4)
    chart_h = max(180, min(380, 15 * len(labels) + 45))
    drawing = Drawing(500, chart_h + 35)
    chart = HorizontalBarChart()
    chart.x = left_pad
    chart.y = 18
    chart.width = 480 - left_pad
    chart.height = chart_h
    chart.data = tuple(tuple(row) for row in series)
    chart.categoryAxis.categoryNames = [str(x) if len(str(x)) <= 28 else f"{str(x)[:25]}..." for x in labels]
    chart.categoryAxis.labels.boxAnchor = "e"
    chart.categoryAxis.labels.fontSize = 7 if len(labels) > 12 else 8
    chart.categoryAxis.labels.dx = -6
    chart.categoryAxis.labels.fillColor = colors.HexColor("#cbd5e1")
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.labels.fillColor = colors.HexColor("#94a3b8")
    chart.valueAxis.strokeColor = colors.HexColor("#334155")
    chart.categoryAxis.strokeColor = colors.HexColor("#334155")
    chart.groupSpacing = 5
    chart.barSpacing = 2
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = colors.HexColor("#1f2937")

    for i, ds in enumerate(datasets):
        c = ds.get("backgroundColor")
        if isinstance(c, list) and c:
            c = c[0]
        fill = _to_rl_color(c, colors.HexColor("#60a5fa"))
        chart.bars[i].fillColor = fill
        chart.bars[i].strokeColor = fill

    drawing.add(chart)
    return drawing


def _is_number_like(value) -> bool:
    if value is None:
        return False
    s = str(value).strip().replace(",", "")
    if not s:
        return False
    if s.startswith("Rs "):
        s = s[3:].strip()
    if s.startswith("-"):
        s = s[1:]
    try:
        float(s)
        return True
    except Exception:
        return False


def _build_export_pdf(state: dict, user_query: str) -> bytes:
    from io import BytesIO
    from xml.sax.saxutils import escape
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether,
        HRFlowable, CondPageBreak
    )

    chart_config = state.get("chart_config")
    items = state.get("formattedItems", []) or []
    text_answer = state.get("formattedText", "") or ""
    token_totals = state.get("token_totals", {}) or {}
    generated_at = datetime.now().strftime("%d %B %Y, %H:%M")

    summary = _safe_text(text_answer.split("─── Token Usage")[0].strip())
    query = _safe_text(user_query)

    styles = getSampleStyleSheet()
    title_s = ParagraphStyle("title_s", parent=styles["Heading1"], fontSize=18, leading=22, spaceAfter=4, textColor=colors.HexColor("#f8fafc"))
    meta_s = ParagraphStyle("meta_s", parent=styles["Normal"], fontSize=9, textColor=colors.HexColor("#94a3b8"))
    h2_s = ParagraphStyle("h2_s", parent=styles["Heading2"], fontSize=12, textColor=colors.HexColor("#e2e8f0"), spaceBefore=8, spaceAfter=6)
    body_s = ParagraphStyle("body_s", parent=styles["Normal"], fontSize=10.5, leading=14, textColor=colors.HexColor("#cbd5e1"))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.55 * inch,
        bottomMargin=0.55 * inch,
        title=f"Analytics Report - {query[:80]}",
    )
    story = []

    story.append(Paragraph(escape(query), title_s))
    story.append(Paragraph(f"Generated {generated_at}", meta_s))
    story.append(Spacer(1, 6))
    story.append(HRFlowable(width="100%", thickness=0.6, color=colors.HexColor("#1e293b")))
    story.append(Spacer(1, 10))

    summary_html = "<br/>".join(escape(x) for x in summary.splitlines()) if summary else "-"
    answer_card = Table([[Paragraph(summary_html, body_s)]], colWidths=[doc.width])
    answer_card.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0b1220")),
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#374151")),
        ("LINEBEFORE", (0, 0), (0, -1), 2.0, colors.HexColor("#60a5fa")),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(KeepTogether([Paragraph("Answer", h2_s), answer_card]))
    story.append(Spacer(1, 8))

    chart = _build_pdf_chart(chart_config)
    if chart is not None:
        chart_title = (chart_config or {}).get("title")
        chart_nodes = [Paragraph("Chart", h2_s)]
        if chart_title:
            chart_nodes.append(Paragraph(escape(_safe_text(chart_title)), meta_s))
            chart_nodes.append(Spacer(1, 4))
        chart_nodes.append(chart)
        story.append(KeepTogether(chart_nodes))
        story.append(Spacer(1, 10))

    if items:
        skip_keys = {"raw_value", "raw_delta"}
        cols = [k for k in items[0].keys() if k not in skip_keys]
        table_data = [[_pretty_col(c) for c in cols]]
        for row in items:
            table_data.append([_safe_text(row.get(c, "")) for c in cols])

        avail_w = doc.width
        if len(cols) <= 1:
            col_widths = [avail_w]
        else:
            first_w = min(max(190, avail_w * 0.36), avail_w * 0.5)
            rest_w = (avail_w - first_w) / (len(cols) - 1)
            col_widths = [first_w] + [rest_w] * (len(cols) - 1)

        tbl = Table(table_data, repeatRows=1, hAlign="LEFT", colWidths=col_widths)
        align_cmds = [("ALIGN", (0, 0), (-1, 0), "LEFT"), ("ALIGN", (0, 1), (0, -1), "LEFT")]
        for ci, col_name in enumerate(cols[1:], start=1):
            numeric_col = ("rank" in col_name.lower()) or all(_is_number_like(r.get(col_name)) for r in items[: min(20, len(items))])
            align_cmds.append(("ALIGN", (ci, 1), (ci, -1), "RIGHT" if numeric_col else "LEFT"))

        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9.2),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#cbd5e1")),
            ("LINEABOVE", (0, 0), (-1, 0), 0.8, colors.HexColor("#334155")),
            ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#334155")),
            ("GRID", (0, 1), (-1, -1), 0.35, colors.HexColor("#1f2937")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#020617"), colors.HexColor("#0b1220")]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ] + align_cmds))
        story.append(CondPageBreak(1.6 * inch))
        story.append(Paragraph("Data Table", h2_s))
        story.append(tbl)
        story.append(Spacer(1, 8))

    if token_totals.get("total_tokens"):
        token_txt = (
            f"Token usage - prompt: {token_totals.get('prompt_tokens', 0)} | "
            f"completion: {token_totals.get('completion_tokens', 0)} | "
            f"total: {token_totals.get('total_tokens', 0)}"
        )
        story.append(Paragraph(escape(token_txt), meta_s))

    def _on_page(canv, _doc):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#020817"))
        canv.rect(0, 0, letter[0], letter[1], stroke=0, fill=1)
        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#64748b"))
        canv.drawString(_doc.leftMargin, 0.30 * inch, "Sales Analytics Report")
        canv.drawRightString(letter[0] - _doc.rightMargin, 0.30 * inch, f"Page {_doc.page}")
        canv.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buf.getvalue()


# ── API helpers ───────────────────────────────────────────────────────────────

def api_ok():
    try:
        r = requests.get(f"{API}/schema", timeout=5)
        r.raise_for_status()
        return True
    except Exception:
        return False

def submit_query(query, session_id=None):
    body = {"query": query}
    if session_id:
        body["sessionId"] = session_id
    r = requests.post(f"{API}/query", json=body, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_state(query_id):
    r = requests.get(f"{API}/query/{query_id}", timeout=10)
    r.raise_for_status()
    return r.json()

def _wait_for_ingest_ready(max_wait_seconds=20.0):
    """Wait until core endpoints are reachable after Motia hot-reload."""
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            schema_r = requests.get(f"{API}/schema", timeout=3)
            if schema_r.status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(0.6)
    return False

def ingest_uploaded_files(uploaded_files, reset_db=False, use_llm_grouping=False):
    files_payload = []
    SHARED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for f in uploaded_files or []:
        raw = f.getvalue()
        if not raw:
            continue
        safe_name = Path(f.name).name
        local_path = SHARED_UPLOAD_DIR / safe_name
        local_path.write_bytes(raw)
        files_payload.append({
            "name": safe_name,
            "path": f"{CONTAINER_UPLOAD_DIR}/{safe_name}",
        })
    if not files_payload:
        raise ValueError("No valid file content to upload.")

    body = {
        "files": files_payload,
        "reset_db": bool(reset_db),
        "use_llm_grouping": bool(use_llm_grouping),
    }
    _wait_for_ingest_ready(max_wait_seconds=20.0)
    last_err = None
    # Motia can briefly unregister/re-register routes during hot reload.
    for attempt in range(12):
        try:
            r = requests.post(f"{API}/ingest", json=body, timeout=300)
            if r.status_code >= 400:
                msg = None
                try:
                    msg = r.json().get("error")
                except Exception:
                    msg = r.text[:500]
                raise RuntimeError(f"HTTP {r.status_code}: {msg or 'ingest failed'}")
            return r.json()
        except Exception as exc:
            last_err = exc
            err_s = str(exc)
            # Retry route-not-found / invocation-stop windows with backoff.
            if ("HTTP 404" in err_s) or ("invocation_stopped" in err_s) or ("Connection aborted" in err_s):
                _wait_for_ingest_ready(max_wait_seconds=6.0)
                time.sleep(0.5 + attempt * 0.35)
                continue
            # Other failures: still retry a few times before giving up.
            if attempt < 11:
                time.sleep(0.4 + attempt * 0.25)
                continue
            raise last_err
    raise last_err

def fetch_chart_html(query_id):
    try:
        r = requests.get(f"{API}/query/{query_id}/chart", timeout=10)
        r.raise_for_status()
        text = r.text
        if text.startswith('"') and text.endswith('"'):
            try:
                import json as _j
                return _j.loads(text)
            except Exception:
                return text
        return text
    except Exception as e:
        return f"<p style='color:#ef4444'>Could not load chart: {e}</p>"

def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _compute_backend_step_times(state):
    ts_map     = state.get("status_timestamps", {}) or {}
    created_at = _parse_iso(state.get("createdAt"))
    step_at    = {}

    for idx in range(len(STEPS)):
        for status_name in STEP_STATUS_OPTIONS.get(idx, []):
            dt = _parse_iso(ts_map.get(status_name))
            if dt:
                step_at[idx] = dt
                break

    if 0 not in step_at and created_at:
        step_at[0] = created_at

    times = {}
    prev  = created_at or step_at.get(0)
    for idx in range(len(STEPS)):
        dt = step_at.get(idx)
        if not dt:
            continue
        if prev:
            times[idx] = max((dt - prev).total_seconds(), 0.0)
        else:
            times[idx] = 0.0
        prev = dt

    return times

def render_steps(completed_idx, is_error, step_times):
    parts = []
    for i, s in enumerate(STEPS):
        if is_error and i == completed_idx:
            cls, icon = "error", "✕"
        elif i <= completed_idx:
            cls, icon = "done", "✓"
        elif i == completed_idx + 1:
            cls, icon = "active", s["icon"]
        else:
            cls, icon = "idle", s["icon"]

        t = step_times.get(i)
        if t is not None:
            t_label = f"{t:.2f}s" if t < 1 else f"{t:.1f}s"
            time_html = f'<div class="step-time">{t_label}</div>'
        elif cls == "active":
            time_html = '<div class="step-time">…</div>'
        else:
            time_html = '<div class="step-time"></div>'

        parts.append(f"""
        <div class="step-wrap {cls}">
            <div class="step-circle {cls}">{icon}</div>
            <div class="step-label">{s['label']}</div>
            <div class="step-sub">{s['sub']}</div>
            {time_html}
        </div>""")

    st.markdown(f'<div class="step-row">{"".join(parts)}</div>', unsafe_allow_html=True)

def render_tokens(usage, totals):
    if not usage:
        return
    rows = [{"Step": e.get("step","?"),
             "Model": (e.get("model") or "").split("/")[-1].replace("-instant",""),
             "Prompt": e.get("prompt_tokens",0),
             "Completion": e.get("completion_tokens",0),
             "Total": e.get("total_tokens",0)} for e in usage]
    st.dataframe(rows, width="stretch", hide_index=True)
    if totals and totals.get("total_tokens"):
        c1, c2, c3 = st.columns(3)
        c1.metric("Prompt tokens",     totals.get("prompt_tokens",0))
        c2.metric("Completion tokens", totals.get("completion_tokens",0))
        c3.metric("Total tokens",      totals.get("total_tokens",0))


# ── Layout ────────────────────────────────────────────────────────────────────
st.markdown("## 📊 Sales Analytics — Live Pipeline")

if api_ok():
    st.success("Motia API connected (localhost:3121)", icon="🟢")
else:
    st.error("Motia API not reachable — run `npm run dev` first", icon="🔴")
    st.stop()

st.divider()

left, right = st.columns([3, 2], gap="large")

with left:
    st.markdown('<div class="section-lbl">Pipeline steps</div>', unsafe_allow_html=True)
    steps_placeholder = st.empty()
    with steps_placeholder.container():
        render_steps(-1, False, {})

    st.divider()
    st.markdown('<div class="section-lbl">Data source</div>', unsafe_allow_html=True)
    with st.expander("Upload dataset files (CSV/JSON/Parquet)", expanded=False):
        upload_files = st.file_uploader(
            "Upload files",
            type=["csv", "tsv", "json", "jsonl", "parquet"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
        reset_db = st.checkbox("Replace existing dataset", value=True)
        ingest_btn = st.button("Ingest files", type="secondary")
        if ingest_btn:
            try:
                if not upload_files:
                    st.warning("Select at least one file.")
                else:
                    with st.spinner("Ingesting files into DuckDB..."):
                        ingest_result = ingest_uploaded_files(
                            upload_files,
                            reset_db=reset_db,
                            use_llm_grouping=False,
                        )
                    tnames = ", ".join(ingest_result.get("tables_created", []))
                    st.success(f"Ingested tables: {tnames}" if tnames else "Ingestion completed.")
            except Exception as e:
                st.error(f"Ingestion failed: {e}")
                
    # ── Schema viewer ─────────────────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-lbl">Loaded dataset schema</div>',
                unsafe_allow_html=True)
    schema_data = fetch_schema()
 
    if not schema_data or not schema_data.get("tables"):
        st.caption("No data loaded yet — upload a file above.")
    else:
        tables = schema_data.get("tables", [])
        rels   = schema_data.get("relationships", [])
 
        # Summary line
        total_tables = len(tables)
        st.caption(f"{total_tables} table{'s' if total_tables != 1 else ''} loaded")
 
        # One expander per table
        for tbl in tables:
            tname = tbl.get("table", "unknown")
            cols  = tbl.get("columns", [])
            with st.expander(f"📋 {tname}  ({len(cols)} columns)", expanded=False):
                if cols:
                    col_data = [
                        {"Column": c["name"], "Type": c["type"]}
                        for c in cols
                    ]
                    st.dataframe(col_data, hide_index=True, use_container_width=True)
                else:
                    st.caption("No column info available.")
 
        # Relationships
        if rels:
            with st.expander(f"🔗 Relationships detected ({len(rels)})", expanded=False):
                for r in rels:
                    confidence = r.get("confidence", "")
                    badge = "🟢" if confidence == "HIGH" else "🟡"
                    st.markdown(
                        f"{badge} `{r.get('from_table')}.{r.get('from_column')}` "
                        f"→ `{r.get('to_table')}.{r.get('to_column')}`  "
                        f"*({r.get('type', 'FK')})*"
                    )
 
        # Refresh button
        if st.button("↻ Refresh schema", key="refresh_schema", type="secondary"):
            st.rerun()

    st.divider()
    st.markdown('<div class="section-lbl">Ask a question</div>', unsafe_allow_html=True)

    query_input = st.text_input(
        "query", label_visibility="collapsed",
        placeholder="e.g. Top 5 drivers by earnings in 2024",
        disabled=st.session_state.polling,
        on_change=_on_enter,          # ← fires when user presses Enter
        key="query_input_field",
    )
    send_col, status_col = st.columns([1, 3])
    send_btn = send_col.button(
        "Send",
        disabled=st.session_state.polling,
        width="stretch",
        type="primary",
        on_click=_on_enter,           # ← same handler for button click
    )
    status_placeholder   = status_col.empty()
    clarify_placeholder  = st.empty()
    result_placeholder   = st.empty()

with right:
    st.markdown('<div class="section-lbl">Token usage</div>', unsafe_allow_html=True)
    token_placeholder = st.empty()
    with token_placeholder.container():
        st.caption("No data yet — submit a query to begin.")

    st.divider()
    st.markdown('<div class="section-lbl">Generated SQL</div>', unsafe_allow_html=True)
    sql_placeholder = st.empty()
    with sql_placeholder.container():
        st.markdown('<div class="sql-box">Waiting for SQL generation…</div>',
                    unsafe_allow_html=True)

if st.session_state.history:
    with st.expander(f"History — {len(st.session_state.history)} queries", expanded=False):
        for item in reversed(st.session_state.history):
            st.markdown(f"""
            <div class="hist-item">
                <div class="hist-q">🔹 {item['query']}</div>
                <div class="hist-a">{item['result'][:300]}{'…' if len(item['result'])>300 else ''}</div>
            </div>""", unsafe_allow_html=True)




# ── Polling OR Final State ────────────────────────────────────────────────────
state = None
if st.session_state.polling and st.session_state.query_id:
    try:
        state = fetch_state(st.session_state.query_id)
    except Exception as e:
        st.session_state.polling = False
        st.error(f"Polling error: {e}")
elif not st.session_state.polling and st.session_state.final_state:
    state = st.session_state.final_state

if state:
    status        = state.get("status", "")
    target_idx    = STATUS_MAP.get(status, -1)
    is_error      = status == "error"
    backend_times = _compute_backend_step_times(state)
    if backend_times:
        st.session_state.step_times.update(backend_times)

    if target_idx > st.session_state.last_completed:
        st.session_state.last_completed = target_idx

    render_idx = st.session_state.last_completed if st.session_state.polling else target_idx
    with steps_placeholder.container():
        render_steps(render_idx, is_error, st.session_state.step_times)

    if st.session_state.polling:
        status_placeholder.caption(f"Status: `{status}`")
    elif is_error:
        status_placeholder.caption("Pipeline error — check Motia logs")
    else:
        status_placeholder.caption("Done ✓ — ask another question")

    if state.get("generated_sql"):
        with sql_placeholder.container():
            st.markdown(f'<div class="sql-box">{state["generated_sql"].strip()}</div>',
                        unsafe_allow_html=True)

    if state.get("token_usage"):
        with token_placeholder.container():
            render_tokens(state.get("token_usage"), state.get("token_totals"))

    # ── Terminal states ───────────────────────────────────────────────────────
    if status == "needs_clarification":
        if st.session_state.polling:
            st.session_state.polling         = False
            st.session_state.pending_session = st.session_state.query_id
            st.session_state.final_state     = state
            st.rerun()

        q = state.get("clarification", "Please clarify your query.")
        with clarify_placeholder.container():
            st.markdown(f'<div class="clarify-box">💬 {q}</div>', unsafe_allow_html=True)
            answer = st.text_input("Your answer",
                                   key=f"clarify_{st.session_state.query_id}",
                                   placeholder="Type your answer and press Enter")
            if st.button("Reply", key=f"reply_{st.session_state.query_id}", type="primary"):
                if answer.strip():
                    st.session_state.polling        = True
                    st.session_state.step_times     = {}
                    st.session_state.last_completed = -1
                    st.session_state.poll_start     = time.time()
                    resp = submit_query(answer.strip(),
                                        session_id=st.session_state.pending_session)
                    st.session_state.query_id        = resp["queryId"]
                    st.session_state.pending_session = None
                    st.rerun()

    elif status == "completed":
        if st.session_state.polling:
            st.session_state.polling     = False
            st.session_state.final_state = state
            hist_query = query_input if query_input else "(clarification reply)"
            st.session_state.history.append({
                "query":  hist_query,
                "result": state.get("formattedText", ""),
            })
            st.rerun()

        text = state.get("formattedText", "Query complete.")
        with result_placeholder.container():
            st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="result-box">{text}</div>', unsafe_allow_html=True)

            # ── Chart preview ─────────────────────────────────────────────────
            if state.get("chart_config"):
                st.markdown("<br>", unsafe_allow_html=True)
                with st.expander("📈 Interactive chart", expanded=True):
                    chart_html = fetch_chart_html(st.session_state.query_id)
                    components.html(chart_html, height=520, scrolling=False)

            # ── Single direct PDF download button ─────────────────────────────
            st.markdown("<br>", unsafe_allow_html=True)
            last_query = (st.session_state.history[-1]["query"]
                          if st.session_state.history else "query")
            safe_name = "".join(ch for ch in last_query[:48] if ch.isalnum() or ch in (" ", "_", "-")).strip().replace(" ", "_")
            if not safe_name:
                safe_name = "report"
            try:
                pdf_bytes = _build_export_pdf(state, last_query)
                st.download_button(
                    label="Download",
                    data=pdf_bytes,
                    file_name=f"{safe_name}_{datetime.now().strftime('%Y-%m-%d')}.pdf",
                    mime="application/pdf",
                    use_container_width=False,
                )
            except ImportError:
                st.error("PDF export requires `reportlab`. Install it with: pip install reportlab")

    elif status == "error":
        if st.session_state.polling:
            st.session_state.polling = False
            st.session_state.final_state = state
            st.rerun()

        err = state.get("error", "Unknown error.")
        with result_placeholder.container():
            st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="result-box result-error">⚠ {err}</div>',
                        unsafe_allow_html=True)

    else:
        if st.session_state.polling:
            time.sleep(POLL_INTERVAL)
            st.rerun()
