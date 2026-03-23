import streamlit as st
import streamlit.components.v1 as components
import requests
import time

API          = "http://localhost:3121"
POLL_INTERVAL = 0.7

STEPS = [
    {"label": "Receive",      "sub": "REST entry point",    "icon": "⤵"},
    {"label": "Parse intent", "sub": "Qwen 3-32B",          "icon": "◈"},
    {"label": "Ambiguity",    "sub": "Clarification check", "icon": "?"},
    {"label": "Text → SQL",   "sub": "LLaMA 3.1-8B",        "icon": "{}"},
    {"label": "Execute SQL",  "sub": "PostgreSQL",           "icon": "▶"},
    {"label": "Format",       "sub": "Chart.js result",     "icon": "✦"},
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

def api_ok():
    try:
        requests.get(f"{API}/queries", timeout=2)
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

def fetch_chart_html(query_id):
    try:
        r = requests.get(f"{API}/query/{query_id}/chart", timeout=10)
        r.raise_for_status()
        return r.text
    except Exception as e:
        return f"<p style='color:#ef4444'>Could not load chart: {e}</p>"

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
        if t:
            time_html = f'<div class="step-time">{t:.1f}s</div>'
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
    st.dataframe(rows, use_container_width=True, hide_index=True)
    if totals and totals.get("total_tokens"):
        c1, c2, c3 = st.columns(3)
        c1.metric("Prompt tokens",     totals.get("prompt_tokens", 0))
        c2.metric("Completion tokens", totals.get("completion_tokens", 0))
        c3.metric("Total tokens",      totals.get("total_tokens", 0))

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
    st.markdown('<div class="section-lbl">Ask a question</div>', unsafe_allow_html=True)

    query_input = st.text_input(
        "query", label_visibility="collapsed",
        placeholder="e.g. Top 5 products by revenue in March 2024",
        disabled=st.session_state.polling,
    )
    send_col, status_col = st.columns([1, 3])
    send_btn            = send_col.button("Send", disabled=st.session_state.polling,
                                          use_container_width=True, type="primary")
    status_placeholder  = status_col.empty()
    clarify_placeholder = st.empty()
    result_placeholder  = st.empty()

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

# ── Send handler ──────────────────────────────────────────────────────────────
if send_btn and query_input.strip():
    st.session_state.polling        = True
    st.session_state.final_state    = None
    st.session_state.current_status = ""
    st.session_state.step_times     = {}
    st.session_state.last_completed = -1
    st.session_state.poll_start     = time.time()
    try:
        resp = submit_query(query_input.strip(),
                            session_id=st.session_state.pending_session)
        st.session_state.query_id        = resp["queryId"]
        st.session_state.pending_session = None
        st.rerun()
    except Exception as e:
        st.session_state.polling = False
        st.error(f"Failed to submit: {e}")

# ── Polling loop ──────────────────────────────────────────────────────────────
if st.session_state.polling and st.session_state.query_id:
    try:
        state         = fetch_state(st.session_state.query_id)
        status        = state.get("status", "")
        completed_idx = STATUS_MAP.get(status, -1)
        is_error      = status == "error"
        elapsed       = time.time() - (st.session_state.poll_start or time.time())

        # Record step completion times
        if completed_idx > st.session_state.last_completed:
            for idx in range(st.session_state.last_completed + 1, completed_idx + 1):
                if idx not in st.session_state.step_times:
                    st.session_state.step_times[idx] = round(elapsed, 1)
            st.session_state.last_completed = completed_idx

        with steps_placeholder.container():
            render_steps(completed_idx, is_error, st.session_state.step_times)

        status_placeholder.caption(f"Status: `{status}`")

        if state.get("generated_sql"):
            with sql_placeholder.container():
                st.markdown(f'<div class="sql-box">{state["generated_sql"].strip()}</div>',
                            unsafe_allow_html=True)

        if state.get("token_usage"):
            with token_placeholder.container():
                render_tokens(state.get("token_usage"), state.get("token_totals"))

        # ── Terminal states ──────────────────────────────────────────────────
        if status == "needs_clarification":
            st.session_state.polling         = False
            st.session_state.pending_session = st.session_state.query_id
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
            st.session_state.polling     = False
            st.session_state.final_state = state
            text = state.get("formattedText", "Query complete.")
            with result_placeholder.container():
                st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="result-box">{text}</div>', unsafe_allow_html=True)
                if state.get("chart_config"):
                    st.markdown("<br>", unsafe_allow_html=True)
                    with st.expander("📈 View interactive chart", expanded=True):
                        chart_html = fetch_chart_html(st.session_state.query_id)
                        components.html(chart_html, height=520, scrolling=False)
            st.session_state.history.append({"query": query_input or "(reply)", "result": text})
            status_placeholder.caption("Done ✓ — ask another question")

        elif status == "error":
            st.session_state.polling = False
            err = state.get("error", "Unknown error.")
            with result_placeholder.container():
                st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="result-box result-error">⚠ {err}</div>',
                            unsafe_allow_html=True)
            status_placeholder.caption("Pipeline error — check Motia logs")

        else:
            time.sleep(POLL_INTERVAL)
            st.rerun()

    except Exception as e:
        st.session_state.polling = False
        st.error(f"Polling error: {e}")