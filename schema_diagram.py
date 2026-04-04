"""schema_diagram.py

Drop-in replacement for render_schema_section() in streamlit_app.py.

Usage in streamlit_app.py:
    from schema_diagram import render_schema_diagram
    # Replace the render_schema_section() call with:
    render_schema_diagram()
"""

import json
from datetime import datetime
import streamlit as st
import streamlit.components.v1 as components
import requests


API = "http://127.0.0.1:3121"

# ------ Column type classifier ------------------------------------------------------------------------------------------------------------------------------------------------------------

def _classify_col(name: str, dtype: str) -> str:
    n = name.lower()
    d = dtype.upper()
    if any(k in n for k in ("date", "time", "created", "updated", "timestamp", "at")):
        return "date"
    if any(k in n for k in ("_id", "_key", "_uuid", "_code")):
        return "fk"
    if any(t in d for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BIGINT", "REAL")):
        return "metric"
    return "text"


# ------ HTML ER diagram builder ---------------------------------------------------------------------------------------------------------------------------------------------------------

def _build_er_diagram_html(schema_data: dict) -> str:
    tables     = schema_data.get("tables", [])
    rels       = schema_data.get("relationships", [])

    if not tables:
        return "<p style='color:#64748b;text-align:center;padding:40px'>No data loaded yet.</p>"

    tables_json = json.dumps([
        {
            "name": t["table"],
            "columns": [
                {
                    "name": c["name"],
                    "type": c["type"],
                    "kind": _classify_col(c["name"], c["type"]),
                }
                for c in t.get("columns", [])
            ],
        }
        for t in tables
    ])

    rels_json = json.dumps(rels)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: #080c14;
    font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', Consolas, monospace;
    color: #c9d3e0;
    overflow: hidden;
    user-select: none;
  }}

  #canvas-wrap {{
    position: relative;
    width: 100%;
    height: 520px;
    overflow: hidden;
    cursor: grab;
  }}
  #canvas-wrap.dragging {{ cursor: grabbing; }}

  #scene {{
    position: absolute;
    top: 0; left: 0;
    transform-origin: 0 0;
  }}

  svg#lines {{
    position: absolute;
    top: 0; left: 0;
    pointer-events: none;
    overflow: visible;
  }}

  .table-node {{
    position: absolute;
    background: #0e1420;
    border: 1px solid #1e2d45;
    border-radius: 8px;
    min-width: 260px;
    max-width: 360px;
    box-shadow: 0 4px 24px rgba(0,0,0,0.5), 0 0 0 1px rgba(99,179,237,0.05);
    transition: box-shadow 0.2s, border-color 0.2s;
    cursor: move;
  }}
  .table-node:hover {{
    border-color: rgba(99,179,237,0.35);
    box-shadow: 0 6px 32px rgba(0,0,0,0.6), 0 0 0 1px rgba(99,179,237,0.12);
  }}
  .table-node.connected {{
    border-color: rgba(99,179,237,0.25);
  }}

  .table-header {{
    padding: 9px 12px 8px;
    border-bottom: 1px solid #1e2d45;
    display: flex;
    align-items: center;
    gap: 7px;
  }}
  .table-icon {{
    width: 18px; height: 18px;
    background: rgba(99,179,237,0.12);
    border: 1px solid rgba(99,179,237,0.25);
    border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    font-size: 9px; color: #63b3ed; flex-shrink: 0;
  }}
  .table-name {{
    font-size: 11px;
    font-weight: 700;
    color: #e2e8f0;
    letter-spacing: 0.03em;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .row-count {{
    margin-left: auto;
    font-size: 9px;
    color: #475569;
    flex-shrink: 0;
  }}

  .col-list {{
    padding: 4px 0 6px;
    max-height: none;
    overflow-y: visible;
  }}
  .col-list::-webkit-scrollbar {{ width: 3px; }}
  .col-list::-webkit-scrollbar-track {{ background: transparent; }}
  .col-list::-webkit-scrollbar-thumb {{ background: #1e2d45; border-radius: 2px; }}

  .col-row {{
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 3px 12px;
    font-size: 10px;
    line-height: 1.4;
    transition: background 0.1s;
  }}
  .col-row:hover {{ background: rgba(255,255,255,0.03); }}
  .col-row.highlighted {{ background: rgba(99,179,237,0.08); }}

  .col-dot {{
    width: 6px; height: 6px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .dot-date   {{ background: #63b3ed; }}
  .dot-fk     {{ background: #f6ad55; }}
  .dot-metric {{ background: #68d391; }}
  .dot-text   {{ background: #4a5568; }}

  .col-name {{
    color: #94a3b8;
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .col-row.highlighted .col-name {{ color: #cbd5e1; }}

  .col-type {{
    font-size: 8.5px;
    color: #334155;
    text-align: right;
    flex-shrink: 0;
    max-width: 70px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}

  /* Relationship lines */
  .rel-line {{
    fill: none;
    stroke-width: 1.5;
    opacity: 0.55;
    transition: opacity 0.2s;
  }}
  .rel-line.fk   {{ stroke: #f6ad55; stroke-dasharray: none; }}
  .rel-line.dim  {{ stroke: #9f7aea; stroke-dasharray: 5,3; }}
  .rel-line:hover {{ opacity: 1; }}

  .rel-dot {{
    transition: opacity 0.2s;
  }}

  /* Legend */
  #legend {{
    position: absolute;
    bottom: 10px;
    left: 12px;
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
    pointer-events: none;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 9px;
    color: #475569;
  }}
  .legend-dot {{
    width: 7px; height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .legend-line-fk  {{
    width: 18px; height: 0;
    border-top: 1.5px solid #f6ad55;
  }}
  .legend-line-dim {{
    width: 18px; height: 0;
    border-top: 1.5px dashed #9f7aea;
  }}

  /* Controls */
  #controls {{
    position: absolute;
    top: 10px; right: 12px;
    display: flex;
    gap: 5px;
  }}
  .ctrl-btn {{
    background: rgba(14,20,32,0.9);
    border: 1px solid #1e2d45;
    border-radius: 5px;
    color: #64748b;
    font-size: 13px;
    width: 26px; height: 26px;
    display: flex; align-items: center; justify-content: center;
    cursor: pointer;
    transition: color 0.15s, border-color 0.15s;
  }}
  .ctrl-btn:hover {{ color: #94a3b8; border-color: #334155; }}

  /* No-rels note */
  #rel-note {{
    position: absolute;
    bottom: 30px;
    right: 12px;
    font-size: 9px;
    color: #334155;
  }}
</style>
</head>
<body>

<div id="canvas-wrap">
  <div id="scene">
    <svg id="lines" width="4000" height="4000"></svg>
  </div>
  <div id="controls">
    <div class="ctrl-btn" id="btn-fit" title="Fit to view">Fit</div>
    <div class="ctrl-btn" id="btn-zoom-in" title="Zoom in">+</div>
    <div class="ctrl-btn" id="btn-zoom-out" title="Zoom out">-</div>
  </div>
  <div id="legend">
    <div class="legend-item"><div class="legend-dot" style="background:#63b3ed"></div>Date/Time</div>
    <div class="legend-item"><div class="legend-dot" style="background:#f6ad55"></div>Key/ID</div>
    <div class="legend-item"><div class="legend-dot" style="background:#68d391"></div>Metric</div>
    <div class="legend-item"><div class="legend-dot" style="background:#4a5568"></div>Text</div>
    <div class="legend-item"><div class="legend-line-fk"></div>FK link</div>
    <div class="legend-item"><div class="legend-line-dim"></div>Shared dim</div>
  </div>
  <div id="rel-note"></div>
</div>

<script>
const TABLES = {tables_json};
const RELS   = {rels_json};

const scene     = document.getElementById('scene');
const svgLines  = document.getElementById('lines');
const wrap      = document.getElementById('canvas-wrap');
const TABLE_BY_NAME = Object.fromEntries(TABLES.map(t => [t.name, t]));
const COL_INDEX_BY_TABLE = Object.fromEntries(
  TABLES.map(t => [t.name, Object.fromEntries((t.columns || []).map((c, i) => [c.name, i]))])
);

// ------ State ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
let scale = 1, panX = 0, panY = 0;
let draggingNode = null, nodeOffX = 0, nodeOffY = 0;
let nodeOffXScene = 0, nodeOffYScene = 0;
let panStart = null, panOrigin = null;
let nodePositions = {{}};
let renderQueued = false;

// ------ Layout: relationship-aware, schema-agnostic ---------------------------------------------------
function tableHeight(t) {{
  const visibleRows = (t.columns || []).length;
  return 54 + visibleRows * 22 + 10;
}}
function queueRenderLines() {{
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {{
    renderQueued = false;
    renderLines();
  }});
}}
function computeLayout() {{
  nodePositions = {{}};
  const W = 280, H_GAP = 64, V_GAP = 30, INTRA_COL_GAP = 34;
  const startX = 24, startY = 24;
  const allNames = TABLES.map(t => t.name);
  const degree = Object.fromEntries(allNames.map(n => [n, 0]));
  const adj = Object.fromEntries(allNames.map(n => [n, new Set()]));
  const validRels = RELS.filter(r => adj[r.from_table] && adj[r.to_table]);
  validRels.forEach(r => {{
    adj[r.from_table].add(r.to_table);
    adj[r.to_table].add(r.from_table);
    degree[r.from_table] += 1;
    degree[r.to_table] += 1;
  }});
  const connected = allNames.filter(n => degree[n] > 0);
  const seeds = [...connected].sort((a, b) => (degree[b] - degree[a]) || a.localeCompare(b));
  const visited = new Set();
  let yCursor = startY;
  let maxX = startX + W;
  function layoutComponent(seed, compY) {{
    const q = [seed];
    const levels = {{ [seed]: 0 }};
    visited.add(seed);
    const byLevel = new Map();
    while (q.length) {{
      const cur = q.shift();
      const lvl = levels[cur] || 0;
      if (!byLevel.has(lvl)) byLevel.set(lvl, []);
      byLevel.get(lvl).push(cur);
      for (const nb of adj[cur]) {{
        if (visited.has(nb)) continue;
        visited.add(nb);
        levels[nb] = lvl + 1;
        q.push(nb);
      }}
    }}
    const maxLevel = Math.max(...byLevel.keys());
    const levelColsMap = new Map();
    for (let lvl = 0; lvl <= maxLevel; lvl++) {{
      const cnt = (byLevel.get(lvl) || []).length;
      const cols = cnt >= 10 ? 4 : (cnt >= 7 ? 3 : (cnt >= 4 ? 2 : 1));
      levelColsMap.set(lvl, cols);
    }}

    const levelXMap = new Map();
    let xCursor = startX;
    for (let lvl = 0; lvl <= maxLevel; lvl++) {{
      levelXMap.set(lvl, xCursor);
      const cols = levelColsMap.get(lvl) || 1;
      const levelWidth = cols * W + (cols - 1) * INTRA_COL_GAP;
      xCursor += levelWidth + H_GAP;
    }}

    let compBottom = compY;
    for (let lvl = 0; lvl <= maxLevel; lvl++) {{
      const nodes = (byLevel.get(lvl) || []).sort((a, b) => {{
        const da = degree[a], db = degree[b];
        if (db !== da) return db - da;
        return ((TABLE_BY_NAME[b]?.columns?.length || 0) - (TABLE_BY_NAME[a]?.columns?.length || 0));
      }});
      const levelCols = levelColsMap.get(lvl) || 1;
      const levelHeights = Array.from({{ length: levelCols }}, () => compY);
      const baseX = levelXMap.get(lvl) || startX;

      for (const name of nodes) {{
        let col = 0;
        for (let i = 1; i < levelCols; i++) {{
          if (levelHeights[i] < levelHeights[col]) col = i;
        }}
        const h = tableHeight(TABLE_BY_NAME[name]);
        const x = baseX + col * (W + INTRA_COL_GAP);
        const y = levelHeights[col];
        nodePositions[name] = {{ x, y, w: W, h }};
        levelHeights[col] = y + h + V_GAP;
        maxX = Math.max(maxX, x + W);
      }}

      const levelBottom = Math.max(...levelHeights) - V_GAP;
      compBottom = Math.max(compBottom, levelBottom);
    }}
    return compBottom;
  }}
  for (const seed of seeds) {{
    if (visited.has(seed)) continue;
    const bottom = layoutComponent(seed, yCursor);
    yCursor = bottom + 48;
  }}
  const isolated = allNames.filter(n => !visited.has(n));
  if (isolated.length) {{
    const cols = Math.max(2, Math.min(4, Math.ceil(Math.sqrt(isolated.length))));
    const colHeights = Array.from({{ length: cols }}, () => yCursor);
    isolated
      .sort((a, b) => (TABLE_BY_NAME[b]?.columns?.length || 0) - (TABLE_BY_NAME[a]?.columns?.length || 0))
      .forEach(name => {{
        let col = 0;
        for (let i = 1; i < cols; i++) {{
          if (colHeights[i] < colHeights[col]) col = i;
        }}
        const h = tableHeight(TABLE_BY_NAME[name]);
        const x = startX + col * (W + H_GAP);
        const y = colHeights[col];
        nodePositions[name] = {{ x, y, w: W, h }};
        colHeights[col] = y + h + V_GAP;
        maxX = Math.max(maxX, x + W);
      }});
  }}

  // Resolve dense overlaps after initial placement by greedily pushing nodes down.
  const nodes = Object.entries(nodePositions).sort((a, b) => {{
    if (a[1].y !== b[1].y) return a[1].y - b[1].y;
    return a[1].x - b[1].x;
  }});
  const OVERLAP_PAD_X = 22;
  const OVERLAP_PAD_Y = 16;
  for (let i = 0; i < nodes.length; i++) {{
    const [, a] = nodes[i];
    for (let j = 0; j < i; j++) {{
      const [, b] = nodes[j];
      const overlapsX = (a.x < b.x + b.w + OVERLAP_PAD_X) && (a.x + a.w + OVERLAP_PAD_X > b.x);
      const overlapsY = (a.y < b.y + b.h + OVERLAP_PAD_Y) && (a.y + a.h + OVERLAP_PAD_Y > b.y);
      if (overlapsX && overlapsY) {{
        a.y = b.y + b.h + OVERLAP_PAD_Y;
      }}
    }}
  }}

  let maxY = startY + 400;
  Object.values(nodePositions).forEach(p => {{
    maxY = Math.max(maxY, p.y + p.h + 80);
  }});
  svgLines.setAttribute('width', Math.max(maxX + 120, 1800));
  svgLines.setAttribute('height', Math.max(maxY, 1100));
}}

// ------ Render table nodes ------------------------------------------------------------------------------------------------------------------------------------------------------------
const nodeEls = {{}};

function renderNodes() {{
  Object.values(nodeEls).forEach(el => el.remove());
  for (const k in nodeEls) delete nodeEls[k];

  const connectedCols = new Set();
  const connectedTables = new Set();
  RELS.forEach(r => {{
    connectedCols.add(r.from_table + '.' + r.from_column);
    connectedCols.add(r.to_table   + '.' + r.to_column);
    connectedTables.add(r.from_table);
    connectedTables.add(r.to_table);
  }});

  TABLES.forEach(t => {{
    const pos = nodePositions[t.name];
    const div = document.createElement('div');
    div.className = 'table-node' + (connectedTables.has(t.name) ? ' connected' : '');
    div.id = 'node-' + t.name;
    div.style.left = pos.x + 'px';
    div.style.top  = pos.y + 'px';
    div.style.width = pos.w + 'px';

    const dotColors = {{ date:'dot-date', fk:'dot-fk', metric:'dot-metric', text:'dot-text' }};

    const colRows = t.columns.map(c => {{
      const isLinked = connectedCols.has(t.name + '.' + c.name);
      return `<div class="col-row ${{isLinked ? 'highlighted' : ''}}">
        <div class="col-dot ${{dotColors[c.kind] || 'dot-text'}}"></div>
        <div class="col-name">${{c.name}}</div>
        <div class="col-type">${{c.type.replace('VARCHAR','str').replace('DOUBLE','float').replace('BIGINT','int64').replace('INTEGER','int')}}</div>
      </div>`;
    }}).join('');

    div.innerHTML = `
      <div class="table-header">
        <div class="table-icon">[]</div>
        <div class="table-name">${{t.name}}</div>
      </div>
      <div class="col-list">${{colRows}}</div>`;

    scene.appendChild(div);
    nodeEls[t.name] = div;

    // Drag node
    div.addEventListener('mousedown', e => {{
      if (e.button !== 0) return;
      e.stopPropagation();
      draggingNode = t.name;
      const wrapRect = wrap.getBoundingClientRect();
      const mx = e.clientX - wrapRect.left;
      const my = e.clientY - wrapRect.top;
      nodeOffXScene = (mx - panX) / scale - nodePositions[t.name].x;
      nodeOffYScene = (my - panY) / scale - nodePositions[t.name].y;
      div.style.zIndex = 100;
      wrap.classList.add('dragging');
    }});
  }});
}}

// ------ Render SVG relationship lines ---------------------------------------------------------------------------------------------------------------------------
function getColAnchor(tableName, colName, side) {{
  const pos = nodePositions[tableName];
  const el  = nodeEls[tableName];
  if (!pos || !el) return null;

  const idxRaw = COL_INDEX_BY_TABLE[tableName]?.[colName];
  if (idxRaw === undefined || idxRaw === null) return null;
  const idx = idxRaw;

  // Prefer real rendered row geometry to avoid drift from CSS/font changes.
  const rows = el.querySelectorAll('.col-row');
  if (rows && rows[idx]) {{
    const rowEl = rows[idx];
    const y = pos.y + rowEl.offsetTop + rowEl.offsetHeight / 2;
    const x = side === 'right' ? pos.x + pos.w : pos.x;
    return {{ x, y }};
  }}

  // Fallback for rare timing cases where rows are not available yet.
  const headerEl = el.querySelector('.table-header');
  const headerH = headerEl ? headerEl.offsetHeight : 38;
  const rowH = (rows && rows.length && rows[0].offsetHeight) ? rows[0].offsetHeight : 22;
  const yOff = headerH + (idx + 0.5) * rowH;

  const x = side === 'right' ? pos.x + pos.w : pos.x;
  return {{ x, y: pos.y + yOff }};
}}

function renderLines() {{
  svgLines.innerHTML = '';
  document.getElementById('rel-note').textContent = '';

  if (RELS.length === 0) {{
    document.getElementById('rel-note').textContent =
      'No cross-table relationships detected in this dataset';
    return;
  }}

  // Precompute fan-out slots per table+column edge so links spread cleanly.
  const slotTotals = new Map();
  const slotUsed = new Map();
  RELS.forEach(r => {{
    const fromPos = nodePositions[r.from_table];
    const toPos   = nodePositions[r.to_table];
    if (!fromPos || !toPos) return;
    const leftFirst = fromPos.x <= toPos.x;
    const tA = leftFirst ? r.from_table : r.to_table;
    const cA = leftFirst ? r.from_column : r.to_column;
    const tB = leftFirst ? r.to_table : r.from_table;
    const cB = leftFirst ? r.to_column : r.from_column;
    const k1 = `${{tA}}|${{cA}}|right`;
    const k2 = `${{tB}}|${{cB}}|left`;
    slotTotals.set(k1, (slotTotals.get(k1) || 0) + 1);
    slotTotals.set(k2, (slotTotals.get(k2) || 0) + 1);
  }});

  function nextFanOffset(key) {{
    const used = slotUsed.get(key) || 0;
    const total = slotTotals.get(key) || 1;
    slotUsed.set(key, used + 1);
    // Centered offsets: -n..0..+n
    const centered = used - (total - 1) / 2;
    return centered * 14;
  }}

  let drawn = 0;
  RELS.forEach((r, i) => {{
    const isFK  = r.type === 'FK';
    const cls   = isFK ? 'fk' : 'dim';
    const color = isFK ? '#f6ad55' : '#9f7aea';

    const fromPos = nodePositions[r.from_table];
    const toPos   = nodePositions[r.to_table];
    if (!fromPos || !toPos) return;

    // Route by geometry (left-to-right) for visual clarity, independent of FK direction.
    const leftFirst = fromPos.x <= toPos.x;
    const tA = leftFirst ? r.from_table : r.to_table;
    const cA = leftFirst ? r.from_column : r.to_column;
    const tB = leftFirst ? r.to_table : r.from_table;
    const cB = leftFirst ? r.to_column : r.from_column;

    const a1 = getColAnchor(tA, cA, 'right');
    const a2 = getColAnchor(tB, cB, 'left');
    if (!a1 || !a2) return;
    const k1 = `${{tA}}|${{cA}}|right`;
    const k2 = `${{tB}}|${{cB}}|left`;
    const off1 = nextFanOffset(k1);
    const off2 = nextFanOffset(k2);
    const p1 = {{ x: a1.x, y: a1.y }};
    const p2 = {{ x: a2.x, y: a2.y }};
    const s1 = {{ x: a1.x + 16, y: a1.y + off1 }};
    const s2 = {{ x: a2.x - 16, y: a2.y + off2 }};

    const dx = Math.max(40, s2.x - s1.x);
    const c1 = {{ x: s1.x + Math.min(dx * 0.42, 180), y: s1.y }};
    const c2 = {{ x: s2.x - Math.min(dx * 0.42, 180), y: s2.y }};

    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    const d = `M${{p1.x}},${{p1.y}} L${{s1.x}},${{s1.y}} C${{c1.x}},${{c1.y}} ${{c2.x}},${{c2.y}} ${{s2.x}},${{s2.y}} L${{p2.x}},${{p2.y}}`;
    path.setAttribute('d', d);
    path.setAttribute('class', `rel-line ${{cls}}`);
    path.setAttribute('data-idx', i);

    // Tooltip on hover
    const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
    title.textContent = `${{r.from_table}}.${{r.from_column}} -> ${{r.to_table}}.${{r.to_column}} (${{r.type}}, ${{r.confidence}})`;
    path.appendChild(title);

    svgLines.appendChild(path);

    // Dots at endpoints
    [p1, p2].forEach(a => {{
      const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      circle.setAttribute('cx', a.x);
      circle.setAttribute('cy', a.y);
      circle.setAttribute('r', 3);
      circle.setAttribute('fill', color);
      circle.setAttribute('class', 'rel-dot');
      circle.setAttribute('opacity', 0.7);
      svgLines.appendChild(circle);
    }});
    drawn += 1;
  }});

  if (drawn === 0) {{
    document.getElementById('rel-note').textContent =
      'No renderable relationships for the current visible tables';
  }}
}}

// ------ Pan & zoom ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function applyTransform() {{
  scene.style.transform = `translate(${{panX}}px,${{panY}}px) scale(${{scale}})`;
}}

wrap.addEventListener('mousedown', e => {{
  if (draggingNode) return;
  if (e.button !== 0) return;
  panStart = {{ x: e.clientX, y: e.clientY }};
  panOrigin = {{ x: panX, y: panY }};
  wrap.classList.add('dragging');
}});

window.addEventListener('mousemove', e => {{
  if (draggingNode) {{
    const wrapRect = wrap.getBoundingClientRect();
    const mx = e.clientX - wrapRect.left;
    const my = e.clientY - wrapRect.top;
    const x = (mx - panX) / scale - nodeOffXScene;
    const y = (my - panY) / scale - nodeOffYScene;
    nodePositions[draggingNode].x = x;
    nodePositions[draggingNode].y = y;
    nodeEls[draggingNode].style.left = x + 'px';
    nodeEls[draggingNode].style.top  = y + 'px';
    queueRenderLines();
    return;
  }}
  if (panStart) {{
    panX = panOrigin.x + (e.clientX - panStart.x);
    panY = panOrigin.y + (e.clientY - panStart.y);
    applyTransform();
  }}
}});

window.addEventListener('mouseup', e => {{
  if (draggingNode) {{
    nodeEls[draggingNode].style.zIndex = '';
    draggingNode = null;
  }}
  panStart = null;
  wrap.classList.remove('dragging');
}});

wrap.addEventListener('wheel', e => {{
  e.preventDefault();
  const delta = e.deltaY > 0 ? 0.9 : 1.1;
  const rect = wrap.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  panX = mx - (mx - panX) * delta;
  panY = my - (my - panY) * delta;
  scale = Math.max(0.3, Math.min(2.5, scale * delta));
  applyTransform();
}}, {{ passive: false }});

// ------ Controls ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
document.getElementById('btn-zoom-in').addEventListener('click', () => {{
  scale = Math.min(2.5, scale * 1.25);
  applyTransform();
}});
document.getElementById('btn-zoom-out').addEventListener('click', () => {{
  scale = Math.max(0.3, scale * 0.8);
  applyTransform();
}});
document.getElementById('btn-fit').addEventListener('click', fitToView);

function fitToView() {{
  if (!TABLES.length) return;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  TABLES.forEach(t => {{
    const p = nodePositions[t.name];
    minX = Math.min(minX, p.x);
    minY = Math.min(minY, p.y);
    maxX = Math.max(maxX, p.x + p.w);
    maxY = Math.max(maxY, p.y + p.h);
  }});

  const pad  = 30;
  const wW   = wrap.clientWidth;
  const wH   = wrap.clientHeight;
  const cW   = maxX - minX + pad * 2;
  const cH   = maxY - minY + pad * 2;
  const fitted = Math.min(wW / cW, wH / cH, 1.1);
  scale = Math.max(0.62, fitted);
  panX  = (wW - cW * scale) / 2 - (minX - pad) * scale;
  panY  = (wH - cH * scale) / 2 - (minY - pad) * scale;
  applyTransform();
}}

// ------ Boot ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
computeLayout();
renderNodes();
renderLines();
setTimeout(fitToView, 50);
</script>
</body>
</html>"""


# ------ Public function (drop-in for render_schema_section) ------------------------------------------------------------------

def render_schema_diagram(view: str = "derived", refresh_nonce: int = 0):
    """
    Renders an interactive ER diagram of the loaded DuckDB schema.
    Drop-in replacement for render_schema_section() in streamlit_app.py.
    """
    try:
        v = (view or "derived").strip().lower()
        if v not in {"derived", "raw", "all"}:
            v = "derived"
        r = requests.get(
          f"{API}/schema",
          params={"view": v, "_nonce": int(refresh_nonce)},
          timeout=5,
          headers={"Cache-Control": "no-cache"},
        )
        r.raise_for_status()
        schema_data = r.json()
    except Exception as e:
        st.caption(f"Could not load schema: {e}")
        return

    tables = schema_data.get("tables", [])
    rels   = schema_data.get("relationships", [])

    if not tables:
        st.caption("No data loaded yet - upload a dataset to see the schema diagram.")
        return

    # Stats row
    col1, col2, col3 = st.columns(3)
    total_cols = sum(len(t.get("columns", [])) for t in tables)
    col1.metric("Tables", len(tables))
    col2.metric("Columns", total_cols)
    col3.metric("Relationships", len(rels))

    # Diagram
    html = _build_er_diagram_html(schema_data) + f"\n<!-- refresh_nonce:{int(refresh_nonce)} -->"
    components.html(html, height=680, scrolling=False)

    # Relationship details (collapsed)
    if rels:
        with st.expander(f"Relationship details ({len(rels)})", expanded=False):
            for r in rels:
                badge = "[HIGH]" if r.get("confidence") == "HIGH" else "[MED]"
                rel_type = r.get("type", "FK")
                icon = "->" if rel_type == "FK" else "<->"
                st.markdown(
                    f"{badge} `{r['from_table']}.{r['from_column']}` "
                    f"{icon} `{r['to_table']}.{r['to_column']}` "
                    f"*({rel_type}, {r.get('confidence','?')})*"
                )

    if st.button("Refresh diagram", key="refresh_schema_diagram", type="secondary"):
      st.session_state["schema_refresh_nonce"] = int(st.session_state.get("schema_refresh_nonce", 0)) + 1
      st.session_state["schema_last_refresh"] = datetime.now().strftime("%H:%M:%S")
      st.rerun()

