"""Semantic layer foundation for business analytics.

Purpose:
- Abstract raw schema columns into business-friendly dimensions and metrics.
- Provide prompt-time semantic hints for LLMs.
- Resolve parsed intent terms (entity/metric) to concrete physical columns.
"""

from __future__ import annotations

from typing import Any

from db.duckdb_connection import get_read_connection


_DIMENSION_HINTS = (
    "name", "title", "label", "category", "city", "state", "region", "segment",
    "brand", "warehouse", "store", "location", "driver", "customer", "product",
    "item", "vendor", "channel", "department", "team",
)

_METRIC_HINTS = (
    "revenue", "sales", "amount", "total", "fare", "earning", "price", "cost",
    "quantity", "qty", "units", "volume", "discount", "commission", "distance",
    "duration", "score", "rate", "profit", "margin",
)


def _is_text(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))


def _is_numeric(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"))


def _is_date_like(name: str, dtype: str) -> bool:
    n = name.lower()
    d = dtype.upper()
    return (
        any(k in n for k in ("date", "time", "timestamp", "created", "updated"))
        or any(t in d for t in ("DATE", "TIMESTAMP"))
    )


def _normalize_token(text: str) -> str:
    t = (text or "").lower().strip().replace("_", " ")
    t = " ".join(t.split())
    if t.endswith("s") and len(t) > 3:
        t = t[:-1]
    return t


def _semantic_name_from_col(col: str) -> str:
    c = col.lower()
    for suffix in ("_name", "_title", "_label", "_id", "_code", "_key", "_uuid"):
        if c.endswith(suffix):
            return c[: -len(suffix)]
    return c


def _collect_columns(conn, tables: list[str]) -> dict[str, list[tuple[str, str]]]:
    out: dict[str, list[tuple[str, str]]] = {}
    for table in tables:
        try:
            cols = [(c[0], c[1].upper()) for c in conn.execute(f'DESCRIBE "{table}"').fetchall()]
            out[table] = cols
        except Exception:
            continue
    return out


def build_semantic_catalog(conn, tables: list[str]) -> dict[str, Any]:
    columns_by_table = _collect_columns(conn, tables)
    dimensions: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    # Dimensions
    for table, cols in columns_by_table.items():
        col_set = {c.lower() for c, _ in cols}
        for col, dtype in cols:
            n = col.lower()
            if not _is_text(dtype):
                continue
            if _is_date_like(col, dtype):
                continue
            if not (n == "name" or any(h in n for h in _DIMENSION_HINTS)):
                continue

            base = _semantic_name_from_col(n)
            key_col = None
            for cand in (f"{base}_id", f"{base}_code", f"{base}_key", f"{base}_uuid", "id"):
                if cand in col_set:
                    key_col = cand
                    break

            aliases = {
                base,
                n.replace("_", " "),
                base.replace("_", " "),
                f"{base}s".replace("_", " "),
            }
            dimensions.append(
                {
                    "name": base,
                    "table": table,
                    "label_column": n,
                    "key_column": key_col,
                    "aliases": sorted(a for a in aliases if a.strip()),
                }
            )

    # Metrics
    seen_metric_keys: set[tuple[str, str, str]] = set()
    for table, cols in columns_by_table.items():
        col_set = {c.lower() for c, _ in cols}

        for col, dtype in cols:
            n = col.lower()
            if not _is_numeric(dtype):
                continue
            if _is_date_like(col, dtype):
                continue
            if n.endswith("_id"):
                continue

            semantic = _semantic_name_from_col(n)
            aliases = {semantic, n.replace("_", " ")}

            if any(k in n for k in ("revenue", "sales", "amount", "fare", "earning", "total", "final")):
                aliases.update({"revenue", "sales", "earnings"})
            if any(k in n for k in ("quantity", "qty", "units", "volume")):
                aliases.update({"quantity", "units", "volume"})
            if "discount" in n:
                aliases.add("discount")
            if "commission" in n:
                aliases.add("commission")

            key = (table, n, "sum")
            if key not in seen_metric_keys:
                metrics.append(
                    {
                        "name": semantic,
                        "table": table,
                        "column": n,
                        "agg": "sum",
                        "aliases": sorted(a for a in aliases if a.strip()),
                    }
                )
                seen_metric_keys.add(key)

            # Average semantic metric for same physical column
            avg_key = (table, n, "avg")
            if avg_key not in seen_metric_keys:
                metrics.append(
                    {
                        "name": f"avg_{semantic}",
                        "table": table,
                        "column": n,
                        "agg": "avg",
                        "aliases": sorted({f"average {semantic.replace('_', ' ')}", f"avg {semantic.replace('_', ' ')}"}),
                    }
                )
                seen_metric_keys.add(avg_key)

        # Count metric
        count_id = next(
            (c for c in col_set if c.endswith("_id") and any(k in c for k in ("order", "ride", "trip", "invoice", "booking"))),
            None,
        )
        if count_id:
            metrics.append(
                {
                    "name": f"{table}_count",
                    "table": table,
                    "column": count_id,
                    "agg": "count_distinct",
                    "aliases": ["count", "orders", "bookings", "trips", "rides", "number of"],
                }
            )

    return {"dimensions": dimensions, "metrics": metrics}


def render_semantic_layer_lines(conn, tables: list[str]) -> list[str]:
    catalog = build_semantic_catalog(conn, tables)
    dims = catalog["dimensions"][:12]
    mets = catalog["metrics"][:16]

    lines = ["Semantic Layer", "--------------"]
    lines += [
        "Use business terms first, then map to physical columns listed below.",
        "Dimension = what to group by; Metric = what to aggregate.",
    ]

    lines += ["", "Dimensions (semantic -> physical)"]
    if dims:
        for d in dims:
            key_part = f', key="{d["key_column"]}"' if d.get("key_column") else ""
            alias_preview = ", ".join(d["aliases"][:4])
            lines.append(
                f'  {d["name"]} -> table "{d["table"]}", label="{d["label_column"]}"{key_part}; aliases: {alias_preview}'
            )
    else:
        lines += ["  (No dimensions detected)"]

    lines += ["", "Metrics (semantic -> aggregation)"]
    if mets:
        for m in mets:
            if m["agg"] == "sum":
                expr = f'SUM("{m["column"]}")'
            elif m["agg"] == "avg":
                expr = f'AVG("{m["column"]}")'
            else:
                expr = f'COUNT(DISTINCT "{m["column"]}")'
            alias_preview = ", ".join(m["aliases"][:4])
            lines.append(
                f'  {m["name"]} -> {expr} on "{m["table"]}"; aliases: {alias_preview}'
            )
    else:
        lines += ["  (No metrics detected)"]

    lines += [
        "",
        "Semantic Resolution Rules",
        "-------------------------",
        "- Prefer semantic aliases from this section over hardcoded column assumptions.",
        "- If a label is non-unique, group by key+label when key is available.",
        '- For counts, prefer COUNT(DISTINCT order-like id) over COUNT(*).',
    ]
    return lines


def resolve_intent_with_semantic_layer(parsed: dict[str, Any], user_query: str = "") -> dict[str, Any]:
    """
    Resolve parsed entity/metric tokens using live semantic catalog.
    Keeps compatibility with existing downstream SQL generation contracts.
    """
    try:
        conn = get_read_connection()
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_%'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        catalog = build_semantic_catalog(conn, tables)
        all_cols = {
            c[0].lower()
            for t in tables
            for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
        }
    except Exception:
        return parsed
    finally:
        try:
            conn.close()
        except Exception:
            pass

    out = dict(parsed)
    entity = (out.get("entity") or "").strip().lower()
    metric = (out.get("metric") or "").strip().lower()

    # Entity resolution
    if entity and entity not in all_cols:
        target = _normalize_token(entity)
        for d in catalog["dimensions"]:
            alias_tokens = {_normalize_token(d["name"])} | {_normalize_token(a) for a in d["aliases"]}
            if target in alias_tokens:
                out["entity"] = d["label_column"]
                out["semantic_entity"] = d["name"]
                if d.get("key_column"):
                    out["_entity_group_key"] = d["key_column"]
                break

    # Metric resolution
    metric_known = (
        metric in all_cols
        or metric == "count"
        or (metric.startswith("avg_") and metric[4:] in all_cols)
    )
    if metric and not metric_known:
        target = _normalize_token(metric)
        candidates: list[tuple[int, dict[str, Any]]] = []
        for m in catalog["metrics"]:
            alias_tokens = {_normalize_token(m["name"])} | {_normalize_token(a) for a in m["aliases"]}
            if target not in alias_tokens:
                continue

            score = 1
            col = (m.get("column") or "").lower()
            name_norm = _normalize_token(m["name"])
            if target == name_norm:
                score += 4
            if m["agg"] == "sum":
                score += 1

            if target in {"revenue", "sales", "earning", "earnings", "income"}:
                if any(k in col for k in ("revenue", "sales", "amount", "fare", "earning", "total", "final")):
                    score += 5
                if "unit_price" in col or col.startswith("unit_"):
                    score -= 4

            if target in {"quantity", "units", "volume"} and any(k in col for k in ("quantity", "qty", "units", "volume")):
                score += 4

            if target.startswith("avg") and m["agg"] == "avg":
                score += 5

            candidates.append((score, m))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best = candidates[0][1]
            out["semantic_metric"] = best["name"]
            if best["agg"] == "sum":
                out["metric"] = best["column"]
            elif best["agg"] == "avg":
                out["metric"] = f'avg_{best["column"]}'
            else:
                out["metric"] = "count"
                out["_count_distinct_key"] = best["column"]

    return out
