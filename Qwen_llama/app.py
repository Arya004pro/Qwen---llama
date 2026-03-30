"""app.py — CLI analytics assistant (fully generalised).

Works with ANY dataset loaded into DuckDB.  Entity/metric detection,
SQL generation, and display formatting all read the live schema.
"""

from state.conversation_state import ConversationState
from llm.qwen_intent import detect_ambiguity
from llm.llama_schema import extract_schema

from db.sql_builder import build_sql
from db.executor import run_query
from db.duckdb_connection import get_read_connection
from utils.date_parser import (
    parse_date_range,
    parse_comparison_date_ranges,
    parse_both_date_ranges,
)
from utils.token_logger import reset_session_totals, get_session_totals

print("AI Analytics Assistant (type 'exit' to quit)")

state = ConversationState()


# ── Schema helpers (live) ─────────────────────────────────────────────────────

def _get_available_entities_and_metrics():
    """Read the live schema and return entity/metric column lists for the ambiguity detector."""
    entities = []
    metrics  = []
    try:
        conn = get_read_connection()
        tables = [
            r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_name NOT LIKE '_raw_%'"
            ).fetchall()
        ]
        for t in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            for col, dtype, *_ in cols:
                d = str(dtype).upper()
                c = col.lower()
                if any(k in c for k in ("date", "time", "created", "updated", "timestamp")):
                    continue
                if any(tp in d for tp in ("VARCHAR", "CHAR", "TEXT", "STRING")):
                    if not c.endswith("_id") and not c.endswith("_key"):
                        entities.append(col)
                elif any(tp in d for tp in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
                    if not c.endswith("_id") and not c.endswith("_key"):
                        metrics.append(col)
        conn.close()
    except Exception:
        pass
    return entities[:10], metrics[:10]


def _detect_metric_is_monetary(metric_col: str) -> bool:
    """Check if a metric column looks like a monetary value."""
    if not metric_col:
        return False
    m = metric_col.lower()
    return any(k in m for k in (
        "price", "fare", "amount", "revenue", "sales", "earning",
        "cost", "total", "final", "profit", "payment", "fee",
    ))


# ── Formatting ────────────────────────────────────────────────────────────────

def _fmt_indian(val, prefix, decimals=2):
    if val is None:
        return "—"
    try:
        f_val = float(val)
    except (ValueError, TypeError):
        return str(val)

    sign = "-" if f_val < 0 else ""
    abs_val = abs(f_val)

    s = f"{abs_val:.{decimals}f}"
    parts = s.split('.')
    integer_part = parts[0]
    decimal_part = parts[1] if len(parts) > 1 else ""

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

    formatted = f"{sign}{prefix}{res}"
    if decimals > 0:
        formatted += f".{decimal_part}"
    return formatted


def fmt_value(value, prefix):
    return _fmt_indian(value, prefix)


def delta_str(v1, v2, prefix):
    if v1 is None or v2 is None:
        return "N/A"
    delta = v2 - v1
    sign  = "+" if delta >= 0 else ""
    pct   = (delta / v1 * 100) if v1 != 0 else float("inf")
    return f"{sign}{_fmt_indian(abs(delta), prefix)}  ({sign}{pct:.1f}%)"


def print_token_summary():
    totals = get_session_totals()
    if totals["total_tokens"] > 0:
        print(
            f"\n  [token summary] "
            f"prompt={totals['prompt_tokens']}  "
            f"completion={totals['completion_tokens']}  "
            f"total={totals['total_tokens']}"
        )


# ── Main loop ─────────────────────────────────────────────────────────────────

while True:
    user_input = input("\nUser: ").strip()

    if user_input.lower() in ["exit", "quit", "q"]:
        print("Assistant: Goodbye 👋")
        break

    reset_session_totals()
    state.update_from_user(user_input)

    if state.entity is None and state.metric is not None and state.ranking is None:
        # Build a dynamic prompt from loaded schema
        avail_entities, _ = _get_available_entities_and_metrics()
        if avail_entities:
            examples = ", ".join(
                e.replace("_name", "").replace("_", " ")
                for e in avail_entities[:6]
            )
            print(f"Assistant: Which dimension are you asking about? (e.g. {examples})")
        else:
            print("Assistant: Which dimension are you asking about?")
        continue

    if not state.is_complete():
        avail_entities, avail_metrics = _get_available_entities_and_metrics()
        known_state = {
            "entity":     state.entity,
            "metric":     state.metric,
            "time_range": state.time_range,
            "ranking":    state.ranking,
        }
        reply = detect_ambiguity(
            user_input, known_state,
            available_entities=avail_entities,
            available_metrics=avail_metrics,
        )
        if reply.strip() != "CLEAR":
            print("Assistant:", reply)
            print_token_summary()
            continue

    if not state.is_complete():
        avail_entities, avail_metrics = _get_available_entities_and_metrics()
        if state.entity is None:
            examples = ", ".join(
                e.replace("_name", "").replace("_", " ")
                for e in avail_entities[:6]
            ) if avail_entities else "the available dimensions"
            print(f"Assistant: Which dimension? (e.g. {examples})")
        elif state.metric is None:
            examples = ", ".join(
                m.replace("_", " ")
                for m in avail_metrics[:6]
            ) if avail_metrics else "revenue, quantity, count"
            print(f"Assistant: What should I measure? (e.g. {examples})")
        elif state.time_range is None:
            print("Assistant: What time period? (e.g. March 2024, Q1 2024, or 2023 vs 2024)")
        continue

    try:
        extract_schema(
            entity=state.entity,
            metric=state.metric,
            time_range=state.time_range,
            ranking=state.ranking,
        )
    except Exception:
        pass

    if state.ranking is None:
        state.ranking = "aggregate" if state.is_comparison else "top"
    if state.ranking in ("top", "bottom") and state.top_n <= 0:
        state.top_n = 5

    # Dynamic display label and prefix
    entity_label = state.get_entity_display_label()
    prefix = "₹" if _detect_metric_is_monetary(state.metric) else ""

    # ──────────────────────────────────────────────────────────────────────
    # Build SQL dynamically from schema
    # ──────────────────────────────────────────────────────────────────────
    parsed_intent = {
        "entity":     state.entity,
        "metric":     state.metric,
        "query_type": state.ranking if state.ranking != "top_growth" else "top_n",
        "top_n":      state.top_n,
        "filters":    {},
    }

    # ──────────────────────────────────────────────────────────────────────
    # INTERSECTION MODE
    # ──────────────────────────────────────────────────────────────────────
    if state.is_intersection:
        try:
            (start1, end1), (start2, end2) = parse_both_date_ranges(
                state.raw_time_text or user_input
            )
        except ValueError:
            print("Assistant: I couldn't parse the two time periods. "
                  "Try: 'Top 3 items in both January and March 2024'")
            state = ConversationState()
            continue

        import calendar as _cal
        p1 = f"{_cal.month_name[start1.month]} {start1.year}"
        p2 = f"{_cal.month_name[start2.month]} {start2.year}"

        sql = build_sql({**parsed_intent, "query_type": "top_n"})
        if not sql:
            print("Assistant: I can't answer this type of question for the current dataset.")
            state = ConversationState()
            continue

        BIG_LIMIT = 500
        try:
            rows1 = run_query(sql, (start1, end1, BIG_LIMIT))
            rows2 = run_query(sql, (start2, end2, BIG_LIMIT))
        except Exception as e:
            print(f"Assistant: Database error — {e}")
            state = ConversationState()
            continue

        dict1 = {str(r[0]): float(r[1]) for r in rows1 if r[1] is not None}
        dict2 = {str(r[0]): float(r[1]) for r in rows2 if r[1] is not None}
        common_names = set(dict1) & set(dict2)

        if not common_names:
            print(f"Assistant: No {entity_label} found in BOTH {p1} AND {p2}.")
            print_token_summary()
            state = ConversationState()
            continue

        combined = sorted(
            [(n, dict1[n] + dict2[n]) for n in common_names],
            key=lambda x: x[1], reverse=True,
        )[: state.top_n]

        print(f"\nAssistant: 🔀 Top {len(combined)} {entity_label} present in "
              f"BOTH {p1} AND {p2} (combined {state.metric}):")
        for i, (name, value) in enumerate(combined, start=1):
            print(f"  {i}. {name} — {_fmt_indian(value, prefix)}")

        print_token_summary()
        state = ConversationState()
        continue

    # ──────────────────────────────────────────────────────────────────────
    # COMPARISON MODE
    # ──────────────────────────────────────────────────────────────────────
    if state.is_comparison:
        sql = build_sql(parsed_intent)
        if not sql:
            print("Assistant: I can't answer this type of question for the current dataset.")
            state = ConversationState()
            continue

        try:
            (start1, end1), (start2, end2) = parse_comparison_date_ranges(
                state.raw_time_text or user_input
            )
        except ValueError:
            print("Assistant: I couldn't parse the two time periods. "
                  "Try: 'Compare revenue in March vs April 2024'")
            state = ConversationState()
            continue

        import calendar as _cal
        p1 = f"{_cal.month_name[start1.month]} {start1.year}"
        p2 = f"{_cal.month_name[start2.month]} {start2.year}"

        try:
            if state.ranking == "aggregate":
                rows1 = run_query(sql, (start1, end1))
                rows2 = run_query(sql, (start2, end2))
                v1 = float(rows1[0][0]) if rows1 and rows1[0][0] is not None else None
                v2 = float(rows2[0][0]) if rows2 and rows2[0][0] is not None else None
                print(f"\nAssistant: 📊 {state.metric.replace('_', ' ').title()} Comparison: {p1} vs {p2}")
                print("  " + "─" * 46)
                print(f"  {p1:<22} {fmt_value(v1, prefix) if v1 is not None else 'N/A'}")
                print(f"  {p2:<22} {fmt_value(v2, prefix) if v2 is not None else 'N/A'}")
                print(f"  {'Δ Change':<22} {delta_str(v1, v2, prefix)}")
            else:
                rows1 = run_query(sql, (start1, end1, state.top_n))
                rows2 = run_query(sql, (start2, end2, state.top_n))
                dict1 = {str(r[0]): float(r[1]) for r in rows1 if r[1] is not None}
                dict2 = {str(r[0]): float(r[1]) for r in rows2 if r[1] is not None}
                all_names = list(dict.fromkeys(
                    [str(r[0]) for r in rows1] + [str(r[0]) for r in rows2]
                ))
                rank_label = "Top" if state.ranking == "top" else "Bottom"
                print(f"\nAssistant: 📊 {rank_label} {state.top_n} {entity_label} "
                      f"by {state.metric.replace('_', ' ')}: {p1} vs {p2}")
                print(f"  {'#':>3}  {'Name':<28}  {p1:>16}  {p2:>16}  {'Δ Change':>14}")
                print("  " + "─" * 84)
                for i, name in enumerate(all_names, start=1):
                    v1 = dict1.get(name)
                    v2 = dict2.get(name)
                    col1 = fmt_value(v1, prefix) if v1 is not None else "—"
                    col2 = fmt_value(v2, prefix) if v2 is not None else "—"
                    d    = delta_str(v1, v2, prefix)
                    print(f"  {i:>3}. {name:<28}  {col1:>16}  {col2:>16}  {d:>14}")
        except Exception as e:
            print(f"Assistant: Database error — {e}")

        print_token_summary()
        state = ConversationState()
        continue

    # ──────────────────────────────────────────────────────────────────────
    # NORMAL (single period) MODE
    # ──────────────────────────────────────────────────────────────────────
    sql = build_sql(parsed_intent)
    if not sql:
        print("Assistant: I can't build a query for this combination. Try rephrasing.")
        state = ConversationState()
        continue

    try:
        start_date, end_date = parse_date_range(state.time_range, state.raw_time_text)
    except ValueError:
        print("Assistant: I couldn't understand the date range. Can you rephrase it?")
        state = ConversationState()
        continue

    rows = (
        run_query(sql, (start_date, end_date))
        if state.ranking == "aggregate"
        else run_query(sql, (start_date, end_date, state.top_n))
    )

    if not rows or rows[0][0] is None:
        print("Assistant: No data available for the selected period.")
    elif state.ranking == "aggregate":
        value = rows[0][0]
        print(
            f"Assistant: Total {state.metric.replace('_', ' ')} between "
            f"{start_date:%d %B %Y} and {end_date:%d %B %Y} "
            f"is {_fmt_indian(value, prefix)}"
        )
    else:
        rank_label = "Top" if state.ranking == "top" else "Bottom"
        print(
            f"Assistant: {rank_label} {state.top_n} {entity_label} "
            f"by {state.metric.replace('_', ' ')} "
            f"between {start_date:%d %B %Y} and {end_date:%d %B %Y}:"
        )
        for i, (name, value) in enumerate(rows, start=1):
            print(f"  {i}. {name} — {_fmt_indian(value, prefix)}")

    print_token_summary()
    state = ConversationState()