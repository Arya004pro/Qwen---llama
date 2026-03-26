from state.conversation_state import ConversationState
from llm.qwen_intent import detect_ambiguity
from llm.llama_schema import extract_schema

from db.sql_registry import SQL_REGISTRY
from db.executor import run_query
from utils.date_parser import (
    parse_date_range,
    parse_comparison_date_ranges,
    parse_both_date_ranges,           # NEW
)
from utils.token_logger import reset_session_totals, get_session_totals

print("AI Analytics Assistant (type 'exit' to quit)")

state = ConversationState()

ENTITY_LABELS = {
    "product":  "products",
    "customer": "customers",
    "city":     "cities",
    "category": "categories",
}


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


while True:
    user_input = input("\nUser: ").strip()

    if user_input.lower() in ["exit", "quit", "q"]:
        print("Assistant: Goodbye 👋")
        break

    reset_session_totals()
    state.update_from_user(user_input)

    if state.entity is None and any(w in user_input.lower() for w in ["revenue", "sales", "quantity"]):
        print("Assistant: Are you asking about products, customers, cities, or categories?")
        continue

    if not state.is_complete():
        known_state = {
            "entity":     state.entity,
            "metric":     state.metric,
            "time_range": state.time_range,
            "ranking":    state.ranking,
        }
        reply = detect_ambiguity(user_input, known_state)
        if reply.strip() != "CLEAR":
            print("Assistant:", reply)
            print_token_summary()
            continue

    if not state.is_complete():
        if state.entity is None:
            print("Assistant: Are you asking about products, customers, cities, or categories?")
        elif state.metric is None:
            print("Assistant: Should this be measured by revenue or quantity?")
        elif state.time_range is None:
            print("Assistant: What time period? (e.g. March 2024 or Jan to Jun 2024)")
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

    # ──────────────────────────────────────────────────────────────────────
    # INTERSECTION MODE — "both X and Y"
    # ──────────────────────────────────────────────────────────────────────
    if state.is_intersection:
        try:
            (start1, end1), (start2, end2) = parse_both_date_ranges(
                state.raw_time_text or user_input
            )
        except ValueError:
            print("Assistant: I couldn't parse the two time periods. "
                  "Try: 'Top 3 customers by revenue in both January and March 2024'")
            state = ConversationState()
            continue

        import calendar as _cal
        p1     = f"{_cal.month_name[start1.month]} {start1.year}"
        p2     = f"{_cal.month_name[start2.month]} {start2.year}"
        prefix = "₹" if state.metric == "revenue" else ""
        entity_label = ENTITY_LABELS[state.entity]

        # Fetch both periods using top-N SQL with a large limit so we capture
        # all entities that might appear in both sets.
        try:
            top_sql = SQL_REGISTRY[state.entity][state.metric]["top"]
        except KeyError:
            print("Assistant: I can't answer this type of question yet.")
            state = ConversationState()
            continue

        BIG_LIMIT = 500
        try:
            rows1 = run_query(top_sql, (start1, end1, BIG_LIMIT))
            rows2 = run_query(top_sql, (start2, end2, BIG_LIMIT))
        except Exception as e:
            print(f"Assistant: Database error — {e}")
            state = ConversationState()
            continue

        dict1 = {str(r[0]): float(r[1]) for r in rows1 if r[1] is not None}
        dict2 = {str(r[0]): float(r[1]) for r in rows2 if r[1] is not None}

        common_names = set(dict1) & set(dict2)

        if not common_names:
            print(f"Assistant: No {entity_label} placed orders in "
                  f"BOTH {p1} AND {p2}.")
            print_token_summary()
            state = ConversationState()
            continue

        # Rank by combined value, take top_n
        combined = sorted(
            [(n, dict1[n] + dict2[n]) for n in common_names],
            key=lambda x: x[1],
            reverse=True,
        )[: state.top_n]

        print(f"\nAssistant: 🔀 Top {len(combined)} {entity_label} present in "
              f"BOTH {p1} AND {p2} (combined {state.metric}):")
        for i, (name, value) in enumerate(combined, start=1):
            print(f"  {i}. {name} — {_fmt_indian(value, prefix)}")

        print_token_summary()
        state = ConversationState()
        continue

    # ──────────────────────────────────────────────────────────────────────
    # COMPARISON MODE — "X vs Y" / "from Q1 to Q2" / "growth…"
    # ──────────────────────────────────────────────────────────────────────
    if state.is_comparison:
        try:
            sql = SQL_REGISTRY[state.entity][state.metric][state.ranking]
        except KeyError:
            print("Assistant: I can't answer this type of question yet.")
            state = ConversationState()
            continue

        try:
            (start1, end1), (start2, end2) = parse_comparison_date_ranges(
                state.raw_time_text or user_input
            )
        except ValueError:
            print("Assistant: I couldn't parse the two time periods. "
                  "Try: 'Compare revenue in March vs April 2024' or "
                  "'Revenue growth from Q1 to Q2 2024'")
            state = ConversationState()
            continue

        import calendar as _cal
        p1     = f"{_cal.month_name[start1.month]} {start1.year}"
        p2     = f"{_cal.month_name[start2.month]} {start2.year}"
        prefix = "₹" if state.metric == "revenue" else ""
        entity_label = ENTITY_LABELS[state.entity]

        try:
            if state.ranking == "aggregate":
                rows1 = run_query(sql, (start1, end1))
                rows2 = run_query(sql, (start2, end2))
                v1 = float(rows1[0][0]) if rows1 and rows1[0][0] is not None else None
                v2 = float(rows2[0][0]) if rows2 and rows2[0][0] is not None else None
                print(f"\nAssistant: 📊 {state.metric.title()} Comparison: {p1} vs {p2}")
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
                      f"by {state.metric}: {p1} vs {p2}")
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
    try:
        sql = SQL_REGISTRY[state.entity][state.metric][state.ranking]
    except KeyError:
        print("Assistant: I can't answer this type of question yet.")
        state = ConversationState()
        continue

    try:
        start_date, end_date = parse_date_range(state.time_range, state.raw_time_text)
    except ValueError:
        print("Assistant: I couldn't understand the date range. Can you rephrase it?")
        state = ConversationState()
        continue

    prefix       = "₹" if state.metric == "revenue" else ""
    entity_label = ENTITY_LABELS[state.entity]

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
            f"Assistant: Total {state.metric} between "
            f"{start_date:%d %B %Y} and {end_date:%d %B %Y} "
            f"is {_fmt_indian(value, prefix)}"
        )
    else:
        rank_label = "Top" if state.ranking == "top" else "Bottom"
        print(
            f"Assistant: {rank_label} {state.top_n} {entity_label} by {state.metric} "
            f"between {start_date:%d %B %Y} and {end_date:%d %B %Y}:"
        )
        for i, (name, value) in enumerate(rows, start=1):
            print(f"  {i}. {name} — {_fmt_indian(value, prefix)}")

    print_token_summary()
    state = ConversationState()