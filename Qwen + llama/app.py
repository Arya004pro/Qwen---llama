from state.conversation_state import ConversationState
from llm.qwen_intent import detect_ambiguity
from llm.llama_schema import extract_schema

from db.sql_registry import SQL_REGISTRY
from db.executor import run_query
from utils.date_parser import parse_date_range

print("AI Analytics Assistant (type 'exit' to quit)")

state = ConversationState()

ENTITY_LABELS = {
    "product": "products",
    "customer": "customers",
    "city": "cities",
    "category": "categories"
}

while True:
    user_input = input("\nUser: ").strip()

    # ---------------------------
    # Exit handling
    # ---------------------------
    if user_input.lower() in ["exit", "quit", "q"]:
        print("Assistant: Goodbye 👋")
        break

    # ---------------------------
    # Update conversation state
    # ---------------------------
    state.update_from_user(user_input)

    # ---------------------------
    # Early guard: metric mentioned but no entity
    # ---------------------------
    if state.entity is None and any(w in user_input.lower() for w in ["revenue", "sales", "quantity"]):
        print("Assistant: Are you asking about products, customers, cities, or categories?")
        continue

    # ---------------------------
    # Only call Qwen if state is NOT complete
    # ---------------------------
    if not state.is_complete():
        known_state = {
            "entity": state.entity,
            "metric": state.metric,
            "time_range": state.time_range,
            "ranking": state.ranking
        }
        reply = detect_ambiguity(user_input, known_state)
        if reply.strip() != "CLEAR":
            print("Assistant:", reply)
            continue

    # ---------------------------
    # Final completeness check (fallback)
    # ---------------------------
    if not state.is_complete():
        if state.entity is None:
            print("Assistant: Are you asking about products, customers, cities, or categories?")
        elif state.metric is None:
            print("Assistant: Should this be measured by revenue or quantity?")
        elif state.time_range is None:
            print("Assistant: What time period? (e.g. March 2024 or Jan to Jun 2024)")
        continue

    # ---------------------------
    # OPTIONAL schema mapping (LLaMA)
    # ---------------------------
    try:
        extract_schema(
            entity=state.entity,
            metric=state.metric,
            time_range=state.time_range,
            ranking=state.ranking
        )
    except Exception:
        pass

    # ---------------------------
    # Parse date range
    # ---------------------------
    try:
        start_date, end_date = parse_date_range(
            state.time_range,
            state.raw_time_text
        )
    except ValueError:
        print("Assistant: I couldn't understand the date range. Can you rephrase it?")
        continue

    # ---------------------------
    # Ensure ranking defaults
    # ---------------------------
    if state.ranking is None:
        state.ranking = "top"

    if state.ranking == "top" and state.top_n <= 0:
        state.top_n = 5

    # ---------------------------
    # Fetch SQL from registry
    # ---------------------------
    try:
        sql = SQL_REGISTRY[state.entity][state.metric][state.ranking]
    except KeyError:
        print("Assistant: I can't answer this type of question yet.")
        state = ConversationState()
        continue

    # ---------------------------
    # Execute SQL
    # ---------------------------
    if state.ranking == "aggregate":
        rows = run_query(sql, (start_date, end_date))
    else:
        rows = run_query(sql, (start_date, end_date, state.top_n))

    # ---------------------------
    # Output
    # ---------------------------
    if not rows or rows[0][0] is None:
        print("Assistant: No data available for the selected period.")
    else:
        prefix = "₹" if state.metric == "revenue" else ""
        entity_label = ENTITY_LABELS[state.entity]

        if state.ranking == "aggregate":
            value = rows[0][0]
            print(
                f"Assistant: Total {state.metric} between "
                f"{start_date:%d %B %Y} and {end_date:%d %B %Y} "
                f"is {prefix}{value:,.2f}"
            )
        else:
            print(
                f"Assistant: Top {state.top_n} {entity_label} by {state.metric} "
                f"between {start_date:%d %B %Y} and {end_date:%d %B %Y}:"
            )
            for i, (name, value) in enumerate(rows, start=1):
                print(f"{i}. {name} — {prefix}{value:,.2f}")

    # ---------------------------
    # Reset state
    # ---------------------------
    state = ConversationState()