"""llm/qwen_intent.py

Detects ambiguity in a user analytics query and asks ONE clarifying question.

Changes from original:
  - Removed hardcoded "The entity is already known: PRODUCTS"
  - Removed hardcoded "metric (revenue or quantity)" options
  - Now accepts optional available_entities / available_metrics so the
    prompt can name real columns from the live schema instead of guessing
  - Falls back gracefully when no schema context is provided
"""

from llm.client import call_llm
from config import GROQ_API_TOKEN, QWEN_MODEL


def detect_ambiguity(
    user_message: str,
    known_state: dict,
    available_entities: list[str] | None = None,
    available_metrics: list[str] | None = None,
) -> str:
    """
    Detect what is still missing from the parsed query and return ONE
    clarifying question, or the exact string "CLEAR" when complete.

    Parameters
    ----------
    user_message        : The user's raw query text.
    known_state         : Dict with keys entity, metric, time_range, ranking.
                          Values are None when not yet determined.
    available_entities  : List of entity column names from the live schema
                          (e.g. ["driver_name", "city", "vehicle_type"]).
                          When supplied, used in the clarification question.
    available_metrics   : List of metric column names from the live schema
                          (e.g. ["total_fare", "driver_earnings", "quantity"]).
    """
    entity    = known_state.get("entity")
    metric    = known_state.get("metric")
    time_range = known_state.get("time_range")

    # Build human-readable entity/metric examples from live schema (if provided)
    if available_entities:
        # Show at most 5 examples, stripped of underscores for readability
        ent_examples = ", ".join(
            e.replace("_name", "").replace("_", " ")
            for e in available_entities[:5]
        )
        entity_hint = f"available dimensions: {ent_examples}"
    else:
        entity_hint = "e.g. product, customer, city, category, driver"

    if available_metrics:
        met_examples = ", ".join(m.replace("_", " ") for m in available_metrics[:5])
        metric_hint = f"available metrics: {met_examples}"
    else:
        metric_hint = "e.g. revenue, quantity, fare, earnings, count"

    # Describe what is already known so the LLM doesn't re-ask for it
    known_parts = []
    if entity:
        known_parts.append(f"entity: {entity}")
    if metric:
        known_parts.append(f"metric: {metric}")
    if time_range:
        known_parts.append(f"time range: {time_range}")

    known_desc = (
        "Known so far: " + ", ".join(known_parts)
        if known_parts
        else "Nothing is determined yet."
    )

    system_prompt = f"""You are an AI analytics assistant helping a user query their business data.

{known_desc}

Your job is ONLY to detect which single most important piece of information is still missing:
  - entity/dimension (what to group by)
  - metric (what to measure)
  - time range (what period)

Rules:
- NEVER ask about something that is already known (see "Known so far" above).
- Ask ONLY ONE clarification question — the single most important missing item.
- If entity is missing, ask which dimension to group by. Hint: {entity_hint}
- If metric is missing, ask what to measure. Hint: {metric_hint}
- If time range is missing, ask: 'What time period? (e.g. March 2024, Q1 2024, or 2023 vs 2024)'
- If ALL of entity, metric, and time range are known, reply EXACTLY with the single word: CLEAR
- Do NOT add any explanation when replying CLEAR.
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_message},
    ]

    result = call_llm(
        model_name=QWEN_MODEL,
        messages=messages,
        token=GROQ_API_TOKEN,
        max_tokens=500,
    )
    return result["choices"][0]["message"]["content"].strip()


# ── Test block ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Example: nothing known yet, schema has driver_name + total_fare
    out = detect_ambiguity(
        "Which ones are selling the most?",
        known_state={"entity": None, "metric": None, "time_range": None},
        available_entities=["driver_name", "city", "vehicle_type"],
        available_metrics=["total_fare", "driver_earnings", "quantity"],
    )
    print(out)