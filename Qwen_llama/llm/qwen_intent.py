from llm.client import call_llm
from config import GROQ_API_TOKEN, QWEN_MODEL


def detect_ambiguity(user_message, known_state):
    system_prompt = f"""
You are an AI analytics assistant.

The entity is already known: PRODUCTS.

Your job is ONLY to detect missing:
- time range
- metric (revenue or quantity)

Rules:
- NEVER ask about entity or category
- Ask ONLY ONE clarification question
- If time range is missing, ask: 'What time period are you asking about? (e.g. March 2024 or Jan to Jun 2024)'
- If both metric and time range are known, reply exactly: CLEAR

Known state:
{known_state}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ]

    result = call_llm(
        model_name=QWEN_MODEL,
        messages=messages,
        token=GROQ_API_TOKEN,
        max_tokens=500
    )

    return result["choices"][0]["message"]["content"].strip()


# 🔹 TEST BLOCK (MUST be at bottom)
if __name__ == "__main__":
    known_state = {
        "entity": None,
        "metric": None,
        "time_range": None
    }

    out = detect_ambiguity(
        "Which products are selling well?",
        known_state
    )

    print(out)
