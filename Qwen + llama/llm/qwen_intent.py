from llm.hf_client import call_hf_chat
from config import HF_API_TOKEN, QWEN_MODEL


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
- If both metric and time range are known, reply exactly: CLEAR

Known state:
{known_state}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ]

    result = call_hf_chat(
        model_name=QWEN_MODEL,
        messages=messages,
        token=HF_API_TOKEN,
        max_tokens=100
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
