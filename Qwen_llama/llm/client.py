"""llm/client.py — LLM call wrapper with token logging."""

import requests
from config import GROQ_API_TOKEN
from utils.token_logger import print_token_usage

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def call_llm(model_name, messages, token, max_tokens=500, step_name="LLM"):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":      model_name,
        "messages":   messages,
        "max_tokens": max_tokens,
    }

    response = requests.post(GROQ_URL, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()

    usage = result.get("usage", {})
    print_token_usage(step_name, model_name, usage)

    return result


# ── Test block ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = call_llm(
        model_name="qwen/qwen3-32b",
        messages=[{"role": "user", "content": "Reply with exactly OK"}],
        token=GROQ_API_TOKEN,
        max_tokens=5,
        step_name="TestCall",
    )
    print(result["choices"][0]["message"]["content"])