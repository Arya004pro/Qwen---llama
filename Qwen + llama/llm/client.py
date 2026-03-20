import requests
from config import GROQ_API_TOKEN

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

def call_llm(model_name, messages, token, max_tokens=500):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": model_name,
        "messages": messages,
        "max_tokens": max_tokens
    }

    response = requests.post(GROQ_URL, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()

    usage = result.get("usage", {})
    print(
        f"[Tokens] prompt={usage.get('prompt_tokens')} | "
        f"completion={usage.get('completion_tokens')} | "
        f"total={usage.get('total_tokens')} | "
        f"model={model_name.split('/')[-1]}"
    )

    return result


# ✅ TEMPORARY TEST BLOCK (MANDATORY)
if __name__ == "__main__":

    result = call_llm(
        model_name="qwen/qwen3-32b",
        messages=[
            {"role": "user", "content": "Reply with exactly OK"}
        ],
        token=GROQ_API_TOKEN,
        max_tokens=5
    )

    print(result["choices"][0]["message"]["content"])