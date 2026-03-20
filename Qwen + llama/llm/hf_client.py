import requests
from config import HF_API_TOKEN

HF_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"

def call_hf_chat(model_name, messages, token, max_tokens=256):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": model_name,
        "messages": messages,
        "max_tokens": max_tokens
    }

    response = requests.post(HF_ROUTER_URL, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()

    usage = result.get("usage", {})
    print(f"[Tokens] prompt={usage.get('prompt_tokens')} | completion={usage.get('completion_tokens')} | total={usage.get('total_tokens')} | model={model_name.split('/')[-1]}")

    return result


# ✅ TEMPORARY TEST BLOCK (MANDATORY)
if __name__ == "__main__":

    result = call_hf_chat(
        model_name="Qwen/Qwen2.5-7B-Instruct",
        messages=[
            {"role": "user", "content": "Reply with exactly OK"}
        ],
        token=HF_API_TOKEN,
        max_tokens=5
    )

    print(result["choices"][0]["message"]["content"])