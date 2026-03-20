import requests

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
    return response.json()


# ✅ TEMPORARY TEST BLOCK (MANDATORY)
if __name__ == "__main__":
    from config import HF_API_TOKEN

    result = call_hf_chat(
        model_name="Qwen/Qwen2.5-7B-Instruct",
        messages=[
            {"role": "user", "content": "Reply with exactly OK"}
        ],
        token=HF_API_TOKEN,
        max_tokens=5
    )

    print(result["choices"][0]["message"]["content"])
