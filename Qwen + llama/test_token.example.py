import requests
import json

url = "https://router.huggingface.co/v1/chat/completions"

headers = {
    "Authorization": "Bearer your_huggingface_api_token_here",
    "Content-Type": "application/json"
}

payload = {
    "model": "Qwen/Qwen2.5-7B-Instruct",
    "messages": [
        {"role": "user", "content": "Reply with exactly the word OK"}
    ],
    "max_tokens": 10
}

response = requests.post(url, headers=headers, json=payload)

print("STATUS CODE:", response.status_code)
print("RAW RESPONSE:")
print(response.text)
