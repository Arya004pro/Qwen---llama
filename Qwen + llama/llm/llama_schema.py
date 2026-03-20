from llm.hf_client import call_hf_chat
from config import HF_API_TOKEN, LLAMA_MODEL
import json

def extract_schema(entity, metric, time_range, ranking=None):
    prompt = f"""
You are a schema mapping assistant.

DO NOT infer intent.
DO NOT change values.
DO NOT invent fields.

Use ONLY the provided values.

Entity: {entity}
Metric: {metric}
Time range: {time_range}
Ranking: {ranking}

Return ONLY valid JSON in this format:
{{
  "entity": "{entity}",
  "metric": "{metric}",
  "time_range": "{time_range}",
  "ranking": {ranking}
}}
"""

    messages = [{"role": "user", "content": prompt}]

    result = call_hf_chat(
        model_name=LLAMA_MODEL,
        messages=messages,
        token=HF_API_TOKEN,
        max_tokens=120
    )

    return result["choices"][0]["message"]["content"]
