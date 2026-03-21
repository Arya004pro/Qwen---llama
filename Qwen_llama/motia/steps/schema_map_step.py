"""Step 4: Schema Map — LLaMA 3.1-8B schema mapping + token logging."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests, json
from typing import Any
from motia import FlowContext, queue

from shared_config import GROQ_API_TOKEN, LLAMA_MODEL, GROQ_URL
from utils.token_logger import log_tokens, add_tokens_to_state

config = {
    "name": "SchemaMap",
    "description": "Converts parsed intent into a strict query schema used by SQL execution",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::schema.map")],
    "enqueues": ["query::execute"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    entity    = parsed.get("entity")
    metric    = parsed.get("metric")
    time_range = parsed.get("time_range")
    ranking   = parsed.get("ranking")

    ctx.logger.info("🗺️ Schema mapping with LLaMA 3.1-8B", {
        "queryId": query_id, "model": LLAMA_MODEL,
        "entity": entity, "metric": metric,
    })

    prompt = f"""
You are a schema mapping assistant.

DO NOT infer intent. DO NOT change values. DO NOT invent fields.
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
  "ranking": "{ranking}"
}}
"""

    headers = {
        "Authorization": f"Bearer {GROQ_API_TOKEN}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":    LLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
    }

    schema_result = None
    try:
        response = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()

        usage = result.get("usage", {})
        # ── Token logging ──────────────────────────────────────────────────
        log_tokens(ctx, query_id, "SchemaMap", LLAMA_MODEL, usage)
        await add_tokens_to_state(ctx, query_id, "SchemaMap", LLAMA_MODEL, usage)

        raw_content = result["choices"][0]["message"]["content"].strip()
        try:
            schema_result = json.loads(raw_content)
            ctx.logger.info("✅ Schema mapped", {"queryId": query_id, "schema": schema_result})
        except json.JSONDecodeError:
            ctx.logger.warn("⚠️ LLaMA returned non-JSON — using parsed fallback",
                            {"queryId": query_id, "raw": raw_content[:200]})

    except Exception as e:
        ctx.logger.error("❌ LLaMA call failed — using parsed fallback",
                         {"error": str(e), "queryId": query_id})

    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "schema_mapped",
            "schema": schema_result or parsed,
        })

    await ctx.enqueue({
        "topic": "query::execute",
        "data":  {
            "queryId": query_id,
            "query":   user_query,
            "parsed":  parsed,
            "schema":  schema_result,
        },
    })