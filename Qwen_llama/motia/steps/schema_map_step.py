"""Step 4: Schema Map — Uses LLaMA 3.1-8B to map parsed fields to structured JSON.

Calls the Groq API with LLaMA 3.1-8B to produce a clean structured
JSON representation of the query parameters. This is an optional
enrichment step — if it fails, the pipeline continues with the
original parsed data.

Trigger: Queue (query::schema.map)
Emits:   query::execute
Flow:    sales-analytics-flow
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import json
from typing import Any
from motia import FlowContext, queue

from shared_config import GROQ_API_TOKEN, LLAMA_MODEL, GROQ_URL

config = {
    "name": "SchemaMap",
    "description": "Converts parsed intent into a strict query schema used by SQL execution",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::schema.map"),
    ],
    "enqueues": ["query::execute"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})

    entity = parsed.get("entity")
    metric = parsed.get("metric")
    time_range = parsed.get("time_range")
    ranking = parsed.get("ranking")

    ctx.logger.info("🗺️ Schema mapping with LLaMA 3.1-8B", {
        "queryId": query_id,
        "model": LLAMA_MODEL,
        "entity": entity,
        "metric": metric,
    })

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
  "ranking": "{ranking}"
}}
"""

    headers = {
        "Authorization": f"Bearer {GROQ_API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": LLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
    }

    schema_result = None
    try:
        response = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()

        usage = result.get("usage", {})
        ctx.logger.info("📊 LLaMA token usage", {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
        })

        raw_content = result["choices"][0]["message"]["content"].strip()

        # Try to parse the JSON from LLaMA's response
        try:
            schema_result = json.loads(raw_content)
            ctx.logger.info("✅ Schema mapped successfully", {
                "queryId": query_id,
                "schema": schema_result,
            })
        except json.JSONDecodeError:
            ctx.logger.warn("⚠️ LLaMA returned non-JSON — using parsed data as fallback", {
                "queryId": query_id,
                "raw": raw_content[:200],
            })
    except Exception as e:
        ctx.logger.error("❌ LLaMA API call failed — using parsed data as fallback", {
            "error": str(e),
            "queryId": query_id,
        })

    # Update state
    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "schema_mapped",
            "schema": schema_result or parsed,
        })

    # Continue pipeline — emit to execute step
    await ctx.enqueue({
        "topic": "query::execute",
        "data": {
            "queryId": query_id,
            "query": user_query,
            "parsed": parsed,
            "schema": schema_result,
        },
    })
