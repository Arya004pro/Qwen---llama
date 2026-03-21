"""Step 3: Ambiguity Check — Uses Qwen 3-32B to detect missing information.

Calls the Groq API with Qwen 3-32B to check whether the user's query
has all the required fields (entity, metric, time_range). If the model
returns "CLEAR", the pipeline continues. Otherwise, the clarification
question is stored in state for the user.

Trigger: Queue (query::ambiguity.check)
Emits:   query::schema.map (if CLEAR)
Flow:    sales-analytics-flow
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from typing import Any
from motia import FlowContext, queue

from shared_config import GROQ_API_TOKEN, QWEN_MODEL, GROQ_URL

config = {
    "name": "AmbiguityCheck",
    "description": "Uses Qwen 3-32B via Groq to check if the query needs clarification",
    "flows": ["sales-analytics-flow"],
    "triggers": [
        queue("query::ambiguity.check"),
    ],
    "enqueues": ["query::schema.map"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})

    ctx.logger.info("🧠 Running ambiguity check with Qwen 3-32B", {
        "queryId": query_id,
        "model": QWEN_MODEL,
    })

    known_state = {
        "entity": parsed.get("entity"),
        "metric": parsed.get("metric"),
        "time_range": parsed.get("time_range"),
        "ranking": parsed.get("ranking"),
    }

    # Check if we even need to call the LLM
    is_complete = all([parsed.get("entity"), parsed.get("metric"), parsed.get("time_range")])

    if is_complete:
        # Skip LLM call — we have everything we need
        ctx.logger.info("✅ Query is already complete — skipping Qwen call", {"queryId": query_id})
        ambiguity_result = "CLEAR"
    else:
        # Call Qwen for ambiguity detection
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

        headers = {
            "Authorization": f"Bearer {GROQ_API_TOKEN}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": QWEN_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query},
            ],
            "max_tokens": 500,
        }

        try:
            response = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()

            usage = result.get("usage", {})
            ctx.logger.info("📊 Qwen token usage", {
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
            })

            ambiguity_result = result["choices"][0]["message"]["content"].strip()
        except Exception as e:
            ctx.logger.error("❌ Qwen API call failed", {"error": str(e), "queryId": query_id})
            ambiguity_result = "CLEAR"  # Fallback: proceed anyway

    ctx.logger.info("🔎 Ambiguity result", {
        "queryId": query_id,
        "result": ambiguity_result[:100],
        "isClear": ambiguity_result == "CLEAR",
    })

    # Update state
    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status": "ambiguity_checked",
            "ambiguityResult": ambiguity_result,
        })

    if ambiguity_result == "CLEAR":
        # Continue the pipeline
        await ctx.enqueue({
            "topic": "query::schema.map",
            "data": {
                "queryId": query_id,
                "query": user_query,
                "parsed": parsed,
            },
        })
    else:
        # Store the clarification question — user needs to re-query
        ctx.logger.warn("⚠️ Query needs clarification", {
            "queryId": query_id,
            "clarification": ambiguity_result,
        })
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status": "needs_clarification",
                "clarification": ambiguity_result,
            })
