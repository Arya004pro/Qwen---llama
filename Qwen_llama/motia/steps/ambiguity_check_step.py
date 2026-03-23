"""Step 3: Ambiguity Check — Qwen 3-32B ambiguity detection + token logging.

Unchanged except: enqueues query::text.to.sql instead of query::schema.map.
"""

import os, sys, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from typing import Any
from motia import FlowContext, queue

from shared_config import GROQ_API_TOKEN, QWEN_MODEL, GROQ_URL
from utils.token_logger import log_tokens, add_tokens_to_state

config = {
    "name": "AmbiguityCheck",
    "description": (
        "Validates whether required details are present; requests clarification "
        "when the question is incomplete. Routes to TextToSQL on success."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::ambiguity.check")],
    "enqueues": ["query::text.to.sql"],   # ← updated (was query::schema.map)
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id   = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed     = input_data.get("parsed", {})

    ctx.logger.info("🧠 Running ambiguity check", {"queryId": query_id})

    known_state = {
        "entity":     parsed.get("entity"),
        "metric":     parsed.get("metric"),
        "time_range": parsed.get("time_range"),
        "ranking":    parsed.get("ranking"),
    }

    # Fast-path: all required fields present
    is_complete = all([parsed.get("entity"), parsed.get("metric"), parsed.get("time_range")])

    if is_complete:
        ctx.logger.info("✅ Query complete — skipping Qwen call", {"queryId": query_id})
        ambiguity_result = "CLEAR"

    else:
        entity_val = parsed.get("entity")
        if entity_val:
            entity_line = f"The entity is already known: {entity_val.upper()}."
            entity_rule = "- NEVER ask about entity or category."
        else:
            entity_line = "No entity detected yet. Valid: products, customers, cities, categories."
            entity_rule = "- If entity is missing, ask: 'Are you asking about products, customers, cities, or categories?'"

        system_prompt = f"""\
You are an AI analytics assistant.

{entity_line}

Your job is ONLY to detect missing required fields:
- entity (if unknown)
- metric (revenue or quantity)
- time range

Rules:
{entity_rule}
- Ask ONLY ONE clarification question at a time.
- If time range is missing ask: 'What time period? (e.g. March 2024 or Jan to Jun 2024)'
- If ALL required fields are known, reply exactly with the single word: CLEAR
- Do NOT output <think> tags or any chain-of-thought. Reply only with the clarification question or CLEAR.

Known state: {known_state}
"""

        headers = {
            "Authorization": f"Bearer {GROQ_API_TOKEN}",
            "Content-Type":  "application/json",
        }
        payload = {
            "model":    QWEN_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_query},
            ],
            "max_tokens": 500,
        }

        try:
            response = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()

            usage = result.get("usage", {})
            log_tokens(ctx, query_id, "AmbiguityCheck", QWEN_MODEL, usage)
            await add_tokens_to_state(ctx, query_id, "AmbiguityCheck", QWEN_MODEL, usage)

            raw = result["choices"][0]["message"]["content"].strip()
            raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
            ambiguity_result = raw

        except Exception as e:
            ctx.logger.error("❌ Qwen call failed — proceeding anyway",
                             {"error": str(e), "queryId": query_id})
            ambiguity_result = "CLEAR"

    ctx.logger.info("🔎 Ambiguity result", {
        "queryId": query_id,
        "result":  ambiguity_result[:100],
        "isClear": ambiguity_result == "CLEAR",
    })

    query_state = await ctx.state.get("queries", query_id)
    if query_state:
        await ctx.state.set("queries", query_id, {
            **query_state,
            "status":          "ambiguity_checked",
            "ambiguityResult": ambiguity_result,
        })

    if ambiguity_result == "CLEAR":
        # ← now routes to TextToSQL instead of SchemaMap
        await ctx.enqueue({
            "topic": "query::text.to.sql",
            "data":  {"queryId": query_id, "query": user_query, "parsed": parsed},
        })
    else:
        ctx.logger.warn("⚠️ Query needs clarification",
                        {"queryId": query_id, "clarification": ambiguity_result})
        if query_state:
            await ctx.state.set("queries", query_id, {
                **query_state,
                "status":        "needs_clarification",
                "clarification": ambiguity_result,
            })