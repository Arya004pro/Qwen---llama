"""Shared helpers for LLM HTTP calls and response cleanup."""

from __future__ import annotations

import re

import requests

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def clean_model_text(raw: str, *, strip_fences: bool = True) -> str:
    text = (raw or "").strip()
    text = _THINK_RE.sub("", text).strip()
    if strip_fences:
        text = re.sub(r"^```(?:json|sql)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text).strip()
    return text


def post_chat_completion(
    *,
    api_url: str,
    api_token: str,
    payload: dict,
    timeout: int = 30,
    retry_without_reasoning_effort: bool = True,
) -> dict:
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
    if (
        retry_without_reasoning_effort
        and resp.status_code >= 400
        and "reasoning_effort" in payload
    ):
        retry_payload = dict(payload)
        retry_payload.pop("reasoning_effort", None)
        resp = requests.post(api_url, headers=headers, json=retry_payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429
    return "429" in str(exc)

