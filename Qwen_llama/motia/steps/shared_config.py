"""Shared configuration for all Motia steps."""

import os
import sys
from dotenv import load_dotenv

STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
MOTIA_DIR = os.path.dirname(STEPS_DIR)
PROJECT_ROOT = os.path.dirname(MOTIA_DIR)

for _p in [STEPS_DIR, MOTIA_DIR, PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

for _env in [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(MOTIA_DIR, ".env"),
    "/app/.env",
]:
    if os.path.exists(_env):
        load_dotenv(_env)
        break

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "llama-3.1-8b-instant")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "motia/data/analytics.duckdb")
