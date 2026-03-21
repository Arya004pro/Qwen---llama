"""Shared configuration for all Motia steps."""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Compute paths ──────────────────────────────────────────────────────────
STEPS_DIR    = os.path.dirname(os.path.abspath(__file__))
MOTIA_DIR    = os.path.dirname(STEPS_DIR)
PROJECT_ROOT = os.path.dirname(MOTIA_DIR)

# ── Add all relevant directories to sys.path ───────────────────────────────
for _p in [STEPS_DIR, MOTIA_DIR, PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Load .env — try multiple locations so it works locally and in Docker ───
for _env in [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(MOTIA_DIR, ".env"),
    "/app/.env",
]:
    if os.path.exists(_env):
        load_dotenv(_env)
        break

# ── API Configuration ──────────────────────────────────────────────────────
GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL     = os.getenv("QWEN_MODEL",  "qwen/qwen3-32b")
LLAMA_MODEL    = os.getenv("LLAMA_MODEL", "llama-3.1-8b-instant")
GROQ_URL       = "https://api.groq.com/openai/v1/chat/completions"

# ── PostgreSQL Configuration ───────────────────────────────────────────────
POSTGRES = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}