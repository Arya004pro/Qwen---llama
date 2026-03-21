"""Shared configuration and utilities for all Motia steps.

This module loads environment variables and provides common imports
that all step files use. It bridges the existing project's modules
into the Motia workflow.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Compute paths ──
STEPS_DIR = str(Path(__file__).resolve().parent)
MOTIA_DIR = str(Path(__file__).resolve().parent.parent)
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)

# ── Add the steps dir and project root to sys.path ──
for p in [STEPS_DIR, MOTIA_DIR, PROJECT_ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)

# ── Load .env from the project root ──
env_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(env_path)

# ── API Configuration ──
GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "llama-3.1-8b-instant")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# ── PostgreSQL Configuration ──
POSTGRES = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
