import os
from dotenv import load_dotenv

load_dotenv()

# ── API Token ─────────────────────────────────────────────
GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")

# ── Model names ───────────────────────────────────────────
QWEN_MODEL  = os.getenv("QWEN_MODEL",  "qwen/qwen3-32b")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "llama-3.1-8b-instant")

# ── PostgreSQL ────────────────────────────────────────────
POSTGRES = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
