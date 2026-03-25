import os
from dotenv import load_dotenv

load_dotenv()

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "llama-3.1-8b-instant")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "motia/data/analytics.duckdb")
