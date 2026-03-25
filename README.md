# Qwen + LLaMA Sales Analytics (DuckDB)

This project lets you:
1. Run the Motia workflow stack (`npm run dev`)
2. Use the dashboard UI (`npm run dashboard`)
3. Use terminal chat client (`npm run start`)

## What changed
- Database is now **DuckDB** (embedded file DB), not PostgreSQL.
- No DB server setup and no account/signup is required.

## Requirements
1. Docker Desktop running
2. Node.js + npm
3. Python 3.11+ (for local dashboard/chatbot deps if needed)

## Setup
From project root:

```powershell
cd C:\Users\DELL\Downloads\Qwen_llama
pip install -r requirements.txt
pip install -r Qwen_llama\requirements.txt
```

Create/edit `Qwen_llama/.env`:

```env
GROQ_API_TOKEN=your_token_here
QWEN_MODEL=qwen/qwen3-32b
LLAMA_MODEL=llama-3.1-8b-instant
DUCKDB_PATH=motia/data/analytics.duckdb
```

## Run
Terminal 1 (Motia stack):

```powershell
npm run dev
```

Terminal 2 (dashboard):

```powershell
npm run dashboard
```

Optional terminal 3 (chat CLI):

```powershell
npm run start
```

## Useful Commands
1. Start stack: `npm run dev`
2. Stop stack: `npm run dev:down`
3. Logs: `npm run dev:logs`
4. Dashboard: `npm run dashboard`
5. Terminal chat: `npm run start`

## Update Workflow
- Changed only `streamlit_app.py`: restart/reload `npm run dashboard`.
- Changed Motia step files (`Qwen_llama/motia/steps/*.py`): usually hot-reloaded by engine watcher; if not, run `docker compose restart` or `npm run dev:down` then `npm run dev`.
- Changed dependency files (`requirements`, `pyproject.toml`, Dockerfile): rerun `npm run dev` (rebuild) and reinstall local Python deps if needed.
