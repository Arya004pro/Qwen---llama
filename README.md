# Qwen + LLaMA Sales Analytics (Live Workflow Demo)

This project lets you:
1. Chat in terminal (`npm run start`).
2. Watch the query run through a live workflow UI (`npm run dev`).

Everything runs through Docker. No Python venv activation is required.

## Requirements (One-Time)
1. Install Docker Desktop and keep it running.
2. Install Node.js (includes npm).
3. Have PostgreSQL running with your sales data.

## Project Location
Run all commands from:

`C:\Users\DELL\Downloads\Qwen_llama`

## Setup
1. Open `Qwen_llama/.env`
2. Set your values:

```env
GROQ_API_TOKEN=your_token_here
QWEN_MODEL=qwen/qwen3-32b
LLAMA_MODEL=llama-3.1-8b-instant

POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=your_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

Important:
If PostgreSQL is on your Windows machine, `POSTGRES_HOST` should be `host.docker.internal` (not `localhost`).

## Run (Live Demo Mode)
Open terminal 1:

```powershell
cd C:\Users\DELL\Downloads\Qwen_llama
npm run dev
```

Open terminal 2:

```powershell
cd C:\Users\DELL\Downloads\Qwen_llama
npm run start
```

Then type in terminal 2:
`Top 4 products by revenue in March 2024`

## UI URLs
1. Workflow graph: `http://localhost:3113/flow`
2. Traces (best for live run view): `http://localhost:3113/traces`
3. Logs (best for step-by-step live logs): `http://localhost:3113/logs`

Note:
Flow view is mainly topology (boxes/arrows). Live activity is most visible in `Traces` and `Logs`.

## Useful Commands
From project root:

1. Start workflow stack: `npm run dev`
2. Stop workflow stack: `npm run dev:down`
3. Tail workflow logs: `npm run dev:logs`
4. Start terminal chat client: `npm run start`

## pgAdmin4 Clarification
pgAdmin4 does not need to stay open.
Only PostgreSQL server must be running.

## Troubleshooting
1. `connection refused` to Postgres:
   check `.env` values, especially `POSTGRES_HOST`.
2. No traces in UI:
   submit at least one query from `npm run start`, then refresh `/traces`.
3. UI works on 3113 but Docker row shows 3111:
   normal Docker Desktop display; use `http://localhost:3113` for console UI.
