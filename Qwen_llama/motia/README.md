# Motia Sales Analytics — Workflow UI

## 🎯 What This Does

Converts the Qwen + LLaMA sales analytics pipeline into a **visual Motia workflow** with a beautiful
UI running on `http://localhost:3113/` showing all steps connected as an interactive graph.

## 📊 Workflow Pipeline

```
POST /query                  GET /query/:id           GET /queries
     │                            │                        │
     ▼                            ▼                        ▼
┌──────────────┐          ┌──────────────┐          ┌──────────────┐
│ ReceiveQuery │          │GetQueryResult│          │ ListQueries  │
│  (HTTP POST) │          │  (HTTP GET)  │          │  (HTTP GET)  │
└──────┬───────┘          └──────────────┘          └──────────────┘
       │ query::intent.parse
       ▼
┌──────────────┐
│ ParseIntent  │  ← Uses ConversationState from state/
│  (Queue)     │
└──────┬───────┘
       │ query::ambiguity.check
       ▼
┌──────────────┐
│AmbiguityCheck│  ← Calls Qwen 3-32B via Groq API
│  (Queue)     │
└──────┬───────┘
       │ query::schema.map (if CLEAR)
       ▼
┌──────────────┐
│  SchemaMap   │  ← Calls LLaMA 3.1-8B via Groq API
│  (Queue)     │
└──────┬───────┘
       │ query::execute
       ▼
┌──────────────┐
│ ExecuteQuery │  ← Runs SQL against PostgreSQL
│  (Queue)     │
└──────┬───────┘
       │ query::format.result
       ▼
┌──────────────┐
│ FormatResult │  ← Formats response with ₹ and labels
│  (Queue)     │
└──────────────┘
```

## 🚀 Quick Start (WSL)

### 1. Open WSL and navigate to this folder
```bash
cd /mnt/c/Users/DELL/Downloads/Qwen_llama/Qwen_llama/motia
```

### 2. Run the setup script
```bash
bash setup.sh
```

### 3. Start the workflow engine
```bash
iii -c iii-config.yaml
```

### 4. Start the workflow UI (new WSL terminal)
```bash
iii-console --enable-flow
```

### 5. Open the UI
Open your browser to: **http://localhost:3113/**

You'll see all 8 steps connected as a beautiful visual workflow!

## 🧪 Test the Pipeline

```bash
# Submit a query
curl -X POST http://localhost:3111/query \
  -H "Content-Type: application/json" \
  -d '{"query": "top 5 products by revenue in March 2024"}'

# Check result (replace QUERY_ID)
curl http://localhost:3111/query/QUERY_ID

# List all queries
curl http://localhost:3111/queries
```

## 📁 Files

| File | Purpose |
|---|---|
| `iii-config.yaml` | iii engine configuration (ports, modules, storage) |
| `pyproject.toml` | Python dependencies (motia SDK, iii SDK, project deps) |
| `setup.sh` | One-command WSL setup script |
| `steps/shared_config.py` | Shared config — loads .env, adds project to sys.path |
| `steps/receive_query_step.py` | Step 1: HTTP POST /query — entry point |
| `steps/parse_intent_step.py` | Step 2: Intent parsing via ConversationState |
| `steps/ambiguity_check_step.py` | Step 3: Qwen 3-32B ambiguity detection |
| `steps/schema_map_step.py` | Step 4: LLaMA 3.1-8B schema mapping |
| `steps/execute_query_step.py` | Step 5: PostgreSQL SQL execution |
| `steps/format_result_step.py` | Step 6: Result formatting |
| `steps/get_result_step.py` | Utility: GET /query/:queryId |
| `steps/list_queries_step.py` | Utility: GET /queries |
