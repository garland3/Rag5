# CLAUDE.md - Project guide for AI assistants

## Project Overview

Rag5 is a multi-tenant Retrieval-Augmented Generation (RAG) system. Backend is FastAPI + MongoDB Atlas Vector Search. Frontend is SvelteKit in `frontend/`.

## Key Commands

```bash
# Install Python deps
pip install -e ".[dev]"

# Run API server
uvicorn app.main:app --reload

# Run all tests
pytest tests/ -v

# Frontend dev
cd frontend && npm install && npm run dev

# Frontend build
cd frontend && npm run build
```

## Architecture

```
app/
  main.py              # FastAPI app, CORS, router setup
  config.py            # Pydantic Settings (loads .env)
  api/
    deps.py            # Auth dependency (DEBUG bypass + proxy header)
    routes/
      query.py         # POST /api/v1/query (sync response)
      query_stream.py  # POST /api/v1/query/stream (SSE streaming)
      corpora.py       # Corpus CRUD
      documents.py     # Document upload/list/delete
      health.py        # Health check
  core/
    database.py        # MongoDB Motor client
    embeddings.py      # OpenAI embedding
    retriever.py       # 5 retriever implementations (vector, keyword, hybrid, multi_query, agent)
    generator.py       # LLM answer generation (OpenAI/Anthropic)
    chunking.py        # Text splitting
  services/
    rag.py             # Sync RAG pipeline
    rag_stream.py      # Streaming RAG pipeline (yields SSE events)
    ingest.py          # Document parsing
    authz.py           # Authorization (group-based multi-tenancy)
  models/
    query.py           # QueryRequest, QueryResponse, SourceChunk
    corpus.py          # Corpus models
    document.py        # Document models

frontend/              # SvelteKit SPA (adapter-static)
  src/
    lib/api.ts         # API client + SSE streaming helper
    routes/
      +page.svelte     # Main search UI
      +layout.svelte   # App layout
      +layout.ts       # SPA mode (ssr=false)

tests/
  test_api/            # API endpoint tests
  test_core/           # Core module tests
```

## Auth Model

- Behind reverse proxy: user comes from `PROXY_USER_HEADER` (default `x-forwarded-user`)
- `DEBUG=true`: bypasses proxy auth, uses `TESTUSER` from .env
- Group-based access: corpora have read/write/owner groups
- Default groups: `rag5-default-readers`, `rag5-default-writers`, `rag5-default-owners`

## Streaming Endpoint

`POST /api/v1/query/stream` returns SSE events with types: `status`, `sources`, `answer`, `error`, `done`. The frontend consumes these via `ReadableStream` in `lib/api.ts`.

## Testing

All tests use pytest-asyncio with `asyncio_mode = "auto"`. Tests mock the DB (Motor) and external APIs. Run `pytest tests/ -v` to verify.
