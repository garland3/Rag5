# Rag5

RAG (Retrieval-Augmented Generation) system using MongoDB Atlas Vector Search, exposed via a FastAPI REST API with a Svelte frontend.

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Copy and fill in environment variables
cp .env.example .env

# Run the API server
uvicorn app.main:app --reload

# Run tests
pytest tests/ -v
```

## Frontend

The frontend is a SvelteKit app in `frontend/`. It connects to the API through a reverse proxy (or Vite dev proxy during development).

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` and `/health` to `http://localhost:8000`, so run the API server first.

To build for production:

```bash
cd frontend
npm run build   # outputs to frontend/build/
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/corpora` | Create a corpus (tenant boundary) |
| GET | `/api/v1/corpora` | List corpora you can read |
| POST | `/api/v1/documents?corpus_id=<id>` | Upload a document into a corpus |
| GET | `/api/v1/documents` | List documents in accessible corpora |
| GET | `/api/v1/documents/{id}` | Get document details |
| DELETE | `/api/v1/documents/{id}` | Delete a document |
| POST | `/api/v1/query` | Ask a question (RAG), `corpus_id` required for tenant isolation |
| POST | `/api/v1/query/stream` | **Streaming** RAG query via SSE with real-time status updates |

### Streaming Search (`/api/v1/query/stream`)

Returns Server-Sent Events (SSE) that report progress as the search pipeline runs:

```
data: {"type": "status", "message": "Checking access permissions..."}
data: {"type": "status", "message": "Searching with vector retriever..."}
data: {"type": "status", "message": "Finding relevant documents for: \"your question\""}
data: {"type": "status", "message": "Found 5 relevant chunks. Resolving sources..."}
data: {"type": "sources", "message": "Retrieved sources from 2 documents", "data": [...]}
data: {"type": "status", "message": "Generating answer from retrieved context..."}
data: {"type": "answer", "data": {"answer": "...", "sources": [...]}}
data: {"type": "done", "message": "Search complete"}
```

Event types: `status`, `sources`, `answer`, `error`, `done`.

## Authentication

The API is designed to sit behind a reverse proxy that handles authentication.

- **Production**: The proxy injects a header (default `x-forwarded-user`) with the authenticated username. Configure the header name via `PROXY_USER_HEADER`.
- **Development**: Set `DEBUG=true` in `.env` to bypass auth checks and use `TESTUSER` (defaults to `bob@test.com`).

Without DEBUG mode, the API also accepts `x-user-id` and `x-user-groups` headers as a fallback.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DB_NAME` | `rag5` | Database name |
| `OPENAI_API_KEY` | | OpenAI API key |
| `ANTHROPIC_API_KEY` | | Anthropic API key |
| `LLM_PROVIDER` | `openai` | `openai` or `anthropic` |
| `DEBUG` | `false` | Bypass auth, use TESTUSER |
| `TESTUSER` | `bob@test.com` | User identity when DEBUG=true |
| `PROXY_USER_HEADER` | `x-forwarded-user` | Header from reverse proxy with username |

## Docker

```bash
docker compose up --build
```

See [PLAN.md](PLAN.md) for full architecture and design details.
